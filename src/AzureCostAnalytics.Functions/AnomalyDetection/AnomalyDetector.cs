using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Azure.Identity;
using Azure.Storage.Blobs;
using System.Text.Json;
using System.IO;
using System.Net.Http;
using System.Text;
using AzureCostAnalytics.Core.Models;
using AzureCostAnalytics.Core.Services;

namespace AzureCostAnalytics.Functions.AnomalyDetection
{
    public class AnomalyDetector
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly ICostDataService _costDataService;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly HttpClient _httpClient;

        // Thresholds for anomaly detection
        private readonly double _zScoreThreshold;
        private readonly double _minCostThreshold;
        private readonly double _confidenceThreshold;

        public AnomalyDetector(IConfiguration configuration, ILogger logger, ICostDataService costDataService)
        {
            _configuration = configuration;
            _logger = logger;
            _costDataService = costDataService;
            _httpClient = new HttpClient();

            // Initialize blob service client if connection string is available
            var storageConnectionString = _configuration["StorageConnectionString"];
            if (!string.IsNullOrEmpty(storageConnectionString))
            {
                _blobServiceClient = new BlobServiceClient(storageConnectionString);
            }

            // Load thresholds from configuration
            _zScoreThreshold = double.Parse(_configuration["ZScoreThreshold"] ?? "2.0");
            _minCostThreshold = double.Parse(_configuration["MinCostThreshold"] ?? "10.0");
            _confidenceThreshold = double.Parse(_configuration["ConfidenceThreshold"] ?? "0.8");
        }

        public async Task<IEnumerable<CostDataPoint>> GetCostDataAsync(string subscriptionId, int daysBack)
        {
            try
            {
                _logger.LogInformation("Retrieving cost data for subscription {SubscriptionId}", subscriptionId);

                // Use the cost data service to get real data from Synapse
                var costData = await _costDataService.GetCostDataAsync(subscriptionId, daysBack);

                _logger.LogInformation("Retrieved {Count} cost data points", costData.Count());
                return costData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving cost data for subscription {SubscriptionId}", subscriptionId);
                throw;
            }
        }

        public List<AnomalyResult> DetectAnomalies(IEnumerable<CostDataPoint> costData)
        {
            var anomalies = new List<AnomalyResult>();

            try
            {
                // Group by resource and date for analysis
                var dailyCosts = costData
                    .GroupBy(x => new { x.ResourceId, x.Date })
                    .Select(g => new { g.Key.ResourceId, g.Key.Date, ActualCost = g.Sum(x => (double)x.ActualCost) })
                    .ToList();

                foreach (var resourceGroup in dailyCosts.GroupBy(x => x.ResourceId))
                {
                    var resourceData = resourceGroup.OrderBy(x => x.Date).ToList();
                    var resourceId = resourceGroup.Key;
                    var subscriptionId = costData.First(x => x.ResourceId == resourceId).SubscriptionId;

                    if (resourceData.Count < 7) // Need at least a week of data
                        continue;

                    var costs = resourceData.Select(x => x.ActualCost).ToArray();
                    var dates = resourceData.Select(x => x.Date).ToArray();

                    // Calculate statistical measures (exclude last day for prediction)
                    var historicalCosts = costs.Take(costs.Length - 1).ToArray();
                    var meanCost = historicalCosts.Average();
                    var stdCost = CalculateStandardDeviation(historicalCosts);

                    if (stdCost == 0)
                        continue;

                    // Detect anomalies for each day
                    for (int i = 0; i < costs.Length; i++)
                    {
                        var cost = costs[i];
                        var date = dates[i];
                        var zScore = (cost - meanCost) / stdCost;

                        // Determine if this is an anomaly
                        var isAnomaly = Math.Abs(zScore) >= _zScoreThreshold && cost >= _minCostThreshold;

                        if (isAnomaly)
                        {
                            // Calculate confidence based on z-score magnitude
                            var confidence = Math.Min(1.0, Math.Abs(zScore) / 3.0);

                            // Determine anomaly type and severity
                            var anomalyType = cost > meanCost ? "Spike" : "Drop";
                            var severity = DetermineSeverity(zScore, cost, meanCost);

                            var anomaly = new AnomalyResult
                            {
                                ResourceId = resourceId,
                                SubscriptionId = subscriptionId,
                                Date = date,
                                ActualCost = (decimal)cost,
                                ExpectedCost = (decimal)meanCost,
                                Variance = (decimal)(cost - meanCost),
                                VariancePercentage = meanCost > 0 ? (decimal)((cost - meanCost) / meanCost * 100) : 0,
                                ZScore = zScore,
                                AnomalyType = anomalyType,
                                Severity = severity,
                                IsAnomaly = true,
                                Confidence = confidence
                            };

                            anomalies.Add(anomaly);

                            _logger.LogInformation("Anomaly detected: {ResourceId} on {Date}, z-score: {ZScore:F2}, severity: {Severity}", 
                                resourceId, date, zScore, severity);
                        }
                    }

                    // Update mean and std for next iteration (sliding window)
                    meanCost = costs.TakeLast(7).Average();
                    stdCost = CalculateStandardDeviation(costs.TakeLast(7).ToArray());
                }

                _logger.LogInformation("Detected {Count} anomalies across {ResourceCount} resources", 
                    anomalies.Count, dailyCosts.Select(x => x.ResourceId).Distinct().Count());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in anomaly detection");
                throw;
            }

            return anomalies;
        }

        public async Task<string> SaveAnomalyResultsAsync(List<AnomalyResult> anomalies)
        {
            try
            {
                if (_blobServiceClient == null)
                {
                    _logger.LogWarning("Blob service client not configured, skipping save");
                    return null;
                }

                // Convert anomalies to JSON
                var anomalyData = anomalies.Select(anomaly => new
                {
                    resource_id = anomaly.ResourceId,
                    subscription_id = anomaly.SubscriptionId,
                    date = anomaly.Date,
                    actual_cost = anomaly.ActualCost,
                    expected_cost = anomaly.ExpectedCost,
                    variance = anomaly.Variance,
                    variance_percentage = anomaly.VariancePercentage,
                    z_score = anomaly.ZScore,
                    anomaly_type = anomaly.AnomalyType,
                    severity = anomaly.Severity,
                    is_anomaly = anomaly.IsAnomaly,
                    confidence = anomaly.Confidence,
                    detected_at = DateTime.UtcNow.ToString("O")
                }).ToList();

                var json = JsonSerializer.Serialize(anomalyData, new JsonSerializerOptions { WriteIndented = true });

                // Save to blob storage
                var containerName = _configuration["ContainerName"] ?? "anomalies";
                var blobName = $"anomalies/anomaly-results-{DateTime.Now:yyyyMMdd-HHmmss}.json";
                
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                await blobClient.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(json)), overwrite: true);

                _logger.LogInformation("Saved {Count} anomalies to {BlobName}", anomalies.Count, blobName);
                return blobName;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving anomaly results");
                throw;
            }
        }

        public async Task<Dictionary<string, int>> TriggerAlertsAsync(List<AnomalyResult> anomalies)
        {
            var alertCounts = new Dictionary<string, int>
            {
                ["High"] = 0,
                ["Medium"] = 0,
                ["Low"] = 0
            };

            try
            {
                // Group anomalies by severity
                var highSeverity = anomalies.Where(a => a.Severity == "High").ToList();
                var mediumSeverity = anomalies.Where(a => a.Severity == "Medium").ToList();
                var lowSeverity = anomalies.Where(a => a.Severity == "Low").ToList();

                // Send alerts for high-severity anomalies
                if (highSeverity.Any())
                {
                    await SendHighSeverityAlertAsync(highSeverity);
                    alertCounts["High"] = highSeverity.Count;
                }

                // Log medium and low severity anomalies
                if (mediumSeverity.Any())
                {
                    LogMediumSeverityAnomalies(mediumSeverity);
                    alertCounts["Medium"] = mediumSeverity.Count;
                }

                if (lowSeverity.Any())
                {
                    LogLowSeverityAnomalies(lowSeverity);
                    alertCounts["Low"] = lowSeverity.Count;
                }

                _logger.LogInformation("Alert summary - High: {High}, Medium: {Medium}, Low: {Low}", 
                    alertCounts["High"], alertCounts["Medium"], alertCounts["Low"]);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error triggering alerts");
                throw;
            }

            return alertCounts;
        }

        private double CalculateStandardDeviation(double[] values)
        {
            if (values.Length == 0) return 0;

            var mean = values.Average();
            var variance = values.Select(x => Math.Pow(x - mean, 2)).Average();
            return Math.Sqrt(variance);
        }

        private string DetermineSeverity(double zScore, double actualCost, double expectedCost)
        {
            var absZScore = Math.Abs(zScore);
            var costImpact = Math.Abs(actualCost - expectedCost);

            if (absZScore >= 3.0 || costImpact >= 1000)
                return "High";
            else if (absZScore >= 2.0 || costImpact >= 100)
                return "Medium";
            else
                return "Low";
        }

        private async Task SendHighSeverityAlertAsync(List<AnomalyResult> anomalies)
        {
            // This would integrate with your alerting system (Logic Apps, Teams, etc.)
            _logger.LogWarning("HIGH SEVERITY ANOMALIES DETECTED: {Count} anomalies", anomalies.Count);
            
            foreach (var anomaly in anomalies)
            {
                _logger.LogWarning("  - {ResourceId}: ${ActualCost:F2} (expected: ${ExpectedCost:F2}), z-score: {ZScore:F2}", 
                    anomaly.ResourceId, anomaly.ActualCost, anomaly.ExpectedCost, anomaly.ZScore);
            }

            // TODO: Send to Teams webhook, email, etc.
            await Task.CompletedTask;
        }

        private void LogMediumSeverityAnomalies(List<AnomalyResult> anomalies)
        {
            _logger.LogInformation("MEDIUM SEVERITY ANOMALIES: {Count} anomalies detected", anomalies.Count);
        }

        private void LogLowSeverityAnomalies(List<AnomalyResult> anomalies)
        {
            _logger.LogInformation("LOW SEVERITY ANOMALIES: {Count} anomalies detected", anomalies.Count);
        }
    }
}