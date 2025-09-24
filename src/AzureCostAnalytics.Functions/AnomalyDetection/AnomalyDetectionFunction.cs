using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Azure.Monitor.Query;
using System.Text.Json;
using System.Text.Json.Serialization;
using AzureCostAnalytics.Core.Models;
using AzureCostAnalytics.Core.Services;

namespace AzureCostAnalytics.Functions.AnomalyDetection
{
    public class AnomalyDetectionFunction
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<AnomalyDetectionFunction> _logger;
        private readonly ICostDataService _costDataService;
        private readonly DefaultAzureCredential _credential;

        public AnomalyDetectionFunction(IConfiguration configuration, ILogger<AnomalyDetectionFunction> logger, ICostDataService costDataService)
        {
            _configuration = configuration;
            _logger = logger;
            _costDataService = costDataService;
            _credential = new DefaultAzureCredential();
        }

        [Function("AnomalyDetection")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequestData req)
        {
            try
            {
                _logger.LogInformation("Starting anomaly detection function");

                // Parse request body
                var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var request = JsonSerializer.Deserialize<AnomalyDetectionRequest>(requestBody);

                if (string.IsNullOrEmpty(request?.SubscriptionId))
                {
                    var badRequestResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                    await badRequestResponse.WriteStringAsync(JsonSerializer.Serialize(new { error = "subscription_id parameter is required" }));
                    return badRequestResponse;
                }

                _logger.LogInformation("Processing subscription: {SubscriptionId}", request.SubscriptionId);

                // Initialize anomaly detector
                var detector = new AnomalyDetector(_configuration, _logger, _costDataService);

                // Get cost data
                var costData = await detector.GetCostDataAsync(request.SubscriptionId, request.DaysBack ?? 30);

                if (!costData.Any())
                {
                    var okResponse = req.CreateResponse(HttpStatusCode.OK);
                    await okResponse.WriteStringAsync(JsonSerializer.Serialize(new
                    {
                        message = "No cost data available for analysis",
                        subscription_id = request.SubscriptionId,
                        anomalies_detected = 0
                    }));
                    return okResponse;
                }

                // Detect anomalies
                var anomalies = detector.DetectAnomalies(costData);

                // Save results
                var blobPath = await detector.SaveAnomalyResultsAsync(anomalies);

                // Trigger alerts
                var alertCounts = await detector.TriggerAlertsAsync(anomalies);

                // Prepare response
                var responseData = new
                {
                    subscription_id = request.SubscriptionId,
                    analysis_period_days = request.DaysBack ?? 30,
                    total_resources_analyzed = costData.Select(x => x.ResourceId).Distinct().Count(),
                    anomalies_detected = anomalies.Count,
                    alert_counts = alertCounts,
                    results_blob_path = blobPath,
                    timestamp = DateTime.UtcNow.ToString("O"),
                    high_severity_anomalies = anomalies
                        .Where(a => a.Severity == "High")
                        .Select(a => new
                        {
                            resource_id = a.ResourceId,
                            date = a.Date,
                            actual_cost = a.ActualCost,
                            expected_cost = a.ExpectedCost,
                            variance_percentage = a.VariancePercentage,
                            z_score = a.ZScore
                        })
                        .ToList()
                };

                _logger.LogInformation("Anomaly detection completed successfully for subscription {SubscriptionId}", request.SubscriptionId);

                var response = req.CreateResponse(HttpStatusCode.OK);
                await response.WriteStringAsync(JsonSerializer.Serialize(responseData));
                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in anomaly detection function");
                var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
                await errorResponse.WriteStringAsync(JsonSerializer.Serialize(new { error = ex.Message }));
                return errorResponse;
            }
        }
    }

    public class AnomalyDetectionRequest
    {
        [JsonPropertyName("subscription_id")]
        public string SubscriptionId { get; set; } = string.Empty;

        [JsonPropertyName("days_back")]
        public int? DaysBack { get; set; }
    }
}