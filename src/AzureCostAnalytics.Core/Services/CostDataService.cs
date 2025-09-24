using System.Data.SqlClient;
using AzureCostAnalytics.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;

namespace AzureCostAnalytics.Core.Services
{
    public class CostDataService : ICostDataService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<CostDataService> _logger;
        private readonly string _connectionString;

        public CostDataService(IConfiguration configuration, ILogger<CostDataService> logger)
        {
            _configuration = configuration;
            _logger = logger;
            _connectionString = _configuration.GetConnectionString("SynapseConnectionString") 
                ?? throw new InvalidOperationException("SynapseConnectionString not configured");
        }

        public async Task<IEnumerable<CostDataPoint>> GetCostDataAsync(string subscriptionId, int daysBack = 30)
        {
            var startDate = DateTime.Now.AddDays(-daysBack);
            var endDate = DateTime.Now;

            return await GetCostDataByDateRangeAsync(subscriptionId, startDate, endDate);
        }

        public async Task<IEnumerable<CostDataPoint>> GetCostDataByDateRangeAsync(string subscriptionId, DateTime startDate, DateTime endDate)
        {
            var costData = new List<CostDataPoint>();

            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var query = @"
                    SELECT 
                        fcd.actualCost,
                        fcd.amortizedCost,
                        fcd.costInUSD,
                        fcd.usageQuantity,
                        fcd.effectivePrice,
                        fcd.billingPeriodStartDate,
                        fcd.billingPeriodEndDate,
                        fcd.isReservationCharge,
                        fcd.isSavingsPlanCharge,
                        fcd.chargeType,
                        fcd.additionalInfo,
                        dr.resourceId,
                        dr.resourceName,
                        dr.resourceType,
                        dr.resourceGroupName,
                        dr.location,
                        dr.subscriptionId,
                        dr.subscriptionName,
                        dr.env,
                        dr.app,
                        dr.team,
                        dr.costCenter,
                        dr.businessUnit,
                        dr.project,
                        dr.service,
                        dr.compliance,
                        dr.dataSensitivity,
                        dm.meterCategory,
                        dm.meterSubCategory,
                        dm.serviceName,
                        dm.serviceTier,
                        dm.unitOfMeasure,
                        dd.date,
                        dd.billingPeriodStart,
                        dd.billingPeriodEnd
                    FROM fact_cost_daily fcd
                    INNER JOIN dim_resource dr ON fcd.resourceKey = dr.resourceKey AND dr.isCurrent = 1
                    INNER JOIN dim_meter dm ON fcd.meterKey = dm.meterKey
                    INNER JOIN dim_date dd ON fcd.dateKey = dd.dateKey
                    INNER JOIN dim_subscription ds ON fcd.subscriptionKey = ds.subscriptionKey
                    WHERE ds.subscriptionId = @subscriptionId
                    AND dd.date >= @startDate
                    AND dd.date <= @endDate
                    ORDER BY dd.date DESC, fcd.actualCost DESC";

                using var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@subscriptionId", subscriptionId);
                command.Parameters.AddWithValue("@startDate", startDate);
                command.Parameters.AddWithValue("@endDate", endDate);

                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    costData.Add(new CostDataPoint
                    {
                        ResourceId = reader.GetString("resourceId"),
                        SubscriptionId = reader.GetString("subscriptionId"),
                        Date = reader.GetDateTime("date").ToString("yyyy-MM-dd"),
                        ActualCost = reader.GetDecimal("actualCost"),
                        AmortizedCost = reader.GetDecimal("amortizedCost"),
                        CostInUSD = reader.GetDecimal("costInUSD"),
                        UsageQuantity = reader.GetDecimal("usageQuantity"),
                        EffectivePrice = reader.GetDecimal("effectivePrice"),
                        MeterCategory = reader.GetString("meterCategory"),
                        MeterSubCategory = reader.IsDBNull("meterSubCategory") ? string.Empty : reader.GetString("meterSubCategory"),
                        ServiceName = reader.GetString("serviceName"),
                        ServiceTier = reader.IsDBNull("serviceTier") ? string.Empty : reader.GetString("serviceTier"),
                        Location = reader.IsDBNull("location") ? string.Empty : reader.GetString("location"),
                        ResourceGroup = reader.IsDBNull("resourceGroupName") ? string.Empty : reader.GetString("resourceGroupName"),
                        BillingPeriodStartDate = reader.GetDateTime("billingPeriodStart").ToString("yyyy-MM-dd"),
                        BillingPeriodEndDate = reader.GetDateTime("billingPeriodEnd").ToString("yyyy-MM-dd"),
                        ChargeType = reader.IsDBNull("chargeType") ? string.Empty : reader.GetString("chargeType"),
                        IsReservationCharge = reader.GetBoolean("isReservationCharge"),
                        IsSavingsPlanCharge = reader.GetBoolean("isSavingsPlanCharge"),
                        AdditionalInfo = reader.IsDBNull("additionalInfo") ? string.Empty : reader.GetString("additionalInfo"),
                        Tags = new Dictionary<string, string>
                        {
                            ["Environment"] = reader.IsDBNull("env") ? string.Empty : reader.GetString("env"),
                            ["Application"] = reader.IsDBNull("app") ? string.Empty : reader.GetString("app"),
                            ["Team"] = reader.IsDBNull("team") ? string.Empty : reader.GetString("team"),
                            ["CostCenter"] = reader.IsDBNull("costCenter") ? string.Empty : reader.GetString("costCenter"),
                            ["BusinessUnit"] = reader.IsDBNull("businessUnit") ? string.Empty : reader.GetString("businessUnit"),
                            ["Project"] = reader.IsDBNull("project") ? string.Empty : reader.GetString("project"),
                            ["Service"] = reader.IsDBNull("service") ? string.Empty : reader.GetString("service"),
                            ["Compliance"] = reader.IsDBNull("compliance") ? string.Empty : reader.GetString("compliance"),
                            ["DataSensitivity"] = reader.IsDBNull("dataSensitivity") ? string.Empty : reader.GetString("dataSensitivity")
                        }
                    });
                }

                _logger.LogInformation("Retrieved {Count} cost data points for subscription {SubscriptionId}", 
                    costData.Count, subscriptionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving cost data for subscription {SubscriptionId}", subscriptionId);
                throw;
            }

            return costData;
        }

        public async Task<IEnumerable<CostDataPoint>> GetCostDataByResourceAsync(string resourceId, int daysBack = 30)
        {
            var startDate = DateTime.Now.AddDays(-daysBack);
            var endDate = DateTime.Now;

            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var query = @"
                    SELECT 
                        fcd.actualCost,
                        fcd.amortizedCost,
                        fcd.costInUSD,
                        fcd.usageQuantity,
                        fcd.effectivePrice,
                        dr.resourceId,
                        dr.resourceName,
                        dr.resourceType,
                        dd.date,
                        dm.meterCategory,
                        dm.serviceName
                    FROM fact_cost_daily fcd
                    INNER JOIN dim_resource dr ON fcd.resourceKey = dr.resourceKey AND dr.isCurrent = 1
                    INNER JOIN dim_meter dm ON fcd.meterKey = dm.meterKey
                    INNER JOIN dim_date dd ON fcd.dateKey = dd.dateKey
                    WHERE dr.resourceId = @resourceId
                    AND dd.date >= @startDate
                    AND dd.date <= @endDate
                    ORDER BY dd.date DESC";

                using var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@resourceId", resourceId);
                command.Parameters.AddWithValue("@startDate", startDate);
                command.Parameters.AddWithValue("@endDate", endDate);

                var costData = new List<CostDataPoint>();
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    costData.Add(new CostDataPoint
                    {
                        ResourceId = reader.GetString("resourceId"),
                        Date = reader.GetDateTime("date").ToString("yyyy-MM-dd"),
                        ActualCost = reader.GetDecimal("actualCost"),
                        AmortizedCost = reader.GetDecimal("amortizedCost"),
                        CostInUSD = reader.GetDecimal("costInUSD"),
                        UsageQuantity = reader.GetDecimal("usageQuantity"),
                        EffectivePrice = reader.GetDecimal("effectivePrice"),
                        MeterCategory = reader.GetString("meterCategory"),
                        ServiceName = reader.GetString("serviceName")
                    });
                }

                return costData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving cost data for resource {ResourceId}", resourceId);
                throw;
            }
        }

        public async Task<decimal> GetTotalCostAsync(string subscriptionId, DateTime startDate, DateTime endDate)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var query = @"
                    SELECT SUM(fcd.actualCost) as TotalCost
                    FROM fact_cost_daily fcd
                    INNER JOIN dim_subscription ds ON fcd.subscriptionKey = ds.subscriptionKey
                    INNER JOIN dim_date dd ON fcd.dateKey = dd.dateKey
                    WHERE ds.subscriptionId = @subscriptionId
                    AND dd.date >= @startDate
                    AND dd.date <= @endDate";

                using var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@subscriptionId", subscriptionId);
                command.Parameters.AddWithValue("@startDate", startDate);
                command.Parameters.AddWithValue("@endDate", endDate);

                var result = await command.ExecuteScalarAsync();
                return result == DBNull.Value ? 0 : Convert.ToDecimal(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating total cost for subscription {SubscriptionId}", subscriptionId);
                throw;
            }
        }

        public async Task<Dictionary<string, decimal>> GetCostByServiceAsync(string subscriptionId, DateTime startDate, DateTime endDate)
        {
            var serviceCosts = new Dictionary<string, decimal>();

            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var query = @"
                    SELECT 
                        dm.serviceName,
                        SUM(fcd.actualCost) as TotalCost
                    FROM fact_cost_daily fcd
                    INNER JOIN dim_subscription ds ON fcd.subscriptionKey = ds.subscriptionKey
                    INNER JOIN dim_meter dm ON fcd.meterKey = dm.meterKey
                    INNER JOIN dim_date dd ON fcd.dateKey = dd.dateKey
                    WHERE ds.subscriptionId = @subscriptionId
                    AND dd.date >= @startDate
                    AND dd.date <= @endDate
                    GROUP BY dm.serviceName
                    ORDER BY TotalCost DESC";

                using var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@subscriptionId", subscriptionId);
                command.Parameters.AddWithValue("@startDate", startDate);
                command.Parameters.AddWithValue("@endDate", endDate);

                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var serviceName = reader.GetString("serviceName");
                    var totalCost = reader.GetDecimal("TotalCost");
                    serviceCosts[serviceName] = totalCost;
                }

                return serviceCosts;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving cost by service for subscription {SubscriptionId}", subscriptionId);
                throw;
            }
        }

        public async Task<Dictionary<string, decimal>> GetCostByResourceGroupAsync(string subscriptionId, DateTime startDate, DateTime endDate)
        {
            var resourceGroupCosts = new Dictionary<string, decimal>();

            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var query = @"
                    SELECT 
                        dr.resourceGroupName,
                        SUM(fcd.actualCost) as TotalCost
                    FROM fact_cost_daily fcd
                    INNER JOIN dim_subscription ds ON fcd.subscriptionKey = ds.subscriptionKey
                    INNER JOIN dim_resource dr ON fcd.resourceKey = dr.resourceKey AND dr.isCurrent = 1
                    INNER JOIN dim_date dd ON fcd.dateKey = dd.dateKey
                    WHERE ds.subscriptionId = @subscriptionId
                    AND dd.date >= @startDate
                    AND dd.date <= @endDate
                    GROUP BY dr.resourceGroupName
                    ORDER BY TotalCost DESC";

                using var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@subscriptionId", subscriptionId);
                command.Parameters.AddWithValue("@startDate", startDate);
                command.Parameters.AddWithValue("@endDate", endDate);

                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var resourceGroupName = reader.GetString("resourceGroupName");
                    var totalCost = reader.GetDecimal("TotalCost");
                    resourceGroupCosts[resourceGroupName] = totalCost;
                }

                return resourceGroupCosts;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving cost by resource group for subscription {SubscriptionId}", subscriptionId);
                throw;
            }
        }

        public async Task<IEnumerable<CostDataPoint>> GetAnomalousCostDataAsync(string subscriptionId, int daysBack = 30)
        {
            var startDate = DateTime.Now.AddDays(-daysBack);
            var endDate = DateTime.Now;

            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var query = @"
                    WITH DailyCosts AS (
                        SELECT 
                            fcd.dateKey,
                            fcd.resourceKey,
                            SUM(fcd.actualCost) as DailyCost,
                            AVG(fcd.actualCost) OVER (
                                PARTITION BY fcd.resourceKey 
                                ORDER BY fcd.dateKey 
                                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                            ) as RollingAverage,
                            STDEV(fcd.actualCost) OVER (
                                PARTITION BY fcd.resourceKey 
                                ORDER BY fcd.dateKey 
                                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                            ) as RollingStdDev
                        FROM fact_cost_daily fcd
                        INNER JOIN dim_subscription ds ON fcd.subscriptionKey = ds.subscriptionKey
                        INNER JOIN dim_date dd ON fcd.dateKey = dd.dateKey
                        WHERE ds.subscriptionId = @subscriptionId
                        AND dd.date >= @startDate
                        AND dd.date <= @endDate
                        GROUP BY fcd.dateKey, fcd.resourceKey
                    ),
                    Anomalies AS (
                        SELECT 
                            dc.dateKey,
                            dc.resourceKey,
                            dc.DailyCost,
                            dc.RollingAverage,
                            dc.RollingStdDev,
                            CASE 
                                WHEN dc.RollingStdDev > 0 
                                THEN ABS((dc.DailyCost - dc.RollingAverage) / dc.RollingStdDev)
                                ELSE 0 
                            END as ZScore
                        FROM DailyCosts dc
                        WHERE dc.RollingStdDev > 0
                        AND ABS((dc.DailyCost - dc.RollingAverage) / dc.RollingStdDev) >= 2.0
                        AND dc.DailyCost >= 10
                    )
                    SELECT 
                        fcd.actualCost,
                        fcd.amortizedCost,
                        fcd.costInUSD,
                        dr.resourceId,
                        dr.resourceName,
                        dd.date,
                        dm.meterCategory,
                        dm.serviceName,
                        a.ZScore
                    FROM fact_cost_daily fcd
                    INNER JOIN Anomalies a ON fcd.dateKey = a.dateKey AND fcd.resourceKey = a.resourceKey
                    INNER JOIN dim_resource dr ON fcd.resourceKey = dr.resourceKey AND dr.isCurrent = 1
                    INNER JOIN dim_date dd ON fcd.dateKey = dd.dateKey
                    INNER JOIN dim_meter dm ON fcd.meterKey = dm.meterKey
                    ORDER BY a.ZScore DESC, fcd.actualCost DESC";

                using var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@subscriptionId", subscriptionId);
                command.Parameters.AddWithValue("@startDate", startDate);
                command.Parameters.AddWithValue("@endDate", endDate);

                var costData = new List<CostDataPoint>();
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    costData.Add(new CostDataPoint
                    {
                        ResourceId = reader.GetString("resourceId"),
                        Date = reader.GetDateTime("date").ToString("yyyy-MM-dd"),
                        ActualCost = reader.GetDecimal("actualCost"),
                        AmortizedCost = reader.GetDecimal("amortizedCost"),
                        CostInUSD = reader.GetDecimal("costInUSD"),
                        MeterCategory = reader.GetString("meterCategory"),
                        ServiceName = reader.GetString("serviceName"),
                        AdditionalInfo = $"Z-Score: {reader.GetDouble("ZScore"):F2}"
                    });
                }

                return costData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving anomalous cost data for subscription {SubscriptionId}", subscriptionId);
                throw;
            }
        }
    }
}
