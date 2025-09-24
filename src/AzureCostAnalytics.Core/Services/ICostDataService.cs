using AzureCostAnalytics.Core.Models;

namespace AzureCostAnalytics.Core.Services
{
    public interface ICostDataService
    {
        Task<IEnumerable<CostDataPoint>> GetCostDataAsync(string subscriptionId, int daysBack = 30);
        Task<IEnumerable<CostDataPoint>> GetCostDataByDateRangeAsync(string subscriptionId, DateTime startDate, DateTime endDate);
        Task<IEnumerable<CostDataPoint>> GetCostDataByResourceAsync(string resourceId, int daysBack = 30);
        Task<decimal> GetTotalCostAsync(string subscriptionId, DateTime startDate, DateTime endDate);
        Task<Dictionary<string, decimal>> GetCostByServiceAsync(string subscriptionId, DateTime startDate, DateTime endDate);
        Task<Dictionary<string, decimal>> GetCostByResourceGroupAsync(string subscriptionId, DateTime startDate, DateTime endDate);
        Task<IEnumerable<CostDataPoint>> GetAnomalousCostDataAsync(string subscriptionId, int daysBack = 30);
    }
}
