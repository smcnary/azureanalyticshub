using System.Text.Json.Serialization;

namespace AzureCostAnalytics.Core.Models
{
    public class CostDataPoint
    {
        [JsonPropertyName("resourceId")]
        public string ResourceId { get; set; } = string.Empty;

        [JsonPropertyName("subscriptionId")]
        public string SubscriptionId { get; set; } = string.Empty;

        [JsonPropertyName("date")]
        public string Date { get; set; } = string.Empty;

        [JsonPropertyName("actualCost")]
        public decimal ActualCost { get; set; }

        [JsonPropertyName("amortizedCost")]
        public decimal AmortizedCost { get; set; }

        [JsonPropertyName("costInUSD")]
        public decimal CostInUSD { get; set; }

        [JsonPropertyName("usageQuantity")]
        public decimal UsageQuantity { get; set; }

        [JsonPropertyName("effectivePrice")]
        public decimal EffectivePrice { get; set; }

        [JsonPropertyName("meterCategory")]
        public string MeterCategory { get; set; } = string.Empty;

        [JsonPropertyName("meterSubCategory")]
        public string MeterSubCategory { get; set; } = string.Empty;

        [JsonPropertyName("serviceName")]
        public string ServiceName { get; set; } = string.Empty;

        [JsonPropertyName("serviceTier")]
        public string ServiceTier { get; set; } = string.Empty;

        [JsonPropertyName("location")]
        public string Location { get; set; } = string.Empty;

        [JsonPropertyName("resourceGroup")]
        public string ResourceGroup { get; set; } = string.Empty;

        [JsonPropertyName("billingPeriodStartDate")]
        public string BillingPeriodStartDate { get; set; } = string.Empty;

        [JsonPropertyName("billingPeriodEndDate")]
        public string BillingPeriodEndDate { get; set; } = string.Empty;

        [JsonPropertyName("chargeType")]
        public string ChargeType { get; set; } = string.Empty;

        [JsonPropertyName("isReservationCharge")]
        public bool IsReservationCharge { get; set; }

        [JsonPropertyName("isSavingsPlanCharge")]
        public bool IsSavingsPlanCharge { get; set; }

        [JsonPropertyName("tags")]
        public Dictionary<string, string> Tags { get; set; } = new Dictionary<string, string>();

        [JsonPropertyName("additionalInfo")]
        public string AdditionalInfo { get; set; } = string.Empty;
    }
}
