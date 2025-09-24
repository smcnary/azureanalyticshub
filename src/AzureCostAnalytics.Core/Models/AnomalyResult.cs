using System.Text.Json.Serialization;

namespace AzureCostAnalytics.Core.Models
{
    public class AnomalyResult
    {
        [JsonPropertyName("resourceId")]
        public string ResourceId { get; set; } = string.Empty;

        [JsonPropertyName("subscriptionId")]
        public string SubscriptionId { get; set; } = string.Empty;

        [JsonPropertyName("date")]
        public string Date { get; set; } = string.Empty;

        [JsonPropertyName("actualCost")]
        public decimal ActualCost { get; set; }

        [JsonPropertyName("expectedCost")]
        public decimal ExpectedCost { get; set; }

        [JsonPropertyName("variance")]
        public decimal Variance { get; set; }

        [JsonPropertyName("variancePercentage")]
        public decimal VariancePercentage { get; set; }

        [JsonPropertyName("zScore")]
        public double ZScore { get; set; }

        [JsonPropertyName("anomalyType")]
        public string AnomalyType { get; set; } = string.Empty;

        [JsonPropertyName("severity")]
        public string Severity { get; set; } = string.Empty;

        [JsonPropertyName("isAnomaly")]
        public bool IsAnomaly { get; set; }

        [JsonPropertyName("confidence")]
        public double Confidence { get; set; }

        [JsonPropertyName("detectedAt")]
        public DateTime DetectedAt { get; set; } = DateTime.UtcNow;

        [JsonPropertyName("previousDayCost")]
        public decimal? PreviousDayCost { get; set; }

        [JsonPropertyName("previousWeekCost")]
        public decimal? PreviousWeekCost { get; set; }

        [JsonPropertyName("previousMonthCost")]
        public decimal? PreviousMonthCost { get; set; }
    }
}
