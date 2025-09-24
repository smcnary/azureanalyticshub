using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using AzureCostAnalytics.Functions.AnomalyDetection;
using AzureCostAnalytics.Core.Models;

namespace AzureCostAnalytics.Tests
{
    public class AnomalyDetectionTests
    {
        private readonly Mock<IConfiguration> _mockConfiguration;
        private readonly Mock<ILogger<AnomalyDetector>> _mockLogger;

        public AnomalyDetectionTests()
        {
            _mockConfiguration = new Mock<IConfiguration>();
            _mockConfiguration.Setup(x => x["ZScoreThreshold"]).Returns("2.0");
            _mockConfiguration.Setup(x => x["MinCostThreshold"]).Returns("10.0");
            _mockConfiguration.Setup(x => x["ConfidenceThreshold"]).Returns("0.8");

            _mockLogger = new Mock<ILogger<AnomalyDetector>>();
        }

        [Fact]
        public void AnomalyDetector_Initialization_SetsCorrectThresholds()
        {
            // Arrange & Act
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);

            // Assert
            Assert.Equal(2.0, detector.GetZScoreThreshold());
            Assert.Equal(10.0, detector.GetMinCostThreshold());
            Assert.Equal(0.8, detector.GetConfidenceThreshold());
        }

        [Fact]
        public void DetectAnomalies_WithNormalData_ReturnsNoAnomalies()
        {
            // Arrange
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);
            var costData = CreateNormalCostData();

            // Act
            var anomalies = detector.DetectAnomalies(costData);

            // Assert
            Assert.Empty(anomalies);
        }

        [Fact]
        public void DetectAnomalies_WithSpikeData_ReturnsAnomalies()
        {
            // Arrange
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);
            var costData = CreateCostDataWithSpike();

            // Act
            var anomalies = detector.DetectAnomalies(costData);

            // Assert
            Assert.NotEmpty(anomalies);
            Assert.Contains(anomalies, a => a.AnomalyType == "Spike");
            Assert.Contains(anomalies, a => a.Severity == "High");
        }

        [Fact]
        public void DetectAnomalies_WithDropData_ReturnsAnomalies()
        {
            // Arrange
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);
            var costData = CreateCostDataWithDrop();

            // Act
            var anomalies = detector.DetectAnomalies(costData);

            // Assert
            Assert.NotEmpty(anomalies);
            Assert.Contains(anomalies, a => a.AnomalyType == "Drop");
        }

        [Theory]
        [InlineData(3.5, 1500, 100, "High")]
        [InlineData(2.5, 500, 100, "Medium")]
        [InlineData(1.5, 150, 100, "Low")]
        public void DetermineSeverity_WithVariousInputs_ReturnsCorrectSeverity(
            double zScore, double actualCost, double expectedCost, string expectedSeverity)
        {
            // Arrange
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);

            // Act
            var severity = detector.DetermineSeverity(zScore, actualCost, expectedCost);

            // Assert
            Assert.Equal(expectedSeverity, severity);
        }

        [Fact]
        public void CalculateStandardDeviation_WithValidData_ReturnsCorrectValue()
        {
            // Arrange
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);
            var values = new double[] { 100, 105, 95, 110, 90, 105, 100, 95, 110, 100 };

            // Act
            var stdDev = detector.CalculateStandardDeviation(values);

            // Assert
            Assert.True(stdDev > 0);
            Assert.True(stdDev < 10); // Should be reasonable for this dataset
        }

        [Fact]
        public void CalculateStandardDeviation_WithEmptyArray_ReturnsZero()
        {
            // Arrange
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);
            var values = new double[0];

            // Act
            var stdDev = detector.CalculateStandardDeviation(values);

            // Assert
            Assert.Equal(0, stdDev);
        }

        [Fact]
        public async Task SaveAnomalyResults_WithValidData_ReturnsBlobPath()
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            var mockContainerClient = new Mock<BlobContainerClient>();
            var mockBlobClient = new Mock<BlobClient>();

            mockBlobServiceClient.Setup(x => x.GetBlobContainerClient(It.IsAny<string>()))
                .Returns(mockContainerClient.Object);
            mockContainerClient.Setup(x => x.GetBlobClient(It.IsAny<string>()))
                .Returns(mockBlobClient.Object);

            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);
            detector.SetBlobServiceClient(mockBlobServiceClient.Object);

            var anomalies = new List<AnomalyResult>
            {
                new AnomalyResult
                {
                    ResourceId = "test-resource",
                    SubscriptionId = "test-sub",
                    Date = "2024-01-01",
                    ActualCost = 1000,
                    ExpectedCost = 100,
                    Variance = 900,
                    VariancePercentage = 900,
                    ZScore = 3.0,
                    AnomalyType = "Spike",
                    Severity = "High",
                    IsAnomaly = true,
                    Confidence = 0.95
                }
            };

            // Act
            var result = await detector.SaveAnomalyResultsAsync(anomalies);

            // Assert
            Assert.NotNull(result);
            Assert.Contains("anomaly-results-", result);
            mockBlobClient.Verify(x => x.UploadAsync(It.IsAny<Stream>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task TriggerAlerts_WithMixedSeverityAnomalies_ReturnsCorrectCounts()
        {
            // Arrange
            var detector = new AnomalyDetector(_mockConfiguration.Object, _mockLogger.Object);
            var anomalies = new List<AnomalyResult>
            {
                new AnomalyResult { Severity = "High", ResourceId = "resource1" },
                new AnomalyResult { Severity = "Medium", ResourceId = "resource2" },
                new AnomalyResult { Severity = "Low", ResourceId = "resource3" }
            };

            // Act
            var alertCounts = await detector.TriggerAlertsAsync(anomalies);

            // Assert
            Assert.Equal(1, alertCounts["High"]);
            Assert.Equal(1, alertCounts["Medium"]);
            Assert.Equal(1, alertCounts["Low"]);
        }

        private List<CostDataPoint> CreateNormalCostData()
        {
            var costData = new List<CostDataPoint>();
            var random = new Random(42); // Fixed seed for consistent results

            for (int i = 0; i < 30; i++)
            {
                var date = DateTime.Now.AddDays(-i);
                var baseCost = 100 + (random.NextDouble() - 0.5) * 20; // 100 +/- 10

                costData.Add(new CostDataPoint
                {
                    ResourceId = "/subscriptions/test-sub/resourceGroups/rg-1/providers/Microsoft.Compute/virtualMachines/vm-1",
                    SubscriptionId = "test-sub",
                    Date = date.ToString("yyyy-MM-dd"),
                    ActualCost = (decimal)Math.Max(0, baseCost),
                    MeterCategory = "Virtual Machines",
                    ServiceName = "Virtual Machines"
                });
            }

            return costData;
        }

        private List<CostDataPoint> CreateCostDataWithSpike()
        {
            var costData = CreateNormalCostData();
            
            // Add a spike on day 15
            var spikeIndex = costData.FindIndex(x => x.Date == DateTime.Now.AddDays(-15).ToString("yyyy-MM-dd"));
            if (spikeIndex >= 0)
            {
                costData[spikeIndex] = costData[spikeIndex] with { ActualCost = 1000 }; // 10x normal cost
            }

            return costData;
        }

        private List<CostDataPoint> CreateCostDataWithDrop()
        {
            var costData = CreateNormalCostData();
            
            // Add a drop on day 20
            var dropIndex = costData.FindIndex(x => x.Date == DateTime.Now.AddDays(-20).ToString("yyyy-MM-dd"));
            if (dropIndex >= 0)
            {
                costData[dropIndex] = costData[dropIndex] with { ActualCost = 10 }; // 10x lower than normal
            }

            return costData;
        }
    }

    // Extension methods to access private members for testing
    public static class AnomalyDetectorExtensions
    {
        public static double GetZScoreThreshold(this AnomalyDetector detector)
        {
            var field = typeof(AnomalyDetector).GetField("_zScoreThreshold", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (double)field.GetValue(detector);
        }

        public static double GetMinCostThreshold(this AnomalyDetector detector)
        {
            var field = typeof(AnomalyDetector).GetField("_minCostThreshold", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (double)field.GetValue(detector);
        }

        public static double GetConfidenceThreshold(this AnomalyDetector detector)
        {
            var field = typeof(AnomalyDetector).GetField("_confidenceThreshold", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (double)field.GetValue(detector);
        }

        public static void SetBlobServiceClient(this AnomalyDetector detector, BlobServiceClient client)
        {
            var field = typeof(AnomalyDetector).GetField("_blobServiceClient", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            field.SetValue(detector, client);
        }

        public static double CalculateStandardDeviation(this AnomalyDetector detector, double[] values)
        {
            var method = typeof(AnomalyDetector).GetMethod("CalculateStandardDeviation", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (double)method.Invoke(detector, new object[] { values });
        }

        public static string DetermineSeverity(this AnomalyDetector detector, double zScore, double actualCost, double expectedCost)
        {
            var method = typeof(AnomalyDetector).GetMethod("DetermineSeverity", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (string)method.Invoke(detector, new object[] { zScore, actualCost, expectedCost });
        }
    }
}
