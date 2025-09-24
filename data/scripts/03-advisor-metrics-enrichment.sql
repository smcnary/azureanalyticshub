-- Azure Cost Analytics - Advisor and Metrics Enrichment
-- This script creates tables and procedures for Azure Advisor recommendations and metrics data

USE CostAnalytics;
GO

-- Create Azure Advisor Recommendations Table
CREATE TABLE dbo.raw_advisor_recommendations (
    id NVARCHAR(100) NOT NULL PRIMARY KEY,
    resourceId NVARCHAR(500) NOT NULL,
    resourceName NVARCHAR(255) NOT NULL,
    resourceType NVARCHAR(100) NOT NULL,
    resourceGroup NVARCHAR(255) NOT NULL,
    subscriptionId NVARCHAR(50) NOT NULL,
    category NVARCHAR(50) NOT NULL, -- Cost, Performance, Security, etc.
    impact NVARCHAR(20) NOT NULL, -- High, Medium, Low
    recommendationTypeId NVARCHAR(100) NOT NULL,
    shortDescription NVARCHAR(MAX) NOT NULL,
    longDescription NVARCHAR(MAX),
    potentialBenefits NVARCHAR(MAX),
    actions NVARCHAR(MAX), -- JSON array of actions
    remediation NVARCHAR(MAX),
    metadata NVARCHAR(MAX), -- Additional metadata as JSON
    createdTime DATETIME2 NOT NULL,
    lastUpdated DATETIME2 NOT NULL,
    status NVARCHAR(50) NOT NULL, -- Active, Dismissed, Postponed
    extendedProperties NVARCHAR(MAX), -- Additional properties as JSON
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Azure Monitor Metrics Table
CREATE TABLE dbo.raw_metrics_data (
    id NVARCHAR(100) NOT NULL PRIMARY KEY,
    resourceId NVARCHAR(500) NOT NULL,
    metricName NVARCHAR(100) NOT NULL,
    metricNamespace NVARCHAR(100) NOT NULL,
    timeGrain NVARCHAR(20) NOT NULL, -- PT1M, PT5M, PT15M, PT1H, PT6H, PT12H, P1D
    aggregationType NVARCHAR(20) NOT NULL, -- Average, Count, Maximum, Minimum, Total
    startTime DATETIME2 NOT NULL,
    endTime DATETIME2 NOT NULL,
    metricValue DECIMAL(18,6) NOT NULL,
    unit NVARCHAR(50) NOT NULL,
    dimensions NVARCHAR(MAX), -- JSON object with dimension key-value pairs
    subscriptionId NVARCHAR(50) NOT NULL,
    resourceGroup NVARCHAR(255) NOT NULL,
    location NVARCHAR(100),
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Cost-Specific Advisor Recommendations View
CREATE VIEW vw_cost_advisor_recommendations AS
SELECT 
    rar.id,
    rar.resourceId,
    rar.resourceName,
    rar.resourceType,
    rar.resourceGroup,
    rar.subscriptionId,
    rar.impact,
    rar.recommendationTypeId,
    rar.shortDescription,
    rar.longDescription,
    rar.potentialBenefits,
    rar.actions,
    rar.remediation,
    rar.createdTime,
    rar.lastUpdated,
    rar.status,
    -- Parse potential savings from benefits
    CASE 
        WHEN rar.potentialBenefits LIKE '%$%' 
        THEN TRY_CAST(
            SUBSTRING(rar.potentialBenefits, 
                CHARINDEX('$', rar.potentialBenefits) + 1,
                PATINDEX('%[^0-9.]%', SUBSTRING(rar.potentialBenefits, CHARINDEX('$', rar.potentialBenefits) + 1, 50)) - 1
            ) AS DECIMAL(18,2)
        )
        ELSE 0 
    END AS potentialMonthlySavings,
    -- Categorize recommendation types
    CASE 
        WHEN rar.recommendationTypeId LIKE '%Idle%' THEN 'Idle Resources'
        WHEN rar.recommendationTypeId LIKE '%RightSize%' THEN 'Right Size'
        WHEN rar.recommendationTypeId LIKE '%Reserved%' THEN 'Reserved Instances'
        WHEN rar.recommendationTypeId LIKE '%Spot%' THEN 'Spot Instances'
        WHEN rar.recommendationTypeId LIKE '%Unattached%' THEN 'Unattached Resources'
        WHEN rar.recommendationTypeId LIKE '%Premium%' THEN 'Premium Tier Optimization'
        ELSE 'Other'
    END AS recommendationCategory,
    -- Priority based on impact and savings
    CASE 
        WHEN rar.impact = 'High' AND 
             CASE WHEN rar.potentialBenefits LIKE '%$%' 
                  THEN TRY_CAST(SUBSTRING(rar.potentialBenefits, CHARINDEX('$', rar.potentialBenefits) + 1, 10) AS DECIMAL(18,2))
                  ELSE 0 END > 1000 
        THEN 'Critical'
        WHEN rar.impact = 'High' OR 
             CASE WHEN rar.potentialBenefits LIKE '%$%' 
                  THEN TRY_CAST(SUBSTRING(rar.potentialBenefits, CHARINDEX('$', rar.potentialBenefits) + 1, 10) AS DECIMAL(18,2))
                  ELSE 0 END > 500 
        THEN 'High'
        WHEN rar.impact = 'Medium' OR 
             CASE WHEN rar.potentialBenefits LIKE '%$%' 
                  THEN TRY_CAST(SUBSTRING(rar.potentialBenefits, CHARINDEX('$', rar.potentialBenefits) + 1, 10) AS DECIMAL(18,2))
                  ELSE 0 END > 100 
        THEN 'Medium'
        ELSE 'Low'
    END AS priority
FROM raw_advisor_recommendations rar
WHERE rar.category = 'Cost'
    AND rar.status = 'Active';

-- Create Resource Utilization Metrics View
CREATE VIEW vw_resource_utilization_metrics AS
SELECT 
    rmd.resourceId,
    rmd.subscriptionId,
    rmd.resourceGroup,
    rmd.location,
    rmd.metricName,
    rmd.metricNamespace,
    rmd.startTime,
    rmd.endTime,
    rmd.timeGrain,
    rmd.aggregationType,
    rmd.metricValue,
    rmd.unit,
    -- Calculate utilization percentage for CPU and Memory
    CASE 
        WHEN rmd.metricName = 'Percentage CPU' THEN rmd.metricValue
        WHEN rmd.metricName = 'Available Memory Bytes' AND rmd.unit = 'Bytes' THEN 
            CASE 
                WHEN TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL) > 0
                THEN ((TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL) - rmd.metricValue) / 
                      TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL)) * 100
                ELSE NULL
            END
        ELSE rmd.metricValue
    END AS utilizationPercentage,
    -- Resource type classification
    CASE 
        WHEN rmd.metricNamespace = 'Microsoft.Compute/virtualMachines' THEN 'Virtual Machine'
        WHEN rmd.metricNamespace = 'Microsoft.Web/sites' THEN 'App Service'
        WHEN rmd.metricNamespace = 'Microsoft.Sql/servers' THEN 'SQL Database'
        WHEN rmd.metricNamespace = 'Microsoft.Storage/storageAccounts' THEN 'Storage Account'
        WHEN rmd.metricNamespace = 'Microsoft.Network/loadBalancers' THEN 'Load Balancer'
        ELSE rmd.metricNamespace
    END AS resourceType
FROM raw_metrics_data rmd
WHERE rmd.startTime >= DATEADD(DAY, -30, GETDATE());

-- Create Daily Aggregated Metrics View
CREATE VIEW vw_daily_metrics_summary AS
SELECT 
    CAST(rmd.startTime AS DATE) AS metricDate,
    rmd.resourceId,
    rmd.subscriptionId,
    rmd.resourceGroup,
    rmd.metricName,
    rmd.metricNamespace,
    -- Aggregations
    AVG(rmd.metricValue) AS avgValue,
    MAX(rmd.metricValue) AS maxValue,
    MIN(rmd.metricValue) AS minValue,
    COUNT(*) AS sampleCount,
    -- Percentiles (approximated)
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY rmd.metricValue) OVER (
        PARTITION BY CAST(rmd.startTime AS DATE), rmd.resourceId, rmd.metricName
    ) AS p50Value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rmd.metricValue) OVER (
        PARTITION BY CAST(rmd.startTime AS DATE), rmd.resourceId, rmd.metricName
    ) AS p95Value,
    -- Utilization status
    CASE 
        WHEN rmd.metricName = 'Percentage CPU' THEN
            CASE 
                WHEN AVG(rmd.metricValue) < 20 THEN 'Underutilized'
                WHEN AVG(rmd.metricValue) > 80 THEN 'Overutilized'
                ELSE 'Optimal'
            END
        WHEN rmd.metricName = 'Available Memory Bytes' THEN
            CASE 
                WHEN AVG(rmd.metricValue) > (MAX(rmd.metricValue) * 0.8) THEN 'Underutilized'
                WHEN AVG(rmd.metricValue) < (MAX(rmd.metricValue) * 0.2) THEN 'Overutilized'
                ELSE 'Optimal'
            END
        ELSE 'Unknown'
    END AS utilizationStatus
FROM raw_metrics_data rmd
WHERE rmd.startTime >= DATEADD(DAY, -30, GETDATE())
GROUP BY CAST(rmd.startTime AS DATE), rmd.resourceId, rmd.subscriptionId, rmd.resourceGroup, 
         rmd.metricName, rmd.metricNamespace;

-- Create stored procedure to process Advisor recommendations
CREATE PROCEDURE sp_process_advisor_recommendations
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update existing savings opportunities with new advisor data
    UPDATE so
    SET 
        recommendationDescription = ISNULL(so.recommendationDescription, car.shortDescription),
        potentialMonthlySavings = CASE 
            WHEN car.potentialMonthlySavings > 0 THEN car.potentialMonthlySavings
            ELSE so.potentialMonthlySavings
        END,
        potentialAnnualSavings = CASE 
            WHEN car.potentialMonthlySavings > 0 THEN car.potentialMonthlySavings * 12
            ELSE so.potentialAnnualSavings
        END,
        status = CASE 
            WHEN car.status = 'Dismissed' THEN 'Rejected'
            WHEN car.status = 'Postponed' THEN 'Pending'
            ELSE so.status
        END,
        priority = car.priority,
        rowModifiedDate = GETUTCDATE()
    FROM fact_savings_opportunity so
    JOIN vw_cost_advisor_recommendations car ON so.resourceId = car.resourceId
    WHERE car.recommendationTypeId LIKE '%' + so.recommendationType + '%'
        OR (so.recommendationType = 'RightSize' AND car.recommendationCategory = 'Right Size')
        OR (so.recommendationType = 'Idle' AND car.recommendationCategory = 'Idle Resources');
    
    -- Insert new advisor recommendations as savings opportunities
    INSERT INTO fact_savings_opportunity (
        dateKey, resourceKey, resourceId, recommendationType, 
        recommendationDescription, potentialMonthlySavings, 
        potentialAnnualSavings, status, priority, recommendationDate
    )
    SELECT 
        (SELECT dateKey FROM dim_date WHERE date = CAST(car.createdTime AS DATE)) as dateKey,
        (SELECT TOP 1 resourceKey FROM dim_resource WHERE resourceId = car.resourceId AND isCurrent = 1) as resourceKey,
        car.resourceId,
        car.recommendationCategory,
        car.shortDescription,
        car.potentialMonthlySavings,
        car.potentialAnnualSavings,
        CASE car.status
            WHEN 'Dismissed' THEN 'Rejected'
            WHEN 'Postponed' THEN 'Pending'
            ELSE 'Pending'
        END,
        car.priority,
        CAST(car.createdTime AS DATE)
    FROM vw_cost_advisor_recommendations car
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_savings_opportunity so 
        WHERE so.resourceId = car.resourceId 
        AND so.recommendationType = car.recommendationCategory
        AND so.status IN ('Pending', 'InProgress', 'Completed')
    )
    AND car.potentialMonthlySavings > 0;
    
    PRINT 'Advisor recommendations processed. Updated: ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records';
END;
GO

-- Create stored procedure to aggregate daily metrics
CREATE PROCEDURE sp_aggregate_daily_metrics
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update or insert daily utilization metrics
    MERGE fact_utilization_daily AS target
    USING (
        SELECT 
            (SELECT dateKey FROM dim_date WHERE date = CAST(rmd.startTime AS DATE)) as dateKey,
            (SELECT TOP 1 resourceKey FROM dim_resource WHERE resourceId = rmd.resourceId AND isCurrent = 1) as resourceKey,
            rmd.resourceId,
            -- CPU metrics
            AVG(CASE WHEN rmd.metricName = 'Percentage CPU' THEN rmd.metricValue END) as avgCpuPercentage,
            MAX(CASE WHEN rmd.metricName = 'Percentage CPU' THEN rmd.metricValue END) as maxCpuPercentage,
            -- Memory metrics (converted from available bytes to percentage)
            AVG(CASE 
                WHEN rmd.metricName = 'Available Memory Bytes' AND rmd.unit = 'Bytes' THEN
                    CASE 
                        WHEN TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL) > 0
                        THEN ((TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL) - rmd.metricValue) / 
                              TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL)) * 100
                        ELSE NULL
                    END
                END) as avgMemoryPercentage,
            MAX(CASE 
                WHEN rmd.metricName = 'Available Memory Bytes' AND rmd.unit = 'Bytes' THEN
                    CASE 
                        WHEN TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL) > 0
                        THEN ((TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL) - rmd.metricValue) / 
                              TRY_CAST(JSON_VALUE(rmd.dimensions, '$.TotalMemory') AS DECIMAL)) * 100
                        ELSE NULL
                    END
                END) as maxMemoryPercentage,
            -- Disk metrics
            AVG(CASE WHEN rmd.metricName LIKE '%Disk%' AND rmd.aggregationType = 'Total' THEN rmd.metricValue END) as avgDiskIOPS,
            MAX(CASE WHEN rmd.metricName LIKE '%Disk%' AND rmd.aggregationType = 'Total' THEN rmd.metricValue END) as maxDiskIOPS,
            -- Network metrics
            AVG(CASE WHEN rmd.metricName LIKE '%Network%' AND rmd.aggregationType = 'Total' THEN rmd.metricValue END) as avgNetworkIOPS,
            MAX(CASE WHEN rmd.metricName LIKE '%Network%' AND rmd.aggregationType = 'Total' THEN rmd.metricValue END) as maxNetworkIOPS,
            -- Availability metrics
            COUNT(CASE WHEN rmd.metricName = 'Percentage CPU' THEN 1 END) * 
            CASE 
                WHEN MAX(rmd.timeGrain) = 'PT1M' THEN 1.0/60
                WHEN MAX(rmd.timeGrain) = 'PT5M' THEN 5.0/60
                WHEN MAX(rmd.timeGrain) = 'PT15M' THEN 15.0/60
                WHEN MAX(rmd.timeGrain) = 'PT1H' THEN 1.0
                ELSE 1.0
            END as uptimeHours,
            -- Idle percentage calculation
            CASE 
                WHEN AVG(CASE WHEN rmd.metricName = 'Percentage CPU' THEN rmd.metricValue END) < 5 THEN 95.0
                WHEN AVG(CASE WHEN rmd.metricName = 'Percentage CPU' THEN rmd.metricValue END) < 20 THEN 80.0
                WHEN AVG(CASE WHEN rmd.metricName = 'Percentage CPU' THEN rmd.metricValue END) < 50 THEN 50.0
                ELSE 10.0
            END as idlePercentage,
            95.0 as availabilityPercentage -- Placeholder, would need actual availability data
        FROM raw_metrics_data rmd
        WHERE rmd.startTime >= DATEADD(DAY, -7, GETDATE())
        GROUP BY CAST(rmd.startTime AS DATE), rmd.resourceId
    ) AS source ON target.dateKey = source.dateKey AND target.resourceKey = source.resourceKey
    WHEN MATCHED THEN
        UPDATE SET 
            avgCpuPercentage = source.avgCpuPercentage,
            maxCpuPercentage = source.maxCpuPercentage,
            avgMemoryPercentage = source.avgMemoryPercentage,
            maxMemoryPercentage = source.maxMemoryPercentage,
            avgDiskIOPS = source.avgDiskIOPS,
            maxDiskIOPS = source.maxDiskIOPS,
            avgNetworkIOPS = source.avgNetworkIOPS,
            maxNetworkIOPS = source.maxNetworkIOPS,
            uptimeHours = source.uptimeHours,
            idlePercentage = source.idlePercentage,
            availabilityPercentage = source.availabilityPercentage
    WHEN NOT MATCHED THEN
        INSERT (dateKey, resourceKey, resourceId, avgCpuPercentage, maxCpuPercentage, 
                avgMemoryPercentage, maxMemoryPercentage, avgDiskIOPS, maxDiskIOPS,
                avgNetworkIOPS, maxNetworkIOPS, uptimeHours, idlePercentage, availabilityPercentage)
        VALUES (source.dateKey, source.resourceKey, source.resourceId, source.avgCpuPercentage, 
                source.maxCpuPercentage, source.avgMemoryPercentage, source.maxMemoryPercentage,
                source.avgDiskIOPS, source.maxDiskIOPS, source.avgNetworkIOPS, source.maxNetworkIOPS,
                source.uptimeHours, source.idlePercentage, source.availabilityPercentage);
    
    PRINT 'Daily metrics aggregation completed for ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records';
END;
GO

-- Create indexes for performance
CREATE INDEX IX_raw_advisor_recommendations_resourceId ON dbo.raw_advisor_recommendations(resourceId);
CREATE INDEX IX_raw_advisor_recommendations_subscriptionId ON dbo.raw_advisor_recommendations(subscriptionId);
CREATE INDEX IX_raw_advisor_recommendations_category ON dbo.raw_advisor_recommendations(category);
CREATE INDEX IX_raw_advisor_recommendations_createdTime ON dbo.raw_advisor_recommendations(createdTime);

CREATE INDEX IX_raw_metrics_data_resourceId ON dbo.raw_metrics_data(resourceId);
CREATE INDEX IX_raw_metrics_data_startTime ON dbo.raw_metrics_data(startTime);
CREATE INDEX IX_raw_metrics_data_metricName ON dbo.raw_metrics_data(metricName);
CREATE INDEX IX_raw_metrics_data_subscriptionId ON dbo.raw_metrics_data(subscriptionId);

PRINT 'Advisor and metrics enrichment tables and procedures created successfully!';
