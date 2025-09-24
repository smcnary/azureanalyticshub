-- Azure Cost Analytics - KPI Calculations and Views
-- This script creates views and stored procedures for key performance indicators

USE CostAnalytics;
GO

-- Create KPI Views for Power BI consumption

-- 1. Executive Overview KPIs
CREATE VIEW vw_executive_kpis AS
SELECT 
    d.date,
    d.year,
    d.month,
    d.quarter,
    s.subscriptionName,
    s.businessUnit,
    
    -- Cost Metrics
    SUM(fc.actualCost) AS totalActualCost,
    SUM(fc.amortizedCost) AS totalAmortizedCost,
    SUM(fc.costInUSD) AS totalCostUSD,
    
    -- Growth Metrics
    LAG(SUM(fc.actualCost)) OVER (PARTITION BY s.subscriptionId ORDER BY d.date) AS previousDayCost,
    LAG(SUM(fc.actualCost)) OVER (PARTITION BY s.subscriptionId ORDER BY d.year, d.month) AS previousMonthCost,
    
    -- Counts
    COUNT(DISTINCT fc.resourceKey) AS activeResourceCount,
    COUNT(DISTINCT fc.meterKey) AS uniqueServiceCount,
    
    -- Forecast (placeholder for now)
    0 AS forecastedCost,
    
    -- Metadata
    GETUTCDATE() AS calculationDate
FROM fact_cost_daily fc
JOIN dim_date d ON fc.dateKey = d.dateKey
JOIN dim_subscription s ON fc.subscriptionKey = s.subscriptionKey
WHERE d.date >= DATEADD(MONTH, -13, GETDATE()) -- Last 13 months for incremental refresh
GROUP BY d.date, d.year, d.month, d.quarter, s.subscriptionId, s.subscriptionName, s.businessUnit;

-- 2. Month-over-Month Growth Calculation
CREATE VIEW vw_mom_growth AS
SELECT 
    current_month.subscriptionId,
    current_month.subscriptionName,
    current_month.year,
    current_month.month,
    current_month.totalCost AS currentMonthCost,
    previous_month.totalCost AS previousMonthCost,
    CASE 
        WHEN previous_month.totalCost > 0 
        THEN ((current_month.totalCost - previous_month.totalCost) / previous_month.totalCost) * 100
        ELSE 0 
    END AS momGrowthPercentage,
    current_month.totalCost - previous_month.totalCost AS momCostChange
FROM (
    SELECT 
        s.subscriptionId,
        s.subscriptionName,
        d.year,
        d.month,
        SUM(fc.actualCost) AS totalCost
    FROM fact_cost_daily fc
    JOIN dim_date d ON fc.dateKey = d.dateKey
    JOIN dim_subscription s ON fc.subscriptionKey = s.subscriptionKey
    WHERE d.date >= DATEADD(MONTH, -2, GETDATE())
    GROUP BY s.subscriptionId, s.subscriptionName, d.year, d.month
) current_month
LEFT JOIN (
    SELECT 
        s.subscriptionId,
        d.year,
        d.month,
        SUM(fc.actualCost) AS totalCost
    FROM fact_cost_daily fc
    JOIN dim_date d ON fc.dateKey = d.dateKey
    JOIN dim_subscription s ON fc.subscriptionKey = s.subscriptionKey
    WHERE d.date >= DATEADD(MONTH, -2, GETDATE())
    GROUP BY s.subscriptionId, d.year, d.month
) previous_month ON current_month.subscriptionId = previous_month.subscriptionId
    AND (current_month.year = previous_month.year AND current_month.month = previous_month.month + 1
         OR current_month.year = previous_month.year + 1 AND current_month.month = 1 AND previous_month.month = 12);

-- 3. Service Cost Breakdown
CREATE VIEW vw_service_cost_breakdown AS
SELECT 
    d.date,
    s.subscriptionName,
    m.serviceName,
    m.meterCategory,
    m.meterSubCategory,
    SUM(fc.actualCost) AS totalCost,
    SUM(fc.usageQuantity) AS totalUsage,
    AVG(fc.effectivePrice) AS avgEffectivePrice,
    COUNT(DISTINCT fc.resourceKey) AS resourceCount,
    -- Cost per resource
    CASE 
        WHEN COUNT(DISTINCT fc.resourceKey) > 0 
        THEN SUM(fc.actualCost) / COUNT(DISTINCT fc.resourceKey)
        ELSE 0 
    END AS costPerResource
FROM fact_cost_daily fc
JOIN dim_date d ON fc.dateKey = d.dateKey
JOIN dim_subscription s ON fc.subscriptionKey = s.subscriptionKey
JOIN dim_meter m ON fc.meterKey = m.meterKey
WHERE d.date >= DATEADD(MONTH, -3, GETDATE())
GROUP BY d.date, s.subscriptionId, s.subscriptionName, m.serviceName, m.meterCategory, m.meterSubCategory;

-- 4. Resource Utilization Analysis
CREATE VIEW vw_resource_utilization AS
SELECT 
    d.date,
    r.resourceId,
    r.resourceName,
    r.resourceType,
    r.subscriptionName,
    r.businessUnit,
    r.team,
    r.costCenter,
    fu.avgCpuPercentage,
    fu.maxCpuPercentage,
    fu.avgMemoryPercentage,
    fu.maxMemoryPercentage,
    fu.idlePercentage,
    fu.uptimeHours,
    fu.availabilityPercentage,
    -- Cost from cost fact
    fc.totalCost,
    fc.dailyCost,
    -- Efficiency metrics
    CASE 
        WHEN fc.totalCost > 0 AND fu.avgCpuPercentage > 0
        THEN fc.dailyCost / fu.avgCpuPercentage
        ELSE 0 
    END AS costPerCpuPercent,
    -- Utilization status
    CASE 
        WHEN fu.avgCpuPercentage < 20 THEN 'Underutilized'
        WHEN fu.avgCpuPercentage > 80 THEN 'Overutilized'
        ELSE 'Optimal'
    END AS utilizationStatus
FROM fact_utilization_daily fu
JOIN dim_date d ON fu.dateKey = d.dateKey
JOIN dim_resource r ON fu.resourceKey = r.resourceKey AND r.isCurrent = 1
LEFT JOIN (
    SELECT 
        dateKey,
        resourceKey,
        SUM(actualCost) AS totalCost,
        SUM(actualCost) AS dailyCost
    FROM fact_cost_daily
    GROUP BY dateKey, resourceKey
) fc ON fu.dateKey = fc.dateKey AND fu.resourceKey = fc.resourceKey
WHERE d.date >= DATEADD(DAY, -30, GETDATE());

-- 5. Savings Opportunities Summary
CREATE VIEW vw_savings_opportunities AS
SELECT 
    r.subscriptionName,
    r.businessUnit,
    r.team,
    so.recommendationType,
    so.status,
    so.priority,
    COUNT(*) AS opportunityCount,
    SUM(so.potentialMonthlySavings) AS totalPotentialMonthlySavings,
    SUM(so.potentialAnnualSavings) AS totalPotentialAnnualSavings,
    SUM(so.realizedMonthlySavings) AS totalRealizedMonthlySavings,
    SUM(so.realizedAnnualSavings) AS totalRealizedAnnualSavings,
    AVG(so.potentialMonthlySavings) AS avgPotentialMonthlySavings,
    -- Implementation rate
    CASE 
        WHEN SUM(so.potentialMonthlySavings) > 0
        THEN (SUM(so.realizedMonthlySavings) / SUM(so.potentialMonthlySavings)) * 100
        ELSE 0 
    END AS implementationRate
FROM fact_savings_opportunity so
JOIN dim_resource r ON so.resourceKey = r.resourceKey AND r.isCurrent = 1
WHERE so.recommendationDate >= DATEADD(MONTH, -12, GETDATE())
GROUP BY r.subscriptionName, r.businessUnit, r.team, so.recommendationType, so.status, so.priority;

-- 6. Anomaly Detection Summary
CREATE VIEW vw_anomaly_summary AS
SELECT 
    d.date,
    s.subscriptionName,
    r.businessUnit,
    COUNT(*) AS totalAnomalies,
    SUM(CASE WHEN fa.severity = 'High' THEN 1 ELSE 0 END) AS highSeverityAnomalies,
    SUM(CASE WHEN fa.severity = 'Medium' THEN 1 ELSE 0 END) AS mediumSeverityAnomalies,
    SUM(CASE WHEN fa.severity = 'Low' THEN 1 ELSE 0 END) AS lowSeverityAnomalies,
    SUM(fa.costVariance) AS totalCostVariance,
    AVG(fa.variancePercentage) AS avgVariancePercentage,
    MAX(fa.zScore) AS maxZScore,
    AVG(fa.zScore) AS avgZScore
FROM fact_anomaly_daily fa
JOIN dim_date d ON fa.dateKey = d.dateKey
JOIN dim_subscription s ON fa.subscriptionKey = s.subscriptionKey
LEFT JOIN dim_resource r ON fa.resourceKey = r.resourceKey AND r.isCurrent = 1
WHERE fa.isAnomaly = 1 AND d.date >= DATEADD(DAY, -30, GETDATE())
GROUP BY d.date, s.subscriptionId, s.subscriptionName, r.businessUnit;

-- 7. Reservation and Savings Plan Coverage
CREATE VIEW vw_reservation_coverage AS
SELECT 
    d.date,
    s.subscriptionName,
    b.benefitType,
    b.productName,
    b.term,
    b.scope,
    b.utilizationPercentage,
    SUM(fc.actualCost) AS totalCost,
    SUM(CASE WHEN fc.isReservationCharge = 1 THEN fc.actualCost ELSE 0 END) AS reservationCost,
    SUM(CASE WHEN fc.isSavingsPlanCharge = 1 THEN fc.actualCost ELSE 0 END) AS savingsPlanCost,
    -- Coverage percentage
    CASE 
        WHEN SUM(fc.actualCost) > 0 
        THEN (SUM(CASE WHEN fc.isReservationCharge = 1 OR fc.isSavingsPlanCharge = 1 THEN fc.actualCost ELSE 0 END) / SUM(fc.actualCost)) * 100
        ELSE 0 
    END AS coveragePercentage,
    -- Unused reservation cost
    CASE 
        WHEN b.utilizationPercentage < 100 
        THEN (SUM(CASE WHEN fc.isReservationCharge = 1 THEN fc.actualCost ELSE 0 END) * (100 - b.utilizationPercentage)) / 100
        ELSE 0 
    END AS unusedReservationCost
FROM fact_cost_daily fc
JOIN dim_date d ON fc.dateKey = d.dateKey
JOIN dim_subscription s ON fc.subscriptionKey = s.subscriptionKey
LEFT JOIN dim_benefit b ON fc.benefitKey = b.benefitKey
WHERE d.date >= DATEADD(MONTH, -3, GETDATE())
    AND (fc.isReservationCharge = 1 OR fc.isSavingsPlanCharge = 1)
GROUP BY d.date, s.subscriptionId, s.subscriptionName, b.benefitType, b.productName, b.term, b.scope, b.utilizationPercentage;

-- 8. Budget vs Actual Analysis
CREATE VIEW vw_budget_vs_actual AS
SELECT 
    d.year,
    d.month,
    d.quarter,
    s.subscriptionName,
    s.businessUnit,
    SUM(fc.actualCost) AS actualCost,
    -- Budget amounts would come from a budget table (to be created)
    0 AS budgetAmount, -- Placeholder
    SUM(fc.actualCost) - 0 AS variance, -- Placeholder
    CASE 
        WHEN 0 > 0 
        THEN (SUM(fc.actualCost) / 0) * 100 
        ELSE 0 
    END AS budgetUtilizationPercentage -- Placeholder
FROM fact_cost_daily fc
JOIN dim_date d ON fc.dateKey = d.dateKey
JOIN dim_subscription s ON fc.subscriptionKey = s.subscriptionKey
WHERE d.date >= DATEADD(MONTH, -12, GETDATE())
GROUP BY d.year, d.month, d.quarter, s.subscriptionId, s.subscriptionName, s.businessUnit;

-- Create stored procedure for anomaly detection
CREATE PROCEDURE sp_detect_cost_anomalies
    @thresholdZScore DECIMAL(8,4) = 2.0,
    @minCostThreshold DECIMAL(18,2) = 10.0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Calculate daily costs by subscription
    WITH daily_costs AS (
        SELECT 
            fc.dateKey,
            fc.subscriptionKey,
            SUM(fc.actualCost) AS dailyCost
        FROM fact_cost_daily fc
        WHERE fc.dateKey >= (SELECT MIN(dateKey) FROM dim_date WHERE date >= DATEADD(DAY, -30, GETDATE()))
        GROUP BY fc.dateKey, fc.subscriptionKey
    ),
    cost_stats AS (
        SELECT 
            subscriptionKey,
            AVG(dailyCost) AS avgCost,
            STDEV(dailyCost) AS stdCost
        FROM daily_costs
        GROUP BY subscriptionKey
    )
    
    -- Insert anomalies
    INSERT INTO fact_anomaly_daily (
        dateKey, resourceKey, subscriptionKey, actualCost, expectedCost, 
        costVariance, variancePercentage, zScore, anomalyType, severity, isAnomaly
    )
    SELECT 
        dc.dateKey,
        NULL as resourceKey, -- Subscription level anomalies
        dc.subscriptionKey,
        dc.dailyCost as actualCost,
        cs.avgCost as expectedCost,
        dc.dailyCost - cs.avgCost as costVariance,
        CASE 
            WHEN cs.avgCost > 0 
            THEN ((dc.dailyCost - cs.avgCost) / cs.avgCost) * 100 
            ELSE 0 
        END as variancePercentage,
        CASE 
            WHEN cs.stdCost > 0 
            THEN (dc.dailyCost - cs.avgCost) / cs.stdCost 
            ELSE 0 
        END as zScore,
        CASE 
            WHEN dc.dailyCost > cs.avgCost THEN 'Spike'
            ELSE 'Drop'
        END as anomalyType,
        CASE 
            WHEN ABS(CASE WHEN cs.stdCost > 0 THEN (dc.dailyCost - cs.avgCost) / cs.stdCost ELSE 0 END) >= 3.0 THEN 'High'
            WHEN ABS(CASE WHEN cs.stdCost > 0 THEN (dc.dailyCost - cs.avgCost) / cs.stdCost ELSE 0 END) >= 2.0 THEN 'Medium'
            ELSE 'Low'
        END as severity,
        CASE 
            WHEN ABS(CASE WHEN cs.stdCost > 0 THEN (dc.dailyCost - cs.avgCost) / cs.stdCost ELSE 0 END) >= @thresholdZScore 
                AND dc.dailyCost >= @minCostThreshold
            THEN 1 
            ELSE 0 
        END as isAnomaly
    FROM daily_costs dc
    JOIN cost_stats cs ON dc.subscriptionKey = cs.subscriptionKey
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_anomaly_daily fa 
        WHERE fa.dateKey = dc.dateKey 
        AND fa.subscriptionKey = dc.subscriptionKey 
        AND fa.resourceKey IS NULL
    );
    
    PRINT 'Anomaly detection completed for ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records';
END;
GO

-- Create stored procedure for rightsizing recommendations
CREATE PROCEDURE sp_generate_rightsizing_recommendations
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert rightsizing recommendations based on utilization
    INSERT INTO fact_savings_opportunity (
        dateKey, resourceKey, resourceId, recommendationType, 
        recommendationDescription, potentialMonthlySavings, 
        potentialAnnualSavings, status, priority
    )
    SELECT 
        (SELECT MAX(dateKey) FROM dim_date WHERE date <= GETDATE()) as dateKey,
        ru.resourceKey,
        ru.resourceId,
        'RightSize' as recommendationType,
        'Resource has been underutilized for 7+ days. Consider downsizing.' as recommendationDescription,
        ru.totalCost * 0.3 as potentialMonthlySavings, -- Assume 30% savings
        ru.totalCost * 0.3 * 12 as potentialAnnualSavings,
        'Pending' as status,
        CASE 
            WHEN ru.totalCost > 1000 THEN 'High'
            WHEN ru.totalCost > 100 THEN 'Medium'
            ELSE 'Low'
        END as priority
    FROM vw_resource_utilization ru
    WHERE ru.utilizationStatus = 'Underutilized'
        AND ru.avgCpuPercentage < 20
        AND ru.totalCost > 50 -- Only recommend for resources costing more than $50/month
        AND NOT EXISTS (
            SELECT 1 FROM fact_savings_opportunity so 
            WHERE so.resourceKey = ru.resourceKey 
            AND so.recommendationType = 'RightSize' 
            AND so.status IN ('Pending', 'InProgress')
        );
    
    PRINT 'Rightsizing recommendations generated for ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' resources';
END;
GO

PRINT 'KPI calculations and stored procedures created successfully!';
