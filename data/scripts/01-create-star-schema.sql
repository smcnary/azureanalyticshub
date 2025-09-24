-- Azure Cost Analytics - Star Schema Creation
-- This script creates the star schema tables for the cost analytics platform

-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'CostAnalytics')
BEGIN
    CREATE DATABASE CostAnalytics;
END;
GO

USE CostAnalytics;
GO

-- Create Date Dimension
CREATE TABLE dbo.dim_date (
    dateKey INT NOT NULL PRIMARY KEY,
    [date] DATE NOT NULL,
    [year] INT NOT NULL,
    [month] INT NOT NULL,
    [day] INT NOT NULL,
    [quarter] INT NOT NULL,
    [yearMonth] NVARCHAR(7) NOT NULL, -- YYYY-MM format
    [yearQuarter] NVARCHAR(7) NOT NULL, -- YYYY-Q format
    billingPeriodStart DATE NOT NULL,
    billingPeriodEnd DATE NOT NULL,
    dayOfWeek INT NOT NULL,
    dayOfWeekName NVARCHAR(10) NOT NULL,
    isWeekend BIT NOT NULL,
    isMonthEnd BIT NOT NULL,
    isQuarterEnd BIT NOT NULL,
    isYearEnd BIT NOT NULL
);

-- Create Resource Dimension (SCD2)
CREATE TABLE dbo.dim_resource (
    resourceKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    resourceId NVARCHAR(500) NOT NULL,
    resourceName NVARCHAR(255) NOT NULL,
    resourceType NVARCHAR(100) NOT NULL,
    resourceGroupName NVARCHAR(255) NOT NULL,
    subscriptionId NVARCHAR(50) NOT NULL,
    subscriptionName NVARCHAR(255) NOT NULL,
    location NVARCHAR(100),
    sku NVARCHAR(255),
    capacity NVARCHAR(100),
    [env] NVARCHAR(50),
    [app] NVARCHAR(100),
    [team] NVARCHAR(100),
    costCenter NVARCHAR(100),
    ownerUpn NVARCHAR(255),
    businessUnit NVARCHAR(100),
    project NVARCHAR(100),
    service NVARCHAR(100),
    compliance NVARCHAR(50),
    dataSensitivity NVARCHAR(50),
    createdDate DATE,
    deletedDate DATE,
    effectiveStartDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    effectiveEndDate DATETIME2 NULL,
    isCurrent BIT NOT NULL DEFAULT 1,
    -- Additional metadata
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    rowModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Subscription Dimension
CREATE TABLE dbo.dim_subscription (
    subscriptionKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    subscriptionId NVARCHAR(50) NOT NULL,
    subscriptionName NVARCHAR(255) NOT NULL,
    tenantId NVARCHAR(50) NOT NULL,
    billingAccountId NVARCHAR(50),
    billingAccountName NVARCHAR(255),
    billingProfileId NVARCHAR(50),
    billingProfileName NVARCHAR(255),
    invoiceSectionId NVARCHAR(50),
    invoiceSectionName NVARCHAR(255),
    offerType NVARCHAR(50),
    currency NVARCHAR(3),
    isActive BIT NOT NULL DEFAULT 1,
    createdDate DATE,
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Resource Group Dimension
CREATE TABLE dbo.dim_resource_group (
    resourceGroupKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    resourceGroupName NVARCHAR(255) NOT NULL,
    subscriptionId NVARCHAR(50) NOT NULL,
    location NVARCHAR(100),
    [env] NVARCHAR(50),
    [app] NVARCHAR(100),
    [team] NVARCHAR(100),
    costCenter NVARCHAR(100),
    businessUnit NVARCHAR(100),
    project NVARCHAR(100),
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Region Dimension
CREATE TABLE dbo.dim_region (
    regionKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    regionName NVARCHAR(100) NOT NULL,
    regionDisplayName NVARCHAR(255) NOT NULL,
    country NVARCHAR(100),
    continent NVARCHAR(50),
    timeZone NVARCHAR(50),
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Meter Dimension
CREATE TABLE dbo.dim_meter (
    meterKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meterId NVARCHAR(100) NOT NULL,
    meterName NVARCHAR(255) NOT NULL,
    meterCategory NVARCHAR(100) NOT NULL,
    meterSubCategory NVARCHAR(100),
    serviceName NVARCHAR(100) NOT NULL,
    serviceTier NVARCHAR(100),
    serviceFamily NVARCHAR(100),
    unitOfMeasure NVARCHAR(50) NOT NULL,
    unitPrice DECIMAL(18,6),
    currency NVARCHAR(3),
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Offer Dimension
CREATE TABLE dbo.dim_offer (
    offerKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    offerId NVARCHAR(100) NOT NULL,
    offerName NVARCHAR(255),
    offerType NVARCHAR(50),
    pricingModel NVARCHAR(50),
    isAzureCreditEligible BIT,
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Benefit Dimension (Reservations/Savings Plans)
CREATE TABLE dbo.dim_benefit (
    benefitKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    reservationId NVARCHAR(100),
    reservationName NVARCHAR(255),
    benefitType NVARCHAR(50) NOT NULL, -- 'Reservation', 'SavingsPlan'
    productName NVARCHAR(255),
    term NVARCHAR(50), -- '1Year', '3Years'
    scope NVARCHAR(50), -- 'Single', 'Shared'
    instanceSize NVARCHAR(100),
    region NVARCHAR(100),
    quantity INT,
    utilizationPercentage DECIMAL(5,2),
    monthlySavings DECIMAL(18,2),
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Tag Key-Value Dimension
CREATE TABLE dbo.dim_tag_kv (
    tagKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    tagKeyName NVARCHAR(100) NOT NULL,
    tagValue NVARCHAR(255) NOT NULL,
    tagCategory NVARCHAR(50), -- 'Business', 'Technical', 'Compliance'
    isRequired BIT NOT NULL DEFAULT 0,
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Cost Fact Table
CREATE TABLE dbo.fact_cost_daily (
    -- Surrogate Keys
    dateKey INT NOT NULL,
    resourceKey INT NOT NULL,
    subscriptionKey INT NOT NULL,
    resourceGroupKey INT NOT NULL,
    regionKey INT NOT NULL,
    meterKey INT NOT NULL,
    offerKey INT NOT NULL,
    benefitKey INT NULL,
    
    -- Natural Keys
    resourceId NVARCHAR(500) NOT NULL,
    subscriptionId NVARCHAR(50) NOT NULL,
    meterId NVARCHAR(100) NOT NULL,
    billingPeriodStartDate DATE NOT NULL,
    billingPeriodEndDate DATE NOT NULL,
    
    -- Measures
    usageQuantity DECIMAL(18,6) NOT NULL,
    effectivePrice DECIMAL(18,6) NOT NULL,
    unitPrice DECIMAL(18,6),
    actualCost DECIMAL(18,2) NOT NULL,
    amortizedCost DECIMAL(18,2) NOT NULL,
    costInBillingCurrency DECIMAL(18,2) NOT NULL,
    costInUSD DECIMAL(18,2) NOT NULL,
    exchangeRate DECIMAL(18,6),
    
    -- Flags
    isReservationCharge BIT NOT NULL DEFAULT 0,
    isSavingsPlanCharge BIT NOT NULL DEFAULT 0,
    isCredit BIT NOT NULL DEFAULT 0,
    isRefund BIT NOT NULL DEFAULT 0,
    
    -- Additional Info
    chargeType NVARCHAR(50),
    frequency NVARCHAR(50),
    invoiceId NVARCHAR(100),
    additionalInfo NVARCHAR(MAX),
    
    -- Metadata
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    -- Constraints
    CONSTRAINT FK_fact_cost_daily_dateKey FOREIGN KEY (dateKey) REFERENCES dbo.dim_date(dateKey),
    CONSTRAINT FK_fact_cost_daily_resourceKey FOREIGN KEY (resourceKey) REFERENCES dbo.dim_resource(resourceKey),
    CONSTRAINT FK_fact_cost_daily_subscriptionKey FOREIGN KEY (subscriptionKey) REFERENCES dbo.dim_subscription(subscriptionKey),
    CONSTRAINT FK_fact_cost_daily_resourceGroupKey FOREIGN KEY (resourceGroupKey) REFERENCES dbo.dim_resource_group(resourceGroupKey),
    CONSTRAINT FK_fact_cost_daily_regionKey FOREIGN KEY (regionKey) REFERENCES dbo.dim_region(regionKey),
    CONSTRAINT FK_fact_cost_daily_meterKey FOREIGN KEY (meterKey) REFERENCES dbo.dim_meter(meterKey),
    CONSTRAINT FK_fact_cost_daily_offerKey FOREIGN KEY (offerKey) REFERENCES dbo.dim_offer(offerKey),
    CONSTRAINT FK_fact_cost_daily_benefitKey FOREIGN KEY (benefitKey) REFERENCES dbo.dim_benefit(benefitKey)
);

-- Create Utilization Fact Table
CREATE TABLE dbo.fact_utilization_daily (
    dateKey INT NOT NULL,
    resourceKey INT NOT NULL,
    resourceId NVARCHAR(500) NOT NULL,
    
    -- Utilization Metrics
    avgCpuPercentage DECIMAL(5,2),
    maxCpuPercentage DECIMAL(5,2),
    avgMemoryPercentage DECIMAL(5,2),
    maxMemoryPercentage DECIMAL(5,2),
    avgDiskIOPS DECIMAL(18,2),
    maxDiskIOPS DECIMAL(18,2),
    avgNetworkIOPS DECIMAL(18,2),
    maxNetworkIOPS DECIMAL(18,2),
    avgThroughput DECIMAL(18,2),
    maxThroughput DECIMAL(18,2),
    idlePercentage DECIMAL(5,2),
    
    -- Availability Metrics
    uptimeHours DECIMAL(8,2),
    downtimeHours DECIMAL(8,2),
    availabilityPercentage DECIMAL(5,2),
    
    -- Metadata
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    -- Constraints
    CONSTRAINT FK_fact_utilization_daily_dateKey FOREIGN KEY (dateKey) REFERENCES dbo.dim_date(dateKey),
    CONSTRAINT FK_fact_utilization_daily_resourceKey FOREIGN KEY (resourceKey) REFERENCES dbo.dim_resource(resourceKey)
);

-- Create Savings Opportunity Fact Table
CREATE TABLE dbo.fact_savings_opportunity (
    dateKey INT NOT NULL,
    resourceKey INT NOT NULL,
    resourceId NVARCHAR(500) NOT NULL,
    
    -- Opportunity Details
    recommendationType NVARCHAR(100) NOT NULL,
    recommendationDescription NVARCHAR(MAX),
    potentialMonthlySavings DECIMAL(18,2) NOT NULL,
    potentialAnnualSavings DECIMAL(18,2) NOT NULL,
    realizedMonthlySavings DECIMAL(18,2) DEFAULT 0,
    realizedAnnualSavings DECIMAL(18,2) DEFAULT 0,
    
    -- Implementation Status
    status NVARCHAR(50) NOT NULL, -- 'Pending', 'InProgress', 'Completed', 'Rejected'
    priority NVARCHAR(20) NOT NULL, -- 'High', 'Medium', 'Low'
    estimatedImplementationHours INT,
    actualImplementationHours INT,
    
    -- Metadata
    recommendationDate DATE NOT NULL,
    implementationDate DATE,
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    -- Constraints
    CONSTRAINT FK_fact_savings_opportunity_dateKey FOREIGN KEY (dateKey) REFERENCES dbo.dim_date(dateKey),
    CONSTRAINT FK_fact_savings_opportunity_resourceKey FOREIGN KEY (resourceKey) REFERENCES dbo.dim_resource(resourceKey)
);

-- Create Anomaly Detection Fact Table
CREATE TABLE dbo.fact_anomaly_daily (
    dateKey INT NOT NULL,
    resourceKey INT NOT NULL,
    subscriptionKey INT NOT NULL,
    
    -- Anomaly Metrics
    actualCost DECIMAL(18,2) NOT NULL,
    expectedCost DECIMAL(18,2) NOT NULL,
    costVariance DECIMAL(18,2) NOT NULL,
    variancePercentage DECIMAL(5,2) NOT NULL,
    zScore DECIMAL(8,4) NOT NULL,
    
    -- Anomaly Classification
    anomalyType NVARCHAR(50) NOT NULL, -- 'Spike', 'Drop', 'Pattern'
    severity NVARCHAR(20) NOT NULL, -- 'High', 'Medium', 'Low'
    isAnomaly BIT NOT NULL DEFAULT 0,
    
    -- Context
    previousDayCost DECIMAL(18,2),
    previousWeekCost DECIMAL(18,2),
    previousMonthCost DECIMAL(18,2),
    
    -- Metadata
    detectionDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    -- Constraints
    CONSTRAINT FK_fact_anomaly_daily_dateKey FOREIGN KEY (dateKey) REFERENCES dbo.dim_date(dateKey),
    CONSTRAINT FK_fact_anomaly_daily_resourceKey FOREIGN KEY (resourceKey) REFERENCES dbo.dim_resource(resourceKey),
    CONSTRAINT FK_fact_anomaly_daily_subscriptionKey FOREIGN KEY (subscriptionKey) REFERENCES dbo.dim_subscription(subscriptionKey)
);

-- Create Forecast Fact Table
CREATE TABLE dbo.fact_forecast_daily (
    dateKey INT NOT NULL,
    subscriptionKey INT NOT NULL,
    
    -- Forecast Values
    forecastedCost DECIMAL(18,2) NOT NULL,
    confidenceIntervalLower DECIMAL(18,2) NOT NULL,
    confidenceIntervalUpper DECIMAL(18,2) NOT NULL,
    p50Forecast DECIMAL(18,2) NOT NULL,
    p90Forecast DECIMAL(18,2) NOT NULL,
    
    -- Model Information
    modelType NVARCHAR(50) NOT NULL, -- 'ARIMA', 'Prophet', 'Linear'
    modelAccuracy DECIMAL(5,2),
    forecastHorizonDays INT NOT NULL,
    
    -- Metadata
    forecastDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    rowCreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    -- Constraints
    CONSTRAINT FK_fact_forecast_daily_dateKey FOREIGN KEY (dateKey) REFERENCES dbo.dim_date(dateKey),
    CONSTRAINT FK_fact_forecast_daily_subscriptionKey FOREIGN KEY (subscriptionKey) REFERENCES dbo.dim_subscription(subscriptionKey)
);

-- Create indexes for performance
CREATE INDEX IX_fact_cost_daily_dateKey ON dbo.fact_cost_daily(dateKey);
CREATE INDEX IX_fact_cost_daily_resourceKey ON dbo.fact_cost_daily(resourceKey);
CREATE INDEX IX_fact_cost_daily_subscriptionKey ON dbo.fact_cost_daily(subscriptionKey);
CREATE INDEX IX_fact_cost_daily_meterKey ON dbo.fact_cost_daily(meterKey);
CREATE INDEX IX_fact_cost_daily_billingPeriod ON dbo.fact_cost_daily(billingPeriodStartDate, billingPeriodEndDate);

CREATE INDEX IX_fact_utilization_daily_dateKey ON dbo.fact_utilization_daily(dateKey);
CREATE INDEX IX_fact_utilization_daily_resourceKey ON dbo.fact_utilization_daily(resourceKey);

CREATE INDEX IX_fact_savings_opportunity_dateKey ON dbo.fact_savings_opportunity(dateKey);
CREATE INDEX IX_fact_savings_opportunity_resourceKey ON dbo.fact_savings_opportunity(resourceKey);
CREATE INDEX IX_fact_savings_opportunity_status ON dbo.fact_savings_opportunity(status);

CREATE INDEX IX_fact_anomaly_daily_dateKey ON dbo.fact_anomaly_daily(dateKey);
CREATE INDEX IX_fact_anomaly_daily_isAnomaly ON dbo.fact_anomaly_daily(isAnomaly);

CREATE INDEX IX_fact_forecast_daily_dateKey ON dbo.fact_forecast_daily(dateKey);
CREATE INDEX IX_fact_forecast_daily_subscriptionKey ON dbo.fact_forecast_daily(subscriptionKey);

-- Create Resource Dimension Indexes
CREATE INDEX IX_dim_resource_resourceId ON dbo.dim_resource(resourceId);
CREATE INDEX IX_dim_resource_subscriptionId ON dbo.dim_resource(subscriptionId);
CREATE INDEX IX_dim_resource_isCurrent ON dbo.dim_resource(isCurrent);
CREATE INDEX IX_dim_resource_effectiveDates ON dbo.dim_resource(effectiveStartDate, effectiveEndDate);

PRINT 'Star schema tables created successfully!';
