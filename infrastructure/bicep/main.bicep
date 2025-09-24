targetScope = 'subscription'

// Parameters
@description('The location for all resources')
param location string = resourceGroup().location

@description('Environment name (dev, test, prod)')
param environment string = 'prod'

@description('Project name prefix')
param projectName string = 'azurecostanalytics'

@description('Billing account ID for cost exports')
param billingAccountId string

@description('Storage account SKU')
@allowed([
  'Standard_LRS'
  'Standard_GRS'
  'Standard_RAGRS'
  'Standard_ZRS'
  'Premium_LRS'
  'Premium_ZRS'
])
param storageAccountSku string = 'Standard_GRS'

@description('Enable private endpoints')
param enablePrivateEndpoints bool = true

@description('Tags to apply to all resources')
param tags object = {
  Environment: environment
  Project: 'AzureCostAnalytics'
  ManagedBy: 'Bicep'
}

// Variables
var resourcePrefix = '${projectName}-${environment}'
var storageAccountName = '${replace(resourcePrefix, '-', '')}stg'
var dataFactoryName = '${resourcePrefix}-adf'
var synapseWorkspaceName = '${resourcePrefix}-synapse'
var keyVaultName = '${resourcePrefix}-kv'
var logAnalyticsName = '${resourcePrefix}-logs'
var applicationInsightsName = '${resourcePrefix}-ai'

// Resource Group
resource resourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: resourcePrefix
  location: location
  tags: tags
}

// Storage Account (ADLS Gen2)
resource storageAccount 'Microsoft.Storage/storageAccounts@2021-09-01' = {
  name: storageAccountName
  location: location
  resourceGroup: resourceGroup.name
  kind: 'StorageV2'
  sku: {
    name: storageAccountSku
  }
  properties: {
    isHnsEnabled: true
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: enablePrivateEndpoints ? 'Deny' : 'Allow'
      bypass: 'AzureServices'
    }
  }
  tags: tags
}

// Storage Account Containers
resource costExportsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-09-01' = {
  parent: storageAccount::storageAccount::blobServices
  name: 'cost-exports'
  properties: {
    publicAccess: 'None'
  }
}

resource rawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-09-01' = {
  parent: storageAccount::storageAccount::blobServices
  name: 'raw'
  properties: {
    publicAccess: 'None'
  }
}

resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-09-01' = {
  parent: storageAccount::storageAccount::blobServices
  name: 'silver'
  properties: {
    publicAccess: 'None'
  }
}

resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-09-01' = {
  parent: storageAccount::storageAccount::blobServices
  name: 'gold'
  properties: {
    publicAccess: 'None'
  }
}

// Key Vault
resource keyVault 'Microsoft.KeyVault/vaults@2021-10-01' = {
  name: keyVaultName
  location: location
  resourceGroup: resourceGroup.name
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    networkAcls: {
      defaultAction: enablePrivateEndpoints ? 'Deny' : 'Allow'
      bypass: 'AzureServices'
    }
  }
  tags: tags
}

// Log Analytics Workspace
resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2021-12-01-preview' = {
  name: logAnalyticsName
  location: location
  resourceGroup: resourceGroup.name
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 90
  }
  tags: tags
}

// Application Insights
resource applicationInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: applicationInsightsName
  location: location
  resourceGroup: resourceGroup.name
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalyticsWorkspace.id
  }
  tags: tags
}

// Synapse Workspace
resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: synapseWorkspaceName
  location: location
  resourceGroup: resourceGroup.name
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountName: storageAccount.name
      filesystem: 'synapse'
    }
    sqlAdministratorLogin: 'sqladmin'
    managedVirtualNetwork: 'default'
    publicNetworkAccess: enablePrivateEndpoints ? 'Disabled' : 'Enabled'
  }
  tags: tags
}

// Synapse SQL Pool (Serverless)
resource synapseSqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  parent: synapseWorkspace
  name: 'serverless'
  location: location
  sku: {
    name: 'DataWarehouse'
    tier: 'DataWarehouse'
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    createMode: 'Default'
  }
}

// Data Factory
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  resourceGroup: resourceGroup.name
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: enablePrivateEndpoints ? 'Disabled' : 'Enabled'
  }
  tags: tags
}

// Service Principal for Data Factory
resource dataFactorySp 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: guid(dataFactory.id, 'StorageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Storage Blob Data Contributor
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// RBAC for Synapse
resource synapseSpRole 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: guid(synapseWorkspace.id, 'StorageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Storage Blob Data Contributor
    principalId: synapseWorkspace.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Private Endpoints (if enabled)
resource storagePrivateEndpoint 'Microsoft.Network/privateEndpoints@2021-05-01' = if (enablePrivateEndpoints) {
  name: '${storageAccount.name}-pe'
  location: location
  resourceGroup: resourceGroup.name
  properties: {
    subnet: {
      id: subscriptionResourceId('Microsoft.Network/virtualNetworks/subnets', 'vnet-${environment}', 'default')
    }
    privateLinkServiceConnections: [
      {
        name: 'storage-connection'
        properties: {
          privateLinkServiceId: storageAccount.id
          groupIds: ['blob']
        }
      }
    ]
  }
  tags: tags
}

// Cost Management Export
resource costExport 'Microsoft.CostManagement/exports@2021-10-01' = {
  name: 'daily-cost-export'
  scope: '/providers/Microsoft.Billing/billingAccounts/${billingAccountId}'
  properties: {
    format: 'Csv'
    deliveryInfo: {
      destination: {
        resourceId: storageAccount.id
        container: costExportsContainer.name
        rootFolderPath: 'cost-exports'
      }
    }
    definition: {
      type: 'ActualCost'
      timeframe: 'MonthToDate'
      dataSet: {
        granularity: 'Daily'
        configuration: {
          columns: [
            'Date'
            'BillingAccountId'
            'BillingAccountName'
            'BillingPeriodStartDate'
            'BillingPeriodEndDate'
            'AccountOwnerId'
            'AccountName'
            'SubscriptionId'
            'SubscriptionName'
            'Date'
            'Product'
            'PartNumber'
            'MeterId'
            'Quantity'
            'EffectivePrice'
            'CostInBillingCurrency'
            'CostInPricingCurrency'
            'BillingCurrency'
            'PricingCurrency'
            'ChargeType'
            'Frequency'
            'InvoiceSection'
            'CostCenter'
            'UnitOfMeasure'
            'Location'
            'MeterName'
            'MeterCategory'
            'MeterSubCategory'
            'MeterRegion'
            'SubscriptionGuid'
            'OfferId'
            'IsAzureCreditEligible'
            'ServiceName'
            'ServiceTier'
            'ServiceFamily'
            'UnitPrice'
            'BillingPeriodStartDate'
            'BillingPeriodEndDate'
            'CostInUSD'
            'ExchangeRate'
            'ExchangeRateDate'
            'InvoiceId'
            'PreviousInvoiceId'
            'PricingModel'
            'ServiceInfo1'
            'ServiceInfo2'
            'AdditionalInfo'
            'Tags'
            'ReservationId'
            'ReservationName'
            'PricingQuantity'
            'UnitOfMeasure'
            'AvailabilityZone'
            'BillingAccountId'
            'BillingAccountName'
            'BillingProfileId'
            'BillingProfileName'
            'AccountOwnerId'
            'AccountName'
            'SubscriptionId'
            'SubscriptionName'
            'Date'
            'Product'
            'PartNumber'
            'MeterId'
            'Quantity'
            'EffectivePrice'
            'CostInBillingCurrency'
            'CostInPricingCurrency'
            'BillingCurrency'
            'PricingCurrency'
            'ChargeType'
            'Frequency'
            'InvoiceSection'
            'CostCenter'
            'UnitOfMeasure'
            'Location'
            'MeterName'
            'MeterCategory'
            'MeterSubCategory'
            'MeterRegion'
            'SubscriptionGuid'
            'OfferId'
            'IsAzureCreditEligible'
            'ServiceName'
            'ServiceTier'
            'ServiceFamily'
            'UnitPrice'
            'BillingPeriodStartDate'
            'BillingPeriodEndDate'
            'CostInUSD'
            'ExchangeRate'
            'ExchangeRateDate'
            'InvoiceId'
            'PreviousInvoiceId'
            'PricingModel'
            'ServiceInfo1'
            'ServiceInfo2'
            'AdditionalInfo'
            'Tags'
            'ReservationId'
            'ReservationName'
            'PricingQuantity'
            'UnitOfMeasure'
            'AvailabilityZone'
          ]
        }
      }
    }
    schedule: {
      status: 'Active'
      recurrence: 'Daily'
      recurrencePeriod: {
        from: '2024-01-01T00:00:00Z'
        to: '2024-12-31T23:59:59Z'
      }
    }
  }
}

// Outputs
output resourceGroupName string = resourceGroup.name
output storageAccountName string = storageAccount.name
output dataFactoryName string = dataFactory.name
output synapseWorkspaceName string = synapseWorkspace.name
output keyVaultName string = keyVault.name
output logAnalyticsWorkspaceId string = logAnalyticsWorkspace.id
output applicationInsightsId string = applicationInsights.id
output synapseSqlEndpoint string = synapseWorkspace.properties.connectivityEndpoints.sql
output storageAccountId string = storageAccount.id
