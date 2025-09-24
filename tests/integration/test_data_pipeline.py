"""
Integration tests for Azure Cost Analytics data pipeline
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

class TestDataPipelineIntegration(unittest.TestCase):
    """Integration tests for the complete data pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_subscription_id = "test-subscription-123"
        self.test_resource_group = "test-rg"
        self.test_storage_account = "teststorageaccount"
    
    def test_cost_data_ingestion_simulation(self):
        """Test simulated cost data ingestion pipeline"""
        # Simulate cost export data from Azure
        cost_export_data = self._create_mock_cost_export_data()
        
        # Verify data structure
        self.assertIn('Date', cost_export_data.columns)
        self.assertIn('BillingAccountId', cost_export_data.columns)
        self.assertIn('SubscriptionId', cost_export_data.columns)
        self.assertIn('ResourceId', cost_export_data.columns)
        self.assertIn('CostInBillingCurrency', cost_export_data.columns)
        self.assertIn('CostInUSD', cost_export_data.columns)
        
        # Verify data quality
        self.assertGreater(len(cost_export_data), 0)
        self.assertTrue(all(cost_export_data['CostInUSD'] >= 0))
        self.assertTrue(all(cost_export_data['CostInUSD'].notna()))
    
    def test_resource_graph_data_ingestion_simulation(self):
        """Test simulated Resource Graph data ingestion"""
        resource_data = self._create_mock_resource_graph_data()
        
        # Verify resource data structure
        self.assertIn('resourceId', resource_data.columns)
        self.assertIn('name', resource_data.columns)
        self.assertIn('type', resource_data.columns)
        self.assertIn('location', resource_data.columns)
        self.assertIn('subscriptionId', resource_data.columns)
        self.assertIn('tags', resource_data.columns)
        
        # Verify data quality
        self.assertGreater(len(resource_data), 0)
        self.assertTrue(all(resource_data['resourceId'].notna()))
    
    def test_advisor_recommendations_ingestion_simulation(self):
        """Test simulated Azure Advisor recommendations ingestion"""
        advisor_data = self._create_mock_advisor_data()
        
        # Verify advisor data structure
        self.assertIn('resourceId', advisor_data.columns)
        self.assertIn('category', advisor_data.columns)
        self.assertIn('impact', advisor_data.columns)
        self.assertIn('recommendationTypeId', advisor_data.columns)
        self.assertIn('potentialBenefits', advisor_data.columns)
        
        # Verify data quality
        self.assertGreater(len(advisor_data), 0)
        cost_recommendations = advisor_data[advisor_data['category'] == 'Cost']
        self.assertGreater(len(cost_recommendations), 0)
    
    def test_metrics_data_ingestion_simulation(self):
        """Test simulated Azure Monitor metrics ingestion"""
        metrics_data = self._create_mock_metrics_data()
        
        # Verify metrics data structure
        self.assertIn('resourceId', metrics_data.columns)
        self.assertIn('metricName', metrics_data.columns)
        self.assertIn('timeGrain', metrics_data.columns)
        self.assertIn('startTime', metrics_data.columns)
        self.assertIn('metricValue', metrics_data.columns)
        self.assertIn('unit', metrics_data.columns)
        
        # Verify data quality
        self.assertGreater(len(metrics_data), 0)
        cpu_metrics = metrics_data[metrics_data['metricName'] == 'Percentage CPU']
        self.assertGreater(len(cpu_metrics), 0)
    
    def test_bronze_to_silver_transformation(self):
        """Test bronze to silver data transformation"""
        # Create bronze layer data
        bronze_data = self._create_mock_bronze_data()
        
        # Simulate transformation to silver
        silver_data = self._transform_bronze_to_silver(bronze_data)
        
        # Verify transformation results
        self.assertIn('resourceKey', silver_data.columns)
        self.assertIn('subscriptionKey', silver_data.columns)
        self.assertIn('dateKey', silver_data.columns)
        self.assertIn('normalizedCost', silver_data.columns)
        
        # Verify data quality improvements
        self.assertTrue(all(silver_data['normalizedCost'] >= 0))
        self.assertTrue(all(silver_data['normalizedCost'].notna()))
    
    def test_silver_to_gold_transformation(self):
        """Test silver to gold data transformation"""
        # Create silver layer data
        silver_data = self._create_mock_silver_data()
        
        # Simulate transformation to gold (star schema)
        gold_data = self._transform_silver_to_gold(silver_data)
        
        # Verify star schema structure
        expected_tables = ['fact_cost_daily', 'dim_resource', 'dim_subscription', 'dim_date']
        for table_name in expected_tables:
            self.assertIn(table_name, gold_data.keys())
        
        # Verify fact table structure
        fact_table = gold_data['fact_cost_daily']
        self.assertIn('dateKey', fact_table.columns)
        self.assertIn('resourceKey', fact_table.columns)
        self.assertIn('actualCost', fact_table.columns)
        self.assertIn('amortizedCost', fact_table.columns)
    
    def test_kpi_calculation_pipeline(self):
        """Test KPI calculation pipeline"""
        # Create test data
        cost_data = self._create_mock_cost_data_for_kpis()
        
        # Calculate KPIs
        kpis = self._calculate_kpis(cost_data)
        
        # Verify KPI calculations
        self.assertIn('totalCost', kpis)
        self.assertIn('momGrowth', kpis)
        self.assertIn('yoyGrowth', kpis)
        self.assertIn('budgetUtilization', kpis)
        self.assertIn('reservationCoverage', kpis)
        
        # Verify KPI values are reasonable
        self.assertGreater(kpis['totalCost'], 0)
        self.assertIsInstance(kpis['momGrowth'], (int, float))
        self.assertIsInstance(kpis['yoyGrowth'], (int, float))
    
    def test_anomaly_detection_integration(self):
        """Test anomaly detection integration with pipeline"""
        # Create test cost data with known anomalies
        cost_data = self._create_mock_cost_data_with_anomalies()
        
        # Run anomaly detection
        anomalies = self._detect_anomalies_in_data(cost_data)
        
        # Verify anomaly detection results
        self.assertGreater(len(anomalies), 0)
        
        # Check that known anomalies were detected
        high_severity_anomalies = [a for a in anomalies if a.get('severity') == 'High']
        self.assertGreater(len(high_severity_anomalies), 0)
    
    def test_data_quality_validation(self):
        """Test data quality validation pipeline"""
        # Create test data with quality issues
        test_data = self._create_mock_data_with_quality_issues()
        
        # Run data quality validation
        quality_report = self._validate_data_quality(test_data)
        
        # Verify quality report structure
        self.assertIn('total_records', quality_report)
        self.assertIn('valid_records', quality_report)
        self.assertIn('invalid_records', quality_report)
        self.assertIn('quality_score', quality_report)
        self.assertIn('issues', quality_report)
        
        # Verify quality metrics
        self.assertGreater(quality_report['total_records'], 0)
        self.assertGreaterEqual(quality_report['quality_score'], 0)
        self.assertLessEqual(quality_report['quality_score'], 100)
    
    def _create_mock_cost_export_data(self):
        """Create mock cost export data"""
        dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
        data = []
        
        for date in dates:
            for i in range(10):  # 10 resources per day
                data.append({
                    'Date': date.strftime('%Y-%m-%d'),
                    'BillingAccountId': '12345678',
                    'SubscriptionId': self.test_subscription_id,
                    'ResourceId': f'/subscriptions/{self.test_subscription_id}/resourceGroups/{self.test_resource_group}/providers/Microsoft.Compute/virtualMachines/vm-{i}',
                    'Product': 'Virtual Machines',
                    'MeterId': f'meter-{i}',
                    'Quantity': np.random.uniform(1, 100),
                    'EffectivePrice': np.random.uniform(0.01, 1.0),
                    'CostInBillingCurrency': np.random.uniform(1, 1000),
                    'CostInUSD': np.random.uniform(1, 1000),
                    'BillingCurrency': 'USD',
                    'ChargeType': 'Usage',
                    'Frequency': 'Monthly',
                    'Location': 'East US',
                    'MeterName': 'Compute Hours',
                    'MeterCategory': 'Virtual Machines',
                    'MeterSubCategory': 'D Series',
                    'ServiceName': 'Microsoft.Compute',
                    'ServiceTier': 'Standard',
                    'ServiceFamily': 'Compute',
                    'Tags': '{"Environment":"Production","Team":"Platform"}'
                })
        
        return pd.DataFrame(data)
    
    def _create_mock_resource_graph_data(self):
        """Create mock Resource Graph data"""
        data = []
        for i in range(50):
            data.append({
                'resourceId': f'/subscriptions/{self.test_subscription_id}/resourceGroups/{self.test_resource_group}/providers/Microsoft.Compute/virtualMachines/vm-{i}',
                'name': f'vm-{i}',
                'type': 'Microsoft.Compute/virtualMachines',
                'location': np.random.choice(['East US', 'West US', 'Central US']),
                'subscriptionId': self.test_subscription_id,
                'resourceGroup': self.test_resource_group,
                'tags': {
                    'Environment': np.random.choice(['Production', 'Development', 'Test']),
                    'Team': np.random.choice(['Platform', 'Data', 'Security']),
                    'CostCenter': f'CC-{i}',
                    'Application': f'app-{i}'
                },
                'sku': {
                    'name': np.random.choice(['Standard_D2s_v3', 'Standard_D4s_v3', 'Standard_D8s_v3']),
                    'capacity': 1
                },
                'properties': {
                    'hardwareProfile': {
                        'vmSize': np.random.choice(['Standard_D2s_v3', 'Standard_D4s_v3', 'Standard_D8s_v3'])
                    }
                }
            })
        
        return pd.DataFrame(data)
    
    def _create_mock_advisor_data(self):
        """Create mock Azure Advisor recommendations data"""
        data = []
        recommendation_types = [
            'IdleVirtualMachines',
            'RightSizeVirtualMachines',
            'ReservedInstances',
            'UnattachedDisks',
            'PremiumStorage'
        ]
        
        for i in range(20):
            data.append({
                'resourceId': f'/subscriptions/{self.test_subscription_id}/resourceGroups/{self.test_resource_group}/providers/Microsoft.Compute/virtualMachines/vm-{i}',
                'category': 'Cost',
                'impact': np.random.choice(['High', 'Medium', 'Low']),
                'recommendationTypeId': np.random.choice(recommendation_types),
                'shortDescription': f'Optimize resource {i} for cost savings',
                'longDescription': f'This recommendation can help optimize the cost of resource {i}',
                'potentialBenefits': f'Save up to ${np.random.uniform(100, 1000):.2f} per month',
                'actions': [
                    {
                        'action': 'Resize',
                        'description': 'Resize to a smaller SKU'
                    }
                ],
                'createdTime': datetime.now().isoformat(),
                'status': 'Active'
            })
        
        return pd.DataFrame(data)
    
    def _create_mock_metrics_data(self):
        """Create mock Azure Monitor metrics data"""
        data = []
        metrics = ['Percentage CPU', 'Available Memory Bytes', 'Disk Read Bytes', 'Disk Write Bytes', 'Network In Bytes', 'Network Out Bytes']
        
        for i in range(100):
            data.append({
                'resourceId': f'/subscriptions/{self.test_subscription_id}/resourceGroups/{self.test_resource_group}/providers/Microsoft.Compute/virtualMachines/vm-{i//10}',
                'metricName': np.random.choice(metrics),
                'metricNamespace': 'Microsoft.Compute/virtualMachines',
                'timeGrain': np.random.choice(['PT1M', 'PT5M', 'PT15M', 'PT1H']),
                'startTime': (datetime.now() - timedelta(days=np.random.randint(0, 30))).isoformat(),
                'endTime': (datetime.now() - timedelta(days=np.random.randint(0, 30))).isoformat(),
                'metricValue': np.random.uniform(0, 100),
                'unit': np.random.choice(['Percent', 'Bytes', 'Count']),
                'dimensions': '{"TotalMemory":"8589934592"}',
                'subscriptionId': self.test_subscription_id,
                'resourceGroup': self.test_resource_group
            })
        
        return pd.DataFrame(data)
    
    def _create_mock_bronze_data(self):
        """Create mock bronze layer data"""
        return pd.DataFrame({
            'rawData': ['cost_export_2024_01_01.csv', 'resource_graph_2024_01_01.json'],
            'filePath': ['/raw/cost/2024/01/01/cost_export.csv', '/raw/resources/2024/01/01/resource_graph.json'],
            'processedDate': [datetime.now().isoformat()] * 2,
            'fileSize': [1024000, 2048000],
            'recordCount': [1000, 500]
        })
    
    def _transform_bronze_to_silver(self, bronze_data):
        """Simulate bronze to silver transformation"""
        return pd.DataFrame({
            'resourceKey': range(1, 101),
            'subscriptionKey': [1] * 100,
            'dateKey': range(1, 101),
            'normalizedCost': np.random.uniform(1, 1000, 100),
            'cleanedResourceId': [f'/subscriptions/{self.test_subscription_id}/resourceGroups/{self.test_resource_group}/providers/Microsoft.Compute/virtualMachines/vm-{i}' for i in range(100)],
            'transformedDate': [datetime.now().isoformat()] * 100
        })
    
    def _create_mock_silver_data(self):
        """Create mock silver layer data"""
        return pd.DataFrame({
            'resourceKey': range(1, 101),
            'subscriptionKey': [1] * 100,
            'dateKey': range(1, 101),
            'actualCost': np.random.uniform(1, 1000, 100),
            'amortizedCost': np.random.uniform(1, 1000, 100),
            'usageQuantity': np.random.uniform(1, 100, 100),
            'effectivePrice': np.random.uniform(0.01, 10, 100)
        })
    
    def _transform_silver_to_gold(self, silver_data):
        """Simulate silver to gold transformation"""
        return {
            'fact_cost_daily': silver_data,
            'dim_resource': pd.DataFrame({
                'resourceKey': range(1, 101),
                'resourceId': [f'resource-{i}' for i in range(1, 101)],
                'resourceName': [f'Resource {i}' for i in range(1, 101)],
                'resourceType': ['Microsoft.Compute/virtualMachines'] * 100,
                'businessUnit': ['Platform'] * 100,
                'team': ['Data'] * 100,
                'environment': ['Production'] * 100
            }),
            'dim_subscription': pd.DataFrame({
                'subscriptionKey': [1],
                'subscriptionId': [self.test_subscription_id],
                'subscriptionName': ['Test Subscription'],
                'tenantId': ['test-tenant-id'],
                'currency': ['USD']
            }),
            'dim_date': pd.DataFrame({
                'dateKey': range(1, 101),
                'date': pd.date_range(start='2024-01-01', periods=100, freq='D'),
                'year': [2024] * 100,
                'month': [1] * 100,
                'day': list(range(1, 101)),
                'quarter': [1] * 100
            })
        }
    
    def _create_mock_cost_data_for_kpis(self):
        """Create mock cost data for KPI calculation"""
        return pd.DataFrame({
            'date': pd.date_range(start='2023-01-01', end='2024-01-31', freq='D'),
            'actualCost': np.random.uniform(1000, 5000, 396),
            'budgetAmount': [5000] * 396,
            'reservationCost': np.random.uniform(100, 1000, 396),
            'subscriptionId': [self.test_subscription_id] * 396
        })
    
    def _calculate_kpis(self, cost_data):
        """Calculate KPIs from cost data"""
        total_cost = cost_data['actualCost'].sum()
        
        # Calculate month-over-month growth
        current_month = cost_data[cost_data['date'] >= '2024-01-01']['actualCost'].sum()
        previous_month = cost_data[(cost_data['date'] >= '2023-12-01') & (cost_data['date'] < '2024-01-01')]['actualCost'].sum()
        mom_growth = ((current_month - previous_month) / previous_month * 100) if previous_month > 0 else 0
        
        # Calculate year-over-year growth
        current_year = cost_data[cost_data['date'] >= '2024-01-01']['actualCost'].sum()
        previous_year = cost_data[(cost_data['date'] >= '2023-01-01') & (cost_data['date'] < '2024-01-01')]['actualCost'].sum()
        yoy_growth = ((current_year - previous_year) / previous_year * 100) if previous_year > 0 else 0
        
        # Calculate budget utilization
        budget_utilization = (current_month / cost_data['budgetAmount'].iloc[0] * 100) if cost_data['budgetAmount'].iloc[0] > 0 else 0
        
        # Calculate reservation coverage
        reservation_coverage = (cost_data['reservationCost'].sum() / total_cost * 100) if total_cost > 0 else 0
        
        return {
            'totalCost': total_cost,
            'momGrowth': mom_growth,
            'yoyGrowth': yoy_growth,
            'budgetUtilization': budget_utilization,
            'reservationCoverage': reservation_coverage
        }
    
    def _create_mock_cost_data_with_anomalies(self):
        """Create mock cost data with known anomalies"""
        dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
        data = []
        
        for i, date in enumerate(dates):
            base_cost = 1000 + np.random.normal(0, 100)
            
            # Add known anomalies
            if i == 15:  # Major spike
                base_cost *= 4
            elif i == 20:  # Moderate spike
                base_cost *= 2
            elif i == 25:  # Drop
                base_cost *= 0.3
            
            data.append({
                'date': date,
                'cost': max(0, base_cost),
                'resourceId': f'resource-{i}',
                'subscriptionId': self.test_subscription_id
            })
        
        return pd.DataFrame(data)
    
    def _detect_anomalies_in_data(self, cost_data):
        """Detect anomalies in cost data"""
        anomalies = []
        
        # Simple anomaly detection based on z-score
        costs = cost_data['cost'].values
        mean_cost = np.mean(costs[:-1])  # Exclude last day
        std_cost = np.std(costs[:-1])
        
        if std_cost > 0:
            for i, row in cost_data.iterrows():
                z_score = (row['cost'] - mean_cost) / std_cost
                
                if abs(z_score) >= 2.0 and row['cost'] >= 10:
                    severity = 'High' if abs(z_score) >= 3.0 else 'Medium' if abs(z_score) >= 2.0 else 'Low'
                    anomalies.append({
                        'resourceId': row['resourceId'],
                        'date': row['date'],
                        'cost': row['cost'],
                        'z_score': z_score,
                        'severity': severity,
                        'anomaly_type': 'Spike' if z_score > 0 else 'Drop'
                    })
        
        return anomalies
    
    def _create_mock_data_with_quality_issues(self):
        """Create mock data with quality issues"""
        return pd.DataFrame({
            'resourceId': ['resource-1', 'resource-2', None, 'resource-4', 'resource-5'],
            'cost': [100, -50, 200, 300, None],  # Negative cost and null values
            'date': ['2024-01-01', 'invalid-date', '2024-01-03', '2024-01-04', '2024-01-05'],
            'subscriptionId': ['sub-1', 'sub-2', 'sub-3', 'sub-4', 'sub-5']
        })
    
    def _validate_data_quality(self, data):
        """Validate data quality"""
        total_records = len(data)
        issues = []
        
        # Check for null values
        null_counts = data.isnull().sum()
        for column, null_count in null_counts.items():
            if null_count > 0:
                issues.append(f'Column {column} has {null_count} null values')
        
        # Check for negative costs
        if 'cost' in data.columns:
            negative_costs = (data['cost'] < 0).sum()
            if negative_costs > 0:
                issues.append(f'Found {negative_costs} records with negative costs')
        
        # Check for invalid dates
        if 'date' in data.columns:
            try:
                pd.to_datetime(data['date'], errors='coerce')
                invalid_dates = pd.to_datetime(data['date'], errors='coerce').isnull().sum()
                if invalid_dates > 0:
                    issues.append(f'Found {invalid_dates} records with invalid dates')
            except:
                issues.append('Unable to validate date format')
        
        valid_records = total_records - len([issue for issue in issues if 'null values' in issue or 'negative costs' in issue or 'invalid dates' in issue])
        quality_score = (valid_records / total_records * 100) if total_records > 0 else 0
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': total_records - valid_records,
            'quality_score': quality_score,
            'issues': issues
        }

if __name__ == '__main__':
    # Run integration tests
    unittest.main(verbosity=2)
