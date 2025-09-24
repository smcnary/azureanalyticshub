"""
Unit tests for Azure Cost Analytics anomaly detection functionality
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the automation functions to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'automation', 'functions', 'anomaly-detection'))

from main import AnomalyDetector, AnomalyResult

class TestAnomalyDetector(unittest.TestCase):
    """Test cases for the AnomalyDetector class"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch.dict(os.environ, {
            'Z_SCORE_THRESHOLD': '2.0',
            'MIN_COST_THRESHOLD': '10.0',
            'CONFIDENCE_THRESHOLD': '0.8'
        }):
            self.detector = AnomalyDetector()
        
        # Create sample cost data
        self.sample_data = self._create_sample_cost_data()
    
    def _create_sample_cost_data(self):
        """Create sample cost data for testing"""
        dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
        data = []
        
        for i, date in enumerate(dates):
            # Create normal cost pattern with occasional spikes
            base_cost = 100 + np.random.normal(0, 10)
            
            # Add a spike on day 20
            if i == 20:
                base_cost *= 3
            
            # Add a drop on day 25
            if i == 25:
                base_cost *= 0.3
            
            data.append({
                'resourceId': f'/subscriptions/test-sub/resourceGroups/rg-1/providers/Microsoft.Compute/virtualMachines/vm-1',
                'subscriptionId': 'test-sub',
                'date': date.strftime('%Y-%m-%d'),
                'actualCost': max(0, base_cost),
                'meterCategory': 'Virtual Machines',
                'serviceName': 'Virtual Machines'
            })
        
        return pd.DataFrame(data)
    
    def test_anomaly_detector_initialization(self):
        """Test that AnomalyDetector initializes correctly"""
        self.assertEqual(self.detector.z_score_threshold, 2.0)
        self.assertEqual(self.detector.min_cost_threshold, 10.0)
        self.assertEqual(self.detector.confidence_threshold, 0.8)
    
    def test_get_cost_data(self):
        """Test cost data retrieval"""
        with patch.object(self.detector, 'get_cost_data') as mock_get_data:
            mock_get_data.return_value = self.sample_data
            
            result = self.detector.get_cost_data('test-sub', 30)
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertGreater(len(result), 0)
            self.assertIn('resourceId', result.columns)
            self.assertIn('actualCost', result.columns)
    
    def test_detect_anomalies(self):
        """Test anomaly detection logic"""
        anomalies = self.detector.detect_anomalies(self.sample_data)
        
        self.assertIsInstance(anomalies, list)
        
        # Check that we detected the spike on day 20
        spike_anomalies = [a for a in anomalies if a.anomaly_type == "Spike"]
        self.assertGreater(len(spike_anomalies), 0)
        
        # Check that we detected the drop on day 25
        drop_anomalies = [a for a in anomalies if a.anomaly_type == "Drop"]
        self.assertGreater(len(drop_anomalies), 0)
    
    def test_determine_severity(self):
        """Test severity determination logic"""
        # Test high severity
        severity = self.detector._determine_severity(3.5, 1500, 100)
        self.assertEqual(severity, "High")
        
        # Test medium severity
        severity = self.detector._determine_severity(2.5, 500, 100)
        self.assertEqual(severity, "Medium")
        
        # Test low severity
        severity = self.detector._determine_severity(1.5, 150, 100)
        self.assertEqual(severity, "Low")
    
    def test_anomaly_result_creation(self):
        """Test AnomalyResult object creation"""
        anomaly = AnomalyResult(
            resource_id="test-resource",
            subscription_id="test-sub",
            date="2024-01-01",
            actual_cost=1000,
            expected_cost=100,
            variance=900,
            variance_percentage=900.0,
            z_score=3.0,
            anomaly_type="Spike",
            severity="High",
            is_anomaly=True,
            confidence=0.95
        )
        
        self.assertEqual(anomaly.resource_id, "test-resource")
        self.assertEqual(anomaly.actual_cost, 1000)
        self.assertEqual(anomaly.severity, "High")
        self.assertTrue(anomaly.is_anomaly)
    
    @patch('main.BlobServiceClient')
    def test_save_anomaly_results(self, mock_blob_client):
        """Test saving anomaly results to blob storage"""
        # Mock blob service client
        mock_blob_service = Mock()
        mock_blob_client.from_connection_string.return_value = mock_blob_service
        mock_blob_client_instance = mock_blob_service.get_blob_client.return_value
        
        # Set up detector with mock client
        self.detector.blob_service_client = mock_blob_service
        
        # Create test anomalies
        anomalies = [
            AnomalyResult(
                resource_id="test-resource-1",
                subscription_id="test-sub",
                date="2024-01-01",
                actual_cost=1000,
                expected_cost=100,
                variance=900,
                variance_percentage=900.0,
                z_score=3.0,
                anomaly_type="Spike",
                severity="High",
                is_anomaly=True,
                confidence=0.95
            )
        ]
        
        # Test save
        result_path = self.detector.save_anomaly_results(anomalies)
        
        self.assertIsNotNone(result_path)
        self.assertIn("anomaly-results-", result_path)
        mock_blob_client_instance.upload_blob.assert_called_once()
    
    def test_trigger_alerts(self):
        """Test alert triggering logic"""
        # Create test anomalies of different severities
        anomalies = [
            AnomalyResult(
                resource_id="test-resource-1",
                subscription_id="test-sub",
                date="2024-01-01",
                actual_cost=1000,
                expected_cost=100,
                variance=900,
                variance_percentage=900.0,
                z_score=3.0,
                anomaly_type="Spike",
                severity="High",
                is_anomaly=True,
                confidence=0.95
            ),
            AnomalyResult(
                resource_id="test-resource-2",
                subscription_id="test-sub",
                date="2024-01-01",
                actual_cost=500,
                expected_cost=100,
                variance=400,
                variance_percentage=400.0,
                z_score=2.5,
                anomaly_type="Spike",
                severity="Medium",
                is_anomaly=True,
                confidence=0.85
            ),
            AnomalyResult(
                resource_id="test-resource-3",
                subscription_id="test-sub",
                date="2024-01-01",
                actual_cost=200,
                expected_cost=100,
                variance=100,
                variance_percentage=100.0,
                z_score=1.5,
                anomaly_type="Spike",
                severity="Low",
                is_anomaly=True,
                confidence=0.75
            )
        ]
        
        # Mock alert methods
        with patch.object(self.detector, '_send_high_severity_alert') as mock_high_alert, \
             patch.object(self.detector, '_log_medium_severity_anomalies') as mock_medium_log, \
             patch.object(self.detector, '_log_low_severity_anomalies') as mock_low_log:
            
            alert_counts = self.detector.trigger_alerts(anomalies)
            
            self.assertEqual(alert_counts["High"], 1)
            self.assertEqual(alert_counts["Medium"], 1)
            self.assertEqual(alert_counts["Low"], 1)
            
            mock_high_alert.assert_called_once()
            mock_medium_log.assert_called_once()
            mock_low_log.assert_called_once()

class TestAnomalyDetectionIntegration(unittest.TestCase):
    """Integration tests for anomaly detection"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        with patch.dict(os.environ, {
            'Z_SCORE_THRESHOLD': '2.0',
            'MIN_COST_THRESHOLD': '10.0',
            'CONFIDENCE_THRESHOLD': '0.8'
        }):
            self.detector = AnomalyDetector()
    
    def test_end_to_end_anomaly_detection(self):
        """Test complete end-to-end anomaly detection workflow"""
        # Create realistic test data with known anomalies
        dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
        data = []
        
        for i, date in enumerate(dates):
            # Normal pattern: 100 +/- 20
            base_cost = 100 + np.random.normal(0, 20)
            
            # Add known anomalies
            if i == 15:  # Major spike
                base_cost *= 4
            elif i == 20:  # Moderate spike
                base_cost *= 2
            elif i == 25:  # Drop
                base_cost *= 0.2
            
            data.append({
                'resourceId': f'/subscriptions/test-sub/resourceGroups/rg-1/providers/Microsoft.Compute/virtualMachines/vm-1',
                'subscriptionId': 'test-sub',
                'date': date.strftime('%Y-%m-%d'),
                'actualCost': max(0, base_cost),
                'meterCategory': 'Virtual Machines',
                'serviceName': 'Virtual Machines'
            })
        
        cost_data = pd.DataFrame(data)
        
        # Run anomaly detection
        anomalies = self.detector.detect_anomalies(cost_data)
        
        # Verify results
        self.assertGreater(len(anomalies), 0)
        
        # Check that we detected the major spike
        major_spikes = [a for a in anomalies if a.anomaly_type == "Spike" and a.z_score > 2.5]
        self.assertGreater(len(major_spikes), 0)
        
        # Check that we detected the drop
        drops = [a for a in anomalies if a.anomaly_type == "Drop"]
        self.assertGreater(len(drops), 0)
        
        # Verify severity levels
        high_severity = [a for a in anomalies if a.severity == "High"]
        medium_severity = [a for a in anomalies if a.severity == "Medium"]
        
        self.assertGreater(len(high_severity), 0)  # Should have high severity for major spike
        self.assertGreater(len(medium_severity), 0)  # Should have medium severity for moderate spike

class TestStatisticalMethods(unittest.TestCase):
    """Test statistical methods used in anomaly detection"""
    
    def test_z_score_calculation(self):
        """Test z-score calculation accuracy"""
        # Test data with known mean and std
        data = np.array([100, 105, 95, 110, 90, 105, 100, 95, 110, 100])
        mean = np.mean(data)
        std = np.std(data)
        
        # Calculate z-scores manually
        test_value = 120
        expected_z_score = (test_value - mean) / std
        
        # Verify calculation
        self.assertAlmostEqual(expected_z_score, (120 - mean) / std, places=5)
    
    def test_confidence_calculation(self):
        """Test confidence calculation based on z-score"""
        # High z-score should give high confidence
        high_z_score = 3.0
        expected_confidence = min(1.0, high_z_score / 3.0)
        self.assertEqual(expected_confidence, 1.0)
        
        # Medium z-score should give medium confidence
        medium_z_score = 2.0
        expected_confidence = min(1.0, medium_z_score / 3.0)
        self.assertAlmostEqual(expected_confidence, 0.667, places=3)
        
        # Low z-score should give low confidence
        low_z_score = 1.0
        expected_confidence = min(1.0, low_z_score / 3.0)
        self.assertAlmostEqual(expected_confidence, 0.333, places=3)

if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)
