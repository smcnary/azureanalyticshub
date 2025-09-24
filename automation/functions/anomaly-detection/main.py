"""
Azure Cost Analytics - Anomaly Detection Function
This function detects cost anomalies and triggers appropriate alerts and actions.
"""

import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from azure.monitor.query import MetricsQueryClient
from azure.monitor.query.models import MetricsQueryOptions
import pandas as pd
import numpy as np
from scipy import stats
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AnomalyResult:
    """Data class for anomaly detection results"""
    resource_id: str
    subscription_id: str
    date: str
    actual_cost: float
    expected_cost: float
    variance: float
    variance_percentage: float
    z_score: float
    anomaly_type: str
    severity: str
    is_anomaly: bool
    confidence: float

class AnomalyDetector:
    """Main class for anomaly detection logic"""
    
    def __init__(self):
        """Initialize the anomaly detector with Azure credentials and configuration"""
        self.credential = DefaultAzureCredential()
        self.key_vault_url = os.environ.get("KEY_VAULT_URL")
        self.storage_connection_string = os.environ.get("STORAGE_CONNECTION_STRING")
        self.container_name = os.environ.get("CONTAINER_NAME", "anomalies")
        
        # Thresholds for anomaly detection
        self.z_score_threshold = float(os.environ.get("Z_SCORE_THRESHOLD", "2.0"))
        self.min_cost_threshold = float(os.environ.get("MIN_COST_THRESHOLD", "10.0"))
        self.confidence_threshold = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.8"))
        
        # Initialize clients
        if self.key_vault_url:
            self.secret_client = SecretClient(
                vault_url=self.key_vault_url, 
                credential=self.credential
            )
        
        if self.storage_connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                self.storage_connection_string
            )
    
    def get_cost_data(self, subscription_id: str, days_back: int = 30) -> pd.DataFrame:
        """
        Retrieve cost data for anomaly detection
        
        Args:
            subscription_id: Azure subscription ID
            days_back: Number of days to look back for historical data
            
        Returns:
            DataFrame with cost data
        """
        try:
            # This would typically query from your data lake or Synapse
            # For now, we'll simulate with sample data
            logger.info(f"Retrieving cost data for subscription {subscription_id}")
            
            # In a real implementation, this would query your Synapse/Data Lake
            # Example query:
            # query = """
            # SELECT 
            #     resourceId,
            #     subscriptionId,
            #     date,
            #     actualCost,
            #     meterCategory,
            #     serviceName
            # FROM fact_cost_daily fcd
            # JOIN dim_resource dr ON fcd.resourceKey = dr.resourceKey
            # WHERE fcd.subscriptionKey = (
            #     SELECT subscriptionKey FROM dim_subscription 
            #     WHERE subscriptionId = '{subscription_id}'
            # )
            # AND fcd.dateKey >= (
            #     SELECT dateKey FROM dim_date 
            #     WHERE date >= DATEADD(day, -{days_back}, GETDATE())
            # )
            # """.format(subscription_id=subscription_id, days_back=days_back)
            
            # For demo purposes, create sample data
            dates = pd.date_range(end=datetime.now(), periods=days_back, freq='D')
            sample_data = []
            
            for date in dates:
                # Simulate some cost data with occasional spikes
                base_cost = 100 + np.random.normal(0, 20)
                if date.day == 15:  # Simulate a monthly spike
                    base_cost *= 2.5
                elif np.random.random() < 0.05:  # 5% chance of anomaly
                    base_cost *= np.random.uniform(3, 8)
                
                sample_data.append({
                    'resourceId': f'/subscriptions/{subscription_id}/resourceGroups/rg-{np.random.randint(1,5)}/providers/Microsoft.Compute/virtualMachines/vm-{np.random.randint(1,20)}',
                    'subscriptionId': subscription_id,
                    'date': date.strftime('%Y-%m-%d'),
                    'actualCost': max(0, base_cost),
                    'meterCategory': np.random.choice(['Virtual Machines', 'Storage', 'Networking']),
                    'serviceName': np.random.choice(['Virtual Machines', 'Blob Storage', 'Load Balancer'])
                })
            
            return pd.DataFrame(sample_data)
            
        except Exception as e:
            logger.error(f"Error retrieving cost data: {str(e)}")
            raise
    
    def detect_anomalies(self, cost_data: pd.DataFrame) -> List[AnomalyResult]:
        """
        Detect anomalies in cost data using statistical methods
        
        Args:
            cost_data: DataFrame with cost data
            
        Returns:
            List of AnomalyResult objects
        """
        anomalies = []
        
        try:
            # Group by resource and date for analysis
            daily_costs = cost_data.groupby(['resourceId', 'date'])['actualCost'].sum().reset_index()
            
            for resource_id in daily_costs['resourceId'].unique():
                resource_data = daily_costs[daily_costs['resourceId'] == resource_id].copy()
                resource_data = resource_data.sort_values('date')
                
                if len(resource_data) < 7:  # Need at least a week of data
                    continue
                
                costs = resource_data['actualCost'].values
                dates = resource_data['date'].values
                subscription_id = cost_data[cost_data['resourceId'] == resource_id]['subscriptionId'].iloc[0]
                
                # Calculate statistical measures
                mean_cost = np.mean(costs[:-1])  # Exclude today for prediction
                std_cost = np.std(costs[:-1])
                
                if std_cost == 0:
                    continue
                
                # Detect anomalies for each day
                for i, (date, cost) in enumerate(zip(dates, costs)):
                    z_score = (cost - mean_cost) / std_cost
                    
                    # Determine if this is an anomaly
                    is_anomaly = (
                        abs(z_score) >= self.z_score_threshold and 
                        cost >= self.min_cost_threshold
                    )
                    
                    if is_anomaly:
                        # Calculate confidence based on z-score magnitude
                        confidence = min(1.0, abs(z_score) / 3.0)
                        
                        # Determine anomaly type and severity
                        anomaly_type = "Spike" if cost > mean_cost else "Drop"
                        severity = self._determine_severity(z_score, cost, mean_cost)
                        
                        anomaly = AnomalyResult(
                            resource_id=resource_id,
                            subscription_id=subscription_id,
                            date=date,
                            actual_cost=cost,
                            expected_cost=mean_cost,
                            variance=cost - mean_cost,
                            variance_percentage=((cost - mean_cost) / mean_cost * 100) if mean_cost > 0 else 0,
                            z_score=z_score,
                            anomaly_type=anomaly_type,
                            severity=severity,
                            is_anomaly=True,
                            confidence=confidence
                        )
                        anomalies.append(anomaly)
                        
                        logger.info(f"Anomaly detected: {resource_id} on {date}, z-score: {z_score:.2f}, severity: {severity}")
                
                # Update mean and std for next iteration (sliding window)
                mean_cost = np.mean(costs[-7:])  # Last 7 days
                std_cost = np.std(costs[-7:])
            
            logger.info(f"Detected {len(anomalies)} anomalies across {len(daily_costs['resourceId'].unique())} resources")
            
        except Exception as e:
            logger.error(f"Error in anomaly detection: {str(e)}")
            raise
        
        return anomalies
    
    def _determine_severity(self, z_score: float, actual_cost: float, expected_cost: float) -> str:
        """
        Determine severity level based on z-score and cost impact
        
        Args:
            z_score: Statistical z-score
            actual_cost: Actual cost
            expected_cost: Expected cost
            
        Returns:
            Severity level (High, Medium, Low)
        """
        abs_z_score = abs(z_score)
        cost_impact = abs(actual_cost - expected_cost)
        
        if abs_z_score >= 3.0 or cost_impact >= 1000:
            return "High"
        elif abs_z_score >= 2.0 or cost_impact >= 100:
            return "Medium"
        else:
            return "Low"
    
    def save_anomaly_results(self, anomalies: List[AnomalyResult]) -> str:
        """
        Save anomaly results to blob storage
        
        Args:
            anomalies: List of detected anomalies
            
        Returns:
            Blob path where results were saved
        """
        try:
            if not self.blob_service_client:
                logger.warning("Blob service client not configured, skipping save")
                return None
            
            # Convert anomalies to DataFrame
            anomaly_data = []
            for anomaly in anomalies:
                anomaly_data.append({
                    'resource_id': anomaly.resource_id,
                    'subscription_id': anomaly.subscription_id,
                    'date': anomaly.date,
                    'actual_cost': anomaly.actual_cost,
                    'expected_cost': anomaly.expected_cost,
                    'variance': anomaly.variance,
                    'variance_percentage': anomaly.variance_percentage,
                    'z_score': anomaly.z_score,
                    'anomaly_type': anomaly.anomaly_type,
                    'severity': anomaly.severity,
                    'is_anomaly': anomaly.is_anomaly,
                    'confidence': anomaly.confidence,
                    'detected_at': datetime.utcnow().isoformat()
                })
            
            df = pd.DataFrame(anomaly_data)
            
            # Save to blob storage
            blob_name = f"anomalies/anomaly-results-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            blob_client.upload_blob(
                df.to_json(orient='records', indent=2),
                overwrite=True
            )
            
            logger.info(f"Saved {len(anomalies)} anomalies to {blob_name}")
            return blob_name
            
        except Exception as e:
            logger.error(f"Error saving anomaly results: {str(e)}")
            raise
    
    def trigger_alerts(self, anomalies: List[AnomalyResult]) -> Dict[str, int]:
        """
        Trigger alerts for high-severity anomalies
        
        Args:
            anomalies: List of detected anomalies
            
        Returns:
            Dictionary with alert counts by severity
        """
        alert_counts = {"High": 0, "Medium": 0, "Low": 0}
        
        try:
            # Group anomalies by severity
            high_severity = [a for a in anomalies if a.severity == "High"]
            medium_severity = [a for a in anomalies if a.severity == "Medium"]
            low_severity = [a for a in anomalies if a.severity == "Low"]
            
            # Send alerts for high-severity anomalies
            if high_severity:
                self._send_high_severity_alert(high_severity)
                alert_counts["High"] = len(high_severity)
            
            # Log medium and low severity anomalies
            if medium_severity:
                self._log_medium_severity_anomalies(medium_severity)
                alert_counts["Medium"] = len(medium_severity)
            
            if low_severity:
                self._log_low_severity_anomalies(low_severity)
                alert_counts["Low"] = len(low_severity)
            
            logger.info(f"Alert summary - High: {alert_counts['High']}, Medium: {alert_counts['Medium']}, Low: {alert_counts['Low']}")
            
        except Exception as e:
            logger.error(f"Error triggering alerts: {str(e)}")
            raise
        
        return alert_counts
    
    def _send_high_severity_alert(self, anomalies: List[AnomalyResult]):
        """Send high-severity anomaly alerts"""
        # This would integrate with your alerting system (Logic Apps, Teams, etc.)
        logger.warning(f"HIGH SEVERITY ANOMALIES DETECTED: {len(anomalies)} anomalies")
        for anomaly in anomalies:
            logger.warning(f"  - {anomaly.resource_id}: ${anomaly.actual_cost:.2f} (expected: ${anomaly.expected_cost:.2f}), z-score: {anomaly.z_score:.2f}")
    
    def _log_medium_severity_anomalies(self, anomalies: List[AnomalyResult]):
        """Log medium-severity anomalies"""
        logger.info(f"MEDIUM SEVERITY ANOMALIES: {len(anomalies)} anomalies detected")
    
    def _log_low_severity_anomalies(self, anomalies: List[AnomalyResult]):
        """Log low-severity anomalies"""
        logger.info(f"LOW SEVERITY ANOMALIES: {len(anomalies)} anomalies detected")

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main Azure Function entry point for anomaly detection
    
    Args:
        req: HTTP request object
        
    Returns:
        HTTP response with anomaly detection results
    """
    try:
        # Parse request parameters
        req_body = req.get_json()
        subscription_id = req_body.get('subscription_id') if req_body else None
        days_back = int(req_body.get('days_back', 30)) if req_body else 30
        
        if not subscription_id:
            return func.HttpResponse(
                json.dumps({"error": "subscription_id parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        logger.info(f"Starting anomaly detection for subscription: {subscription_id}")
        
        # Initialize anomaly detector
        detector = AnomalyDetector()
        
        # Get cost data
        cost_data = detector.get_cost_data(subscription_id, days_back)
        
        if cost_data.empty:
            return func.HttpResponse(
                json.dumps({
                    "message": "No cost data available for analysis",
                    "subscription_id": subscription_id,
                    "anomalies_detected": 0
                }),
                status_code=200,
                mimetype="application/json"
            )
        
        # Detect anomalies
        anomalies = detector.detect_anomalies(cost_data)
        
        # Save results
        blob_path = detector.save_anomaly_results(anomalies)
        
        # Trigger alerts
        alert_counts = detector.trigger_alerts(anomalies)
        
        # Prepare response
        response_data = {
            "subscription_id": subscription_id,
            "analysis_period_days": days_back,
            "total_resources_analyzed": len(cost_data['resourceId'].unique()),
            "anomalies_detected": len(anomalies),
            "alert_counts": alert_counts,
            "results_blob_path": blob_path,
            "timestamp": datetime.utcnow().isoformat(),
            "high_severity_anomalies": [
                {
                    "resource_id": a.resource_id,
                    "date": a.date,
                    "actual_cost": a.actual_cost,
                    "expected_cost": a.expected_cost,
                    "variance_percentage": a.variance_percentage,
                    "z_score": a.z_score
                }
                for a in anomalies if a.severity == "High"
            ]
        }
        
        logger.info(f"Anomaly detection completed successfully for subscription {subscription_id}")
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error in anomaly detection function: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
