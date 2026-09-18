"""
test_anomaly_detector.py
Unit tests for the Isolation Forest ML Anomaly Detector.
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ml.anomaly_detector import StreamingAnomalyDetector


class TestAnomalyDetector:

    def setup_method(self):
        self.detector = StreamingAnomalyDetector()

    def test_detector_initializes_and_prewarms(self):
        assert self.detector.is_fitted is True
        assert len(self.detector.feature_names) == 4

    def test_normal_metrics_classified_normal(self):
        normal_record = {
            "window_start": "2025-01-01T12:00:00Z",
            "total_orders": 2100,
            "total_revenue": 2500000.0,
            "unique_users": 1800,
            "payment_failure_rate": 0.08,  # 8% failure rate
        }
        is_anomaly, score, severity, details = self.detector.evaluate_record(normal_record)
        assert not is_anomaly
        assert severity == "NORMAL"

    def test_extreme_payment_failure_spike_flagged(self):
        anomalous_record = {
            "window_start": "2025-01-01T12:05:00Z",
            "total_orders": 1900,
            "total_revenue": 2200000.0,
            "unique_users": 1700,
            "payment_failure_rate": 0.85,  # 85% catastrophic failure spike
        }
        is_anomaly, score, severity, details = self.detector.evaluate_record(anomalous_record)
        assert is_anomaly
        assert severity in ["HIGH", "CRITICAL"]
        assert "Payment failure spike" in details["description"]

    def test_extreme_traffic_surge_flagged(self):
        surge_record = {
            "window_start": "2025-01-01T12:10:00Z",
            "total_orders": 12000,          # 6x normal volume
            "total_revenue": 18000000.0,
            "unique_users": 10500,
            "payment_failure_rate": 0.10,
        }
        is_anomaly, score, severity, details = self.detector.evaluate_record(surge_record)
        assert is_anomaly
        assert severity in ["MEDIUM", "HIGH", "CRITICAL"]
