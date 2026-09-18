"""
anomaly_detector.py
Unsupervised Streaming Anomaly Detection using Isolation Forest.

Monitors real-time operational e-commerce KPIs:
  - Orders velocity per minute
  - Revenue velocity per minute
  - Payment failure percentage
  - Average transaction order value (AOV)

Flags anomalies (traffic surges, flash crashes, sudden payment gateway failures)
and persists structured alerts with severity levels to PostgreSQL `anomaly_alerts`.

Usage:
  python ml/anomaly_detector.py --dry-run
  python ml/anomaly_detector.py --eval-once
  python ml/anomaly_detector.py --daemon --interval 30
"""

import sys
import os
import json
import time
import uuid
import logging
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("anomaly_detector")


def get_db_connection():
    import psycopg2
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10,
    )


class StreamingAnomalyDetector:
    """
    Isolation Forest anomaly detector for e-commerce streaming metrics.
    Uses feature standardization to ensure equal sensitivity across metrics.
    """

    def __init__(self, contamination: float = 0.05, n_estimators: int = 100):
        self.scaler = StandardScaler()
        self.model = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=42,
            n_jobs=-1
        )
        self.is_fitted = False
        self.feature_names = [
            "total_orders",
            "total_revenue",
            "unique_users",
            "payment_failure_rate"
        ]
        # Baseline synthetic distribution to pre-warm model if database is newly started
        self._prewarm_model()

    def _prewarm_model(self):
        """Pre-warm model with expected baseline operational patterns (35 orders/sec -> ~2100/min)."""
        np.random.seed(42)
        n_samples = 500
        # Normal operations: ~1500-2500 orders/min, ~1.5M - 3M revenue, 5-15% payment failures
        orders = np.random.normal(2000, 250, n_samples).clip(1200, 3000)
        revenue = orders * np.random.normal(1200, 150, n_samples).clip(800, 2500)
        users = orders * np.random.uniform(0.75, 0.90, n_samples)
        failure_rate = np.random.beta(2, 20, n_samples) * 0.15 # ~5-12% failure rate

        baseline_features = np.column_stack([orders, revenue, users, failure_rate])
        scaled_features = self.scaler.fit_transform(baseline_features)
        self.model.fit(scaled_features)
        self.is_fitted = True
        logger.info("Anomaly detector model pre-warmed with baseline distribution.")

    def extract_features_from_db(self, window_limit: int = 60) -> List[Dict[str, Any]]:
        """Fetch recent minute-level aggregations from PostgreSQL."""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        window_start,
                        total_orders,
                        total_revenue,
                        unique_users,
                        failed_payments,
                        successful_payments
                    FROM analytics_per_minute
                    ORDER BY window_start DESC
                    LIMIT %s;
                """, (window_limit,))
                rows = cur.fetchall()

            records = []
            for r in rows:
                window_start = r[0]
                orders = float(r[1] or 0)
                revenue = float(r[2] or 0)
                users = float(r[3] or 0)
                failed = float(r[4] or 0)
                success = float(r[5] or 0)
                total_pmts = failed + success
                failure_rate = (failed / total_pmts) if total_pmts > 0 else 0.0

                records.append({
                    "window_start": window_start,
                    "total_orders": orders,
                    "total_revenue": revenue,
                    "unique_users": users,
                    "payment_failure_rate": failure_rate,
                })
            return records
        except Exception as e:
            logger.warning(f"Could not connect or extract features from database: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def evaluate_record(self, record: Dict[str, Any]) -> Tuple[bool, float, str, Dict[str, Any]]:
        """
        Evaluate a single streaming metric point.
        Returns: (is_anomaly, anomaly_score, severity, details)
        """
        features = np.array([[
            record["total_orders"],
            record["total_revenue"],
            record["unique_users"],
            record["payment_failure_rate"],
        ]])

        features_scaled = self.scaler.transform(features)
        pred = int(self.model.predict(features_scaled)[0])  # -1 = anomaly, 1 = normal
        score = float(self.model.decision_function(features_scaled)[0]) # lower = more anomalous

        # Heuristic guards for extreme operational spikes
        is_heuristic_spike = (
            record["payment_failure_rate"] > 0.30
            or record["total_orders"] > 3500
            or record["total_revenue"] > 5000000
        )

        is_anomaly = bool(pred == -1 or is_heuristic_spike)
        severity = "NORMAL"

        if is_anomaly:
            if score < -0.15 or record["payment_failure_rate"] > 0.50:
                severity = "CRITICAL"
            elif score < -0.08 or record["payment_failure_rate"] > 0.30:
                severity = "HIGH"
            elif score < -0.03:
                severity = "MEDIUM"
            else:
                severity = "LOW"

        details = {
            "total_orders": record["total_orders"],
            "total_revenue": record["total_revenue"],
            "payment_failure_rate_pct": round(record["payment_failure_rate"] * 100, 2),
            "decision_function_score": round(score, 4),
            "evaluated_window": str(record.get("window_start")),
        }

        # Identify anomalous root cause
        if is_anomaly:
            if record["payment_failure_rate"] > 0.25:
                details["description"] = f"Payment failure spike detected ({details['payment_failure_rate_pct']}%)"
            elif record["total_orders"] > 3500:
                details["description"] = f"Unusual traffic/order surge detected ({record['total_orders']} orders/min)"
            elif record["total_revenue"] > 5000000:
                details["description"] = f"Extreme revenue velocity spike detected (₹{record['total_revenue']:,})"
            else:
                details["description"] = "Multivariate KPI behavioral shift detected by Isolation Forest"
        else:
            details["description"] = "Metrics within normal baseline range"

        return is_anomaly, score, severity, details

    def save_alert(self, metric_name: str, score: float, current_val: float,
                   severity: str, details: Dict[str, Any]):
        """Persist detected anomaly alert into PostgreSQL anomaly_alerts table."""
        conn = get_db_connection()
        try:
            alert_id = str(uuid.uuid4())
            sql = """
                INSERT INTO anomaly_alerts
                    (alert_id, metric_name, anomaly_score, current_value, expected_range, severity, details, detected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (alert_id) DO NOTHING;
            """
            with conn.cursor() as cur:
                cur.execute(sql, (
                    alert_id,
                    metric_name,
                    round(score, 4),
                    round(current_val, 2),
                    "baseline_inlier_envelope",
                    severity,
                    json.dumps(details),
                ))
            conn.commit()
            logger.info(f"Persisted [{severity}] Anomaly Alert: {details.get('description')}")
        except Exception as e:
            logger.error(f"Failed to persist anomaly alert: {e}")
        finally:
            conn.close()


def run_evaluation(detector: StreamingAnomalyDetector, dry_run: bool = False):
    """Run one evaluation cycle across recent windows."""
    records = detector.extract_features_from_db(window_limit=5)
    if not records:
        logger.info("No records to evaluate (streaming tables may be starting up).")
        # Run evaluation on synthetic probe to test pipeline
        synthetic_probe = {
            "window_start": datetime.now(timezone.utc).isoformat(),
            "total_orders": 4500,
            "total_revenue": 6500000,
            "unique_users": 4000,
            "payment_failure_rate": 0.42,
        }
        is_anom, score, severity, details = detector.evaluate_record(synthetic_probe)
        logger.info(f"[PROBE TEST] Detected Anomaly={is_anom} | Severity={severity} | Score={score:.4f}")
        if not dry_run and is_anom:
            detector.save_alert(
                metric_name="e2e_platform_kpis",
                score=score,
                current_val=synthetic_probe["total_revenue"],
                severity=severity,
                details=details
            )
        return

    for rec in records:
        is_anom, score, severity, details = detector.evaluate_record(rec)
        if is_anom:
            logger.warning(f"ANOMALY DETECTED [{severity}]: {details['description']} (score={score:.4f})")
            if not dry_run:
                detector.save_alert(
                    metric_name="e2e_platform_kpis",
                    score=score,
                    current_val=rec["total_revenue"],
                    severity=severity,
                    details=details
                )
        else:
            logger.debug(f"Normal window {rec['window_start']}: score={score:.4f}")


def main():
    parser = argparse.ArgumentParser(description="Streaming ML Anomaly Detector (Isolation Forest)")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate without saving alerts to database")
    parser.add_argument("--eval-once", action="store_true", help="Run a single evaluation pass and exit")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in a loop")
    parser.add_argument("--interval", type=int, default=30, help="Evaluation interval in seconds for daemon mode")

    args = parser.parse_args()

    detector = StreamingAnomalyDetector()

    if args.eval_once or not args.daemon:
        logger.info("Running single anomaly detection evaluation pass...")
        run_evaluation(detector, dry_run=args.dry_run)
        logger.info("Evaluation complete.")
        return

    logger.info(f"Starting Anomaly Detector daemon (evaluation every {args.interval}s)...")
    try:
        while True:
            run_evaluation(detector, dry_run=args.dry_run)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Daemon stopped by user.")


if __name__ == "__main__":
    main()
