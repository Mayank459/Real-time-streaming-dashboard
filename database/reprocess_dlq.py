"""
reprocess_dlq.py
Automated Dead Letter Queue (DLQ) inspection, remediation, and replay utility.

Capabilities:
  1. Inspect error distributions and quarantine counts across topics.
  2. Apply automated remediation rules for recoverable errors (e.g. missing schema_version).
  3. Replay corrected messages to Kafka retry topics or re-insert directly into PostgreSQL.
  4. Mark reprocessed records to avoid duplicate replay.

Usage:
  python database/reprocess_dlq.py --stats
  python database/reprocess_dlq.py --dry-run --limit 100
  python database/reprocess_dlq.py --direct-insert --limit 500
  python database/reprocess_dlq.py --republish-kafka --topic orders --limit 100
"""

import sys
import os
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, PRODUCER_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("dlq_reprocessor")


def get_db_connection():
    import psycopg2
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10,
    )


def get_dlq_stats() -> Dict[str, Any]:
    """Retrieve summary metrics about quarantined records in the DLQ."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    topic,
                    reprocessed,
                    COUNT(*) AS count
                FROM dead_letter_queue
                GROUP BY topic, reprocessed
                ORDER BY topic, reprocessed;
            """)
            status_counts = cur.fetchall()

            cur.execute("""
                SELECT
                    error_reason,
                    COUNT(*) AS count
                FROM dead_letter_queue
                WHERE reprocessed = FALSE
                GROUP BY error_reason
                ORDER BY count DESC
                LIMIT 10;
            """)
            top_errors = cur.fetchall()

            return {
                "status_counts": status_counts,
                "top_errors": top_errors,
            }
    except Exception as e:
        logger.warning(f"Could not connect to PostgreSQL for DLQ stats: {e}")
        return {"status_counts": [], "top_errors": []}
    finally:
        if conn:
            conn.close()


def print_dlq_stats():
    """Print formatted summary of DLQ status."""
    stats = get_dlq_stats()
    print("=" * 60)
    print(" DEAD LETTER QUEUE (DLQ) HEALTH SUMMARY")
    print("=" * 60)
    print("Status by Topic:")
    if not stats["status_counts"]:
        print("  (DLQ is empty - no quarantined events)")
    for topic, reprocessed, count in stats["status_counts"]:
        status_label = "Reprocessed" if reprocessed else "Pending Quarantine"
        print(f"  [{topic:<10}] {status_label:<20}: {count:>6,}")

    print("\nTop Unresolved Error Reasons:")
    if not stats["top_errors"]:
        print("  (No pending unresolved errors)")
    for reason, count in stats["top_errors"]:
        print(f"  - {reason[:45]:<45}: {count:>6,}")
    print("=" * 60)


def remediate_payload(raw_message: str, topic: str) -> Tuple[bool, Dict[str, Any], str]:
    """
    Apply automated remediation rules to a quarantined JSON message.
    Returns: (is_repaired, repaired_dict, notes)
    """
    try:
        if isinstance(raw_message, str):
            payload = json.loads(raw_message)
        else:
            payload = dict(raw_message)
    except Exception as e:
        return False, {}, f"JSON Deserialization failed: {e}"

    modified = False
    reasons = []

    # Remediation 1: Missing schema_version
    if "schema_version" not in payload or not payload["schema_version"]:
        payload["schema_version"] = "1.0"
        modified = True
        reasons.append("Backfilled schema_version=1.0")

    # Remediation 2: Missing or malformed timestamp
    if "timestamp" not in payload or not payload["timestamp"]:
        payload["timestamp"] = datetime.now(timezone.utc).isoformat()
        modified = True
        reasons.append("Backfilled timestamp")

    # Remediation 3: Missing event_timestamp_ms
    if "event_timestamp_ms" not in payload:
        payload["event_timestamp_ms"] = int(datetime.now(timezone.utc).timestamp() * 1000)
        modified = True
        reasons.append("Backfilled event_timestamp_ms")

    # Topic-specific remediation
    if topic == "orders":
        if "quantity" in payload and payload["quantity"] is not None and payload["quantity"] <= 0:
            payload["quantity"] = 1
            modified = True
            reasons.append("Adjusted non-positive quantity to 1")

        if "price" in payload and payload.get("price", 0) > 0 and "quantity" in payload:
            expected_total = round(payload["price"] * payload["quantity"], 2)
            if payload.get("total_amount") != expected_total:
                payload["total_amount"] = expected_total
                modified = True
                reasons.append("Recalculated total_amount")

    return True, payload, "; ".join(reasons) if modified else "No modifications required"


def reprocess_records(limit: int = 100, topic_filter: str | None = None,
                      dry_run: bool = True, direct_insert: bool = False,
                      republish_kafka: bool = False):
    """Fetch unhandled DLQ records, apply remediation, and replay."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = "SELECT id, topic, raw_message, error_reason FROM dead_letter_queue WHERE reprocessed = FALSE"
            params = []
            if topic_filter:
                query += " AND topic = %s"
                params.append(topic_filter)
            query += " ORDER BY ingested_at ASC LIMIT %s;"
            params.append(limit)

            cur.execute(query, tuple(params))
            records = cur.fetchall()

        if not records:
            logger.info("No pending DLQ records found matching criteria.")
            return

        logger.info(f"Found {len(records)} pending DLQ records to process (dry_run={dry_run}).")

        repaired_records = []
        unrepairable_ids = []
        mark_reprocessed_ids = []

        for record_id, topic, raw_msg, error_reason in records:
            success, repaired, notes = remediate_payload(raw_msg, topic)
            if success:
                repaired_records.append((record_id, topic, repaired, notes))
                mark_reprocessed_ids.append(record_id)
            else:
                unrepairable_ids.append((record_id, notes))

        logger.info(f"Remediation results: {len(repaired_records)} repairable, {len(unrepairable_ids)} unrepairable.")

        if dry_run:
            print("\n=== DRY RUN SAMPLING (First 5 records) ===")
            for record_id, topic, payload, notes in repaired_records[:5]:
                print(f"ID={record_id} | Topic={topic} | Fix: {notes}")
                print(f"Repaired Payload: {json.dumps(payload, default=str)[:120]}...\n")
            return

        # Direct database insertion
        if direct_insert:
            import psycopg2.extras
            table_records = {}
            for record_id, topic, payload, _ in repaired_records:
                target_table = topic  # orders, payments, clicks, reviews
                if target_table not in table_records:
                    table_records[target_table] = []
                table_records[target_table].append(payload)

            with conn.cursor() as cur:
                for target_table, rows in table_records.items():
                    if not rows:
                        continue
                    columns = list(rows[0].keys())
                    # Clean out fields not present in SQL table
                    allowed_cols = [c for c in columns if c not in ["event_timestamp_ms", "event_type"]]
                    col_str = ", ".join(f'"{c}"' for c in allowed_cols)
                    placeholders = ", ".join(["%s"] * len(allowed_cols))
                    values = [[row.get(c) for c in allowed_cols] for row in rows]

                    conflict_map = {
                        "orders": "order_id",
                        "payments": "payment_id",
                        "clicks": "click_id",
                        "reviews": "review_id",
                    }
                    pk = conflict_map.get(target_table)
                    conflict_sql = f"ON CONFLICT ({pk}) DO NOTHING" if pk else ""

                    sql = f"INSERT INTO {target_table} ({col_str}) VALUES ({placeholders}) {conflict_sql};"
                    psycopg2.extras.execute_batch(cur, sql, values, page_size=200)
                    logger.info(f"Direct inserted {len(values)} repaired records into '{target_table}'.")

        # Republish to Kafka
        if republish_kafka:
            from confluent_kafka import Producer
            producer = Producer(PRODUCER_CONFIG)
            for record_id, topic, payload, _ in repaired_records:
                retry_topic = f"{topic}_retry"
                serialized = json.dumps(payload, default=str).encode("utf-8")
                key = payload.get("order_id") or payload.get("payment_id") or payload.get("click_id") or payload.get("review_id")
                producer.produce(
                    topic=retry_topic,
                    key=key.encode("utf-8") if key else None,
                    value=serialized
                )
            producer.flush(timeout=10)
            logger.info(f"Republished {len(repaired_records)} records to Kafka retry topics.")

        # Mark reprocessed in PostgreSQL
        if mark_reprocessed_ids:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE dead_letter_queue
                    SET reprocessed = TRUE, reprocessed_at = NOW()
                    WHERE id = ANY(%s);
                """, (mark_reprocessed_ids,))
            conn.commit()
            logger.info(f"Marked {len(mark_reprocessed_ids)} DLQ records as reprocessed.")

    except Exception as e:
        logger.warning(f"Could not connect or process DLQ records: {e}")
    finally:
        if conn:
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="E-Commerce Streaming DLQ Reprocessor")
    parser.add_argument("--stats", action="store_true", help="Print DLQ health stats and exit")
    parser.add_argument("--limit", type=int, default=100, help="Number of records to process")
    parser.add_argument("--topic", type=str, default=None, help="Filter by topic (orders, payments, etc.)")
    parser.add_argument("--dry-run", action="store_true", help="Inspect and simulate remediation without writing")
    parser.add_argument("--direct-insert", action="store_true", help="Directly insert repaired records into DB")
    parser.add_argument("--republish-kafka", action="store_true", help="Publish repaired records to Kafka retry topics")

    args = parser.parse_args()

    if args.stats:
        print_dlq_stats()
        return

    is_dry = args.dry_run or (not args.direct_insert and not args.republish_kafka)
    reprocess_records(
        limit=args.limit,
        topic_filter=args.topic,
        dry_run=is_dry,
        direct_insert=args.direct_insert,
        republish_kafka=args.republish_kafka
    )


if __name__ == "__main__":
    main()
