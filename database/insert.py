"""
insert.py
PostgreSQL batch insert helpers with retry logic.
Used for direct Python inserts (not via Spark).
"""

import sys
import os
import logging
import time
import json
from typing import List, Dict, Any

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    BATCH_SIZE, DB_MAX_RETRIES, DB_RETRY_DELAY_SEC, DB_RETRY_BACKOFF,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Connection Helper
# ─────────────────────────────────────────────

def get_connection(retries: int = DB_MAX_RETRIES) -> psycopg2.extensions.connection:
    """
    Get a PostgreSQL connection with exponential backoff retry.

    Args:
        retries: Maximum number of connection attempts

    Returns:
        Active psycopg2 connection

    Raises:
        RuntimeError: If all retries fail
    """
    delay = DB_RETRY_DELAY_SEC
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD,
                connect_timeout=10,
            )
            logger.debug(f"DB connected on attempt {attempt}")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"DB connection failed (attempt {attempt}/{retries}): {e}")
            if attempt == retries:
                raise RuntimeError(f"Could not connect to PostgreSQL after {retries} retries") from e
            time.sleep(delay)
            delay *= DB_RETRY_BACKOFF


# ─────────────────────────────────────────────
# Batch Insert Helper
# ─────────────────────────────────────────────

def batch_insert(table: str, rows: List[Dict[str, Any]], conflict_column: str | None = None):
    """
    Insert a list of dicts into a table using execute_values (fast batch insert).

    Args:
        table:           Target table name
        rows:            List of dicts — keys must match column names
        conflict_column: If set, uses ON CONFLICT DO NOTHING on this column
    """
    if not rows:
        return

    columns = list(rows[0].keys())
    values  = [[row[col] for col in columns] for row in rows]

    col_str = ", ".join(columns)
    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"

    conflict_clause = ""
    if conflict_column:
        conflict_clause = f"ON CONFLICT ({conflict_column}) DO NOTHING"

    sql = f"INSERT INTO {table} ({col_str}) VALUES %s {conflict_clause};"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values, template=placeholders, page_size=BATCH_SIZE)
        conn.commit()
        logger.info(f"Inserted {len(rows)} rows into {table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Batch insert failed for {table}: {e}")
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Dead Letter Queue Insert
# ─────────────────────────────────────────────

def insert_dlq(topic: str, raw_message: Any, error_reason: str):
    """
    Insert a bad event into the dead_letter_queue table.

    Args:
        topic:        Kafka topic the message came from
        raw_message:  Original message (dict or str)
        error_reason: Why it was rejected
    """
    if isinstance(raw_message, dict):
        raw_message = json.dumps(raw_message)

    batch_insert(
        table="dead_letter_queue",
        rows=[{"topic": topic, "raw_message": raw_message, "error_reason": error_reason}],
    )
