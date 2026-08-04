"""
spark_stream.py
Spark Structured Streaming job — reads from Kafka, transforms, writes to PostgreSQL.

Run:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
        spark/spark_stream.py

Environment:
    KAFKA_BOOTSTRAP_SERVERS  (default: localhost:29092)
    DB_HOST                  (default: localhost)
"""

import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDERS, TOPIC_PAYMENTS, TOPIC_CLICKS, TOPIC_REVIEWS,
)
from config.db_config import JDBC_URL, JDBC_PROPERTIES, SPARK_BATCH_SIZE
from spark.transformations import (
    ORDER_SCHEMA, PAYMENT_SCHEMA, CLICK_SCHEMA, REVIEW_SCHEMA,
    parse_kafka_stream,
    clean_orders, clean_payments, clean_clicks, clean_reviews,
    orders_per_minute, revenue_by_country, top_products, payment_method_stats,
)

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/spark.log", mode="a"),
    ],
)
logger = logging.getLogger("spark_stream")


# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Create and return a configured SparkSession.
    Packages are managed via spark-submit --packages flag.
    """
    spark = (
        SparkSession.builder
        .appName("ECommerceStreamingPipeline")
        .config("spark.sql.shuffle.partitions", "4")           # Low for local/small cluster
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────
# Kafka Source Builder
# ─────────────────────────────────────────────

def read_kafka_topic(spark: SparkSession, topic: str):
    """
    Create a streaming DataFrame from a Kafka topic.

    Args:
        spark: Active SparkSession
        topic: Kafka topic name

    Returns:
        Streaming DataFrame with raw Kafka fields
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)          # Backpressure control
        .load()
    )


# ─────────────────────────────────────────────
# PostgreSQL Sink Functions
# ─────────────────────────────────────────────

# Unique key per table — used for ON CONFLICT DO NOTHING
_CONFLICT_COLS = {
    "orders":                 "order_id",
    "payments":               "payment_id",
    "clicks":                 "click_id",
    "reviews":                "review_id",
    "dead_letter_queue":      None,
    "analytics_per_minute":   "window_start",
    "revenue_by_country":     None,
    "top_products":           None,
    "payment_method_stats":   None,
}


def write_to_postgres(batch_df, batch_id: int, table: str, mode: str = "append"):
    """
    Idempotent micro-batch writer using psycopg2 executemany.
    Uses ON CONFLICT DO NOTHING for tables with a unique key — safe on retries/replays.
    Falls back to JDBC overwrite for aggregation tables (mode='overwrite').
    Called by foreachBatch.
    """
    if batch_df.isEmpty():
        return

    # Aggregation tables are always fully replaced — use JDBC overwrite
    if mode == "overwrite":
        try:
            (
                batch_df.write
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", table)
                .option("user", JDBC_PROPERTIES["user"])
                .option("password", JDBC_PROPERTIES["password"])
                .option("driver", JDBC_PROPERTIES["driver"])
                .option("batchsize", SPARK_BATCH_SIZE)
                .mode("overwrite")
                .save()
            )
            logger.info(f"[batch={batch_id}] Overwrote {table}")
        except Exception as e:
            logger.error(f"[batch={batch_id}] Overwrite failed for {table}: {e}")
        return

    # Raw event tables — use psycopg2 with ON CONFLICT DO NOTHING (idempotent)
    try:
        import psycopg2
        import psycopg2.extras
        from config.db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return

        columns      = list(rows[0].keys())
        col_str      = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        values       = [[row[c] for c in columns] for row in rows]

        conflict_col  = _CONFLICT_COLS.get(table)
        conflict_clause = f"ON CONFLICT ({conflict_col}) DO NOTHING" if conflict_col else ""

        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) {conflict_clause};"

        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, values, page_size=500)
        conn.commit()
        conn.close()

        logger.info(f"[batch={batch_id}] Inserted {len(rows)} rows into {table} (idempotent)")

    except Exception as e:
        logger.error(f"[batch={batch_id}] Failed writing to {table}: {e}")
        raise


def write_dlq_to_postgres(batch_df, batch_id: int, topic: str):
    """Write invalid/bad events to the dead_letter_queue table."""
    if batch_df.isEmpty():
        return

    dlq_df = (
        batch_df
        .withColumn("topic",       F.lit(topic))
        .withColumn("raw_message", F.to_json(F.struct("*")))
        .select("topic", "raw_message", "error_reason")
    )
    write_to_postgres(dlq_df, batch_id, "dead_letter_queue")


def upsert_analytics(batch_df, batch_id: int):
    """
    Upsert analytics_per_minute using ON CONFLICT DO UPDATE.
    Runs on the driver via psycopg2 (not on executors — safe for foreachBatch).
    Falls back to JDBC append if psycopg2 is unavailable.
    """
    if batch_df.isEmpty():
        return

    try:
        import psycopg2
        from config.db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

        rows = batch_df.collect()

        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            connect_timeout=10,
        )
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO analytics_per_minute
                (window_start, window_end, total_orders, total_revenue, unique_users, top_product)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (window_start) DO UPDATE SET
                total_orders  = EXCLUDED.total_orders,
                total_revenue = EXCLUDED.total_revenue,
                unique_users  = EXCLUDED.unique_users,
                top_product   = COALESCE(EXCLUDED.top_product, analytics_per_minute.top_product),
                created_at    = NOW();
        """

        for row in rows:
            r = row.asDict()          # ← convert Row → dict so .get() works
            cur.execute(upsert_sql, (
                r["window_start"],
                r["window_end"],
                int(r["total_orders"]),
                float(r["total_revenue"]),
                int(r["unique_users"]),
                r.get("top_product"),
            ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"[batch={batch_id}] Upserted {len(rows)} analytics rows")

    except ImportError:
        logger.warning(f"[batch={batch_id}] psycopg2 not found — using JDBC append fallback")
        write_to_postgres(
            batch_df.select("window_start", "window_end", "total_orders",
                            "total_revenue", "unique_users", "top_product"),
            batch_id, "analytics_per_minute"
        )
    except Exception as e:
        logger.error(f"[batch={batch_id}] Analytics upsert failed: {e}")


# ─────────────────────────────────────────────
# Streaming Queries
# ─────────────────────────────────────────────

def start_orders_stream(spark: SparkSession):
    """Start the orders streaming query."""
    raw_df   = read_kafka_topic(spark, TOPIC_ORDERS)
    parsed   = parse_kafka_stream(raw_df, ORDER_SCHEMA)
    valid, invalid = clean_orders(parsed)

    # Write raw valid orders
    orders_q = (
        valid.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "orders"))
        .option("checkpointLocation", "/tmp/spark_checkpoints/orders")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Write invalid orders to DLQ
    dlq_q = (
        invalid.writeStream
        .foreachBatch(lambda df, bid: write_dlq_to_postgres(df, bid, TOPIC_ORDERS))
        .option("checkpointLocation", "/tmp/spark_checkpoints/orders_dlq")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Per-minute analytics
    agg_df = orders_per_minute(valid)
    analytics_q = (
        agg_df.writeStream
        .outputMode("update")
        .foreachBatch(upsert_analytics)
        .option("checkpointLocation", "/tmp/spark_checkpoints/analytics")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Revenue by country
    country_df = revenue_by_country(valid)
    country_q = (
        country_df.writeStream
        .outputMode("complete")
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "revenue_by_country", "overwrite"))
        .option("checkpointLocation", "/tmp/spark_checkpoints/country")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Top products
    products_df = top_products(valid)
    products_q = (
        products_df.writeStream
        .outputMode("complete")
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "top_products", "overwrite"))
        .option("checkpointLocation", "/tmp/spark_checkpoints/products")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return [orders_q, dlq_q, analytics_q, country_q, products_q]


def start_payments_stream(spark: SparkSession):
    """Start the payments streaming query."""
    raw_df = read_kafka_topic(spark, TOPIC_PAYMENTS)
    parsed = parse_kafka_stream(raw_df, PAYMENT_SCHEMA)
    valid, invalid = clean_payments(parsed)

    payments_q = (
        valid.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "payments"))
        .option("checkpointLocation", "/tmp/spark_checkpoints/payments")
        .trigger(processingTime="5 seconds")
        .start()
    )

    dlq_q = (
        invalid.writeStream
        .foreachBatch(lambda df, bid: write_dlq_to_postgres(df, bid, TOPIC_PAYMENTS))
        .option("checkpointLocation", "/tmp/spark_checkpoints/payments_dlq")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Payment method stats
    stats_df = payment_method_stats(valid)
    stats_q = (
        stats_df.writeStream
        .outputMode("complete")
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "payment_method_stats", "overwrite"))
        .option("checkpointLocation", "/tmp/spark_checkpoints/payment_stats")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return [payments_q, dlq_q, stats_q]


def start_clicks_stream(spark: SparkSession):
    """Start the clicks streaming query."""
    raw_df = read_kafka_topic(spark, TOPIC_CLICKS)
    parsed = parse_kafka_stream(raw_df, CLICK_SCHEMA)
    valid, invalid = clean_clicks(parsed)

    clicks_q = (
        valid.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "clicks"))
        .option("checkpointLocation", "/tmp/spark_checkpoints/clicks")
        .trigger(processingTime="5 seconds")
        .start()
    )

    return [clicks_q]


def start_reviews_stream(spark: SparkSession):
    """Start the reviews streaming query."""
    raw_df = read_kafka_topic(spark, TOPIC_REVIEWS)
    parsed = parse_kafka_stream(raw_df, REVIEW_SCHEMA)
    valid, invalid = clean_reviews(parsed)

    reviews_q = (
        valid.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "reviews"))
        .option("checkpointLocation", "/tmp/spark_checkpoints/reviews")
        .trigger(processingTime="5 seconds")
        .start()
    )

    return [reviews_q]


# ─────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info(" Spark Structured Streaming — E-Commerce Pipeline")
    logger.info(f" Kafka  : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f" DB     : {JDBC_URL}")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        all_queries = []
        all_queries.extend(start_orders_stream(spark))
        all_queries.extend(start_payments_stream(spark))
        all_queries.extend(start_clicks_stream(spark))
        all_queries.extend(start_reviews_stream(spark))

        logger.info(f"Started {len(all_queries)} streaming queries. Waiting for termination...")

        # Block until all queries stop or an exception occurs
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Stopping all queries...")
    finally:
        for q in spark.streams.active:
            logger.info(f"Stopping query: {q.name}")
            q.stop()
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
