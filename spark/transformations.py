"""
transformations.py
Pure Spark transformation functions — each is independently testable.

Functions:
  - clean_orders()
  - clean_payments()
  - clean_clicks()
  - clean_reviews()
  - orders_per_minute()
  - revenue_by_country()
  - top_products()
  - payment_method_stats()
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType, BooleanType, TimestampType
)


# ─────────────────────────────────────────────
# Supported Schema Versions
# ─────────────────────────────────────────────
SUPPORTED_SCHEMA_VERSIONS = ["1.0"]


# ─────────────────────────────────────────────
# Kafka Message Schema Definitions
# ─────────────────────────────────────────────

ORDER_SCHEMA = StructType([
    StructField("event_type",         StringType(),  True),
    StructField("schema_version",     StringType(),  True),
    StructField("order_id",           StringType(),  True),
    StructField("user_id",            StringType(),  True),
    StructField("product_id",         StringType(),  True),
    StructField("product_name",       StringType(),  True),
    StructField("category",           StringType(),  True),
    StructField("price",              DoubleType(),  True),
    StructField("quantity",           IntegerType(), True),
    StructField("total_amount",       DoubleType(),  True),
    StructField("country",            StringType(),  True),
    StructField("device",             StringType(),  True),
    StructField("browser",            StringType(),  True),
    StructField("status",             StringType(),  True),
    StructField("timestamp",          StringType(),  True),
    StructField("event_timestamp_ms", LongType(),    True),
])

PAYMENT_SCHEMA = StructType([
    StructField("event_type",         StringType(), True),
    StructField("schema_version",     StringType(), True),
    StructField("payment_id",         StringType(), True),
    StructField("order_id",           StringType(), True),
    StructField("user_id",            StringType(), True),
    StructField("payment_type",       StringType(), True),
    StructField("amount",             DoubleType(), True),
    StructField("status",             StringType(), True),
    StructField("gateway",            StringType(), True),
    StructField("timestamp",          StringType(), True),
    StructField("event_timestamp_ms", LongType(),   True),
])

CLICK_SCHEMA = StructType([
    StructField("event_type",         StringType(),  True),
    StructField("schema_version",     StringType(),  True),
    StructField("click_id",           StringType(),  True),
    StructField("user_id",            StringType(),  True),
    StructField("product_id",         StringType(),  True),
    StructField("session_id",         StringType(),  True),
    StructField("page",               StringType(),  True),
    StructField("action",             StringType(),  True),
    StructField("duration_sec",       IntegerType(), True),
    StructField("device",             StringType(),  True),
    StructField("timestamp",          StringType(),  True),
    StructField("event_timestamp_ms", LongType(),    True),
])

REVIEW_SCHEMA = StructType([
    StructField("event_type",         StringType(),  True),
    StructField("schema_version",     StringType(),  True),
    StructField("review_id",          StringType(),  True),
    StructField("user_id",            StringType(),  True),
    StructField("product_id",         StringType(),  True),
    StructField("product_name",       StringType(),  True),
    StructField("rating",             IntegerType(), True),
    StructField("sentiment",          StringType(),  True),
    StructField("verified",           BooleanType(), True),
    StructField("timestamp",          StringType(),  True),
    StructField("event_timestamp_ms", LongType(),    True),
])


# ─────────────────────────────────────────────
# Parse Kafka Raw Messages
# ─────────────────────────────────────────────

def parse_kafka_stream(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parse raw Kafka binary messages (value field) using the given schema.

    Args:
        raw_df: DataFrame with raw Kafka messages (key, value as binary)
        schema: Expected JSON schema of the message payload

    Returns:
        DataFrame with parsed fields + kafka metadata columns
    """
    return (
        raw_df
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
        )
        .select("topic", "partition", "offset", "kafka_timestamp", "data.*")
    )


# ─────────────────────────────────────────────
# Cleaning Functions
# ─────────────────────────────────────────────

def _enrich_timestamps_and_latency(df: DataFrame) -> DataFrame:
    """Helper to parse event_timestamp and compute pipeline_latency_ms."""
    df = df.withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
    
    if "event_timestamp_ms" in df.columns:
        df = df.withColumn(
            "pipeline_latency_ms",
            F.when(
                F.col("event_timestamp_ms").isNotNull() & (F.col("event_timestamp_ms") > 0),
                F.greatest(F.lit(0), (F.unix_timestamp(F.current_timestamp()) * 1000 - F.col("event_timestamp_ms")).cast(IntegerType()))
            ).otherwise(
                F.greatest(F.lit(0), (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("event_timestamp"))) * 1000).cast(IntegerType())
            )
        )
    else:
        df = df.withColumn(
            "pipeline_latency_ms",
            F.greatest(F.lit(0), (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("event_timestamp"))) * 1000).cast(IntegerType())
        )

    if "schema_version" in df.columns:
        df = df.withColumn("schema_version", F.coalesce(F.col("schema_version"), F.lit("1.0")))
    else:
        df = df.withColumn("schema_version", F.lit("1.0"))

    return df


def clean_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Clean and validate orders.

    Validations:
        - schema_version in supported versions
        - order_id, user_id, product_id must not be null
        - price > 0 and total_amount > 0
        - quantity >= 1
        - country, category must not be null

    Returns:
        (valid_df, invalid_df) — split into good and bad rows
    """
    df = _enrich_timestamps_and_latency(df)

    valid_condition = (
        F.col("schema_version").isin(SUPPORTED_SCHEMA_VERSIONS)
        & F.col("order_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("price") > 0)
        & (F.col("total_amount") > 0)
        & (F.col("quantity") >= 1)
        & F.col("country").isNotNull()
        & F.col("category").isNotNull()
    )

    drop_cols = [c for c in ["timestamp", "event_type", "topic", "partition", "offset", "kafka_timestamp", "event_timestamp_ms"] if c in df.columns]

    valid_df = (
        df.filter(valid_condition)
        .dropDuplicates(["order_id"])
        .drop(*drop_cols)
    )

    invalid_df = (
        df.filter(~valid_condition)
        .withColumn("error_reason", F.lit("Failed order validation or incompatible schema_version"))
    )

    return valid_df, invalid_df


def clean_payments(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Clean and validate payment events.

    Validations:
        - schema_version in supported versions
        - payment_id, order_id, user_id must not be null
        - amount > 0
        - status must be in (success, failed, pending)
    """
    df = _enrich_timestamps_and_latency(df)

    valid_statuses = ["success", "failed", "pending"]
    valid_condition = (
        F.col("schema_version").isin(SUPPORTED_SCHEMA_VERSIONS)
        & F.col("payment_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("user_id").isNotNull()
        & (F.col("amount") > 0)
        & F.col("status").isin(valid_statuses)
    )

    drop_cols = [c for c in ["timestamp", "event_type", "topic", "partition", "offset", "kafka_timestamp", "event_timestamp_ms"] if c in df.columns]

    valid_df   = df.filter(valid_condition).dropDuplicates(["payment_id"]).drop(*drop_cols)
    invalid_df = df.filter(~valid_condition).withColumn("error_reason", F.lit("Failed payment validation or incompatible schema_version"))

    return valid_df, invalid_df


def clean_clicks(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean and validate click events."""
    df = _enrich_timestamps_and_latency(df)

    valid_condition = (
        F.col("schema_version").isin(SUPPORTED_SCHEMA_VERSIONS)
        & F.col("click_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("session_id").isNotNull()
    )

    drop_cols = [c for c in ["timestamp", "event_type", "topic", "partition", "offset", "kafka_timestamp", "event_timestamp_ms"] if c in df.columns]

    valid_df   = df.filter(valid_condition).dropDuplicates(["click_id"]).drop(*drop_cols)
    invalid_df = df.filter(~valid_condition).withColumn("error_reason", F.lit("Failed click validation or incompatible schema_version"))

    return valid_df, invalid_df


def clean_reviews(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean and validate review events."""
    df = _enrich_timestamps_and_latency(df)

    valid_condition = (
        F.col("schema_version").isin(SUPPORTED_SCHEMA_VERSIONS)
        & F.col("review_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("rating").between(1, 5)
    )

    drop_cols = [c for c in ["timestamp", "event_type", "topic", "partition", "offset", "kafka_timestamp", "event_timestamp_ms"] if c in df.columns]

    valid_df   = df.filter(valid_condition).dropDuplicates(["review_id"]).drop(*drop_cols)
    invalid_df = df.filter(~valid_condition).withColumn("error_reason", F.lit("Failed review validation or incompatible schema_version"))

    return valid_df, invalid_df



# ─────────────────────────────────────────────
# Aggregation Functions
# ─────────────────────────────────────────────

def orders_per_minute(orders_df: DataFrame) -> DataFrame:
    """
    Tumbling window aggregation — orders and revenue per minute.

    Uses Spark watermark to handle late-arriving events (up to 10 minutes late).
    Writes to analytics_per_minute table.

    Args:
        orders_df: Cleaned orders DataFrame with event_timestamp column

    Returns:
        Aggregated DataFrame with window_start, window_end, metrics
    """
    return (
        orders_df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "1 minute").alias("window")
        )
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.first(
                F.struct(F.col("product_name"), F.col("total_amount"))
            ).alias("sample_product"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("total_orders"),
            F.round("total_revenue", 2).alias("total_revenue"),
            F.col("unique_users"),
            F.col("sample_product.product_name").alias("top_product"),
        )
    )


def revenue_by_country(orders_df: DataFrame) -> DataFrame:
    """
    Aggregate revenue and order count by country.
    Refreshes every micro-batch (append mode).

    Returns:
        DataFrame grouped by country with revenue metrics
    """
    return (
        orders_df
        .groupBy("country")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_orders"),
            F.approx_count_distinct("user_id").alias("unique_users"),
        )
        .withColumn("total_revenue", F.round("total_revenue", 2))
        .withColumn("snapshot_time", F.current_timestamp())
    )


def top_products(orders_df: DataFrame, limit: int = 10) -> DataFrame:
    """
    Calculate top products by revenue in current micro-batch.

    Args:
        orders_df: Cleaned orders DataFrame
        limit: Number of top products to return

    Returns:
        DataFrame with top N products by revenue
    """
    return (
        orders_df
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .withColumn("total_revenue", F.round("total_revenue", 2))
        .withColumn("snapshot_time", F.current_timestamp())
        .orderBy(F.desc("total_revenue"))
        .limit(limit)
    )


def payment_method_stats(payments_df: DataFrame) -> DataFrame:
    """
    Calculate payment method distribution and success rates.

    Returns:
        DataFrame grouped by payment_type with counts and success rate
    """
    return (
        payments_df
        .groupBy("payment_type")
        .agg(
            F.count("*").alias("total_count"),
            F.sum("amount").alias("total_amount"),
            F.sum(
                F.when(F.col("status") == "success", 1).otherwise(0)
            ).alias("success_count"),
        )
        .withColumn(
            "success_rate",
            F.round(F.col("success_count") * 100.0 / F.col("total_count"), 2)
        )
        .withColumn("total_amount", F.round("total_amount", 2))
        .withColumn("snapshot_time", F.current_timestamp())
        .drop("success_count")
    )


# ─────────────────────────────────────────────
# Payment-Order Join (Advanced Feature)
# ─────────────────────────────────────────────

def join_orders_payments(orders_df: DataFrame, payments_df: DataFrame) -> DataFrame:
    """
    Stream-stream join: match orders with their payments.
    Creates 'confirmed orders' — orders that have a successful payment.

    Uses a 2-minute event-time window for the join.

    Args:
        orders_df: Watermarked orders stream
        payments_df: Watermarked payments stream

    Returns:
        Joined DataFrame of confirmed (paid) orders
    """
    orders_wm = (
        orders_df
        .withWatermark("event_timestamp", "2 minutes")
        .select(
            F.col("order_id"),
            F.col("user_id"),
            F.col("product_name"),
            F.col("total_amount"),
            F.col("country"),
            F.col("event_timestamp").alias("order_time"),
        )
    )

    payments_wm = (
        payments_df
        .withWatermark("event_timestamp", "2 minutes")
        .filter(F.col("status") == "success")
        .select(
            F.col("order_id").alias("pay_order_id"),
            F.col("payment_type"),
            F.col("gateway"),
            F.col("event_timestamp").alias("payment_time"),
        )
    )

    return (
        orders_wm
        .join(
            payments_wm,
            F.expr("""
                order_id = pay_order_id AND
                payment_time BETWEEN order_time AND order_time + INTERVAL 2 MINUTES
            """),
            how="inner",
        )
        .drop("pay_order_id")
        .withColumn("confirmed_at", F.current_timestamp())
    )
