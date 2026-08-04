"""
test_transformations.py
Unit tests for Spark transformation functions using PySpark local mode.
Run: pytest tests/test_transformations.py -v
"""

import pytest
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing (no cluster needed)."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_transformations")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_orders(spark):
    """Sample valid and invalid orders for testing."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    schema = StructType([
        StructField("order_id",     StringType(),  True),
        StructField("user_id",      StringType(),  True),
        StructField("product_id",   StringType(),  True),
        StructField("product_name", StringType(),  True),
        StructField("category",     StringType(),  True),
        StructField("price",        DoubleType(),  True),
        StructField("quantity",     IntegerType(), True),
        StructField("total_amount", DoubleType(),  True),
        StructField("country",      StringType(),  True),
        StructField("device",       StringType(),  True),
        StructField("browser",      StringType(),  True),
        StructField("status",       StringType(),  True),
        StructField("timestamp",    StringType(),  True),
    ])
    data = [
        # Valid orders
        ("ORD-001", "USR-1", "P001", "Nike Air Max", "Footwear",     8999.0, 1, 8999.0,  "India", "mobile",  "Chrome",  "confirmed",  "2025-01-01T10:00:00+00:00"),
        ("ORD-002", "USR-2", "P002", "iPhone 15",    "Electronics", 79999.0, 1, 79999.0, "USA",   "desktop", "Safari",  "confirmed",  "2025-01-01T10:01:00+00:00"),
        ("ORD-003", "USR-3", "P003", "Galaxy S24",   "Electronics", 69999.0, 2, 139998.0,"India", "mobile",  "Chrome",  "processing", "2025-01-01T10:02:00+00:00"),
        # Duplicate order_id (should be deduplicated)
        ("ORD-001", "USR-4", "P001", "Nike Air Max", "Footwear",     8999.0, 1, 8999.0,  "India", "mobile",  "Chrome",  "confirmed",  "2025-01-01T10:03:00+00:00"),
        # Invalid: null order_id
        (None,      "USR-5", "P004", "Laptop",       "Electronics", 54999.0, 1, 54999.0, "India", "desktop", "Firefox", "confirmed",  "2025-01-01T10:04:00+00:00"),
        # Invalid: price = 0
        ("ORD-005", "USR-6", "P005", "Free Item",    "Misc",          0.0,   1,   0.0,   "India", "mobile",  "Chrome",  "confirmed",  "2025-01-01T10:05:00+00:00"),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestCleanOrders:

    def test_valid_count(self, spark, sample_orders):
        from spark.transformations import clean_orders
        valid, invalid = clean_orders(sample_orders)
        # 3 valid unique orders (ORD-001 dedup'd, ORD-005 invalid price, null removed)
        assert valid.count() == 3

    def test_invalid_count(self, spark, sample_orders):
        from spark.transformations import clean_orders
        valid, invalid = clean_orders(sample_orders)
        assert invalid.count() == 2   # null order_id + zero price

    def test_no_duplicates_in_valid(self, spark, sample_orders):
        from spark.transformations import clean_orders
        valid, _ = clean_orders(sample_orders)
        total   = valid.count()
        unique  = valid.select("order_id").distinct().count()
        assert total == unique

    def test_invalid_has_error_reason(self, spark, sample_orders):
        from spark.transformations import clean_orders
        _, invalid = clean_orders(sample_orders)
        assert "error_reason" in invalid.columns

    def test_event_timestamp_parsed(self, spark, sample_orders):
        from spark.transformations import clean_orders
        from pyspark.sql.types import TimestampType
        valid, _ = clean_orders(sample_orders)
        ts_type = dict(valid.dtypes)["event_timestamp"]
        assert "timestamp" in ts_type.lower()


class TestAggregations:

    def test_orders_per_minute_columns(self, spark, sample_orders):
        from spark.transformations import clean_orders, orders_per_minute
        valid, _ = clean_orders(sample_orders)
        result = orders_per_minute(valid)
        expected_cols = {"window_start", "window_end", "total_orders", "total_revenue", "unique_users"}
        assert expected_cols.issubset(set(result.columns))

    def test_revenue_by_country_columns(self, spark, sample_orders):
        from spark.transformations import clean_orders, revenue_by_country
        valid, _ = clean_orders(sample_orders)
        result = revenue_by_country(valid)
        assert "country" in result.columns
        assert "total_revenue" in result.columns
        assert "total_orders" in result.columns

    def test_revenue_by_country_grouping(self, spark, sample_orders):
        from spark.transformations import clean_orders, revenue_by_country
        valid, _ = clean_orders(sample_orders)
        result = revenue_by_country(valid)
        countries = [r["country"] for r in result.collect()]
        assert "India" in countries
        assert "USA" in countries

    def test_top_products_limit(self, spark, sample_orders):
        from spark.transformations import clean_orders, top_products
        valid, _ = clean_orders(sample_orders)
        result = top_products(valid, limit=2)
        assert result.count() <= 2

    def test_top_products_columns(self, spark, sample_orders):
        from spark.transformations import clean_orders, top_products
        valid, _ = clean_orders(sample_orders)
        result = top_products(valid)
        assert "product_name" in result.columns
        assert "total_revenue" in result.columns


class TestPaymentAggregations:

    @pytest.fixture
    def sample_payments(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("payment_id",   StringType(), True),
            StructField("order_id",     StringType(), True),
            StructField("user_id",      StringType(), True),
            StructField("payment_type", StringType(), True),
            StructField("amount",       DoubleType(), True),
            StructField("status",       StringType(), True),
            StructField("gateway",      StringType(), True),
            StructField("timestamp",    StringType(), True),
        ])
        data = [
            ("PAY-001", "ORD-001", "USR-1", "UPI",         8999.0,  "success", "PhonePe", "2025-01-01T10:00:00+00:00"),
            ("PAY-002", "ORD-002", "USR-2", "Credit Card", 79999.0, "success", "Razorpay","2025-01-01T10:01:00+00:00"),
            ("PAY-003", "ORD-003", "USR-3", "UPI",         1000.0,  "failed",  "Paytm",   "2025-01-01T10:02:00+00:00"),
            ("PAY-004", "ORD-004", "USR-4", "COD",         500.0,   "pending", None,      "2025-01-01T10:03:00+00:00"),
        ]
        return spark.createDataFrame(data, schema=schema)

    def test_payment_stats_columns(self, spark, sample_payments):
        from spark.transformations import clean_payments, payment_method_stats
        valid, _ = clean_payments(sample_payments)
        result = payment_method_stats(valid)
        assert "payment_type" in result.columns
        assert "total_count" in result.columns
        assert "success_rate" in result.columns

    def test_upi_has_highest_count(self, spark, sample_payments):
        from spark.transformations import clean_payments, payment_method_stats
        from pyspark.sql import functions as F
        valid, _ = clean_payments(sample_payments)
        result = payment_method_stats(valid)
        rows = {r["payment_type"]: r["total_count"] for r in result.collect()}
        assert rows["UPI"] == 2
