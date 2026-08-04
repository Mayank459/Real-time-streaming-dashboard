"""
Database Configuration
Centralized PostgreSQL connection settings with retry logic.
"""

import os

# ─────────────────────────────────────────────
# Connection Settings
# ─────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "ecommerce_db")
DB_USER     = os.getenv("DB_USER",     "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")

# ─────────────────────────────────────────────
# Connection Strings
# ─────────────────────────────────────────────
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# For Apache Spark JDBC
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

JDBC_PROPERTIES = {
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ─────────────────────────────────────────────
# Connection Pool Settings
# ─────────────────────────────────────────────
POOL_MIN_CONNECTIONS = 2
POOL_MAX_CONNECTIONS = 10
POOL_TIMEOUT_SECONDS = 30

# ─────────────────────────────────────────────
# Retry Settings
# ─────────────────────────────────────────────
DB_MAX_RETRIES       = 5
DB_RETRY_DELAY_SEC   = 2
DB_RETRY_BACKOFF     = 2.0   # Exponential backoff multiplier

# ─────────────────────────────────────────────
# Batch Write Settings (for Spark sink)
# ─────────────────────────────────────────────
BATCH_SIZE           = 500   # rows per batch insert
SPARK_BATCH_SIZE     = 1000  # Spark JDBC batch size
