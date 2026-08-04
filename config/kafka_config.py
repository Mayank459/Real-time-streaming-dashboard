"""
Kafka Configuration
Centralized settings for all Kafka producers and consumers.
"""

import os

# ─────────────────────────────────────────────
# Broker Settings
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# ─────────────────────────────────────────────
# Topic Names
# ─────────────────────────────────────────────
TOPIC_ORDERS   = "orders"
TOPIC_PAYMENTS = "payments"
TOPIC_CLICKS   = "clicks"
TOPIC_REVIEWS  = "reviews"
TOPIC_DLQ      = "dlq"

ALL_TOPICS = [TOPIC_ORDERS, TOPIC_PAYMENTS, TOPIC_CLICKS, TOPIC_REVIEWS]

# ─────────────────────────────────────────────
# Producer Settings
# ─────────────────────────────────────────────
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",                     # Wait for all replicas (durability)
    "retries": 5,                      # Retry on transient failures
    "retry.backoff.ms": 500,           # Wait 500ms between retries
    "linger.ms": 5,                    # Batch up to 5ms for throughput
    "batch.size": 16384,               # 16KB batch size
    "compression.type": "snappy",      # Compress for network efficiency
    "enable.idempotence": True,        # Exactly-once semantics
    "max.in.flight.requests.per.connection": 5,
}

# ─────────────────────────────────────────────
# Consumer Settings (for Spark, not direct use)
# ─────────────────────────────────────────────
CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "spark-streaming-consumer",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,        # Spark manages offsets
    "session.timeout.ms": 30000,
}

# ─────────────────────────────────────────────
# Event Rate Settings
# ─────────────────────────────────────────────
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "100"))

# Distribution across topics (must sum to 1.0)
TOPIC_DISTRIBUTION = {
    TOPIC_ORDERS:   0.35,   # 35 events/sec
    TOPIC_PAYMENTS: 0.30,   # 30 events/sec
    TOPIC_CLICKS:   0.25,   # 25 events/sec
    TOPIC_REVIEWS:  0.10,   # 10 events/sec
}
