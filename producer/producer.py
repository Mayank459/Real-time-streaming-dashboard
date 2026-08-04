"""
producer.py
Multi-threaded Kafka producer that sends 100 events/sec across 4 topics.

Usage:
    python producer.py                    # Run forever
    python producer.py --duration 60      # Run for 60 seconds
    python producer.py --rate 50          # 50 events/sec

Topics:
    orders    (~35 events/sec)
    payments  (~30 events/sec)
    clicks    (~25 events/sec)
    reviews   (~10 events/sec)
"""

import sys
import os
import json
import time
import signal
import logging
import argparse
import threading
from datetime import datetime

# Allow imports from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config.kafka_config import (
    PRODUCER_CONFIG,
    TOPIC_ORDERS, TOPIC_PAYMENTS, TOPIC_CLICKS, TOPIC_REVIEWS, TOPIC_DLQ,
    ALL_TOPICS, EVENTS_PER_SECOND, TOPIC_DISTRIBUTION,
)
from producer.fake_data import (
    generate_order_event,
    generate_payment_event,
    generate_click_event,
    generate_review_event,
)

# ─────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/producer.log", mode="a"),
    ],
)
logger = logging.getLogger("producer")


# ─────────────────────────────────────────────
# Metrics (thread-safe counters)
# ─────────────────────────────────────────────

class Metrics:
    def __init__(self):
        self._lock    = threading.Lock()
        self.sent     = {t: 0 for t in ALL_TOPICS}
        self.errors   = {t: 0 for t in ALL_TOPICS}
        self.start_at = time.time()

    def record_sent(self, topic: str):
        with self._lock:
            self.sent[topic] += 1

    def record_error(self, topic: str):
        with self._lock:
            self.errors[topic] += 1

    def report(self) -> str:
        elapsed  = time.time() - self.start_at
        total    = sum(self.sent.values())
        rate     = total / elapsed if elapsed > 0 else 0
        lines    = [
            f"{'─'*55}",
            f" Runtime   : {elapsed:.1f}s",
            f" Total sent: {total:,}   ({rate:.1f} events/sec)",
            f"{'─'*55}",
        ]
        for t in ALL_TOPICS:
            lines.append(f"  {t:<12}: sent={self.sent[t]:>7,}  errors={self.errors[t]:>4}")
        lines.append(f"{'─'*55}")
        return "\n".join(lines)


metrics = Metrics()
stop_event = threading.Event()


# ─────────────────────────────────────────────
# Delivery Callback
# ─────────────────────────────────────────────

def delivery_report(err, msg):
    """Called by Kafka after each message is delivered or fails."""
    if err:
        logger.warning(f"Delivery failed | topic={msg.topic()} | error={err}")
        metrics.record_error(msg.topic())
    else:
        metrics.record_sent(msg.topic())
        logger.debug(f"Delivered | topic={msg.topic()} | partition={msg.partition()} | offset={msg.offset()}")


# ─────────────────────────────────────────────
# Producer Thread
# ─────────────────────────────────────────────

class TopicProducerThread(threading.Thread):
    """
    A dedicated thread that produces events for ONE Kafka topic.
    Sleep interval is calculated to match target events/sec.
    """

    def __init__(self, producer: Producer, topic: str, generator_fn, rate: float):
        super().__init__(name=f"thread-{topic}", daemon=True)
        self.producer    = producer
        self.topic       = topic
        self.generator   = generator_fn
        self.interval    = 1.0 / rate   # seconds between events
        self._errors_row = 0            # consecutive errors counter

    def run(self):
        logger.info(f"[{self.topic}] Producer thread started | rate={1/self.interval:.1f} events/sec")

        while not stop_event.is_set():
            loop_start = time.perf_counter()

            try:
                event   = self.generator()
                payload = json.dumps(event, default=str).encode("utf-8")
                key     = event.get("order_id") or event.get("payment_id") or event.get("click_id") or event.get("review_id")

                self.producer.produce(
                    topic     = self.topic,
                    key       = key.encode("utf-8") if key else None,
                    value     = payload,
                    callback  = delivery_report,
                )
                # Poll for delivery callbacks (non-blocking)
                self.producer.poll(0)
                self._errors_row = 0

            except KafkaException as e:
                self._errors_row += 1
                logger.error(f"[{self.topic}] KafkaException: {e} (consecutive={self._errors_row})")
                metrics.record_error(self.topic)

                if self._errors_row >= 5:
                    logger.critical(f"[{self.topic}] Too many consecutive errors. Pausing 10s...")
                    time.sleep(10)
                    self._errors_row = 0

            except Exception as e:
                logger.error(f"[{self.topic}] Unexpected error: {e}")
                metrics.record_error(self.topic)

            # Precise sleep to maintain target rate
            elapsed  = time.perf_counter() - loop_start
            sleep_ms = max(0, self.interval - elapsed)
            time.sleep(sleep_ms)

        logger.info(f"[{self.topic}] Thread stopped.")


# ─────────────────────────────────────────────
# Metrics Reporter Thread
# ─────────────────────────────────────────────

def metrics_reporter(interval: int = 10):
    """Logs a summary every N seconds."""
    while not stop_event.is_set():
        time.sleep(interval)
        logger.info(f"\n{metrics.report()}")


# ─────────────────────────────────────────────
# Graceful Shutdown
# ─────────────────────────────────────────────

def handle_shutdown(sig, frame):
    logger.info(f"\nReceived signal {sig}. Shutting down gracefully...")
    stop_event.set()


# ─────────────────────────────────────────────
# Kafka Connection with Retry
# ─────────────────────────────────────────────

def wait_for_kafka(max_retries: int = 20, delay: int = 5) -> Producer:
    """
    Try to connect to Kafka with exponential backoff.
    Raises RuntimeError if all retries fail.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Kafka (attempt {attempt}/{max_retries})...")
            admin  = AdminClient({"bootstrap.servers": PRODUCER_CONFIG["bootstrap.servers"]})
            topics = admin.list_topics(timeout=5)
            logger.info(f"Kafka connected! Topics available: {list(topics.topics.keys())}")
            return Producer(PRODUCER_CONFIG)
        except Exception as e:
            logger.warning(f"Kafka not ready: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 1.5, 30)   # cap at 30s

    raise RuntimeError("Failed to connect to Kafka after all retries.")


# ─────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="E-Commerce Kafka Event Producer")
    parser.add_argument("--rate",     type=int, default=EVENTS_PER_SECOND, help="Total events/sec (default: 100)")
    parser.add_argument("--duration", type=int, default=0, help="Stop after N seconds (0 = forever)")
    args = parser.parse_args()

    # Register signal handlers
    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("=" * 55)
    logger.info(" E-Commerce Kafka Event Producer")
    logger.info(f" Target rate : {args.rate} events/sec")
    logger.info(f" Kafka       : {PRODUCER_CONFIG['bootstrap.servers']}")
    logger.info("=" * 55)

    # Connect to Kafka
    producer = wait_for_kafka()

    # Build topic → (generator, rate) mapping
    topic_config = [
        (TOPIC_ORDERS,   generate_order_event,   int(args.rate * TOPIC_DISTRIBUTION[TOPIC_ORDERS])),
        (TOPIC_PAYMENTS, generate_payment_event, int(args.rate * TOPIC_DISTRIBUTION[TOPIC_PAYMENTS])),
        (TOPIC_CLICKS,   generate_click_event,   int(args.rate * TOPIC_DISTRIBUTION[TOPIC_CLICKS])),
        (TOPIC_REVIEWS,  generate_review_event,  int(args.rate * TOPIC_DISTRIBUTION[TOPIC_REVIEWS])),
    ]

    # Start producer threads
    threads = []
    for topic, gen_fn, rate in topic_config:
        if rate > 0:
            t = TopicProducerThread(producer, topic, gen_fn, rate)
            t.start()
            threads.append(t)

    # Start metrics reporter
    reporter = threading.Thread(target=metrics_reporter, args=(10,), daemon=True)
    reporter.start()

    # Wait for duration or until stopped
    try:
        if args.duration > 0:
            logger.info(f"Running for {args.duration} seconds...")
            stop_event.wait(timeout=args.duration)
            stop_event.set()
        else:
            logger.info("Running indefinitely. Press Ctrl+C to stop.")
            stop_event.wait()
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush(timeout=10)
        logger.info("\nFinal Report:")
        logger.info(metrics.report())
        logger.info("Producer stopped.")


if __name__ == "__main__":
    main()
