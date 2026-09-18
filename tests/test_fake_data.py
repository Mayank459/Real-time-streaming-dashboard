"""
test_fake_data.py
Unit tests for the fake data generators.
Run: pytest tests/test_fake_data.py -v
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.fake_data import (
    generate_order_event,
    generate_payment_event,
    generate_click_event,
    generate_review_event,
)


class TestOrderEvent:
    """Tests for generate_order_event()"""

    def setup_method(self):
        self.event = generate_order_event()

    def test_required_fields_present(self):
        required = ["schema_version", "order_id", "user_id", "product_id", "product_name",
                    "category", "price", "quantity", "total_amount",
                    "country", "device", "browser", "status", "timestamp", "event_timestamp_ms"]
        for field in required:
            assert field in self.event, f"Missing field: {field}"
        assert self.event["schema_version"] == "1.0"
        assert self.event["event_timestamp_ms"] > 0

    def test_price_positive(self):
        assert self.event["price"] > 0

    def test_quantity_positive(self):
        assert self.event["quantity"] >= 1

    def test_total_amount_equals_price_times_quantity(self):
        expected = round(self.event["price"] * self.event["quantity"], 2)
        assert abs(self.event["total_amount"] - expected) < 0.01

    def test_order_id_is_uuid(self):
        import uuid
        try:
            uuid.UUID(self.event["order_id"])
        except ValueError:
            pytest.fail("order_id is not a valid UUID")

    def test_device_is_valid(self):
        assert self.event["device"] in ["mobile", "desktop", "tablet"]

    def test_status_is_valid(self):
        valid_statuses = ["confirmed", "processing", "shipped", "delivered", "cancelled"]
        assert self.event["status"] in valid_statuses

    def test_event_type(self):
        assert self.event["event_type"] == "order"

    def test_multiple_events_are_unique(self):
        events = [generate_order_event() for _ in range(50)]
        order_ids = [e["order_id"] for e in events]
        assert len(set(order_ids)) == 50, "Duplicate order_ids generated"


class TestPaymentEvent:
    """Tests for generate_payment_event()"""

    def setup_method(self):
        self.event = generate_payment_event()

    def test_required_fields_present(self):
        required = ["schema_version", "payment_id", "order_id", "user_id", "payment_type",
                    "amount", "status", "timestamp", "event_timestamp_ms"]
        for field in required:
            assert field in self.event, f"Missing field: {field}"
        assert self.event["schema_version"] == "1.0"
        assert self.event["event_timestamp_ms"] > 0

    def test_amount_positive(self):
        assert self.event["amount"] > 0

    def test_payment_type_valid(self):
        valid_types = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]
        assert self.event["payment_type"] in valid_types

    def test_status_valid(self):
        assert self.event["status"] in ["success", "failed", "pending"]

    def test_linked_payment(self):
        """Test that linked order_id is preserved."""
        linked = generate_payment_event(order_id="TEST-ORDER-123", amount=1000.0)
        assert linked["order_id"] == "TEST-ORDER-123"
        assert linked["amount"] == 1000.0

    def test_event_type(self):
        assert self.event["event_type"] == "payment"


class TestClickEvent:
    """Tests for generate_click_event()"""

    def setup_method(self):
        self.event = generate_click_event()

    def test_required_fields_present(self):
        required = ["schema_version", "click_id", "user_id", "product_id", "session_id",
                    "page", "action", "timestamp", "event_timestamp_ms"]
        for field in required:
            assert field in self.event, f"Missing field: {field}"
        assert self.event["schema_version"] == "1.0"
        assert self.event["event_timestamp_ms"] > 0

    def test_page_is_valid(self):
        valid_pages = ["home", "search", "product", "cart", "checkout", "wishlist"]
        assert self.event["page"] in valid_pages

    def test_duration_non_negative(self):
        assert self.event["duration_sec"] >= 0

    def test_event_type(self):
        assert self.event["event_type"] == "click"


class TestReviewEvent:
    """Tests for generate_review_event()"""

    def setup_method(self):
        self.event = generate_review_event()

    def test_required_fields_present(self):
        required = ["schema_version", "review_id", "user_id", "product_id", "rating",
                    "sentiment", "verified", "timestamp", "event_timestamp_ms"]
        for field in required:
            assert field in self.event, f"Missing field: {field}"
        assert self.event["schema_version"] == "1.0"
        assert self.event["event_timestamp_ms"] > 0

    def test_rating_between_1_and_5(self):
        assert 1 <= self.event["rating"] <= 5

    def test_sentiment_matches_rating(self):
        rating    = self.event["rating"]
        sentiment = self.event["sentiment"]
        if rating >= 4:
            assert sentiment == "positive"
        elif rating == 3:
            assert sentiment == "neutral"
        else:
            assert sentiment == "negative"

    def test_verified_is_boolean(self):
        assert isinstance(self.event["verified"], bool)

    def test_event_type(self):
        assert self.event["event_type"] == "review"


class TestEventRate:
    """Performance and volume tests."""

    def test_generate_1000_orders_quickly(self):
        """Generating 1000 events should complete in under 2 seconds."""
        import time
        start = time.time()
        events = [generate_order_event() for _ in range(1000)]
        elapsed = time.time() - start
        assert elapsed < 2.0, f"Too slow: {elapsed:.2f}s for 1000 events"
        assert len(events) == 1000
