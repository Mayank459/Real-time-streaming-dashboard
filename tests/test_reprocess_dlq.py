"""
test_reprocess_dlq.py
Unit tests for DLQ remediation logic.
"""

import sys
import os
import json
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.reprocess_dlq import remediate_payload


class TestDLQRemediation:

    def test_remediate_missing_schema_version(self):
        raw_msg = json.dumps({
            "order_id": "ORD-123",
            "user_id": "USR-456",
            "product_id": "P001",
            "price": 1000.0,
            "quantity": 2,
            "total_amount": 2000.0,
            "country": "India",
            "category": "Electronics",
            "timestamp": "2025-01-01T10:00:00Z"
        })
        success, payload, notes = remediate_payload(raw_msg, "orders")
        assert success is True
        assert payload["schema_version"] == "1.0"
        assert "schema_version" in notes

    def test_remediate_invalid_quantity(self):
        raw_msg = json.dumps({
            "order_id": "ORD-123",
            "user_id": "USR-456",
            "product_id": "P001",
            "price": 500.0,
            "quantity": 0,
            "total_amount": 0.0,
            "country": "India",
            "category": "Electronics",
            "timestamp": "2025-01-01T10:00:00Z"
        })
        success, payload, notes = remediate_payload(raw_msg, "orders")
        assert success is True
        assert payload["quantity"] == 1
        assert payload["total_amount"] == 500.0

    def test_remediate_unparseable_json(self):
        raw_msg = "{bad json: invalid syntax"
        success, payload, notes = remediate_payload(raw_msg, "orders")
        assert success is False
        assert "failed" in notes.lower()
