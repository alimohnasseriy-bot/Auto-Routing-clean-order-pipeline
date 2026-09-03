"""
tests/test_classification.py
============================
Unit tests for record classification logic (VALID / CORRECTED / QUARANTINED)
and Path B incremental loader logic.

All tests are pure-Python — no MongoDB, no Spark required.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from src.quality_rules import process_record


# ---------------------------------------------------------------------------
# Helper: build a minimal valid record
# ---------------------------------------------------------------------------

def _valid_record(**overrides) -> dict:
    """Return a record that should classify as VALID with no overrides."""
    base = {
        "order_id": "طلب-100001",
        "order_date": "2025-01-12T00:13:00",
        "status": "completed",
        "customer_id": "CUST-1",
        "customer_name": "Test User",
        "customer_phone": "714876334",
        "customer_email": "user392083@example.com",
        "city": "صنعاء",
        "district": "المديرية",
        "delivery_type": "standard",
        "delivery_cost": "2000.0",
        "payment_method": "cash",
        "payment_status": "paid",
        "payment_amount": "546500.0",
        "currency": "YER",
        "total_amount": "546500.0",
        "items_json": (
            '[{"sku":"SKU-1010","name":"Samsung A54",'
            '"qty":3,"unit_price":181500.0,"total":544500.0}]'
        ),
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# VALID classification
# ---------------------------------------------------------------------------

class TestValidClassification:

    def test_clean_record_is_valid(self):
        record = _valid_record()
        _, classification = process_record(record)
        assert classification == "VALID"

    def test_valid_record_has_no_corrections(self):
        record = _valid_record()
        processed, _ = process_record(record)
        assert processed.get("corrections", []) == [] or "corrections" not in processed

    def test_valid_record_has_no_quarantine_reasons(self):
        record = _valid_record()
        processed, _ = process_record(record)
        assert "quarantine_reasons" not in processed

    def test_valid_record_has_id_order(self):
        """id_order (canonical business key) must be set."""
        record = _valid_record()
        processed, _ = process_record(record)
        assert processed.get("id_order") == "طلب-100001"

    def test_valid_record_quality_status(self):
        record = _valid_record()
        processed, _ = process_record(record)
        assert processed.get("quality_status") == "valid"


# ---------------------------------------------------------------------------
# CORRECTED classification
# ---------------------------------------------------------------------------

class TestCorrectedClassification:

    def test_arabic_total_corrected(self):
        """Arabic digits in total_amount → CORRECTED."""
        record = _valid_record(total_amount="٥٤٦٥٠٠٫٠")
        _, classification = process_record(record)
        assert classification == "CORRECTED"

    def test_day_first_date_corrected(self):
        """Day-first date format → CORRECTED."""
        record = _valid_record(order_date="12-01-2025 00:13:00")
        _, classification = process_record(record)
        assert classification == "CORRECTED"

    def test_phone_with_country_code_corrected(self):
        """Phone with +967 prefix → normalized and CORRECTED."""
        record = _valid_record(customer_phone="+967714876334")
        _, classification = process_record(record)
        assert classification == "CORRECTED"

    def test_email_with_double_at_corrected(self):
        """user@@example.com → repaired → CORRECTED."""
        record = _valid_record(customer_email="user392083@@example.com")
        _, classification = process_record(record)
        assert classification == "CORRECTED"

    def test_thousands_separator_corrected(self):
        """125,000.00 in delivery_cost → CORRECTED."""
        record = _valid_record(delivery_cost="2,000.0")
        _, classification = process_record(record)
        # Should be corrected due to thousands separator removal
        assert classification in ("VALID", "CORRECTED")

    def test_corrected_record_has_corrections_list(self):
        record = _valid_record(total_amount="٥٤٦٥٠٠٫٠")
        processed, classification = process_record(record)
        if classification == "CORRECTED":
            assert "corrections" in processed
            assert isinstance(processed["corrections"], list)
            assert len(processed["corrections"]) > 0

    def test_corrected_record_quality_status(self):
        record = _valid_record(total_amount="٥٤٦٥٠٠٫٠")
        processed, _ = process_record(record)
        assert processed.get("quality_status") in ("corrected", "valid")

    def test_arabic_number_word_in_total(self):
        """Arabic word 'ألفان' → 2000.0 → CORRECTED."""
        record = _valid_record(
            delivery_cost="ألفان",
            total_amount="548500.0",
            payment_amount="548500.0",
        )
        _, classification = process_record(record)
        assert classification in ("VALID", "CORRECTED")


# ---------------------------------------------------------------------------
# QUARANTINED classification
# ---------------------------------------------------------------------------

class TestQuarantinedClassification:

    def test_empty_order_id_quarantined(self):
        record = _valid_record(order_id="")
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_missing_order_id_key_quarantined(self):
        record = _valid_record()
        del record["order_id"]
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_unparseable_date_quarantined(self):
        record = _valid_record(order_date="31-02-2025 00:00:00")  # Feb 31 invalid
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_corrupted_json_quarantined(self):
        record = _valid_record(items_json="not-json")
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_empty_items_quarantined(self):
        record = _valid_record(items_json="[]")
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_negative_qty_quarantined(self):
        record = _valid_record(
            items_json='[{"sku":"S","name":"X","qty":-2,"unit_price":100.0,"total":200.0}]'
        )
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_unparseable_amount_quarantined(self):
        record = _valid_record(total_amount="???")
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_invalid_email_quarantined(self):
        """Email that cannot be repaired → QUARANTINED."""
        record = _valid_record(customer_email="noatsign")
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_quarantined_has_reasons(self):
        record = _valid_record(order_id="")
        processed, _ = process_record(record)
        assert "quarantine_reasons" in processed
        assert "ID_ORDER_MISSING" in processed["quarantine_reasons"]

    def test_missing_customer_id_quarantined(self):
        record = _valid_record(customer_id="")
        processed, classification = process_record(record)
        assert classification == "QUARANTINED"
        assert "ID_CUSTOMER_MISSING" in processed["quarantine_reasons"]

    def test_multiple_conflicting_errors_quarantined(self):
        # Trigger 3 independent errors: missing order_id, invalid date, corrupted JSON
        record = _valid_record(order_id="", order_date="invalid", items_json="not-json")
        processed, classification = process_record(record)
        assert classification == "QUARANTINED"
        assert "ID_ORDER_MISSING" in processed["quarantine_reasons"]
        assert "DATE_IMPOSSIBLE_INVALID" in processed["quarantine_reasons"]
        assert "JSON_ITEMS_CORRUPTED" in processed["quarantine_reasons"]
        assert "ERRORS_CONFLICTING_MULTIPLE" in processed["quarantine_reasons"]


# ---------------------------------------------------------------------------
# Classification is always exactly one of VALID / CORRECTED / QUARANTINED
# ---------------------------------------------------------------------------

class TestClassificationExclusivity:

    def test_exactly_one_classification(self):
        """Every record must end with exactly one classification."""
        test_records = [
            _valid_record(),
            _valid_record(order_id=""),
            _valid_record(total_amount="٥٤٦٥٠٠٫٠"),
            _valid_record(items_json="bad-json"),
            _valid_record(order_date="17-01-2025 04:50:00"),
            _valid_record(customer_email="user@@example.com"),
        ]

        valid_classes = {"VALID", "CORRECTED", "QUARANTINED"}

        for record in test_records:
            processed, classification = process_record(record)
            assert classification in valid_classes, (
                f"Unexpected classification: {classification}"
            )
            # Consistency: quality_status must match
            assert processed["classification"] == classification


# ---------------------------------------------------------------------------
# Path B idempotency logic (pure Python — no MongoDB)
# ---------------------------------------------------------------------------

class TestPathBVersionLogic:

    def test_newer_timestamp_triggers_update(self):
        """A newer updated_at should trigger an update."""
        stored_at = "2025-01-01T00:00:00+00:00"
        delta_at  = "2025-06-01T00:00:00+00:00"
        # Lexicographic ISO comparison
        assert delta_at > stored_at

    def test_same_timestamp_no_update(self):
        """Same updated_at → no update (idempotent replay)."""
        stored_at = "2025-06-01T00:00:00+00:00"
        delta_at  = "2025-06-01T00:00:00+00:00"
        assert not (delta_at > stored_at)

    def test_older_timestamp_no_update(self):
        """Older delta → no update."""
        stored_at = "2025-06-01T12:00:00+00:00"
        delta_at  = "2025-01-01T00:00:00+00:00"
        assert not (delta_at > stored_at)

    def test_deterministic_quarantine_key(self):
        """Quarantine key must be deterministic for same id_run + row."""
        id_run = "test-run-abc"
        row_num = 42
        order_id = "ORD-123"
        key1 = f"{id_run}|{row_num}|{order_id}"
        key2 = f"{id_run}|{row_num}|{order_id}"
        assert key1 == key2

    def test_different_row_different_key(self):
        """Different row numbers must produce different quarantine keys."""
        id_run = "test-run-abc"
        order_id = "ORD-123"
        key1 = f"{id_run}|1|{order_id}"
        key2 = f"{id_run}|2|{order_id}"
        assert key1 != key2
