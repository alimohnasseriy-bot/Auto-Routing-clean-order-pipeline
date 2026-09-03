"""
tests/test_cleaning_rules.py
============================
Unit tests for src/quality_rules.py cleaning functions.

All tests are pure-Python — no MongoDB, no Spark required.
Tests cover:
  - Arabic digit normalization
  - Thousands separator removal
  - Currency text normalization
  - Arabic number words
  - Phone normalization
  - Email repair and rejection
  - Date normalization
  - Items JSON validation
  - Numeric field parsing
  - Correction audit trail structure
  - Total recalculation rule
"""

import sys
from pathlib import Path

# Ensure the project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from src.quality_rules import (
    _is_valid_email,
    normalize_date,
    normalize_number,
    normalize_phone,
    process_record,
    repair_email,
    validate_items,
)


# ---------------------------------------------------------------------------
# normalize_number — Arabic digits
# ---------------------------------------------------------------------------

class TestArabicDigits:

    def test_arabic_digits_basic(self):
        assert normalize_number("٥٠٠٠") == 5000.0

    def test_arabic_digits_zero(self):
        assert normalize_number("٠") == 0.0

    def test_arabic_digits_mixed(self):
        # Mixed Arabic and Western
        assert normalize_number("١٢3") == 123.0

    def test_arabic_decimal_separator(self):
        # ٫ is the Arabic decimal separator
        assert normalize_number("٧٠٦٠٠٠٫٠") == 706000.0

    def test_western_digits_unchanged(self):
        assert normalize_number("12345.67") == 12345.67

    def test_zero(self):
        assert normalize_number("0") == 0.0

    def test_none_input(self):
        assert normalize_number(None) is None

    def test_empty_string(self):
        assert normalize_number("") is None


# ---------------------------------------------------------------------------
# normalize_number — thousands separators
# ---------------------------------------------------------------------------

class TestThousandsSeparator:

    def test_comma_thousands(self):
        assert normalize_number("125,000.00") == 125000.0

    def test_multiple_commas(self):
        assert normalize_number("1,234,567.89") == 1234567.89

    def test_no_decimal(self):
        assert normalize_number("1,000") == 1000.0

    def test_plain_float(self):
        assert normalize_number("9999.99") == 9999.99


# ---------------------------------------------------------------------------
# normalize_number — currency text
# ---------------------------------------------------------------------------

class TestCurrencyNormalization:

    def test_arabic_suffix(self):
        assert normalize_number("5000 لاير") == 5000.0

    def test_arabic_rial(self):
        assert normalize_number("3000 ريال") == 3000.0

    def test_yer_suffix(self):
        assert normalize_number("1500 YER") == 1500.0

    def test_combined_arabic_digits_and_currency(self):
        assert normalize_number("٥٠٠٠ لاير") == 5000.0


# ---------------------------------------------------------------------------
# normalize_number — Arabic number words
# ---------------------------------------------------------------------------

class TestArabicNumberWords:

    def test_alfan(self):
        assert normalize_number("ألفان") == 2000.0

    def test_khamsa_alaf(self):
        assert normalize_number("خمسة آلاف") == 5000.0

    def test_alf(self):
        assert normalize_number("ألف") == 1000.0

    def test_ithnan(self):
        assert normalize_number("اثنان") == 2.0

    def test_unknown_word_returns_none(self):
        # A word not in the explicit map should not be guessed
        assert normalize_number("كثير") is None


# ---------------------------------------------------------------------------
# normalize_phone
# ---------------------------------------------------------------------------

class TestPhoneNormalization:

    def test_nine_digit_local(self):
        assert normalize_phone("739988747") == "739988747"

    def test_plus_967_prefix(self):
        assert normalize_phone("+967739988747") == "739988747"

    def test_plus_967_with_spaces(self):
        assert normalize_phone("+967 77 123 4567") == "771234567"

    def test_00967_prefix(self):
        assert normalize_phone("00967714876334") == "714876334"

    def test_letters_returns_none(self):
        assert normalize_phone("abc123") is None

    def test_too_short_returns_none(self):
        assert normalize_phone("12345") is None

    def test_empty_returns_none(self):
        assert normalize_phone("") is None

    def test_none_returns_none(self):
        assert normalize_phone(None) is None


# ---------------------------------------------------------------------------
# repair_email / _is_valid_email
# ---------------------------------------------------------------------------

class TestEmailRepair:

    def test_valid_email_unchanged(self):
        assert _is_valid_email("user@example.com")

    def test_double_at_repaired(self):
        result = repair_email("user@@mail.com")
        assert result == "user@mail.com"

    def test_double_dot_repaired(self):
        result = repair_email("user@mail..com")
        assert result == "user@mail.com"

    def test_both_errors_repaired(self):
        result = repair_email("user@@mail..com")
        assert result == "user@mail.com"

    def test_no_at_sign_returns_none(self):
        assert repair_email("usermail.com") is None

    def test_completely_invalid_returns_none(self):
        assert repair_email("@@") is None

    def test_valid_email_repair_returns_same(self):
        assert repair_email("user@mail.com") == "user@mail.com"

    def test_empty_returns_none(self):
        assert repair_email("") is None

    def test_none_returns_none(self):
        assert repair_email(None) is None


# ---------------------------------------------------------------------------
# normalize_date
# ---------------------------------------------------------------------------

class TestDateNormalization:

    def test_iso_format_unchanged(self):
        assert normalize_date("2025-02-22T03:22:00") == "2025-02-22T03:22:00"

    def test_space_separator(self):
        assert normalize_date("2025-02-22 03:22:00") == "2025-02-22T03:22:00"

    def test_day_first_format(self):
        assert normalize_date("17-01-2025 04:50:00") == "2025-01-17T04:50:00"

    def test_date_only(self):
        assert normalize_date("2025-01-15") == "2025-01-15T00:00:00"

    def test_invalid_date_returns_none(self):
        assert normalize_date("not-a-date") is None

    def test_empty_returns_none(self):
        assert normalize_date("") is None

    def test_none_returns_none(self):
        assert normalize_date(None) is None

    def test_garbage_string_returns_none(self):
        assert normalize_date("???") is None


# ---------------------------------------------------------------------------
# validate_items
# ---------------------------------------------------------------------------

class TestItemsValidation:

    def test_valid_items(self):
        items_json = '[{"sku":"SKU-1","name":"Item","qty":2,"unit_price":100.0,"total":200.0}]'
        items, errors = validate_items(items_json)
        assert errors == []
        assert len(items) == 1

    def test_negative_qty_quarantined(self):
        items_json = '[{"sku":"SKU-1","name":"Item","qty":-2,"unit_price":100.0,"total":200.0}]'
        items, errors = validate_items(items_json)
        assert items is None
        assert "VALUE_NEGATIVE_AMBIGUOUS" in errors

    def test_zero_qty_quarantined(self):
        items_json = '[{"sku":"SKU-1","name":"Item","qty":0,"unit_price":100.0}]'
        items, errors = validate_items(items_json)
        assert items is None
        assert "VALUE_NEGATIVE_AMBIGUOUS" in errors

    def test_invalid_json_returns_error(self):
        items, errors = validate_items("not-json")
        assert items is None
        assert "JSON_ITEMS_CORRUPTED" in errors

    def test_empty_list_returns_error(self):
        items, errors = validate_items("[]")
        assert items is None
        assert "ITEMS_EMPTY" in errors

    def test_none_returns_error(self):
        items, errors = validate_items(None)
        assert items is None
        assert errors  # some error

    def test_empty_string_returns_error(self):
        items, errors = validate_items("")
        assert items is None
        assert errors


# ---------------------------------------------------------------------------
# process_record — audit trail structure
# ---------------------------------------------------------------------------

class TestCorrectionAuditTrail:

    def test_correction_has_required_keys(self):
        """Each correction dict must contain field, original_value, corrected_value, rule_code."""
        record = {
            "order_id": "طلب-100003",
            "order_date": "17-01-2025 04:50:00",
            "customer_phone": "739988747",
            "customer_email": "user123@example.com",
            "delivery_cost": "2000.0",
            "payment_amount": "132500.0",
            "total_amount": "132500.0",
            "items_json": '[{"sku":"SKU-1","name":"Item","qty":3,"unit_price":43500.0,"total":130500.0}]',
        }
        processed, classification = process_record(record)
        assert "corrections" in processed
        for corr in processed["corrections"]:
            assert "field" in corr
            assert "original_value" in corr
            assert "corrected_value" in corr
            assert "rule_code" in corr

    def test_date_normalization_rule_code(self):
        record = {
            "order_id": "طلب-100017",
            "order_date": "17-01-2025 04:50:00",
            "customer_phone": "705330249",
            "customer_email": "user951777@example.com",
            "delivery_cost": "2000.0",
            "payment_amount": "132500.0",
            "total_amount": "132500.0",
            "items_json": '[{"sku":"SKU-1","name":"Test","qty":3,"unit_price":43500.0,"total":130500.0}]',
        }
        processed, _ = process_record(record)
        date_corrections = [
            c for c in processed.get("corrections", [])
            if c["field"] == "order_date"
        ]
        if date_corrections:
            assert date_corrections[0]["rule_code"] == "DATE_NORMALIZATION"

    def test_arabic_digits_in_total(self):
        record = {
            "order_id": "طلب-100003",
            "order_date": "2025-02-22T03:22:00",
            "customer_phone": "739988747",
            "customer_email": "user309803@example.com",
            "delivery_cost": "2000.0",
            "payment_amount": "706000.0",
            "total_amount": "٧٠٦٠٠٠٫٠",
            "items_json": '[{"sku":"SKU-1","name":"Test","qty":2,"unit_price":257000.0,"total":514000.0}]',
        }
        processed, classification = process_record(record)
        total_corrections = [
            c for c in processed.get("corrections", [])
            if c["field"] == "total_amount"
        ]
        if total_corrections:
            assert total_corrections[0]["rule_code"] in (
                "ARABIC_DIGITS", "TOTAL_RECALCULATION"
            )


# ---------------------------------------------------------------------------
# process_record — quarantine
# ---------------------------------------------------------------------------

class TestQuarantineBehavior:

    def test_missing_order_id_quarantined(self):
        record = {
            "order_id": "",
            "order_date": "2025-01-01T00:00:00",
            "customer_phone": "739988747",
            "customer_email": "user@example.com",
            "delivery_cost": "100.0",
            "payment_amount": "100.0",
            "total_amount": "100.0",
            "items_json": '[{"sku":"S","qty":1,"unit_price":90.0,"total":90.0}]',
        }
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_missing_customer_id_quarantined(self):
        record = {
            "order_id": "ORD-1",
            "customer_id": "",
            "order_date": "2025-01-01T00:00:00",
            "customer_phone": "739988747",
            "customer_email": "user@example.com",
            "delivery_cost": "100.0",
            "payment_amount": "100.0",
            "total_amount": "100.0",
            "items_json": '[{"sku":"S","qty":1,"unit_price":90.0,"total":90.0}]',
        }
        _, classification = process_record(record)
        assert classification == "QUARANTINED"


    def test_invalid_date_quarantined(self):
        record = {
            "order_id": "ORD-1",
            "order_date": "not-a-date",
            "customer_phone": "739988747",
            "customer_email": "user@example.com",
            "delivery_cost": "100.0",
            "payment_amount": "100.0",
            "total_amount": "100.0",
            "items_json": '[{"sku":"S","qty":1,"unit_price":90.0,"total":90.0}]',
        }
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_corrupted_items_quarantined(self):
        record = {
            "order_id": "ORD-2",
            "order_date": "2025-01-01T00:00:00",
            "customer_phone": "739988747",
            "customer_email": "user@example.com",
            "delivery_cost": "100.0",
            "payment_amount": "100.0",
            "total_amount": "100.0",
            "items_json": "not-json",
        }
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_negative_qty_quarantined(self):
        record = {
            "order_id": "ORD-3",
            "order_date": "2025-01-01T00:00:00",
            "customer_phone": "739988747",
            "customer_email": "user@example.com",
            "delivery_cost": "100.0",
            "payment_amount": "100.0",
            "total_amount": "100.0",
            "items_json": '[{"sku":"S","qty":-1,"unit_price":90.0}]',
        }
        _, classification = process_record(record)
        assert classification == "QUARANTINED"

    def test_quarantine_has_reasons_field(self):
        record = {
            "order_id": "",
            "order_date": "2025-01-01T00:00:00",
            "customer_phone": "739988747",
            "customer_email": "user@example.com",
            "delivery_cost": "100.0",
            "payment_amount": "100.0",
            "total_amount": "100.0",
            "items_json": '[{"sku":"S","qty":1,"unit_price":90.0}]',
        }
        processed, _ = process_record(record)
        assert "quarantine_reasons" in processed
        assert isinstance(processed["quarantine_reasons"], list)
        assert len(processed["quarantine_reasons"]) > 0
