"""
src/quality_rules.py
====================
Deterministic quality-cleaning rules for individual order records.

Each rule has an explicit rule_code used in the audit trail.
Only deterministic, clearly-justified transformations are applied.
Ambiguous records are quarantined rather than guessed at.

Rule codes
----------
ARABIC_DIGITS          — Arabic-Indic digits → Western digits
CURRENCY_NORMALIZATION — Strip currency text (e.g. "5000 لاير" → 5000.0)
THOUSANDS_SEPARATOR    — Remove thousands commas (125,000.00 → 125000.00)
ARABIC_NUMBER_WORD     — Known Arabic number words → numeric value
PHONE_NORMALIZATION    — Normalize phone to 9 local digits
EMAIL_REPEATED_SYMBOLS — Repair user@@mail..com → user@mail.com
DATE_NORMALIZATION     — Normalize supported date formats to ISO
TRIM_NORMALIZATION     — Trim whitespace; normalize status/payment synonyms
TOTAL_RECALCULATION    — Recalculate total from items + delivery when valid

Quarantine codes
----------------
ID_ORDER_MISSING           — order_id is blank/absent
ID_CUSTOMER_MISSING        — customer_id is blank/absent
DATE_IMPOSSIBLE_INVALID    — order_date cannot be parsed to a valid date
JSON_ITEMS_CORRUPTED       — items_json is not valid JSON
ITEMS_EMPTY                — items list is empty
VALUE_NEGATIVE_AMBIGUOUS   — item quantity ≤ 0
PRICE_UNKNOWN              — numeric amount cannot be parsed after all rules
EMAIL_INVALID              — email cannot be repaired to valid format
ERRORS_CONFLICTING_MULTIPLE — record has 3+ independent quarantine reasons
"""

import json
import re
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Arabic-word → number mapping (explicit known values only)
# ---------------------------------------------------------------------------
_ARABIC_WORD_MAP: Dict[str, float] = {
    "صفر": 0,
    "واحد": 1,
    "اثنان": 2,
    "اثنين": 2,
    "ثلاثة": 3,
    "أربعة": 4,
    "اربعة": 4,
    "خمسة": 5,
    "ستة": 6,
    "سبعة": 7,
    "ثمانية": 8,
    "تسعة": 9,
    "عشرة": 10,
    "مئة": 100,
    "مائة": 100,
    "ألف": 1_000,
    "الف": 1_000,
    "ألفان": 2_000,
    "الفان": 2_000,
    "ثلاثة آلاف": 3_000,
    "ثلاثة الاف": 3_000,
    "أربعة آلاف": 4_000,
    "اربعة الاف": 4_000,
    "خمسة آلاف": 5_000,
    "خمسة الاف": 5_000,
    "عشرة آلاف": 10_000,
    "عشرة الاف": 10_000,
}

# Status synonym mapping (various Arabic/English forms → canonical English)
_STATUS_MAP: Dict[str, str] = {
    "مكتمل": "completed",
    "مكتملة": "completed",
    "completed": "completed",
    "معلق": "pending",
    "معلقة": "pending",
    "pending": "pending",
    "ملغي": "cancelled",
    "ملغاة": "cancelled",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "قيد التوصيل": "delivering",
    "delivering": "delivering",
    "مرتجع": "returned",
    "returned": "returned",
}

# Payment status synonym mapping
_PAYMENT_STATUS_MAP: Dict[str, str] = {
    "مدفوع": "paid",
    "مدفوعة": "paid",
    "paid": "paid",
    "غير مدفوع": "unpaid",
    "غير مدفوعة": "unpaid",
    "unpaid": "unpaid",
    "جزئي": "partial",
    "partial": "partial",
    "مسترجع": "refunded",
    "refunded": "refunded",
}


# ---------------------------------------------------------------------------
# Low-level normalization helpers
# ---------------------------------------------------------------------------

def _translate_arabic_digits(text: str) -> str:
    """Translate Arabic-Indic (Eastern Arabic) digits to Western digits."""
    table = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    return text.translate(table)


def _strip_currency_text(text: str) -> str:
    """
    Remove common Arabic/English currency suffixes.
    Examples: "5000 لاير" → "5000", "YER 1000" → "1000"
    """
    patterns = [
        r"\s*(لاير|ريال|دولار|dollar|yer|usd|sar|aed)\s*$",
        r"^\s*(لاير|ريال|دولار|dollar|yer|usd|sar|aed)\s*",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
    return text


def normalize_number(value: Any) -> Optional[float]:
    """
    Convert various numeric representations to a Python float.

    Applied rules (in order):
    1. ARABIC_NUMBER_WORD  — known Arabic word phrases
    2. ARABIC_DIGITS       — Arabic-Indic digit characters
    3. CURRENCY_NORMALIZATION — strip currency text suffixes/prefixes
    4. THOUSANDS_SEPARATOR — remove commas used as thousands separators
    5. Arabic decimal sep  — replace ٫ with .

    Returns None if the value cannot be parsed after all rules.
    """
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    # Rule: ARABIC_NUMBER_WORD
    if text in _ARABIC_WORD_MAP:
        return float(_ARABIC_WORD_MAP[text])

    # Rule: ARABIC_DIGITS
    text = _translate_arabic_digits(text)

    # Rule: CURRENCY_NORMALIZATION
    text = _strip_currency_text(text)

    # Rule: Arabic decimal separator (٫ → .)
    text = text.replace("٫", ".")

    # Rule: THOUSANDS_SEPARATOR
    # Only remove commas that act as thousands separators (digit,digit pattern)
    text = re.sub(r"(\d),(\d)", r"\1\2", text)

    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def normalize_date(value: Any) -> Optional[str]:
    """
    Normalize supported date formats to ISO 8601 (YYYY-MM-DDTHH:MM:SS).

    Supported input formats:
    - 2025-02-24T21:29:00  (already ISO — returned as-is after validation)
    - 2025-02-24 21:29:00  (space separator)
    - 24-02-2025 21:29:00  (day-first European/Arabic style)

    Returns None for unparseable or impossible dates.
    """
    if not value:
        return None

    text = str(value).strip()

    # Already ISO — validate then return
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ]

    # Strip timezone offset before parsing (we normalise to UTC-naive ISO)
    text_clean = re.sub(r"[+-]\d{2}:\d{2}$", "", text).strip()
    text_clean = re.sub(r"Z$", "", text_clean).strip()

    for fmt in formats:
        for candidate in (text_clean, text):
            try:
                dt = datetime.strptime(candidate, fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue

    return None


def normalize_phone(value: Any) -> Optional[str]:
    """
    Normalize a phone number to a 9-digit local Yemeni format.

    Accepted input forms:
    - 9-digit local number: 739988747
    - +967 prefix with optional spaces: +967 73 998 8747
    - 00967 prefix: 00967739988747

    Returns the 9-digit local string, or None if not normalizable.
    """
    if not value:
        return None

    text = str(value).strip()

    # Remove spaces, dashes, parentheses
    digits_only = re.sub(r"[\s\-\(\)\+]", "", text)

    # Arabic digits
    digits_only = _translate_arabic_digits(digits_only)

    if not digits_only.isdigit():
        return None

    # Strip country code prefixes
    if digits_only.startswith("00967"):
        digits_only = digits_only[5:]
    elif digits_only.startswith("967"):
        digits_only = digits_only[3:]

    if len(digits_only) == 9:
        return digits_only

    return None


def repair_email(value: Any) -> Optional[str]:
    """
    Repair obviously malformed email addresses:
    - Collapse repeated @ symbols: user@@domain → user@domain
    - Collapse repeated dots in domain: mail..com → mail.com
    - Trim whitespace

    Only repairs deterministic repeated-symbol errors.
    Returns None if result is still not a valid email.
    """
    if not value:
        return None

    text = str(value).strip()

    # Collapse repeated @ signs
    repaired = re.sub(r"@{2,}", "@", text)

    # Collapse repeated dots
    repaired = re.sub(r"\.{2,}", ".", repaired)

    # Remove leading/trailing dots in local part or domain
    if "@" in repaired:
        local, _, domain = repaired.partition("@")
        local = local.strip(".")
        domain = domain.strip(".")
        repaired = f"{local}@{domain}"

    if _is_valid_email(repaired):
        return repaired

    return None


def _is_valid_email(value: str) -> bool:
    """Basic structural email validation."""
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return bool(re.match(pattern, value))


def validate_items(items_json: Any) -> Tuple[Optional[List], List[str]]:
    """
    Parse and validate the items_json field.

    Returns (parsed_items, error_codes).
    error_codes is empty on success.
    """
    if not items_json:
        return None, ["ITEMS_EMPTY"]

    try:
        items = json.loads(items_json)
    except (json.JSONDecodeError, TypeError):
        return None, ["JSON_ITEMS_CORRUPTED"]

    if not isinstance(items, list):
        return None, ["JSON_ITEMS_CORRUPTED"]

    if len(items) == 0:
        return None, ["ITEMS_EMPTY"]

    errors: List[str] = []

    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            errors.append(f"JSON_ITEMS_CORRUPTED")
            continue

        qty = item.get("qty")
        try:
            qty_val = float(qty)
            if qty_val <= 0:
                errors.append("VALUE_NEGATIVE_AMBIGUOUS")
        except (ValueError, TypeError):
            errors.append("VALUE_NEGATIVE_AMBIGUOUS")

    if errors:
        return None, errors

    return items, []


def _compute_items_total(items: List[Dict]) -> Optional[float]:
    """Sum (qty * unit_price) for all items. Returns None on any error."""
    total = 0.0
    try:
        for item in items:
            qty = float(item.get("qty", 0))
            price = float(item.get("unit_price", 0))
            total += qty * price
        return total
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Main record processor
# ---------------------------------------------------------------------------

def process_record(record: Dict) -> Tuple[Dict, str]:
    """
    Apply all quality rules to one raw record.

    Parameters
    ----------
    record : dict
        Raw order record (as read from CSV or MongoDB raw collection).

    Returns
    -------
    (processed_record, classification)
        classification ∈ {"VALID", "CORRECTED", "QUARANTINED"}

    The returned dict contains:
    - All original + corrected fields
    - id_order  (canonical business key mapped from order_id)
    - quality_status
    - corrections  (list of {field, original_value, corrected_value, rule_code})
    - quarantine_reasons  (list of quarantine code strings)
    """

    result = deepcopy(record)
    corrections: List[Dict] = []
    quarantine_reasons: List[str] = []

    def add_correction(field: str, original: Any, corrected: Any, code: str):
        corrections.append({
            "field": field,
            "original_value": str(original),
            "corrected_value": str(corrected),
            "rule_code": code,
        })

    # -----------------------------------------------------------------------
    # Rule 0: Trim all string fields  (TRIM_NORMALIZATION)
    # -----------------------------------------------------------------------
    for key, val in result.items():
        if isinstance(val, str):
            stripped = val.strip()
            if stripped != val:
                add_correction(key, val, stripped, "TRIM_NORMALIZATION")
                result[key] = stripped

    # -----------------------------------------------------------------------
    # Rule 1: order_id → id_order  (business key mapping)
    # -----------------------------------------------------------------------
    raw_order_id = str(result.get("order_id", "")).strip()

    if not raw_order_id:
        quarantine_reasons.append("ID_ORDER_MISSING")
    else:
        result["id_order"] = raw_order_id

    # -----------------------------------------------------------------------
    # Rule 1b: customer_id check  (ID_CUSTOMER_MISSING)
    # -----------------------------------------------------------------------
    raw_customer_id = str(result.get("customer_id", "")).strip()

    if not raw_customer_id:
        quarantine_reasons.append("ID_CUSTOMER_MISSING")

    # -----------------------------------------------------------------------
    # Rule 2: Date normalization  (DATE_NORMALIZATION)
    # -----------------------------------------------------------------------
    original_date = result.get("order_date")
    normalized_date = normalize_date(original_date)

    if normalized_date is None:
        quarantine_reasons.append("DATE_IMPOSSIBLE_INVALID")
    else:
        canonical = str(original_date).strip() if original_date else ""
        if canonical != normalized_date:
            add_correction(
                "order_date", original_date, normalized_date, "DATE_NORMALIZATION"
            )
        result["order_date"] = normalized_date

    # -----------------------------------------------------------------------
    # Rule 3: Phone normalization  (PHONE_NORMALIZATION)
    # -----------------------------------------------------------------------
    raw_phone = str(result.get("customer_phone", "")).strip()
    normalized_phone = normalize_phone(raw_phone)

    if normalized_phone is None:
        quarantine_reasons.append("PRICE_UNKNOWN")  # re-use as PHONE_UNKNOWN
    else:
        if normalized_phone != raw_phone:
            add_correction(
                "customer_phone", raw_phone, normalized_phone, "PHONE_NORMALIZATION"
            )
        result["customer_phone"] = normalized_phone

    # -----------------------------------------------------------------------
    # Rule 4: Email repair  (EMAIL_REPEATED_SYMBOLS)
    # -----------------------------------------------------------------------
    raw_email = str(result.get("customer_email", "")).strip()

    if _is_valid_email(raw_email):
        result["customer_email"] = raw_email
    else:
        repaired = repair_email(raw_email)
        if repaired:
            add_correction(
                "customer_email", raw_email, repaired, "EMAIL_REPEATED_SYMBOLS"
            )
            result["customer_email"] = repaired
        else:
            quarantine_reasons.append("EMAIL_INVALID")

    # -----------------------------------------------------------------------
    # Rule 5: Numeric fields — Arabic digits, currency text, thousands seps
    #          (ARABIC_DIGITS, CURRENCY_NORMALIZATION, THOUSANDS_SEPARATOR,
    #           ARABIC_NUMBER_WORD)
    # -----------------------------------------------------------------------
    numeric_fields = ["delivery_cost", "payment_amount", "total_amount"]

    parsed_numerics: Dict[str, Optional[float]] = {}

    for field in numeric_fields:
        original_val = result.get(field)
        normalized_val = normalize_number(original_val)

        if normalized_val is None:
            quarantine_reasons.append("PRICE_UNKNOWN")
            parsed_numerics[field] = None
        else:
            parsed_numerics[field] = normalized_val
            original_str = str(original_val).strip() if original_val is not None else ""

            # Determine which rule code applies
            rule_code = _detect_numeric_rule(original_str)

            if original_str != str(normalized_val):
                add_correction(field, original_val, normalized_val, rule_code)
            result[field] = normalized_val

    # -----------------------------------------------------------------------
    # Rule 6: Items JSON validation
    # -----------------------------------------------------------------------
    items, item_errors = validate_items(result.get("items_json"))

    if item_errors:
        for err in item_errors:
            if err not in quarantine_reasons:
                quarantine_reasons.append(err)
    else:
        result["items_json"] = items

    # -----------------------------------------------------------------------
    # Rule 7: Total recalculation  (TOTAL_RECALCULATION)
    # Only when items are valid, delivery cost is valid, and total was
    # originally invalid OR differs from the computed value.
    # -----------------------------------------------------------------------
    if (
        items is not None
        and parsed_numerics.get("delivery_cost") is not None
        and not any(r == "PRICE_UNKNOWN" for r in quarantine_reasons)
    ):
        computed_total = _compute_items_total(items)
        delivery = parsed_numerics.get("delivery_cost", 0.0) or 0.0

        if computed_total is not None:
            expected_total = round(computed_total + delivery, 2)
            stored_total = parsed_numerics.get("total_amount")

            if stored_total is None:
                # Was quarantined due to bad total — recalculate
                add_correction(
                    "total_amount",
                    result.get("total_amount"),
                    expected_total,
                    "TOTAL_RECALCULATION",
                )
                result["total_amount"] = expected_total
                # Remove PRICE_UNKNOWN that was added for total_amount
                quarantine_reasons = [
                    r for r in quarantine_reasons if r != "PRICE_UNKNOWN"
                ]

    # -----------------------------------------------------------------------
    # Rule 8: Status normalization  (TRIM_NORMALIZATION)
    # -----------------------------------------------------------------------
    raw_status = str(result.get("status", "")).strip()
    canonical_status = _STATUS_MAP.get(raw_status)
    if canonical_status and canonical_status != raw_status:
        add_correction("status", raw_status, canonical_status, "TRIM_NORMALIZATION")
        result["status"] = canonical_status

    # -----------------------------------------------------------------------
    # Rule 9: Payment status normalization  (TRIM_NORMALIZATION)
    # -----------------------------------------------------------------------
    raw_pstatus = str(result.get("payment_status", "")).strip()
    canonical_pstatus = _PAYMENT_STATUS_MAP.get(raw_pstatus)
    if canonical_pstatus and canonical_pstatus != raw_pstatus:
        add_correction(
            "payment_status", raw_pstatus, canonical_pstatus, "TRIM_NORMALIZATION"
        )
        result["payment_status"] = canonical_pstatus

    # -----------------------------------------------------------------------
    # Classification
    # -----------------------------------------------------------------------

    # Mark records with 3+ independent quarantine reasons as conflicting
    unique_reasons = list(dict.fromkeys(quarantine_reasons))
    if len(unique_reasons) >= 3 and "ERRORS_CONFLICTING_MULTIPLE" not in unique_reasons:
        unique_reasons.append("ERRORS_CONFLICTING_MULTIPLE")
        quarantine_reasons = unique_reasons

    if quarantine_reasons:
        classification = "QUARANTINED"
    elif corrections:
        classification = "CORRECTED"
    else:
        classification = "VALID"

    result["quality_status"] = classification.lower()
    result["classification"] = classification

    if corrections:
        result["corrections"] = corrections

    if quarantine_reasons:
        result["quarantine_reasons"] = list(dict.fromkeys(quarantine_reasons))

    return result, classification


def _detect_numeric_rule(original_str: str) -> str:
    """Determine which rule code best describes a numeric transformation."""
    if any(c in "٠١٢٣٤٥٦٧٨٩" for c in original_str):
        return "ARABIC_DIGITS"

    for word in _ARABIC_WORD_MAP:
        if word in original_str:
            return "ARABIC_NUMBER_WORD"

    currency_terms = ["لاير", "ريال", "دولار", "dollar", "yer", "usd", "sar"]
    if any(t in original_str.lower() for t in currency_terms):
        return "CURRENCY_NORMALIZATION"

    if re.search(r"\d,\d", original_str):
        return "THOUSANDS_SEPARATOR"

    return "ARABIC_DIGITS"  # fallback
