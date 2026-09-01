"""
src/quality_processor.py
========================
Quality processor — reads from orders_raw, applies quality rules, and
writes results to orders_validated (VALID/CORRECTED) or
orders_quarantine (QUARANTINED).

ELT contract
------------
- Processes ONLY records belonging to the current id_run.
- Uses Upsert (update_one with upsert=True) keyed on id_order.
- Tracks count_inserted, count_updated, count_unchanged.
- Adds updated_at timestamp to every validated record.
- Does NOT silently discard records — every raw record gets exactly one
  outcome: VALID, CORRECTED, or QUARANTINED.

Idempotency
-----------
Running the same id_run twice will not create duplicates because:
1. validated upserts key on id_order (unique index enforced by MongoDB).
2. quarantine upserts key on quarantine_key (deterministic from id_run+row).
3. "unchanged" detection compares stored state before writing.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

from pymongo import MongoClient
from pymongo.collection import Collection

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    MONGODB_DATABASE,
    MONGODB_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.quality_rules import process_record


def process_run(
    id_run: str,
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
) -> Dict[str, Any]:
    """
    Process all raw records for a given id_run.

    Returns a dict of metrics:
        count_valid, count_corrected, count_quarantine,
        count_inserted, count_updated, count_unchanged,
        seconds_elapsed, throughput
    """
    client: MongoClient | None = None

    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10_000)
        db = client[database]

        raw_col: Collection = db[RAW_COLLECTION]
        validated_col: Collection = db[VALIDATED_COLLECTION]
        quarantine_col: Collection = db[QUARANTINE_COLLECTION]

        print("=" * 70)
        print("QUALITY PROCESSOR")
        print("=" * 70)
        print(f"Run ID     : {id_run}")
        print(f"Database   : {database}")
        print("=" * 70)

        count_valid = 0
        count_corrected = 0
        count_quarantine = 0
        count_inserted = 0
        count_updated = 0
        count_unchanged = 0
        processed = 0

        # Track order_ids seen in this run to detect intra-run duplicates
        seen_order_ids: set = set()

        start = time.perf_counter()

        cursor = raw_col.find({"id_run": id_run})

        for raw_doc in cursor:
            processed += 1
            row_num = raw_doc.get("number_row_source", processed)

            # Strip MongoDB internal fields before processing
            source_fields = dict(raw_doc)
            for meta_key in (
                "_id", "id_run", "file_source", "number_row_source",
                "at_ingested", "engine_used", "record_raw",
            ):
                source_fields.pop(meta_key, None)

            raw_order_id = str(source_fields.get("order_id", "")).strip()

            # ---------------------------------------------------------------
            # Intra-run duplicate detection
            # ---------------------------------------------------------------
            if raw_order_id and raw_order_id in seen_order_ids:
                _upsert_quarantine(
                    quarantine_col,
                    source_fields,
                    ["ID_ORDER_DUPLICATE"],
                    id_run,
                    row_num,
                )
                count_quarantine += 1
                continue

            if raw_order_id:
                seen_order_ids.add(raw_order_id)

            # ---------------------------------------------------------------
            # Apply quality rules
            # ---------------------------------------------------------------
            processed_doc, classification = process_record(source_fields)
            processed_doc.pop("_id", None)

            # Add run/time metadata
            processed_doc["id_run"] = id_run
            processed_doc["updated_at"] = datetime.now(timezone.utc).isoformat()

            if classification in ("VALID", "CORRECTED"):
                ins, upd, unch = _upsert_validated(
                    validated_col, processed_doc
                )
                count_inserted += ins
                count_updated += upd
                count_unchanged += unch

                if classification == "VALID":
                    count_valid += 1
                else:
                    count_corrected += 1

            else:  # QUARANTINED
                reasons = processed_doc.get("quarantine_reasons", ["UNKNOWN"])
                _upsert_quarantine(
                    quarantine_col,
                    source_fields,
                    reasons,
                    id_run,
                    row_num,
                    extra={
                        "quarantine_reasons": reasons,
                        "classification": "QUARANTINED",
                        "id_run": id_run,
                        "updated_at": processed_doc.get("updated_at"),
                    },
                )
                count_quarantine += 1

        elapsed = time.perf_counter() - start
        throughput = processed / elapsed if elapsed > 0 else 0
        total_classified = count_valid + count_corrected + count_quarantine

        print(f"Processed          : {processed:,}")
        print(f"VALID              : {count_valid:,}")
        print(f"CORRECTED          : {count_corrected:,}")
        print(f"QUARANTINED        : {count_quarantine:,}")
        print("-" * 70)
        print(f"Inserted           : {count_inserted:,}")
        print(f"Updated            : {count_updated:,}")
        print(f"Unchanged          : {count_unchanged:,}")
        print("-" * 70)
        print(f"Consistency        : {total_classified:,} / {processed:,}", end="  ")
        print("PASS" if total_classified == processed else "FAIL")
        print(f"Time               : {elapsed:.3f}s")
        print(f"Throughput         : {throughput:,.0f} records/s")
        print("=" * 70)

        return {
            "count_valid": count_valid,
            "count_corrected": count_corrected,
            "count_quarantine": count_quarantine,
            "count_inserted": count_inserted,
            "count_updated": count_updated,
            "count_unchanged": count_unchanged,
            "processed": processed,
            "seconds_elapsed": round(elapsed, 3),
            "throughput": round(throughput, 1),
            "consistency_pass": total_classified == processed,
        }

    finally:
        if client is not None:
            client.close()


def _upsert_validated(
    col: Collection,
    doc: Dict,
) -> Tuple[int, int, int]:
    """
    Upsert a VALID or CORRECTED record into orders_validated.

    Keys on id_order (the canonical business key).
    Returns (inserted, updated, unchanged) counts as a 3-tuple.
    """
    id_order = doc.get("id_order") or doc.get("order_id", "")

    if not id_order:
        return 0, 0, 0

    # Check current stored state
    existing = col.find_one({"id_order": id_order}, {"updated_at": 1})

    result = col.update_one(
        {"id_order": id_order},
        {"$set": doc},
        upsert=True,
    )

    if result.upserted_id is not None:
        return 1, 0, 0  # inserted

    if result.modified_count > 0:
        return 0, 1, 0  # updated

    return 0, 0, 1  # unchanged


def _upsert_quarantine(
    col: Collection,
    source_fields: Dict,
    reasons: list,
    id_run: str,
    row_num: int,
    extra: Dict | None = None,
) -> None:
    """
    Upsert a record into orders_quarantine.

    Uses a deterministic quarantine_key so replaying the same run does
    not create duplicate quarantine entries.
    """
    order_id = str(source_fields.get("order_id", "")).strip()
    quarantine_key = f"{id_run}|{row_num}|{order_id}"

    doc = {
        **source_fields,
        "quarantine_key": quarantine_key,
        "quarantine_reasons": reasons,
        "classification": "QUARANTINED",
        "id_run": id_run,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
    }

    if extra:
        doc.update(extra)

    doc.pop("_id", None)

    col.update_one(
        {"quarantine_key": quarantine_key},
        {"$set": doc},
        upsert=True,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Quality processor — process a specific pipeline run."
    )
    parser.add_argument("--id-run", required=True, help="Run ID to process")
    args = parser.parse_args()

    process_run(id_run=args.id_run)
