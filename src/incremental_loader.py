"""
src/incremental_loader.py
=========================
PATH B — Incremental Loading and Advanced Reliability.

Implements a complete incremental architecture:

  INITIAL LOAD
      ↓
  validated_orders initial state
      ↓
  DELTA FILE (CSV)
      ├── new record   → INSERT
      └── existing     → UPDATE  (only if delta.updated_at > stored.updated_at)
      ↓
  Idempotent DELTA REPLAY

Version strategy
----------------
Each record carries an `updated_at` ISO timestamp.

  new record        : INSERT into orders_validated
  existing record   : UPDATE only if delta.updated_at > stored.updated_at
  already current   : UNCHANGED (no write)

Replay protection
-----------------
Re-applying the exact same delta produces zero changes because the stored
`updated_at` is already equal to (or newer than) the delta `updated_at`.

Delta file format
-----------------
Same CSV columns as the main data file.  The loader generates a unique
`id_run` for the delta run and processes it through the same quality rules
so corrections and quarantine still apply.
"""

import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    BATCH_SIZE,
    DELTA_DIR,
    MONGODB_DATABASE,
    MONGODB_URI,
    QUARANTINE_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.quality_rules import process_record


# ---------------------------------------------------------------------------
# Delta generation helpers
# ---------------------------------------------------------------------------

def generate_delta_from_sample(
    sample_path: str,
    delta_path: str,
    num_updates: int = 5,
    num_inserts: int = 3,
    update_status: str = "delivered",
) -> str:
    """
    Automatically generate a small delta CSV from the sample dataset.

    Parameters
    ----------
    sample_path  : path to the orders_small_sample.csv
    delta_path   : output path for the delta CSV
    num_updates  : number of existing records to modify
    num_inserts  : number of brand-new records to add
    update_status: new status value to apply to updated records

    Returns
    -------
    Path to the generated delta file.
    """
    sample_path = Path(sample_path)
    delta_path = Path(delta_path)
    delta_path.parent.mkdir(parents=True, exist_ok=True)

    records: List[Dict] = []
    header: List[str] = []

    with sample_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = list(reader.fieldnames or [])
        for i, row in enumerate(reader):
            records.append(dict(row))
            if i >= num_updates + 20:  # read a bit more than needed
                break

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    delta_records: List[Dict] = []

    # Extend header to include updated_at for version tracking
    if "updated_at" not in header:
        header = header + ["updated_at"]

    # --- UPDATES: take first num_updates records, change status + embed timestamp
    for row in records[:num_updates]:
        updated_row = dict(row)
        updated_row["status"] = update_status
        updated_row["updated_at"] = now_str  # fixed per delta generation
        delta_records.append(updated_row)

    # --- INSERTS: synthesize brand-new clean records with guaranteed valid data
    # Using "DELTA-INSERT-" prefix + UUID fragment to guarantee uniqueness
    import uuid as _uuid

    # Find first valid record from sample to use as a safe base (positive qty)
    safe_base: Dict | None = None
    for row in records:
        items_str = row.get("items_json", "")
        try:
            import json as _json
            items = _json.loads(items_str)
            if isinstance(items, list) and items and all(
                float(it.get("qty", -1)) > 0 for it in items if isinstance(it, dict)
            ):
                safe_base = dict(row)
                break
        except Exception:
            continue

    for i in range(num_inserts):
        # Use safe_base if found, else build a fully synthetic record
        if safe_base:
            new_row = dict(safe_base)
        else:
            new_row = {
                "order_date": now_str,
                "status": "pending",
                "customer_id": f"DELTA-CUST-{i+1}",
                "customer_name": "Delta Test User",
                "customer_phone": "714876334",
                "customer_email": f"delta{i+1}@example.com",
                "city": "Sanaa",
                "district": "Central",
                "delivery_type": "standard",
                "delivery_cost": "2000.0",
                "payment_method": "cash",
                "payment_status": "unpaid",
                "payment_amount": "0.0",
                "currency": "YER",
                "total_amount": "2000.0",
                "items_json": (
                    '[{"sku":"SKU-DELTA","name":"Delta Item",'
                    '"qty":1,"unit_price":0.0,"total":0.0}]'
                ),
            }

        new_row["order_id"] = f"DELTA-INSERT-{_uuid.uuid4().hex[:8].upper()}"
        new_row["order_date"] = now_str
        new_row["status"] = "pending"
        new_row["payment_status"] = "unpaid"
        new_row["updated_at"] = now_str  # fixed per delta generation
        delta_records.append(new_row)

    # Write delta CSV
    with delta_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in delta_records:
            writer.writerow({k: row.get(k, "") for k in header})

    print(f"Delta generated: {delta_path}")
    print(f"  Delta timestamp : {now_str}")
    print(f"  Updates : {num_updates}")
    print(f"  Inserts : {num_inserts}")
    print(f"  Total   : {len(delta_records)}")

    return str(delta_path)


# ---------------------------------------------------------------------------
# Incremental processing
# ---------------------------------------------------------------------------

def process_delta(
    delta_path: str,
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
) -> Dict[str, Any]:
    """
    Apply a delta CSV file incrementally to orders_validated.

    Each record is processed through the same quality rules as the main
    pipeline.  Upsert logic:

      - New id_order  → INSERT
      - Existing id_order AND delta.updated_at > stored.updated_at → UPDATE
      - Existing id_order AND delta.updated_at <= stored.updated_at → UNCHANGED

    Version stamp: each row in the delta CSV must have an `updated_at` field.
    This timestamp is fixed per delta generation, so replaying the same delta
    file always produces the same version comparison result (idempotent).

    Returns a metrics dict.
    """
    id_run = str(uuid.uuid4())
    delta_path_obj = Path(delta_path)

    client: MongoClient | None = None

    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10_000)
        db = client[database]
        validated_col = db[VALIDATED_COLLECTION]
        quarantine_col = db[QUARANTINE_COLLECTION]

        print("=" * 70)
        print("INCREMENTAL LOADER -- PATH B DELTA")
        print("=" * 70)
        print(f"Delta file : {delta_path}")
        print(f"Run ID     : {id_run}")
        print("=" * 70)

        start = time.perf_counter()

        count_read = 0
        count_inserted = 0
        count_updated = 0
        count_unchanged = 0
        count_quarantine = 0

        with delta_path_obj.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=1):
                count_read += 1
                source = dict(row)

                # Use the per-row updated_at from the delta file (fixed at generation time)
                # This makes replay deterministic: same delta = same timestamps = UNCHANGED
                delta_updated_at = source.get("updated_at", "")

                # Apply quality rules
                processed, classification = process_record(source)
                processed.pop("_id", None)
                processed["id_run"] = id_run

                order_id = processed.get("id_order") or processed.get("order_id", "")

                if classification == "QUARANTINED" or not order_id:
                    reasons = processed.get("quarantine_reasons", ["UNKNOWN"])
                    qkey = f"{id_run}|delta|{row_num}|{order_id}"
                    quarantine_col.update_one(
                        {"quarantine_key": qkey},
                        {"$set": {
                            **source,
                            "quarantine_key": qkey,
                            "quarantine_reasons": reasons,
                            "classification": "QUARANTINED",
                            "id_run": id_run,
                            "quarantined_at": delta_updated_at,
                        }},
                        upsert=True,
                    )
                    count_quarantine += 1
                    continue

                # Version-aware upsert using the delta's own timestamp
                ins, upd, unch = _version_upsert(
                    validated_col,
                    order_id,
                    processed,
                    delta_updated_at=delta_updated_at,
                )
                count_inserted += ins
                count_updated += upd
                count_unchanged += unch

        elapsed = time.perf_counter() - start
        throughput = count_read / elapsed if elapsed > 0 else 0

        print(f"Read          : {count_read:,}")
        print(f"Inserted      : {count_inserted:,}  (new orders)")
        print(f"Updated       : {count_updated:,}  (newer version)")
        print(f"Unchanged     : {count_unchanged:,}  (already current)")
        print(f"Quarantined   : {count_quarantine:,}")
        print(f"Time          : {elapsed:.3f}s")
        print(f"Throughput    : {throughput:,.0f} rec/s")
        print("=" * 70)

        return {
            "id_run": id_run,
            "delta_file": str(delta_path),
            "count_read": count_read,
            "count_inserted": count_inserted,
            "count_updated": count_updated,
            "count_unchanged": count_unchanged,
            "count_quarantine": count_quarantine,
            "seconds_elapsed": round(elapsed, 3),
            "throughput": round(throughput, 1),
        }

    finally:
        if client is not None:
            client.close()



def _version_upsert(
    col,
    id_order: str,
    doc: Dict,
    delta_updated_at: str,
) -> Tuple[int, int, int]:
    """
    Version-aware upsert into orders_validated.

    Returns (inserted, updated, unchanged).
    """
    doc["updated_at"] = delta_updated_at

    existing = col.find_one({"id_order": id_order}, {"updated_at": 1})

    if existing is None:
        # New record — INSERT
        result = col.update_one(
            {"id_order": id_order},
            {"$set": doc},
            upsert=True,
        )
        if result.upserted_id is not None:
            return 1, 0, 0
        return 0, 1, 0  # race: inserted between find and update

    stored_at_str = existing.get("updated_at", "")

    # Compare timestamps lexicographically (ISO strings sort correctly)
    if delta_updated_at > stored_at_str:
        # Newer delta — UPDATE
        col.update_one({"id_order": id_order}, {"$set": doc})
        return 0, 1, 0

    # Same or older — UNCHANGED (idempotent replay protection)
    return 0, 0, 1


# ---------------------------------------------------------------------------
# Initial load helper (Path B demonstration)
# ---------------------------------------------------------------------------

def initial_load_from_validated(
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
) -> int:
    """
    Return the current count of records in orders_validated.
    Used to verify initial state before delta application.
    """
    client: MongoClient | None = None
    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10_000)
        return client[database][VALIDATED_COLLECTION].count_documents({})
    finally:
        if client is not None:
            client.close()


# ---------------------------------------------------------------------------
# Path B demonstration runner
# ---------------------------------------------------------------------------

def run_path_b_demo(
    sample_path: str,
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
) -> None:
    """
    Reproducible Path B demonstration:

    Stage 1: Show initial validated count
    Stage 2: Generate delta (5 updates + 3 inserts)
    Stage 3: Apply delta #1
    Stage 4: Replay delta #1 (expect all UNCHANGED)
    Stage 5: Report final state
    """
    print()
    print("=" * 70)
    print("PATH B -- INCREMENTAL LOADING DEMONSTRATION")
    print("=" * 70)

    # Stage 1 — Initial state
    initial_count = initial_load_from_validated(mongodb_uri, database)
    print(f"\n[STAGE 1]  Initial validated count : {initial_count:,}")

    if initial_count == 0:
        print("  WARNING: orders_validated is empty.")
        print("  Run the main pipeline first:  python -m src.main --input <csv>")
        print()

    # Stage 2 — Generate delta
    delta_file = str(DELTA_DIR / "delta_001.csv")
    print("\n[STAGE 2]  Generating delta file ...")
    generate_delta_from_sample(
        sample_path=sample_path,
        delta_path=delta_file,
        num_updates=5,
        num_inserts=3,
    )

    # Stage 3 — Apply delta
    print("\n[STAGE 3]  Applying delta #1 ...")
    metrics1 = process_delta(delta_path=delta_file, mongodb_uri=mongodb_uri, database=database)

    expected_inserted = metrics1["count_inserted"]
    expected_updated = metrics1["count_updated"]

    print(f"\n  Delta #1 result:")
    print(f"    Inserted  : {metrics1['count_inserted']}")
    print(f"    Updated   : {metrics1['count_updated']}")
    print(f"    Unchanged : {metrics1['count_unchanged']}")

    # Stage 4 — Replay delta (idempotency test)
    print("\n[STAGE 4]  Replaying delta #1 (idempotency check) ...")
    metrics2 = process_delta(delta_path=delta_file, mongodb_uri=mongodb_uri, database=database)

    print(f"\n  Delta #1 REPLAY result:")
    print(f"    Inserted  : {metrics2['count_inserted']}  (expected: 0)")
    print(f"    Updated   : {metrics2['count_updated']}  (expected: 0)")
    print(f"    Unchanged : {metrics2['count_unchanged']}  (expected: all)")

    # Idempotency verdict
    idempotent = (metrics2["count_inserted"] == 0 and metrics2["count_updated"] == 0)

    print()
    print("=" * 70)
    print("PATH B IDEMPOTENCY RESULT")
    print("=" * 70)
    print(f"  Initial validated count : {initial_count:,}")
    print(f"  After delta #1          : {initial_load_from_validated(mongodb_uri, database):,}")
    print(f"  Delta #1 inserted       : {expected_inserted}")
    print(f"  Delta #1 updated        : {expected_updated}")
    print(f"  Replay inserted         : {metrics2['count_inserted']}  (should be 0)")
    print(f"  Replay updated          : {metrics2['count_updated']}  (should be 0)")
    print(f"  Replay unchanged        : {metrics2['count_unchanged']}")
    print(f"  IDEMPOTENCY             : {'PASS' if idempotent else 'FAIL'}")
    print("=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="PATH B — Incremental loading tools."
    )
    sub = parser.add_subparsers(dest="command")

    # Generate delta
    gen = sub.add_parser("generate", help="Generate a delta CSV from sample")
    gen.add_argument("--sample", required=True, help="Source sample CSV path")
    gen.add_argument("--output", required=True, help="Output delta CSV path")
    gen.add_argument("--updates", type=int, default=5)
    gen.add_argument("--inserts", type=int, default=3)

    # Apply delta
    apply_p = sub.add_parser("apply", help="Apply a delta CSV")
    apply_p.add_argument("--delta", required=True, help="Delta CSV path")

    # Full demo
    demo = sub.add_parser("demo", help="Run the full Path B demo")
    demo.add_argument("--sample", required=True, help="Source sample CSV path")

    args = parser.parse_args()

    if args.command == "generate":
        generate_delta_from_sample(
            sample_path=args.sample,
            delta_path=args.output,
            num_updates=args.updates,
            num_inserts=args.inserts,
        )
    elif args.command == "apply":
        process_delta(delta_path=args.delta)
    elif args.command == "demo":
        run_path_b_demo(sample_path=args.sample)
    else:
        parser.print_help()
