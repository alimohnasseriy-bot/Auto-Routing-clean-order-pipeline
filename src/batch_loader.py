"""
src/batch_loader.py
===================
Python Batch loader for small CSV files (≤ SMALL_FILE_THRESHOLD_MB).

Reads the CSV in a streaming fashion using csv.DictReader — never loads
the entire file into memory.  Inserts records into MongoDB in configurable
batches.

ELT contract
------------
Every source record is inserted into orders_raw BEFORE any quality
processing.  Each document carries:

  id_run          — unique run identifier (UUID4)
  file_source     — original file path / name
  number_row_source — 1-based row index in the source file
  at_ingested     — UTC ISO timestamp of ingestion
  engine_used     — "python_batch"
  record_raw      — the original source fields, unchanged

Returns a dict of run metrics.
"""

import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# Allow running as  python src/batch_loader.py  from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    BATCH_SIZE,
    MONGODB_DATABASE,
    MONGODB_URI,
    RAW_COLLECTION,
)


def load_csv_to_raw(
    file_path: str,
    id_run: str,
    batch_size: int = BATCH_SIZE,
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
    collection_name: str = RAW_COLLECTION,
) -> Dict[str, Any]:
    """
    Stream-load a CSV file into the MongoDB raw collection.

    Parameters
    ----------
    file_path    : path to the CSV file
    id_run       : unique run identifier (caller provides so it is shared
                   across all pipeline stages)
    batch_size   : number of records per bulk insert
    mongodb_uri  : MongoDB connection string
    database     : target database name
    collection_name : target collection name (default: orders_raw)

    Returns
    -------
    dict with keys: loaded_raw, batch_count, seconds_elapsed, throughput
    """
    client: MongoClient | None = None
    file_path = str(file_path)

    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10_000)
        db = client[database]
        collection = db[collection_name]

        print("=" * 60)
        print("PYTHON BATCH LOADER  [ELT -- RAW STAGE]")
        print("=" * 60)
        print(f"File       : {file_path}")
        print(f"Batch size : {batch_size:,}")
        print(f"Database   : {database}")
        print(f"Collection : {collection_name}")
        print(f"Run ID     : {id_run}")
        print("=" * 60)

        total_loaded = 0
        batch_number = 0
        row_number = 0
        start_total = time.perf_counter()

        ingested_at = datetime.now(timezone.utc).isoformat()

        with Path(file_path).open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            batch: list = []

            for row in reader:
                row_number += 1
                raw_fields = dict(row)

                # Build the ELT raw document
                doc = {
                    "id_run": id_run,
                    "file_source": file_path,
                    "number_row_source": row_number,
                    "at_ingested": ingested_at,
                    "engine_used": "python_batch",
                    # Canonical business key (source field is order_id)
                    "order_id": raw_fields.get("order_id", ""),
                    # Preserve every original source field unchanged
                    "record_raw": raw_fields,
                    # Also spread source fields at top level for easy querying
                    **raw_fields,
                }

                batch.append(doc)

                if len(batch) >= batch_size:
                    batch_number, total_loaded = _flush_batch(
                        collection, batch, batch_number, total_loaded
                    )
                    batch = []

            # Final partial batch
            if batch:
                batch_number, total_loaded = _flush_batch(
                    collection, batch, batch_number, total_loaded
                )

        total_elapsed = time.perf_counter() - start_total
        throughput = total_loaded / total_elapsed if total_elapsed > 0 else 0

        print("=" * 60)
        print("BATCH LOAD COMPLETED")
        print("=" * 60)
        print(f"Total records : {total_loaded:,}")
        print(f"Total batches : {batch_number:,}")
        print(f"Total time    : {total_elapsed:.3f}s")
        print(f"Throughput    : {throughput:,.0f} records/s")
        print("=" * 60)

        return {
            "loaded_raw": total_loaded,
            "batch_count": batch_number,
            "seconds_elapsed": round(total_elapsed, 3),
            "throughput": round(throughput, 1),
        }

    finally:
        if client is not None:
            client.close()


def _flush_batch(
    collection, batch: list, batch_number: int, total_loaded: int
) -> tuple:
    """Insert a batch and print progress. Returns updated counters."""
    batch_number += 1
    start_batch = time.perf_counter()

    try:
        result = collection.insert_many(batch, ordered=False)
        inserted = len(result.inserted_ids)
    except BulkWriteError as exc:
        inserted = exc.details.get("nInserted", 0)
        print(f"  [WARN] Batch {batch_number:04d} partial error: {exc.details}")

    elapsed = time.perf_counter() - start_batch
    total_loaded += inserted
    rate = inserted / elapsed if elapsed > 0 else 0

    print(
        f"Batch {batch_number:04d} | "
        f"Records: {inserted:5d} | "
        f"Time: {elapsed:.3f}s | "
        f"Rate: {rate:,.0f} rec/s"
    )

    return batch_number, total_loaded


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="ELT raw loader — stream CSV into MongoDB orders_raw."
    )
    parser.add_argument(
        "--input",
        default="data/orders_small_sample.csv",
        help="CSV input file",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Number of records per bulk insert",
    )
    parser.add_argument(
        "--id-run",
        default=str(uuid.uuid4()),
        help="Run identifier (auto-generated if not provided)",
    )

    args = parser.parse_args()

    load_csv_to_raw(
        file_path=args.input,
        id_run=args.id_run,
        batch_size=args.batch_size,
    )
