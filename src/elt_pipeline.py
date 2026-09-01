"""
src/elt_pipeline.py
===================
ELT pipeline orchestrator.

Executes the full pipeline for one input file:

  1. Generate id_run (UUID4)
  2. Route file → Python Batch or PySpark
  3. RAW LOAD (all records → orders_raw) — ELT Extract + Load
  4. QUALITY PROCESSING (orders_raw → orders_validated / orders_quarantine)
  5. METRICS — collect and return run metrics

This module is designed to be called from main.py.
It can also be run stand-alone:

    python -m src.elt_pipeline --input data/orders_small_sample.csv
"""

import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    BATCH_SIZE,
    MONGODB_DATABASE,
    MONGODB_URI,
    RAW_COLLECTION,
    SMALL_FILE_THRESHOLD_MB,
)
from src.file_router import choose_engine, get_file_size_mb
from src.batch_loader import load_csv_to_raw
from src.quality_processor import process_run


def run_pipeline(
    file_path: str,
    batch_size: int = BATCH_SIZE,
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
) -> Dict[str, Any]:
    """
    Execute the full ELT pipeline for one input file.

    Parameters
    ----------
    file_path  : path to the source CSV file
    batch_size : batch size for the Python Batch loader
    mongodb_uri: MongoDB connection string
    database   : target database name

    Returns
    -------
    dict containing all run metrics
    """
    id_run = str(uuid.uuid4())

    file_path = str(file_path)
    file_name = Path(file_path).name
    size_mb, engine, reason = choose_engine(file_path)

    print()
    print("=" * 70)
    print("ELT PIPELINE")
    print("=" * 70)
    print(f"Run ID     : {id_run}")
    print(f"File       : {file_path}")
    print(f"Size       : {size_mb:.2f} MB")
    print(f"Threshold  : {SMALL_FILE_THRESHOLD_MB} MB")
    print(f"Engine     : {engine.upper()}")
    print(f"Reason     : {reason}")
    print("=" * 70)

    pipeline_start = time.perf_counter()

    # -----------------------------------------------------------------------
    # STAGE 1: Extract + Load → orders_raw  (ELT: load raw FIRST)
    # -----------------------------------------------------------------------
    print("\n[STAGE 1]  RAW LOAD ...")

    if engine == "python_batch":
        raw_metrics = load_csv_to_raw(
            file_path=file_path,
            id_run=id_run,
            batch_size=batch_size,
            mongodb_uri=mongodb_uri,
            database=database,
        )
        partitions = 1  # Python batch is single-threaded

    else:  # pyspark
        from src.spark_large_loader import load_large_csv_to_raw

        raw_metrics = load_large_csv_to_raw(
            file_path=file_path,
            id_run=id_run,
            batch_size=batch_size,
            mongodb_uri=mongodb_uri,
            database=database,
        )
        partitions = raw_metrics.get("partitions", 1)

    loaded_raw = raw_metrics.get("loaded_raw", 0)

    print(f"\n[STAGE 1]  COMPLETE -- {loaded_raw:,} records in orders_raw")

    # -----------------------------------------------------------------------
    # STAGE 2: Transform → quality rules + Upsert
    # -----------------------------------------------------------------------
    print("\n[STAGE 2]  QUALITY PROCESSING ...")

    quality_metrics = process_run(
        id_run=id_run,
        mongodb_uri=mongodb_uri,
        database=database,
    )

    pipeline_elapsed = time.perf_counter() - pipeline_start

    # -----------------------------------------------------------------------
    # Consistency check
    # -----------------------------------------------------------------------
    count_valid = quality_metrics.get("count_valid", 0)
    count_corrected = quality_metrics.get("count_corrected", 0)
    count_quarantine = quality_metrics.get("count_quarantine", 0)
    total_classified = count_valid + count_corrected + count_quarantine
    consistency_ok = (total_classified == loaded_raw)

    print()
    print("=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    print(f"Run ID             : {id_run}")
    print(f"File               : {file_name}")
    print(f"Engine             : {engine.upper()}")
    print(f"Raw loaded         : {loaded_raw:,}")
    print(f"VALID              : {count_valid:,}")
    print(f"CORRECTED          : {count_corrected:,}")
    print(f"QUARANTINED        : {count_quarantine:,}")
    print(f"Classified         : {total_classified:,}")
    print(f"Consistency        : {'PASS' if consistency_ok else 'FAIL'}")
    print(f"Total elapsed      : {pipeline_elapsed:.3f}s")
    print("=" * 70)

    return {
        "id_run": id_run,
        "file_name": file_name,
        "file_size_mb": round(size_mb, 2),
        "used_engine": engine,
        "read_rows": loaded_raw,
        "loaded_raw": loaded_raw,
        "count_valid": count_valid,
        "count_corrected": count_corrected,
        "count_quarantine": count_quarantine,
        "seconds_elapsed": round(pipeline_elapsed, 3),
        "throughput": round(loaded_raw / pipeline_elapsed, 1) if pipeline_elapsed > 0 else 0,
        "partitions": partitions,
        "size_batch": batch_size,
        "count_inserted": quality_metrics.get("count_inserted", 0),
        "count_updated": quality_metrics.get("count_updated", 0),
        "count_unchanged": quality_metrics.get("count_unchanged", 0),
        "counts_case_error": {
            "missing_order_id": 0,  # captured in quarantine_reasons
        },
        "consistency_check": "PASS" if consistency_ok else "FAIL",
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the full ELT pipeline for one input file."
    )
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE, help="Batch size"
    )
    args = parser.parse_args()

    run_pipeline(file_path=args.input, batch_size=args.batch_size)
