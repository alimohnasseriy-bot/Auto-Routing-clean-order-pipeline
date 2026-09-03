"""
src/metrics.py
==============
Metrics collection and results.json writer.

Reads live counts from MongoDB and combines them with the run metrics
produced by the pipeline stages to write a complete results.json.

All required metrics fields are included:
  id_run, file_name, file_size_mb, used_engine,
  read_rows, loaded_raw, count_valid, count_corrected, count_quarantine,
  seconds_elapsed, throughput, partitions, size_batch,
  counts_case_error, count_inserted, count_updated, count_unchanged,
  consistency_check
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    MONGODB_DATABASE,
    MONGODB_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    REPORTS_DIR,
    RESULTS_JSON,
    VALIDATED_COLLECTION,
)


def collect_and_write(
    run_metrics: Optional[Dict[str, Any]] = None,
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
) -> Dict[str, Any]:
    """
    Collect live database counts, merge with run_metrics, write results.json.

    Parameters
    ----------
    run_metrics : dict returned by elt_pipeline.run_pipeline() (can be None
                  for a stand-alone metrics query)
    mongodb_uri : MongoDB connection string
    database    : target database

    Returns
    -------
    The full results dict (also written to RESULTS_JSON).
    """
    client: MongoClient | None = None

    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10_000)
        db = client[database]

        raw_col = db[RAW_COLLECTION]
        validated_col = db[VALIDATED_COLLECTION]
        quarantine_col = db[QUARANTINE_COLLECTION]

        query_start = time.perf_counter()

        # Live collection counts
        raw_total = raw_col.count_documents({})
        validated_total = validated_col.count_documents({})
        quarantine_total = quarantine_col.count_documents({})

        valid_count = validated_col.count_documents({"classification": "VALID"})
        corrected_count = validated_col.count_documents({"classification": "CORRECTED"})

        # Quarantine breakdown by reason
        missing_order_id = quarantine_col.count_documents(
            {"quarantine_reasons": "ID_ORDER_MISSING"}
        )
        missing_customer_id = quarantine_col.count_documents(
            {"quarantine_reasons": "ID_CUSTOMER_MISSING"}
        )
        duplicate_order_id = quarantine_col.count_documents(
            {"quarantine_reasons": "ID_ORDER_DUPLICATE"}
        )
        invalid_date = quarantine_col.count_documents(
            {"quarantine_reasons": "DATE_IMPOSSIBLE_INVALID"}
        )
        corrupted_items = quarantine_col.count_documents(
            {"quarantine_reasons": "JSON_ITEMS_CORRUPTED"}
        )
        invalid_email = quarantine_col.count_documents(
            {"quarantine_reasons": "EMAIL_INVALID"}
        )
        conflicting_multiple = quarantine_col.count_documents(
            {"quarantine_reasons": "ERRORS_CONFLICTING_MULTIPLE"}
        )

        query_elapsed = time.perf_counter() - query_start

        # Pull values from run_metrics if available
        rm = run_metrics or {}

        results: Dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),

            # Run identification
            "id_run": rm.get("id_run", "N/A"),
            "file_name": rm.get("file_name", "N/A"),
            "file_size_mb": rm.get("file_size_mb", 0),
            "used_engine": rm.get("used_engine", "N/A"),

            # Ingestion counts
            "read_rows": rm.get("read_rows", raw_total),
            "loaded_raw": rm.get("loaded_raw", raw_total),

            # Classification counts (from run)
            "count_valid": rm.get("count_valid", valid_count),
            "count_corrected": rm.get("count_corrected", corrected_count),
            "count_quarantine": rm.get("count_quarantine", quarantine_total),

            # Performance
            "seconds_elapsed": rm.get("seconds_elapsed", 0),
            "throughput": rm.get("throughput", 0),
            "partitions": rm.get("partitions", 1),
            "size_batch": rm.get("size_batch", 5000),

            # Upsert breakdown
            "count_inserted": rm.get("count_inserted", 0),
            "count_updated": rm.get("count_updated", 0),
            "count_unchanged": rm.get("count_unchanged", 0),

            # Quarantine error breakdown
            "counts_case_error": {
                "ID_ORDER_MISSING": missing_order_id,
                "ID_CUSTOMER_MISSING": missing_customer_id,
                "ID_ORDER_DUPLICATE": duplicate_order_id,
                "DATE_IMPOSSIBLE_INVALID": invalid_date,
                "JSON_ITEMS_CORRUPTED": corrupted_items,
                "EMAIL_INVALID": invalid_email,
                "ERRORS_CONFLICTING_MULTIPLE": conflicting_multiple,
            },

            # Consistency equation:
            # loaded_raw = count_valid + count_corrected + count_quarantine
            "consistency_check": rm.get("consistency_check", "N/A"),

            # Live database totals (cumulative across all runs)
            "db_totals": {
                "orders_raw": raw_total,
                "orders_validated": validated_total,
                "orders_quarantine": quarantine_total,
                "valid": valid_count,
                "corrected": corrected_count,
            },

            "metrics_query_seconds": round(query_elapsed, 4),
        }

        # Write to disk
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

        with RESULTS_JSON.open("w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

        # Print summary
        print("=" * 70)
        print("METRICS REPORT")
        print("=" * 70)
        print(f"Run ID             : {results['id_run']}")
        print(f"File               : {results['file_name']}")
        print(f"Engine             : {results['used_engine']}")
        print(f"Raw loaded         : {results['loaded_raw']:,}")
        print(f"VALID              : {results['count_valid']:,}")
        print(f"CORRECTED          : {results['count_corrected']:,}")
        print(f"QUARANTINED        : {results['count_quarantine']:,}")
        print(f"Inserted           : {results['count_inserted']:,}")
        print(f"Updated            : {results['count_updated']:,}")
        print(f"Unchanged          : {results['count_unchanged']:,}")
        print(f"Consistency        : {results['consistency_check']}")
        print(f"Elapsed            : {results['seconds_elapsed']}s")
        print(f"Throughput         : {results['throughput']:,} rec/s")
        print(f"Report             : {RESULTS_JSON}")
        print("=" * 70)

        return results

    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    collect_and_write()
