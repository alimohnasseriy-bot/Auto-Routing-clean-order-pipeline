# Architecture — midterm-data-pipeline

## 1. Overview

The pipeline is a **hybrid ELT (Extract–Load–Transform)** system that processes order data from CSV files into MongoDB.

```
CSV Input → File Router → [Python Batch | PySpark] → orders_raw
                                                           ↓
                                             Quality + Cleaning (10 rules)
                                                           ↓
                                          orders_validated | orders_quarantine
                                                           ↓
                                                   reports/results.json
```

---

## 2. File Router

**Module:** `src/file_router.py`  
**Config:** `SMALL_FILE_THRESHOLD_MB = 200`

The router compares the file size in MB against the configurable threshold:

```
file_size ≤ 200 MB → python_batch
file_size  > 200 MB → pyspark
```

This single decision point ensures there are not two disconnected applications.  The router prints:
- File path
- File size in MB
- Threshold
- Selected engine
- Reason for selection

**Why 200 MB?**  
200 MB represents roughly 500,000–1,000,000 rows depending on field width.  Python's `csv.DictReader` streams rows without loading the file into memory, so the actual constraint is the size of the MongoDB batch (default 5,000 docs) not the file size.  200 MB is a conservative default that leaves headroom even on machines with 4 GB RAM.

---

## 3. Python Batch (Small Files)

**Module:** `src/batch_loader.py`

- Reads CSV with `csv.DictReader` — **streaming, never loads entire file**.
- Inserts records into `orders_raw` in configurable batches (default 5,000).
- Each document carries ELT metadata:
  - `id_run` — unique run identifier
  - `file_source` — source file path
  - `number_row_source` — 1-based row index
  - `at_ingested` — UTC ISO timestamp
  - `engine_used` — `"python_batch"`
  - `record_raw` — original source fields, unchanged
- Handles `BulkWriteError` explicitly (partial batch failure does not abort the run).
- Prints per-batch statistics: batch number, records, time, throughput.

---

## 4. PySpark (Large Files)

**Module:** `src/spark_large_loader.py`

- Creates a `SparkSession` with `local[*]` master by default.
- Uses an **explicit schema** (all `StringType`) to preserve dirty values exactly.
- Reads CSV with `inferSchema=False` — no type coercion in the raw stage.
- Processes each Spark partition directly:  writes to MongoDB via PyMongo batch inserts (no Spark–MongoDB connector required).
- Carries the same ELT metadata as the Python Batch path.
- Reports input partition count in metrics.
- `try/finally` guarantees `spark.stop()` even on errors.

**Why no MongoDB Spark Connector?**  
The Spark–MongoDB connector requires a compatible `mongo-spark-connector` JAR that must match the Spark version precisely.  Using PyMongo per-partition avoids this dependency while still using real Spark partitioned processing.

**Spark master configuration:**  
```python
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
```
Override with `SPARK_MASTER=spark://host:7077` for a real cluster.

---

## 5. Raw ELT

**Collection:** `orders_raw`

Every source record must reach `orders_raw` **before any cleaning**.

This is the ELT "Load" stage.  Raw documents are:
- Immutable (never modified after insertion)
- Historical (multiple runs accumulate records, keyed by `id_run`)
- Complete (original source fields preserved in `record_raw`)

The raw collection is **not** quality-filtered.  Bad records go in as-is.

---

## 6. Quality Rules

**Module:** `src/quality_rules.py`

10 deterministic correction rules, applied in order:

| # | Rule | Code | Example |
|---|------|------|---------|
| 0 | Trim whitespace on all string fields | `TRIM_NORMALIZATION` | `" status "` → `"status"` |
| 1 | Business key mapping | — | `order_id` → `id_order` |
| 2 | Date format normalization | `DATE_NORMALIZATION` | `17-01-2025 04:50:00` → `2025-01-17T04:50:00` |
| 3 | Phone normalization | `PHONE_NORMALIZATION` | `+967714876334` → `714876334` |
| 4 | Email repair | `EMAIL_REPEATED_SYMBOLS` | `user@@mail..com` → `user@mail.com` |
| 5 | Numeric: Arabic digits | `ARABIC_DIGITS` | `٧٠٦٠٠٠٫٠` → `706000.0` |
| 5 | Numeric: Currency text | `CURRENCY_NORMALIZATION` | `5000 لاير` → `5000.0` |
| 5 | Numeric: Thousands separator | `THOUSANDS_SEPARATOR` | `125,000.00` → `125000.0` |
| 5 | Numeric: Arabic word | `ARABIC_NUMBER_WORD` | `ألفان` → `2000.0` |
| 6 | Items JSON validation | — | Parses JSON, checks qty > 0 |
| 7 | Total recalculation | `TOTAL_RECALCULATION` | `sum(qty*price) + delivery` → `total_amount` |
| 8 | Status synonym normalization | `TRIM_NORMALIZATION` | `مكتمل` → `completed` |
| 9 | Payment status normalization | `TRIM_NORMALIZATION` | `مدفوع` → `paid` |

**Design principle:** Only apply a transformation when it is **deterministic and clearly justified**.  Ambiguous cases → quarantine.

---

## 7. Classification

Every record exits quality processing with exactly one label:

| Label | Condition |
|-------|-----------|
| `VALID` | No corrections needed, no quarantine reasons |
| `CORRECTED` | One or more deterministic corrections applied |
| `QUARANTINED` | Contains unfixable quality issue |

The consistency equation must hold:
```
loaded_raw = count_valid + count_corrected + count_quarantine
```

---

## 8. Quarantine

**Collection:** `orders_quarantine`

Records that cannot be safely corrected are stored with:
- `quarantine_key` — deterministic string for idempotent upsert
- `quarantine_reasons` — list of issue codes
- `classification` — always `"QUARANTINED"`
- `quarantined_at` — UTC timestamp

Quarantine codes:

| Code | Description |
|------|-------------|
| `ID_ORDER_MISSING` | `order_id` is blank or absent |
| `ID_ORDER_DUPLICATE` | Same `order_id` appears twice in same run |
| `DATE_IMPOSSIBLE_INVALID` | Date cannot be parsed to a valid date |
| `JSON_ITEMS_CORRUPTED` | `items_json` is not valid JSON |
| `ITEMS_EMPTY` | Items list is an empty array `[]` |
| `VALUE_NEGATIVE_AMBIGUOUS` | Item quantity ≤ 0 |
| `PRICE_UNKNOWN` | Numeric amount cannot be parsed after all rules |
| `EMAIL_INVALID` | Email cannot be repaired to valid format |

Records are **never silently deleted**.

---

## 9. Audit Trail

Every `CORRECTED` record carries a `corrections` list:

```json
{
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "total_amount",
      "original_value": "٧٠٦٠٠٠٫٠",
      "corrected_value": "706000.0",
      "rule_code": "ARABIC_DIGITS"
    }
  ]
}
```

The original value is always preserved alongside the corrected value.

---

## 10. Upsert

**Module:** `src/quality_processor.py`  
**Collection:** `orders_validated`

Business key: `id_order` (mapped from source `order_id`).

```python
collection.update_one(
    {"id_order": order_id},
    {"$set": processed_doc},
    upsert=True,
)
```

Results are classified as:
- `count_inserted` — new `id_order` not previously in validated
- `count_updated` — existing `id_order` with new state
- `count_unchanged` — existing `id_order` with identical state

A unique index on `orders_validated.id_order` enforces uniqueness at the database level.

---

## 11. Idempotency

**Mechanism:**  
Re-running the same pipeline input produces the same final state without adding duplicate business records.

**How it works:**
1. `orders_raw` — new records are inserted (raw is historical), but validated upserts are keyed on `id_order`
2. `orders_validated` — unique index prevents duplicate `id_order`
3. `orders_quarantine` — deterministic `quarantine_key = f"{id_run}|{row}|{order_id}"` prevents duplicate quarantine entries within a run
4. `update_one(..., upsert=True)` — MongoDB upsert semantics

---

## 12. Path B — Incremental Loading

**Module:** `src/incremental_loader.py`

### Version strategy: `updated_at`

Each validated record carries an ISO `updated_at` timestamp.  
Delta processing compares the delta's `updated_at` against the stored value:

```python
if delta_updated_at > stored_updated_at:
    # UPDATE — newer version
elif delta_updated_at == stored_updated_at:
    # UNCHANGED — idempotent replay
else:
    # UNCHANGED — stale delta, do not apply
```

### Delta flow

```
1. generate_delta_from_sample()
   ↓ Creates data/delta/delta_001.csv
   ↓ N updates (existing orders with changed status)
   ↓ M inserts (brand-new order IDs)

2. process_delta("data/delta/delta_001.csv")
   ↓ Each record → quality rules → VALID/CORRECTED/QUARANTINED
   ↓ VALID/CORRECTED → version-aware upsert into orders_validated
   ↓ QUARANTINED → quarantine collection

3. process_delta("data/delta/delta_001.csv")  [REPLAY]
   ↓ Same records, same timestamps
   ↓ delta_updated_at == stored_updated_at → all UNCHANGED
   ↓ count_inserted=0, count_updated=0, count_unchanged=N+M
```

---

## 13. Metrics

**Module:** `src/metrics.py`  
**Output:** `reports/results.json`

All required fields are written after each pipeline run:

```
id_run, file_name, file_size_mb, used_engine,
read_rows, loaded_raw,
count_valid, count_corrected, count_quarantine,
seconds_elapsed, throughput, partitions, size_batch,
count_inserted, count_updated, count_unchanged,
counts_case_error (per quarantine code),
consistency_check (PASS/FAIL)
```

The consistency check verifies:
```
loaded_raw == count_valid + count_corrected + count_quarantine
```
