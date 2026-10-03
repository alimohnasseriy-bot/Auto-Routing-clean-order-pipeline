# Midterm Data Pipeline

A **hybrid ELT (Extract–Load–Transform) data pipeline** built for a university Big Data practical midterm.  
Routes small files to **Python Batch**, large files to **Apache PySpark**, stores everything in **MongoDB**, and implements **PATH B: Incremental Loading and Advanced Reliability**.

---

## Architecture Overview

```
                    CSV INPUT
                        |
                        v
                 FILE DISCOVERY
                        |
                        v
                   FILE ROUTER
              (200 MB threshold)
                /              \
               v                v
        PYTHON BATCH          PYSPARK
        (≤ 200 MB)           (> 200 MB)
               \                /
                v              v
              orders_raw  (MongoDB)
                ELT: raw loaded FIRST
                        |
                        v
              QUALITY + CLEANING
              (10 deterministic rules)
                        |
           +------------+------------+
           |                         |
           v                         v
   orders_validated          orders_quarantine
   (VALID + CORRECTED)       (QUARANTINED)
   unique index: id_order
   Idempotent Upsert
           |
           v
   reports/results.json
```

**PATH B — Incremental Loading:**
```
Initial load → orders_validated
     ↓
Delta CSV   → Version-aware upsert (updated_at)
     ↓
Delta replay → All UNCHANGED (idempotent)
```

---

## Environment Requirements

| Component | Version |
|-----------|---------|
| Python    | 3.11.9  |
| Apache Spark / PySpark | 4.2.0 |
| Java      | 17.0.12 |
| MongoDB shell | 2.9.0 |
| Operating System | Windows (also works on Linux/macOS) |

---

## Installation

```bash
# Clone or extract the project
cd midterm-data-pipeline

# Install Python dependencies
pip install -r requirements.txt
```

**requirements.txt contains:**
```
pymongo==4.17.0
pyspark==4.2.0
pytest==9.1.1
python-dotenv>=1.0.0
```

---

## MongoDB Setup

Ensure MongoDB is running locally on port 27017 (default).

```bash
# Create collections and unique index on orders_validated.id_order
python -m src.mongo_setup
```

Expected output:
```
[OK] MongoDB connection successful
[OK] Collection created: orders_raw
[OK] Collection created: orders_validated
[OK] Collection created: orders_quarantine
[OK] Unique index created on orders_validated.id_order
```

---

## Configuration

All configuration lives in [`config/settings.py`](config/settings.py).

| Setting | Default | Environment Variable |
|---------|---------|---------------------|
| `SMALL_FILE_THRESHOLD_MB` | `200` | `SMALL_FILE_THRESHOLD_MB` |
| `BATCH_SIZE` | `5000` | `BATCH_SIZE` |
| `MONGODB_URI` | `mongodb://localhost:27017` | `MONGODB_URI` |
| `MONGODB_DATABASE` | `midterm_data_pipeline` | `MONGODB_DATABASE` |
| `SPARK_MASTER` | `local[*]` | `SPARK_MASTER` |
| `INPUT_DIR` | `data/` | `INPUT_DIR` |
| `REPORTS_DIR` | `reports/` | `REPORTS_DIR` |

**Why is the 200 MB threshold configurable?**  
Different hardware environments have different memory constraints.  On a machine with 8 GB RAM, 200 MB is a safe cutoff that keeps Python Batch responsive.  On servers with 64 GB RAM, you could raise it to 1 GB.  The threshold is in config so it can be tuned without touching any processing code.

**Override an environment variable:**
```bash
set SMALL_FILE_THRESHOLD_MB=500
python -m src.main --input data/orders_small_sample.csv
```

---

## Sample Generation

If you need to regenerate the 100 000-row sample from the 12 GB source:

```bash
python src/create_small_sample.py \
    --input data/orders_huge_mixed_quality.csv \
    --output data/orders_small_sample.csv \
    --rows 100000
```

> **Do NOT modify the original 12 GB file.**  The sample is already present at `data/orders_small_sample.csv`.

---

## Running the Pipeline

### Small file — Python Batch engine (≤ 200 MB)

```bash
python -m src.main --input data/orders_small_sample.csv
```

The router detects the file is ~42 MB → selects **Python Batch**.

### Large file — PySpark engine (> 200 MB)

```bash
python -m src.main --input data/orders_huge_mixed_quality.csv
```

The router detects the file is ~12 GB → selects **PySpark** (runs in `local[*]` mode by default).

### Command-line options

```
python -m src.main [OPTIONS]

  --input PATH         Input CSV file  (default: data/orders_small_sample.csv)
  --batch-size INT     Records per MongoDB batch insert  (default: 5000)
  --mongo-uri URI      MongoDB connection string
  --database NAME      MongoDB database name
  --setup-only         Only setup MongoDB, then exit
  --demo-path-b        Run Path B incremental demo after pipeline
  --skip-pipeline      Skip main pipeline (use with --demo-path-b)
```

---

## Running Tests

```bash
pytest tests/ -v
```

All 89 tests should pass.  Tests cover:

- Arabic digit normalization
- Thousands separator removal
- Currency text normalization
- Arabic number words
- Phone normalization
- Email repair and rejection
- Date normalization
- Items JSON validation
- Correction audit trail structure
- VALID / CORRECTED / QUARANTINED classification exclusivity
- Path B version-comparison logic
- Deterministic quarantine keys

> Tests are **pure Python** — no MongoDB, no Spark, no network required.

---

## Path B — Incremental Loading Demonstration

### Full automated demo (generates delta, applies, replays):

```bash
python -m src.main --input data/orders_small_sample.csv --demo-path-b
```

### Or run it separately after the main pipeline:

```bash
# Step 1: Generate a delta file (5 updates + 3 inserts)
python -m src.incremental_loader generate \
    --sample data/orders_small_sample.csv \
    --output data/delta/delta_001.csv

# Step 2: Apply delta #1
python -m src.incremental_loader apply --delta data/delta/delta_001.csv

# Step 3: Replay delta #1 (expect all UNCHANGED)
python -m src.incremental_loader apply --delta data/delta/delta_001.csv

# Step 4: Full demo (all stages in one command)
python -m src.incremental_loader demo --sample data/orders_small_sample.csv
```

**Expected demo output:**
```
[STAGE 1]  Initial validated count : 86,XXX
[STAGE 2]  Generating delta file ...
[STAGE 3]  Applying delta #1 ...
  Delta #1 result:
    Inserted  : 3
    Updated   : 5
    Unchanged : 0
[STAGE 4]  Replaying delta #1 (idempotency check) ...
  Delta #1 REPLAY result:
    Inserted  : 0  (expected: 0)
    Updated   : 0  (expected: 0)
    Unchanged : 8  (expected: all)
IDEMPOTENCY : PASS
```

---

## Idempotency Demonstration

Running the main pipeline twice with the same input must not create duplicate business records:

```bash
# First run — loads 100,000 records
python -m src.main --input data/orders_small_sample.csv

# Second run — same input, same data
python -m src.main --input data/orders_small_sample.csv

# Verify no duplicates in orders_validated
python -c "
from pymongo import MongoClient
db = MongoClient()['midterm_data_pipeline']
total = db.orders_validated.count_documents({})
unique = len(db.orders_validated.distinct('id_order'))
print(f'Total docs: {total}, Unique id_order: {unique}')
assert total == unique, 'DUPLICATE id_order detected!'
print('IDEMPOTENCY: PASS')
"
```

The second run will report `count_updated` or `count_unchanged` — not `count_inserted` — proving idempotency.

---

## Metrics / Reports

After every pipeline run, results are written to:

```
reports/results.json
```

**Example results.json:**
```json
{
    "id_run": "3f7a-...",
    "file_name": "orders_small_sample.csv",
    "file_size_mb": 41.77,
    "used_engine": "python_batch",
    "loaded_raw": 100000,
    "count_valid": 81000,
    "count_corrected": 5600,
    "count_quarantine": 13400,
    "seconds_elapsed": 42.3,
    "throughput": 2364,
    "partitions": 1,
    "size_batch": 5000,
    "count_inserted": 86600,
    "count_updated": 0,
    "count_unchanged": 0,
    "consistency_check": "PASS",
    "counts_case_error": {
        "ID_ORDER_MISSING": 721,
        "ID_ORDER_DUPLICATE": 672,
        "DATE_IMPOSSIBLE_INVALID": 300,
        "JSON_ITEMS_CORRUPTED": 2000,
        "EMAIL_INVALID": 500
    }
}
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `MongoDB connection refused` | Start MongoDB: `mongod --dbpath <path>` |
| `UnicodeEncodeError` in terminal | The pipeline handles Arabic UTF-8 internally; the Windows terminal display is cosmetic only |
| Spark takes too long | Use `--input data/orders_small_sample.csv` during development |
| `ModuleNotFoundError: src` | Run from the project root: `cd midterm-data-pipeline` then `python -m src.main ...` |
| `SPARK_MASTER` connection refused | Do not set `SPARK_MASTER` — it defaults to `local[*]` which needs no cluster |
| Pytest collects src/ scripts | `pytest.ini` restricts collection to `tests/` only — do not run `pytest src/` |

---

## Project Structure

```
midterm-data-pipeline/
├── config/
│   ├── __init__.py
│   └── settings.py              # All configuration
│
├── data/
│   ├── orders_huge_mixed_quality.csv   # 12 GB source (DO NOT MODIFY)
│   ├── orders_small_sample.csv         # 100K-row sample
│   └── delta/
│       └── delta_001.csv               # Generated delta file
│
├── docs/
│   └── architecture.md          # Detailed architecture documentation
│
├── reports/
│   ├── results.json             # Run metrics (generated)
│   └── results.md               # Human-readable summary
│
├── src/
│   ├── __init__.py
│   ├── main.py                  # PRIMARY ENTRY POINT
│   ├── elt_pipeline.py          # ELT orchestration
│   ├── file_router.py           # Size-based engine routing
│   ├── batch_loader.py          # Python Batch raw loader
│   ├── spark_large_loader.py    # PySpark large-file raw loader
│   ├── quality_rules.py         # 10 deterministic cleaning rules
│   ├── quality_processor.py     # Raw → Validated/Quarantine
│   ├── incremental_loader.py    # PATH B incremental loading
│   ├── metrics.py               # Metrics collection + results.json
│   ├── mongo_setup.py           # MongoDB collections + indexes
│   └── create_small_sample.py   # Sample generator
│
├── tests/
│   ├── test_cleaning_rules.py   # Quality rule unit tests
│   └── test_classification.py  # Classification + Path B tests
│
├── conftest.py                  # Pytest sys.path configuration
├── pytest.ini                   # Pytest settings
└── requirements.txt             # Python dependencies
```

---

## Design Decisions

### ELT vs ETL
Raw data is loaded into `orders_raw` **before** any cleaning.  This preserves the original values and provides a complete audit trail.  Cleaning happens as a second stage reading from `orders_raw`.

### Business Key Mapping
The source CSV uses `order_id`.  The validated collection uses `id_order` as the canonical business key.  This mapping is explicit in the code — `id_order = order_id`.  The original CSV is never modified.

### Python Batch vs PySpark
- Python Batch: simpler, no JVM overhead, sufficient for files ≤ 200 MB.
- PySpark: handles 12 GB+ without loading into driver memory.  Runs in `local[*]` mode by default (no cluster required).

### Idempotency
- `orders_validated`: unique index on `id_order` + `upsert=True` prevents duplicates.
- `orders_quarantine`: deterministic `quarantine_key = f"{id_run}|{row}|{order_id}"`.
- Path B: `updated_at` timestamp comparison prevents re-applying stale deltas.

### Why `local[*]` for Spark?
PATH B is selected, not PATH A.  The project must work without a Spark Standalone cluster.  `local[*]` uses all CPU cores and processes the 12 GB file in distributed partitions without any cluster infrastructure.

---

## Collections Schema

### `orders_raw`
```json
{
  "id_run": "uuid",
  "file_source": "data/orders_small_sample.csv",
  "number_row_source": 42,
  "at_ingested": "2025-08-31T10:30:00+00:00",
  "engine_used": "python_batch",
  "order_id": "طلب-100003",
  "record_raw": { ...original CSV fields... },
  ...original CSV fields spread at top level...
}
```

### `orders_validated`
```json
{
  "id_order": "طلب-100003",
  "order_id": "طلب-100003",
  "classification": "CORRECTED",
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "total_amount",
      "original_value": "٧٠٦٠٠٠٫٠",
      "corrected_value": "706000.0",
      "rule_code": "ARABIC_DIGITS"
    }
  ],
  "updated_at": "2025-08-31T10:35:00+00:00",
  "id_run": "uuid"
}
```

### `orders_quarantine`
```json
{
  "quarantine_key": "uuid|42|طلب-100003",
  "order_id": "طلب-100003",
  "classification": "QUARANTINED",
  "quarantine_reasons": ["JSON_ITEMS_CORRUPTED"],
  "quarantined_at": "2025-08-31T10:35:00+00:00",
  "id_run": "uuid"
}
```

 # #   P h a s e   2 :   F a s t A P I ,   A g g r e g a t i o n s ,   &   J o b s 
 R u n   t h e   A P I   w i t h :   u v i c o r n   s r c . a p i : a p p   - - r e l o a d .   A c c e s s   S w a g g e r   a t   h t t p : / / 1 2 7 . 0 . 0 . 1 : 8 0 0 0 / d o c s  
 