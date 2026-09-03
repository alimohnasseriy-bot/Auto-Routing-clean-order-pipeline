# Pipeline Results Report

**Generated at:** 2026-09-02T23:09:41 UTC

---

## Run Summary

| Metric | Value |
|--------|-------|
| **Run ID** | `5331c0f0-90ef-45c4-a0ef-aea170b166d2` |
| **Input File** | `orders_small_sample.csv` |
| **File Size** | 41.77 MB |
| **Engine Used** | Python Batch |
| **Total Rows Read** | 100,000 |
| **Loaded to Raw** | 100,000 |
| **Time Elapsed** | 357.88 seconds |
| **Throughput** | 279.4 records/s |
| **Batch Size** | 5,000 |
| **Partitions** | 1 |

---

## Classification Results

| Classification | Count | Percentage |
|---------------|-------|------------|
| ✅ **VALID** | 52,151 | 52.15% |
| 🔧 **CORRECTED** | 39,746 | 39.75% |
| 🚫 **QUARANTINED** | 8,103 | 8.10% |
| **Total** | **100,000** | **100%** |

---

## Upsert Statistics

| Operation | Count |
|-----------|-------|
| Inserted | 0 |
| Updated | 91,897 |
| Unchanged | 0 |

---

## Quarantine Breakdown

| Error Code | Count |
|------------|-------|
| `ID_ORDER_MISSING` | 5,142 |
| `ID_CUSTOMER_MISSING` | 2,196 |
| `ID_ORDER_DUPLICATE` | 4,771 |
| `DATE_IMPOSSIBLE_INVALID` | 13,479 |
| `JSON_ITEMS_CORRUPTED` | 9,437 |
| `EMAIL_INVALID` | 9,434 |
| `ERRORS_CONFLICTING_MULTIPLE` | 1,026 |

---

## Consistency Check

```
loaded_raw (100,000) = count_valid (52,151) + count_corrected (39,746) + count_quarantine (8,103)
100,000 = 100,000
Result: PASS ✅
```

---

## Database Totals (Cumulative)

| Collection | Documents |
|-----------|-----------|
| `orders_raw` | 800,000 |
| `orders_validated` | 92,640 |
| `orders_quarantine` | 53,271 |
| Valid in validated | 52,648 |
| Corrected in validated | 39,992 |

---

## Execution & Verification Screenshots

All execution screenshots demonstrating the complete pipeline stages and official requirements are stored in [`reports/screenshots/`](screenshots/):

1. **File Router Selection (Python Batch for Small File):**
   - File: `screenshots/terminal_python_batch_router.png`
   - Demonstrates routing `orders_small_sample.csv` (41.77 MB <= 200.0 MB) to `PYTHON_BATCH`.

2. **File Router Selection (PySpark for Huge File):**
   - File: `screenshots/terminal_pyspark_router.png`
   - Demonstrates routing `orders_huge_mixed_quality.csv` (12650.32 MB > 200.0 MB) to `PYSPARK`.

3. **Raw Ingestion Layer (`orders_raw`):**
   - File: `screenshots/compass_raw_record.png`
   - Demonstrates ELT pattern: raw data stored untouched with metadata (`id_run`, `file_source`, `number_row_source`, `at_ingested`, `engine_used`, `record_raw`).

4. **Validated Record (`VALID`):**
   - File: `screenshots/compass_valid_record.png`
   - Demonstrates business key `id_order` mapping and valid classification without modifications.

5. **Corrected Record (`CORRECTED`):**
   - File: `screenshots/compass_corrected_record.png`
   - Demonstrates the complete audit trail with `corrections` array showing original vs corrected values and rule codes.

6. **Quarantined Record (`QUARANTINED`):**
   - File: `screenshots/compass_quarantined_record.png`
   - Demonstrates deterministic rejection with explicit `quarantine_reasons` array without data loss.

7. **MongoDB Collections Overview:**
   - File: `screenshots/compass_collections.png`
   - Demonstrates total storage and document counts across `orders_raw`, `orders_validated`, and `orders_quarantine`.

8. **Apache Spark Distributed UI:**
   - File: `screenshots/spark_ui.png`
   - Demonstrates real Spark job execution, active stages, and partition distribution on `localhost:4040`.

---

*Report generated automatically by the midterm-data-pipeline metrics module.*
