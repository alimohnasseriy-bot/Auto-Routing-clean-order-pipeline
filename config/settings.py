"""
config/settings.py
==================
Central configuration for the midterm-data-pipeline project.

All important values live here.  Any value can be overridden with an
environment variable of the same name without changing this file.

Usage
-----
    from config.settings import BATCH_SIZE, MONGODB_URI, ...
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root (two levels up from this file: config/ -> project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# File routing
# ---------------------------------------------------------------------------
# Files at or below this threshold use the Python Batch engine.
# Files above this threshold use PySpark.
# Configurable so the threshold can be adjusted without code changes.
SMALL_FILE_THRESHOLD_MB: float = float(
    os.environ.get("SMALL_FILE_THRESHOLD_MB", 200)
)

# ---------------------------------------------------------------------------
# Sample generation
# ---------------------------------------------------------------------------
SMALL_SAMPLE_ROWS: int = int(
    os.environ.get("SMALL_SAMPLE_ROWS", 100_000)
)

# ---------------------------------------------------------------------------
# Directories
# ---------------------------------------------------------------------------
INPUT_DIR: Path = PROJECT_ROOT / os.environ.get("INPUT_DIR", "data")
REPORTS_DIR: Path = PROJECT_ROOT / os.environ.get("REPORTS_DIR", "reports")
DELTA_DIR: Path = INPUT_DIR / "delta"

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGODB_URI: str = os.environ.get(
    "MONGODB_URI", "mongodb://localhost:27017"
)
MONGODB_DATABASE: str = os.environ.get(
    "MONGODB_DATABASE", "midterm_data_pipeline"
)

RAW_COLLECTION: str = "orders_raw"
VALIDATED_COLLECTION: str = "orders_validated"
QUARANTINE_COLLECTION: str = "orders_quarantine"

# ---------------------------------------------------------------------------
# Python Batch loader
# ---------------------------------------------------------------------------
BATCH_SIZE: int = int(os.environ.get("BATCH_SIZE", 5_000))

# ---------------------------------------------------------------------------
# Apache Spark
# ---------------------------------------------------------------------------
# Default: local[*]  — use all available CPU cores on the local machine.
# Override with environment variable SPARK_MASTER to point at a real cluster,
# e.g.:  export SPARK_MASTER=spark://my-host:7077
SPARK_MASTER: str = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_APP_NAME: str = os.environ.get(
    "SPARK_APP_NAME", "midterm-data-pipeline"
)

# ---------------------------------------------------------------------------
# Spark performance tuning (local mode defaults)
# ---------------------------------------------------------------------------
SPARK_SQL_SHUFFLE_PARTITIONS: int = int(
    os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", 4)
)

# ---------------------------------------------------------------------------
# Results / reports
# ---------------------------------------------------------------------------
RESULTS_JSON: Path = REPORTS_DIR / "results.json"
