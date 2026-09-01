"""
src/spark_large_loader.py
=========================
PySpark loader for large CSV files (> SMALL_FILE_THRESHOLD_MB).

Uses SparkSession in local[*] mode by default.  An external Spark cluster
can be used by setting the SPARK_MASTER environment variable.

ELT contract
------------
Every source record is written to orders_raw BEFORE quality processing.
Raw documents include the same metadata fields as the Python Batch loader
(id_run, file_source, number_row_source, at_ingested, engine_used, record_raw).

The MongoDB Spark connector is NOT required.  Instead, processed partitions
are collected in micro-batches and inserted via PyMongo — this avoids a
heavyweight connector dependency while keeping real Spark processing.

Design decisions
----------------
- inferSchema=False: keeps all fields as strings in the raw stage, preserving
  dirty values exactly as they appear in the source.
- local[*]:  uses all available CPU cores on the local machine by default.
- try/finally: SparkSession is always stopped, even on errors.
- Partitions are reported in metrics.
"""

import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    BATCH_SIZE,
    MONGODB_DATABASE,
    MONGODB_URI,
    RAW_COLLECTION,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_SQL_SHUFFLE_PARTITIONS,
)

# ---------------------------------------------------------------------------
# Explicit schema — all string so dirty values are preserved exactly
# ---------------------------------------------------------------------------
CSV_SCHEMA = StructType([
    StructField("order_id",        StringType(), True),
    StructField("order_date",      StringType(), True),
    StructField("status",          StringType(), True),
    StructField("customer_id",     StringType(), True),
    StructField("customer_name",   StringType(), True),
    StructField("customer_phone",  StringType(), True),
    StructField("customer_email",  StringType(), True),
    StructField("city",            StringType(), True),
    StructField("district",        StringType(), True),
    StructField("delivery_type",   StringType(), True),
    StructField("delivery_cost",   StringType(), True),
    StructField("payment_method",  StringType(), True),
    StructField("payment_status",  StringType(), True),
    StructField("payment_amount",  StringType(), True),
    StructField("currency",        StringType(), True),
    StructField("total_amount",    StringType(), True),
    StructField("items_json",      StringType(), True),
])


def load_large_csv_to_raw(
    file_path: str,
    id_run: str,
    batch_size: int = BATCH_SIZE,
    spark_master: str = SPARK_MASTER,
    mongodb_uri: str = MONGODB_URI,
    database: str = MONGODB_DATABASE,
    collection_name: str = RAW_COLLECTION,
) -> Dict[str, Any]:
    """
    Load a large CSV file into orders_raw using PySpark.

    Parameters
    ----------
    file_path    : absolute or relative path to the CSV
    id_run       : unique run identifier shared across pipeline stages
    batch_size   : PyMongo insert batch size per partition
    spark_master : Spark master URL (default: local[*])
    mongodb_uri  : MongoDB connection string
    database     : target MongoDB database
    collection_name : target collection (default: orders_raw)

    Returns
    -------
    dict with: loaded_raw, partitions, seconds_elapsed, throughput
    """
    spark: SparkSession | None = None

    try:
        spark = (
            SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master(spark_master)
            .config("spark.sql.shuffle.partitions", str(SPARK_SQL_SHUFFLE_PARTITIONS))
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        print("=" * 60)
        print("PYSPARK LARGE FILE LOADER  [ELT — RAW STAGE]")
        print("=" * 60)
        print(f"File       : {file_path}")
        print(f"Master     : {spark_master}")
        print(f"Database   : {database}")
        print(f"Collection : {collection_name}")
        print(f"Run ID     : {id_run}")
        print("=" * 60)

        start_total = time.perf_counter()
        ingested_at = datetime.now(timezone.utc).isoformat()

        df = (
            spark.read
            .format("csv")
            .schema(CSV_SCHEMA)
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .option("encoding", "UTF-8")
            .load(file_path)
        )

        num_partitions = df.rdd.getNumPartitions()
        print(f"CSV loaded. Partitions: {num_partitions}")

        # Broadcast metadata to executors
        id_run_bc = spark.sparkContext.broadcast(id_run)
        file_source_bc = spark.sparkContext.broadcast(file_path)
        ingested_at_bc = spark.sparkContext.broadcast(ingested_at)
        mongo_uri_bc = spark.sparkContext.broadcast(mongodb_uri)
        db_name_bc = spark.sparkContext.broadcast(database)
        coll_name_bc = spark.sparkContext.broadcast(collection_name)
        batch_size_bc = spark.sparkContext.broadcast(batch_size)

        # Process each partition directly — write to MongoDB via PyMongo
        def write_partition(rows):
            """Called once per Spark partition on the executor."""
            from pymongo import MongoClient as _MongoClient
            from pymongo.errors import BulkWriteError as _BWError

            _client = _MongoClient(mongo_uri_bc.value, serverSelectionTimeoutMS=30_000)
            _db = _client[db_name_bc.value]
            _coll = _db[coll_name_bc.value]

            _id_run = id_run_bc.value
            _file = file_source_bc.value
            _at = ingested_at_bc.value
            _bsize = batch_size_bc.value

            batch: List[Dict] = []
            row_num = 0
            inserted_total = 0

            try:
                for row in rows:
                    row_num += 1
                    raw_fields = row.asDict()

                    doc = {
                        "id_run": _id_run,
                        "file_source": _file,
                        "number_row_source": row_num,
                        "at_ingested": _at,
                        "engine_used": "pyspark",
                        "order_id": raw_fields.get("order_id", ""),
                        "record_raw": raw_fields,
                        **raw_fields,
                    }

                    batch.append(doc)

                    if len(batch) >= _bsize:
                        try:
                            res = _coll.insert_many(batch, ordered=False)
                            inserted_total += len(res.inserted_ids)
                        except _BWError as exc:
                            inserted_total += exc.details.get("nInserted", 0)
                        batch = []

                if batch:
                    try:
                        res = _coll.insert_many(batch, ordered=False)
                        inserted_total += len(res.inserted_ids)
                    except _BWError as exc:
                        inserted_total += exc.details.get("nInserted", 0)

            finally:
                _client.close()

            yield inserted_total

        loaded_counts = df.rdd.mapPartitions(write_partition).collect()
        total_loaded = sum(loaded_counts)

        total_elapsed = time.perf_counter() - start_total
        throughput = total_loaded / total_elapsed if total_elapsed > 0 else 0

        print("=" * 60)
        print("PYSPARK LOAD COMPLETED")
        print("=" * 60)
        print(f"Records loaded : {total_loaded:,}")
        print(f"Partitions     : {num_partitions}")
        print(f"Total time     : {total_elapsed:.3f}s")
        print(f"Throughput     : {throughput:,.0f} records/s")
        print("=" * 60)

        return {
            "loaded_raw": total_loaded,
            "partitions": num_partitions,
            "seconds_elapsed": round(total_elapsed, 3),
            "throughput": round(throughput, 1),
        }

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="PySpark ELT raw loader for large CSV files."
    )
    parser.add_argument("--input", required=True, help="Path to the CSV file")
    parser.add_argument(
        "--id-run", default=str(uuid.uuid4()), help="Run ID (auto-generated)"
    )
    args = parser.parse_args()

    load_large_csv_to_raw(file_path=args.input, id_run=args.id_run)
