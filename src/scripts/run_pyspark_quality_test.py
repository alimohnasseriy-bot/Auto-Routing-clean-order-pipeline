from pyspark.sql import SparkSession
import json
import re
import time


SPARK_MASTER = "spark://192.168.8.107:7077"

MONGO_URI = "mongodb://127.0.0.1:27017"
DATABASE = "midterm_data_pipeline"
INPUT_COLLECTION = "orders_raw_spark"


def clean_number(value):
    if value is None:
        return None

    value = str(value).strip()

    # Arabic digits
    arabic_digits = "٠١٢٣٤٥٦٧٨٩"
    english_digits = "0123456789"

    for a, e in zip(arabic_digits, english_digits):
        value = value.replace(a, e)

    value = value.replace("٫", ".")
    value = value.replace(",", "")

    try:
        return float(value)
    except:
        return None


def validate_email(email):
    if not email:
        return False

    return bool(
        re.match(
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            str(email)
        )
    )


def process_record(record):

    record = dict(record)

    corrections = []
    quarantine_reasons = []

    # ---------------------------------------------------------
    # Order ID
    # ---------------------------------------------------------

    order_id = record.get("order_id")

    if order_id is None or str(order_id).strip() == "":
        quarantine_reasons.append("missing_order_id")

    # ---------------------------------------------------------
    # Email
    # ---------------------------------------------------------

    email = record.get("customer_email")

    if email and not validate_email(email):
        quarantine_reasons.append("invalid_customer_email")

    # ---------------------------------------------------------
    # Total amount
    # ---------------------------------------------------------

    original_total = record.get("total_amount")
    total = clean_number(original_total)

    if total is None:
        quarantine_reasons.append("invalid_total_amount")
    elif str(original_total) != str(total):
        corrections.append({
            "field": "total_amount",
            "from": original_total,
            "to": total
        })

    # ---------------------------------------------------------
    # Items JSON
    # ---------------------------------------------------------

    items_raw = record.get("items_json")

    try:

        items = json.loads(items_raw)

        if not isinstance(items, list):
            raise ValueError()

        for index, item in enumerate(items):

            qty = item.get("qty")

            if qty is None or float(qty) <= 0:
                quarantine_reasons.append(
                    f"item_{index}_invalid_qty"
                )

    except:

        quarantine_reasons.append("invalid_items_json")

    # ---------------------------------------------------------
    # Classification
    # ---------------------------------------------------------

    if quarantine_reasons:

        classification = "QUARANTINED"

    elif corrections:

        classification = "CORRECTED"

    else:

        classification = "VALID"

    return classification, corrections, quarantine_reasons


spark = (
    SparkSession.builder
    .appName("PySparkQualityTest")
    .master(SPARK_MASTER)
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("PYSPARK QUALITY TEST")
print("=" * 70)

start = time.perf_counter()

df = (
    spark.read
    .format("mongodb")
    .option(
        "spark.mongodb.read.connection.uri",
        f"{MONGO_URI}/{DATABASE}.{INPUT_COLLECTION}"
    )
    .load()
)

print("MongoDB collection loaded.")

records = (
    df
    .limit(100)
    .collect()
)

print(f"Processing sample : {len(records)}")

valid_count = 0
corrected_count = 0
quarantine_count = 0

for row in records:

    record = row.asDict(recursive=True)

    classification, corrections, reasons = process_record(record)

    if classification == "VALID":
        valid_count += 1

    elif classification == "CORRECTED":
        corrected_count += 1

    else:
        quarantine_count += 1

elapsed = time.perf_counter() - start

print("=" * 70)
print("PYSPARK QUALITY TEST COMPLETED")
print("=" * 70)

print(f"Processed   : {len(records)}")
print(f"VALID       : {valid_count}")
print(f"CORRECTED   : {corrected_count}")
print(f"QUARANTINED : {quarantine_count}")
print(f"Time        : {elapsed:.3f}s")

print("=" * 70)

spark.stop()
