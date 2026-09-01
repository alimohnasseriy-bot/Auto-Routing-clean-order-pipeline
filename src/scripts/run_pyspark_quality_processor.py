from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    ArrayType, MapType
)
import json
import re
import time
from datetime import datetime


MASTER = "spark://192.168.8.107:7077"
MONGO = "mongodb://127.0.0.1:27017"
DB = "midterm_data_pipeline"

INPUT = "orders_raw_spark"
VALIDATED = "orders_validated_spark"
QUARANTINE = "orders_quarantine_spark"


def normalize_number(value):
    if value is None:
        return None

    text = str(value).strip()

    text = text.translate(
        str.maketrans(
            "٠١٢٣٤٥٦٧٨٩",
            "0123456789"
        )
    )

    text = text.replace("٫", ".")
    text = text.replace(",", "")

    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def normalize_date(value):
    if not value:
        return None

    value = str(value).strip()

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue

    return None


def is_valid_email(value):
    if not value:
        return False

    return bool(
        re.match(
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            str(value).strip()
        )
    )


def validate_items(items_json):

    errors = []

    try:
        items = json.loads(items_json)
    except (json.JSONDecodeError, TypeError):
        return None, ["invalid_items_json"]

    if not isinstance(items, list) or not items:
        return None, ["items_must_be_non_empty_list"]

    for index, item in enumerate(items):

        if not isinstance(item, dict):
            errors.append(
                f"item_{index}_not_object"
            )
            continue

        qty = item.get("qty")

        try:
            qty_number = float(qty)

            if qty_number <= 0:
                errors.append(
                    f"item_{index}_invalid_qty"
                )

        except (ValueError, TypeError):
            errors.append(
                f"item_{index}_invalid_qty"
            )

    if errors:
        return None, errors

    return items, []


def process_record(record):

    result = dict(record)

    corrections = []
    quarantine_reasons = []

    # 1. order_id
    order_id = str(
        result.get("order_id", "")
    ).strip()

    if not order_id:
        quarantine_reasons.append(
            "missing_order_id"
        )
    else:
        result["order_id"] = order_id

    # 2. order_date
    original_date = result.get("order_date")

    normalized_date = normalize_date(
        original_date
    )

    if normalized_date is None:
        quarantine_reasons.append(
            "invalid_order_date"
        )
    else:

        if (
            str(original_date).strip()
            != normalized_date
        ):
            corrections.append({
                "field": "order_date",
                "from": original_date,
                "to": normalized_date
            })

        result["order_date"] = normalized_date

    # 3. email
    email = str(
        result.get("customer_email", "")
    ).strip()

    if not is_valid_email(email):
        quarantine_reasons.append(
            "invalid_customer_email"
        )
    else:
        result["customer_email"] = email

    # 4. phone
    phone = str(
        result.get("customer_phone", "")
    ).strip()

    if not phone or not phone.isdigit():
        quarantine_reasons.append(
            "invalid_customer_phone"
        )
    elif len(phone) != 9:
        quarantine_reasons.append(
            "invalid_customer_phone_length"
        )
    else:
        result["customer_phone"] = phone

    # 5. numeric fields
    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]:

        original_value = result.get(field)

        normalized_value = normalize_number(
            original_value
        )

        if normalized_value is None:

            quarantine_reasons.append(
                f"invalid_{field}"
            )

        else:

            if (
                str(original_value).strip()
                != str(normalized_value)
            ):
                corrections.append({
                    "field": field,
                    "from": original_value,
                    "to": normalized_value
                })

            result[field] = normalized_value

    # 6. items_json
    items, item_errors = validate_items(
        result.get("items_json")
    )

    if item_errors:

        quarantine_reasons.extend(
            item_errors
        )

    else:

        result["items_json"] = items

    # classification
    if quarantine_reasons:
        classification = "QUARANTINED"
    elif corrections:
        classification = "CORRECTED"
    else:
        classification = "VALID"

    result["classification"] = classification

    if corrections:
        result["corrections"] = corrections

    if quarantine_reasons:
        result["quarantine_reasons"] = (
            quarantine_reasons
        )

    return result


def process_partition(rows):

    for row in rows:

        record = row.asDict(
            recursive=True
        )

        record.pop("_id", None)

        result = process_record(record)

        yield Row(
            order_id=result.get("order_id"),
            order_date=result.get("order_date"),
            status=result.get("status"),
            customer_id=result.get("customer_id"),
            customer_name=result.get("customer_name"),
            customer_phone=result.get("customer_phone"),
            customer_email=result.get("customer_email"),
            city=result.get("city"),
            district=result.get("district"),
            delivery_type=result.get("delivery_type"),
            delivery_cost=result.get("delivery_cost"),
            payment_method=result.get("payment_method"),
            payment_status=result.get("payment_status"),
            payment_amount=result.get("payment_amount"),
            currency=result.get("currency"),
            total_amount=result.get("total_amount"),
            items_json=result.get("items_json"),
            classification=result.get("classification"),
            corrections=result.get("corrections"),
            quarantine_reasons=result.get(
                "quarantine_reasons"
            )
        )


schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("delivery_type", StringType(), True),
    StructField("delivery_cost", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", DoubleType(), True),

    StructField(
        "items_json",
        ArrayType(
            MapType(
                StringType(),
                StringType(),
                True
            ),
            True
        ),
        True
    ),

    StructField(
        "classification",
        StringType(),
        True
    ),

    StructField(
        "corrections",
        ArrayType(
            MapType(
                StringType(),
                StringType(),
                True
            ),
            True
        ),
        True
    ),

    StructField(
        "quarantine_reasons",
        ArrayType(
            StringType(),
            True
        ),
        True
    )
])


spark = (
    SparkSession.builder
    .appName("PySparkQualityProcessorExact")
    .master(MASTER)
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("PYSPARK QUALITY PROCESSOR - EXACT RULES")
print("=" * 70)

start = time.perf_counter()

df = (
    spark.read
    .format("mongodb")
    .option(
        "spark.mongodb.read.connection.uri",
        f"{MONGO}/{DB}.{INPUT}"
    )
    .load()
)

total = df.count()

print(f"Input records : {total:,}")
print("Applying quality rules...")

processed_rdd = (
    df.rdd
    .mapPartitions(process_partition)
)

processed_df = spark.createDataFrame(
    processed_rdd,
    schema=schema
)

processed_df.cache()

processed_df.count()

valid_count = (
    processed_df
    .filter("classification = 'VALID'")
    .count()
)

corrected_count = (
    processed_df
    .filter("classification = 'CORRECTED'")
    .count()
)

quarantine_count = (
    processed_df
    .filter("classification = 'QUARANTINED'")
    .count()
)

validated_df = (
    processed_df
    .filter(
        "classification IN ('VALID','CORRECTED')"
    )
)

quarantine_df = (
    processed_df
    .filter(
        "classification = 'QUARANTINED'"
    )
)

print("Writing validated records...")

(
    validated_df.write
    .format("mongodb")
    .mode("overwrite")
    .option(
        "spark.mongodb.write.connection.uri",
        f"{MONGO}/{DB}.{VALIDATED}"
    )
    .option(
        "spark.mongodb.write.maxBatchSize",
        "1000"
    )
    .save()
)

print("Writing quarantine records...")

(
    quarantine_df.write
    .format("mongodb")
    .mode("overwrite")
    .option(
        "spark.mongodb.write.connection.uri",
        f"{MONGO}/{DB}.{QUARANTINE}"
    )
    .option(
        "spark.mongodb.write.maxBatchSize",
        "1000"
    )
    .save()
)

classified = (
    valid_count
    + corrected_count
    + quarantine_count
)

elapsed = time.perf_counter() - start

print("=" * 70)
print("PYSPARK QUALITY PROCESSOR COMPLETED")
print("=" * 70)

print(f"Processed             : {total:,}")
print(f"VALID                 : {valid_count:,}")
print(f"CORRECTED             : {corrected_count:,}")
print(f"QUARANTINED           : {quarantine_count:,}")

print("-" * 70)

print(
    f"Classification check  : "
    f"{classified:,} / {total:,}"
)

if classified == total:
    print("Coverage check        : PASS")
else:
    print("Coverage check        : FAIL")

print(f"Time                  : {elapsed:.3f}s")

if elapsed > 0:
    print(
        f"Processing rate       : "
        f"{total / elapsed:,.0f} records/s"
    )

print("=" * 70)

processed_df.unpersist()

spark.stop()
