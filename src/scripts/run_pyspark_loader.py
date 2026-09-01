from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import time


INPUT_FILE = "data/orders_huge_mixed_quality.csv"
SPARK_MASTER = "spark://192.168.8.107:7077"

SAMPLE_SIZE = 10000


def main():

    spark = (
        SparkSession.builder
        .appName("LargeFileQualityTest")
        .master(SPARK_MASTER)
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("PYSPARK QUALITY TEST")
    print("=" * 70)
    print(f"Input       : {INPUT_FILE}")
    print(f"Master      : {SPARK_MASTER}")
    print(f"Sample size : {SAMPLE_SIZE:,}")
    print("=" * 70)

    start = time.perf_counter()

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .csv(INPUT_FILE)
    )

    print("CSV loaded successfully.")

    sample = df.limit(SAMPLE_SIZE)

    sample_count = sample.count()

    # ---------------------------------------------------------
    # Basic quality checks
    # ---------------------------------------------------------

    stats = sample.select(
        count(
            when(
                col("order_id").isNull() |
                (col("order_id") == ""),
                True
            )
        ).alias("missing_order_id"),

        count(
            when(
                col("customer_email").isNull() |
                (col("customer_email") == ""),
                True
            )
        ).alias("missing_email"),

        count(
            when(
                col("items_json").isNull() |
                (col("items_json") == ""),
                True
            )
        ).alias("missing_items_json"),

        count(
            when(
                col("total_amount").isNull() |
                (col("total_amount") == ""),
                True
            )
        ).alias("missing_total_amount")
    ).collect()[0]

    elapsed = time.perf_counter() - start

    print("=" * 70)
    print("PYSPARK QUALITY TEST COMPLETED")
    print("=" * 70)

    print(f"Sample records     : {sample_count:,}")
    print(f"Missing order_id   : {stats['missing_order_id']:,}")
    print(f"Missing email      : {stats['missing_email']:,}")
    print(f"Missing items_json : {stats['missing_items_json']:,}")
    print(f"Missing total      : {stats['missing_total_amount']:,}")

    print(f"Time               : {elapsed:.3f}s")

    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
