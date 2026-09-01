from pyspark.sql import SparkSession
import time


INPUT_FILE = "data/orders_small_sample.csv"

SPARK_MASTER = "spark://192.168.8.107:7077"

MONGO_URI = "mongodb://127.0.0.1:27017"
DATABASE = "midterm_data_pipeline"
COLLECTION = "orders_raw_spark"


spark = (
    SparkSession.builder
    .appName("SmallFilePySparkFixedCSV")
    .master(SPARK_MASTER)
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("PYSPARK SMALL FILE -> MONGODB")
print("=" * 70)
print(f"Input      : {INPUT_FILE}")
print(f"Master     : {SPARK_MASTER}")
print(f"Database   : {DATABASE}")
print(f"Collection : {COLLECTION}")
print("=" * 70)

start = time.perf_counter()

df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", False)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .load(INPUT_FILE)
)

print("CSV loaded successfully.")

count = df.count()

print(f"Records read: {count:,}")

print("\nChecking items_json before MongoDB...")

df.select(
    "order_id",
    "items_json"
).show(3, truncate=False)

print("Writing to MongoDB...")

(
    df.write
    .format("mongodb")
    .mode("overwrite")
    .option(
        "spark.mongodb.write.connection.uri",
        f"{MONGO_URI}/{DATABASE}.{COLLECTION}"
    )
    .option(
        "spark.mongodb.write.maxBatchSize",
        "1000"
    )
    .save()
)

elapsed = time.perf_counter() - start

print("=" * 70)
print("PYSPARK SMALL FILE COMPLETED")
print("=" * 70)
print(f"Records : {count:,}")
print(f"Time    : {elapsed:.3f}s")
print(f"Rate    : {count / elapsed:,.0f} records/s")
print("=" * 70)

spark.stop()
