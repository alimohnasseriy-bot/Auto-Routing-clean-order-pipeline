from pyspark.sql import SparkSession

MONGO_URI = "mongodb://127.0.0.1:27017"
DATABASE = "midterm_data_pipeline"
COLLECTION = "spark_connector_test"


spark = (
    SparkSession.builder
    .appName("MongoConnectorTest")
    .master("spark://192.168.8.107:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("MONGODB SPARK CONNECTOR TEST")
print("=" * 70)

df = spark.createDataFrame(
    [
        ("spark-test-1", "VALID"),
        ("spark-test-2", "CORRECTED"),
        ("spark-test-3", "QUARANTINED"),
    ],
    ["test_id", "classification"]
)

print("Writing 3 test records to MongoDB...")

(
    df.write
    .format("mongodb")
    .mode("append")
    .option(
        "spark.mongodb.write.connection.uri",
        f"{MONGO_URI}/{DATABASE}.{COLLECTION}"
    )
    .save()
)

print("=" * 70)
print("MONGODB CONNECTOR TEST COMPLETED")
print("=" * 70)

spark.stop()
