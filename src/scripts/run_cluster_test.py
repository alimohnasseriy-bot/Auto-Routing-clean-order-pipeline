from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ClusterTest")
    .master("spark://192.168.8.107:7077")
    .getOrCreate()
)

print("=" * 60)
print("SPARK CLUSTER TEST")
print("=" * 60)
print("Spark Version:", spark.version)
print("Master:", spark.sparkContext.master)

data = list(range(1, 100001))

rdd = spark.sparkContext.parallelize(data, 8)

result = rdd.map(lambda x: x * 2).sum()

print("Records:", rdd.count())
print("Result:", result)

print("=" * 60)
print("CLUSTER TEST COMPLETED")
print("=" * 60)

spark.stop()
