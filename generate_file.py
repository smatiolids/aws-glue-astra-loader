from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Generate Random CSV") \
    .getOrCreate()


# Generate random data
df = spark.range(0, 100000).select(
    (rand() * 99999999999999).cast("string").alias("f1"),
    (rand() > 0.5).alias("f2"),
    (rand() * 99999999999999).cast("string").alias("f3"),
    (randn() * 10000).cast("float").alias("f4"),
    (rand() * 1000).cast("int").alias("f5"),
    (rand() > 0.5).alias("f6"),
    (rand() > 0.5).alias("f7"),
    (rand() > 0.5).alias("f8"),
    (randn() * 10000).cast("float").alias("f9"),
    (rand() * 100).cast("int").alias("f10"),
    (rand() > 0.5).alias("f11"),
    (rand() * 100).cast("string").alias("f12"),
    (rand() * 100).cast("timestamp").alias("f13"),
    (rand() * 1000).cast("int").alias("f14")
)

# Write DataFrame to CSV
df.write \
    .mode("overwrite") \
    .csv("random_data", header=True)

# Stop SparkSession
spark.stop()
