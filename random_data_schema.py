from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, IntegerType, DateType, TimestampType

schema = StructType([
    StructField("f1", StringType(), True),
    StructField("f2", BooleanType(), True),
    StructField("f3", StringType(), True),
    StructField("f4", FloatType(), True),
    StructField("f5", IntegerType(), True),
    StructField("f6", BooleanType(), True),
    StructField("f7", BooleanType(), True),
    StructField("f8", BooleanType(), True),
    StructField("f9", FloatType(), True),
    StructField("f10", DateType(), True),
    StructField("f11", BooleanType(), True),
    StructField("f12", StringType(), True),
    StructField("f13", TimestampType(), True),
    StructField("f14", IntegerType(), True)
])
