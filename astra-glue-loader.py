from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, IntegerType, DateType, TimestampType
import boto3
import json

secret_name = "AstraGlueCreds"
region_name = "us-east-1"

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

get_secret_value_response = client.get_secret_value(
    SecretId=secret_name
)

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

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

spark = SparkSession.builder \
    .appName("CSV to AstraDB") \
    .getOrCreate()

df = spark.read.csv("s3://astra-glue/random_data", schema=schema)

df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="random_model1", keyspace="workshop") \
    .option("spark.cassandra.connection.config.cloud.path","<secure-connect-DBNAME.zip>") \
    .option("spark.cassandra.auth.username", secret.get('astra_clientid') ) \
    .option("spark.cassandra.auth.password", secret.get('astra_clientsecret') ) \
    .save()

spark.stop()
