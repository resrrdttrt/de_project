from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_date, to_date, lag, avg, date_format
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkMinIO") \
    .getOrCreate()

# Kafka configuration
KAFKA_BROKER = '172.17.0.1:9092'
KAFKA_TOPIC = 'stock_kafka_topic'

# MinIO configuration
MINIO_ENDPOINT = "172.17.0.1:9000"
MINIO_ACCESS_KEY = "kStHEgiS0L8wSMHBoOq6"
MINIO_SECRET_KEY = "6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV"
MINIO_BUCKET = 'mybucket'


# Define the schema for the incoming data
message_schema = StructType([
    # StructField("date", StringType(), True),
    StructField("code", StringType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("open", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", LongType(), True)
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Parse the JSON messages
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), message_schema).alias("data")) \
    .select("data.*")

# Validate the data
validated_df = parsed_df.filter(
    (col("high").isNotNull()) & 
    (col("low").isNotNull()) & 
    (col("open").isNotNull()) & 
    (col("close").isNotNull()) & 
    (col("volume").isNotNull())
)

# Calculate average price
stock_index_df = validated_df.withColumn(
    "average_price", (col("high") + col("low") + col("close")) / 3
).withColumn(
    "date", date_format(current_date(), 'dd-MM-yyyy')
)


# Write to MinIO with dynamic partitioning
query = stock_index_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/stock_data/checkpoint") \
    .option("path", f"s3a://{MINIO_BUCKET}/stock_data/") \
    .partitionBy("date") \
    .start()

# Write to MinIO as CSV
csv_query = stock_index_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/stock_data_csv/checkpoint") \
    .option("path", f"s3a://{MINIO_BUCKET}/stock_data_csv/") \
    .partitionBy("date") \
    .start()

# Await termination for both queries
query.awaitTermination()
csv_query.awaitTermination()

# # Write to console
# console_query = stock_index_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# console_query.awaitTermination()