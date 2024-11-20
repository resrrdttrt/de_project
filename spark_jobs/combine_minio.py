from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_sub
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CombineMinIO") \
    .getOrCreate()

# MinIO configuration
MINIO_ENDPOINT = "172.17.0.1:9000"
MINIO_ACCESS_KEY = "kStHEgiS0L8wSMHBoOq6"
MINIO_SECRET_KEY = "6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV"
MINIO_BUCKET = 'mybucket'
GCS_BUCKET = 'mybucket-qaz'

if len(sys.argv) < 2:
    print("Error: Missing required parameter <date>")
    sys.exit(1)
date = sys.argv[1]

# Load data from MinIO
df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/stock_data/{date}")

# Save as CSV to MinIO
output_path = f"s3a://{MINIO_BUCKET}/stock_data_combined/{date}"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# # Save as CSV to GCS
# output_path = f"gs://{GCS_BUCKET}/stock_data_combined/{date}"
# df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Data saved as CSV for date {date}.")