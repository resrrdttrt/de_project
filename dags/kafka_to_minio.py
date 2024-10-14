import json
import random
import string
import time
from datetime import datetime, timedelta
from io import BytesIO

import boto3
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from kafka import KafkaConsumer

# Constants
KAFKA_BROKER = '172.17.0.1:9092'
KAFKA_TOPIC = 'stock_data'
MINIO_ENDPOINT = "http://172.17.0.1:9000"
MINIO_ACCESS_KEY = "zbtGUapSHomBKAWDPV6F"
MINIO_SECRET_KEY = "9lKZVP3v1UgmJd4KeYnzvHthXXtx8uZ53oda120W"
MINIO_BUCKET = 'mybucket'

@dag(
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': days_ago(1),
    },
    description="DAG to consume Kafka messages and upload to MinIO",
    schedule_interval='@hourly',
    catchup=False,
    tags=["kafka", "minio"],
)
def kafka_to_minio():

    @task()
    def upload_to_minio(data, bucket_name, object_name):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            endpoint_url=MINIO_ENDPOINT,
            region_name="us-east-1"
        )

        # Convert data to bytes
        data_bytes = BytesIO(data.encode())

        # Upload data to MinIO
        s3.upload_fileobj(data_bytes, bucket_name, object_name)
        print(f"Uploaded {object_name} to MinIO")

    @task()
    def consume_and_store_to_minio_in_batches(kafka_topic, minio_bucket, batch_size=100, batch_interval=10):
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        batch = []
        last_upload_time = time.time()

        for message in consumer:
            data = message.value
            batch.append(data)

            if len(batch) >= batch_size or (time.time() - last_upload_time >= batch_interval):
                random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                batch_filename = f"{current_date}_{random_string}_batch.json"

                try:
                    batch_data = json.dumps(batch)
                    upload_to_minio(batch_data, minio_bucket, batch_filename)
                    last_upload_time = time.time()
                    batch.clear()
                except Exception as e:
                    print(f"Error occurred while uploading to MinIO: {e}")

    # Main task to consume messages and upload them to MinIO
    consume_and_upload_task = consume_and_store_to_minio_in_batches(
        kafka_topic=KAFKA_TOPIC,  # Kafka topic
        minio_bucket=MINIO_BUCKET,   # MinIO bucket name
        batch_size=100,             # Number of messages to process in one batch
        batch_interval=10           # Maximum time between batches in seconds
    )

dag_instance = kafka_to_minio()