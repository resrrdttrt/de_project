import json
import random
import string
import time
from datetime import datetime, timedelta
from io import BytesIO

from minio import Minio
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from kafka import KafkaConsumer

# Constants
KAFKA_BROKER = '172.17.0.1:9092'
KAFKA_TOPIC = 'stock_kafka_topic'
MINIO_ENDPOINT = "172.17.0.1:9000"
MINIO_ACCESS_KEY = "kStHEgiS0L8wSMHBoOq6"
MINIO_SECRET_KEY = "6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV"
MINIO_BUCKET = 'mybucket'

@dag(
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'start_date': days_ago(1),
    },
    description="DAG to consume Kafka messages and upload to MinIO",
    schedule_interval=None,
    catchup=False,
    tags=["kafka", "minio"],
)
def kafka_to_minio():
    def upload_to_minio(client, data, bucket_name, object_name):
        print("Start")
        # Convert data to bytes
        data_stream = BytesIO(data.encode('utf-8'))
        # Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        # Upload data to MinIO
        client.put_object(
            bucket_name, 
            object_name, 
            data_stream, 
            length=len(data)
        )
        print(f"Uploaded {object_name} to MinIO")

    @task()
    def consume_and_store_to_minio(kafka_topic, minio_bucket, batch_size=100, batch_interval=10):
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False 
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
                    upload_to_minio(client, batch_data, minio_bucket, batch_filename)
                    print("Uploaded to MinIO")
                    last_upload_time = time.time()
                    batch.clear()
                except Exception as e:
                    print(f"Error occurred while uploading to MinIO: {e}")

    # Main task to consume messages and upload them to MinIO
    consume_and_upload_task = consume_and_store_to_minio(
        kafka_topic=KAFKA_TOPIC,  # Kafka topic
        minio_bucket=MINIO_BUCKET,   # MinIO bucket name
        batch_size=10,             # Number of messages to process in one batch
        batch_interval=10           # Maximum time between batches in seconds
    )

dag_instance = kafka_to_minio()