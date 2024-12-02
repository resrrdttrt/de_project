from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from kafka import KafkaProducer
import pandas as pd
import json
import random

# Configuration Variables
DATA_FILE_PATH = '/opt/airflow/dataset/data_stock.csv'
KAFKA_BROKER = '172.17.0.1:9092'
KAFKA_TOPIC = 'stock_kafka_topic'

# Function to send messages to Kafka
def publish_to_kafka():
    # Read the dataset
    df = pd.read_csv(DATA_FILE_PATH)
    
    # Select 100 random rows
    random_rows = df.sample(n=100)
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        for _, row in random_rows.iterrows():
            # Convert the row to a dictionary and transform it
            message = {
                'date': str(row['date']),
                'code': row['Name'],
                'high': row['high'],
                'low': row['low'],
                'open': row['open'],
                'close': row['close'],
                'volume': row['volume']
            }
            # Send the message to the Kafka topic
            producer.send(KAFKA_TOPIC, message)
            print(f"Sent message to Kafka: {message}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")
    finally:
        producer.flush()
        producer.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'batch_to_kafka',
    default_args=default_args,
    description='Read 100 random rows from CSV and publish to Kafka',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    publish_task = PythonOperator(
        task_id='publish_to_kafka',
        python_callable=publish_to_kafka,
    )

publish_task
