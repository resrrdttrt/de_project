from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json

# Configuration Variables
MQTT_BROKER = 'mqtt_broker_address'
MQTT_TOPIC = 'stock_data_topic'
KAFKA_BROKER = 'kafka_broker_address:9092'
KAFKA_TOPIC = 'kafka_topic'

def on_message(client, userdata, msg):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    try:
        # Validate message
        message = json.loads(msg.payload)
        required_fields = ['date', 'code', 'high', 'low', 'open', 'close', 'adjust', 'volume_match', 'value_match']
        if all(field in message for field in required_fields):
            producer.send(KAFKA_TOPIC, message)
    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        producer.flush()
        producer.close()

def subscribe_and_produce():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect(MQTT_BROKER)
    client.subscribe(MQTT_TOPIC)
    client.loop_forever()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'subscribe_validate_produce',
    default_args=default_args,
    description='Subscribe from MQTT, validate, and produce to Kafka',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    subscribe_and_produce_task = PythonOperator(
        task_id='subscribe_and_produce',
        python_callable=subscribe_and_produce,
    )

subscribe_and_produce_task