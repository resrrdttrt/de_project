from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
from datetime import datetime

# Configuration Variables
MQTT_BROKER = '172.17.0.1'
MQTT_PORT = 1883
MQTT_USER = 'res'
MQTT_PASSWORD = '1'
MQTT_TOPIC = 'stock_mqtt_topic'
KAFKA_BROKER = '172.17.0.1:9092'
KAFKA_TOPIC = 'stock_kafka_topic'

def on_message(client, userdata, msg):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        # Parse and transform message
        row = json.loads(msg.payload.decode('utf-8'))
        message = {
            'date': str(datetime.strptime(row['date'], '%Y-%m-%d').date()),
            'code': row['Name'],
            'high': row['high'],
            'low': row['low'],
            'open': row['open'],
            'close': row['close'],
            'volume': row['volume']
        }
        producer.send(KAFKA_TOPIC, message)
        print("Sent messages to Kafka")
    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        producer.flush()
        producer.close()

def subscribe_and_produce():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    client.loop_start()  # Run the loop in the background
    while (True):
        pass

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'mqtt_to_kafka',
    default_args=default_args,
    description='Subscribe from MQTT, validate, and produce to Kafka',
    schedule_interval= None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    subscribe_and_produce_task = PythonOperator(
        task_id='subscribe_and_produce',
        python_callable=subscribe_and_produce,
    )

subscribe_and_produce_task