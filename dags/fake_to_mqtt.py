from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import paho.mqtt.client as mqtt
import json

# Configuration Variables
FILE_PATH = '/path/to/stock_data.csv'
MQTT_BROKER = 'localhost'
MQTT_TOPIC = 'stock_data_topic'

def publish_to_mqtt():
    # Create MQTT client and connect
    client = mqtt.Client()
    client.connect(MQTT_BROKER)

    # Read stock data from file
    with open(FILE_PATH, 'r') as file:
        for line in file:
            # Assuming CSV format: date,code,high,low,open,close,adjust,volume_match,value_match
            fields = line.strip().split(',')
            message = {
                'date': fields[0],
                'code': fields[1],
                'high': float(fields[2]),
                'low': float(fields[3]),
                'open': float(fields[4]),
                'close': float(fields[5]),
                'adjust': float(fields[6]),
                'volume_match': float(fields[7]),
                'value_match': float(fields[8])
            }
            # Publish message to MQTT
            client.publish(MQTT_TOPIC, json.dumps(message))

    client.disconnect()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'publish_stock_data_to_mqtt',
    default_args=default_args,
    description='Read stock data and publish to MQTT',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    publish_task = PythonOperator(
        task_id='publish_to_mqtt',
        python_callable=publish_to_mqtt,
    )

publish_task