from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import paho.mqtt.client as mqtt
import random
import os
import time

# Configuration Variables
DATA_FILE_PATH = '/opt/airflow/dataset/data_stock.csv'
MQTT_BROKER = '172.17.0.1'
MQTT_PORT = 1883
MQTT_USER = 'res'
MQTT_PASSWORD = '1'
MQTT_TOPIC = 'stock_mqtt_topic'
NUMBER_OF_MESSAGES = 100

def publish_to_mqtt():
    # Read the dataset
    df = pd.read_csv(DATA_FILE_PATH)

    # Establish MQTT connection
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    # Publish random messages
    for _ in range(NUMBER_OF_MESSAGES):
        # Select a random row index
        random_index = random.randint(0, len(df) - 1)
        message = df.iloc[random_index].to_json()
        client.publish(MQTT_TOPIC, message)
        # time.sleep(1)

    
    # Disconnect from MQTT broker
    client.disconnect()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

dag = DAG(
    'fake_to_mqtt',
    default_args=default_args,
    schedule_interval=None,  # Adjust as needed
)

publish_task = PythonOperator(
    task_id='publish_to_mqtt',
    python_callable=publish_to_mqtt,
    dag=dag,
)

publish_task