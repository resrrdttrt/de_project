import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../lib/stock'))
import data as dt

KAFKA_BROKER = '172.17.0.1:9092' 
KAFKA_TOPIC = 'stock_kafka_topic'

@dag(
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    description="DAG to extract stock data, transform, and load to PostgreSQL",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=["stock_data"],
)
def crawl_to_kafka():
    @task()
    def extract(data_source) -> pd.DataFrame:
        print(f"Start extracting from {data_source}")
        admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
        topic_list = admin_client.list_topics()

        if KAFKA_TOPIC not in topic_list:
            topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[topic], validate_only=False)

        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        db_params = {
            'dbname': 'stock',
            'user': 'airflow',
            'password': 'airflow',
            'host': '172.17.0.1',
            'port': '5432'
        }

        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("SELECT code FROM stock LIMIT 10")
        stock_codes = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        today = datetime.now().strftime("%Y-%m-%d")
        all_data = pd.DataFrame()

        for code in stock_codes:
            try:
                loader = dt.DataLoader(
                    symbols=code,
                    start=today,
                    end=today,
                    minimal=True,
                    data_source=data_source
                )
                data = loader.download()
                data = data.stack().reset_index().set_index('date')

                for index, row in data.iterrows():
                    message = {
                        'date': str(index.date()),
                        'code': row['code'],
                        'high': row['high'],
                        'low': row['low'],
                        'open': row['open'],
                        'close': row['close'],
                        'adjust': row['adjust'],
                        'volume_match': row['volume_match'],
                        'value_match': row['value_match']
                    }
                    producer.send(KAFKA_TOPIC, message)
                    print("Send message to Kafka")

                all_data = pd.concat([all_data, data])
                print(f"Add {code} to data")

            except Exception as e:
                print(f"{code} not found")
                continue

        producer.flush()
        print("Extracting successfully")
        all_data.to_csv('output.csv', index=False) 
        return all_data

    @task()
    def inform_crawl_admin() -> None:
        print("Both extraction tasks failed. Informing crawl admin...")

    

    extract_task_1 = extract("CAFE")
    extract_task_2 = extract("VND")
    extract_task_3 = extract("CAFE")
    extract_task_4 = extract("CAFE")
    extract_task_5 = extract("CAFE")
    extract_task_6 = extract("CAFE")
    extract_task_7 = extract("CAFE")

    extract_task_2.set_upstream(extract_task_1)
    extract_task_2.trigger_rule = TriggerRule.ALL_FAILED

    # Task to inform admin if both extract tasks fail
    inform_admin_task_1 = inform_crawl_admin()
    inform_admin_task_1.set_upstream(extract_task_2)
    inform_admin_task_1.trigger_rule = TriggerRule.ALL_FAILED

    inform_admin_task_2 = inform_crawl_admin()
    inform_admin_task_2.set_upstream(extract_task_3)
    inform_admin_task_2.trigger_rule = TriggerRule.ALL_FAILED

    inform_admin_task_3 = inform_crawl_admin()
    inform_admin_task_3.set_upstream(extract_task_4)
    inform_admin_task_3.trigger_rule = TriggerRule.ALL_FAILED

    inform_admin_task_4 = inform_crawl_admin()
    inform_admin_task_4.set_upstream(extract_task_5)
    inform_admin_task_4.trigger_rule = TriggerRule.ALL_FAILED

    inform_admin_task_5 = inform_crawl_admin()
    inform_admin_task_5.set_upstream(extract_task_6)
    inform_admin_task_5.trigger_rule = TriggerRule.ALL_FAILED

    inform_admin_task_6 = inform_crawl_admin()
    inform_admin_task_6.set_upstream(extract_task_7)
    inform_admin_task_6.trigger_rule = TriggerRule.ALL_FAILED




dag_instance = crawl_to_kafka()
