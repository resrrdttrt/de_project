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
KAFKA_TOPIC = 'stock_data'

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
def stock_data_dag():
    @task()
    def extract(data_source) -> pd.DataFrame:
        print(f"Start extracting from {data_source}")
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
                all_data = pd.concat([all_data, data])
                print(f"Add {code} to data")

            except Exception as e:
                print(f"{code} not found")
                continue
                
        print("Extracting successfully")
        return all_data

    @task()
    def inform_crawl_admin() -> None:
        print("Both extraction tasks failed. Informing crawl admin...")

    @task()
    def inform_database_admin() -> None:
        print("Loading tasks failed. Informing database admin...")

    @task()
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        print("Start transforming")
        df = df.reset_index()[['date', 'code', 'high', 'low', 'open', 'close', 'adjust', 'volume_match', 'value_match']]
        df = df.drop_duplicates()
        df = df.dropna()
        print("Transform successfully")
        return df

    @task()
    def load(df: pd.DataFrame) -> None:
        print("Start loading")
        db_params = {
            'dbname': 'stock',
            'user': 'airflow',
            'password': 'airflow',
            'host': '172.17.0.1',
            'port': '5432'
        }

        connection_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        engine = create_engine(connection_string)

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS stock_data (
            date DATE NOT NULL,
            code VARCHAR(10) NOT NULL,
            high FLOAT,
            low FLOAT,
            open FLOAT,
            close FLOAT,
            adjust FLOAT,
            volume_match FLOAT,
            value_match FLOAT,
            PRIMARY KEY (date, code)
        );
        '''

        with engine.connect() as connection:
            connection.execute(text(create_table_query))

        try:
            df.to_sql('stock_data', engine, if_exists='append', index=False)
        except Exception as e:
            print(f"An error occurred: {e}")
        print("Loading successfully")

    # Task 1: Extract stock data from 'CAFE'
    extract_task_1 = extract("CAFE")
    
    # Task 2: Extract stock data from 'VND', triggered only if extract_task_1 fails
    extract_task_2 = extract("VND")
    extract_task_2.trigger_rule = TriggerRule.ONE_FAILED

    # Inform crawl admin only if both extract_task_1 and extract_task_2 fail
    inform_crawl = inform_crawl_admin()
    inform_crawl.trigger_rule = TriggerRule.ALL_FAILED

    # Setting up the task dependencies
    extract_task_1 >> extract_task_2 >> inform_crawl

    # Example continuation with transformation and load (these depend on successful extracts)
    transform_task_1 = transform(extract_task_1)
    transform_task_2 = transform(extract_task_2)

    load_task_1 = load(transform_task_1)
    load_task_2 = load(transform_task_2)

    # Notify database admin in case of load failure
    load_task_1.trigger_rule = TriggerRule.ONE_FAILED
    load_task_2.trigger_rule = TriggerRule.ONE_FAILED

    inform_db_admin = inform_database_admin()
    [load_task_1, load_task_2] >> inform_db_admin
dag_instance = stock_data_dag()
