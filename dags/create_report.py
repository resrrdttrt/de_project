from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import pandas as pd
import os

# Configuration Variables
GCS_KEY_PATH = '/opt/airflow/secret/gcs-key.json'
GCS_BUCKET = 'mybucket-qaz'
GCS_JSON_FILE = 'data.json'
BIGQUERY_DATASET = 'stock_dataset'
BIGQUERY_TABLE = 'stock_table'
BIGQUERY_PROJECT = 'vivid-router-440709-p3'
GCS_REPORT_FILE = 'report.csv'
GCS_REPORT_PATH = '/tmp/report.csv'

# Set the environment variable for Google Cloud authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCS_KEY_PATH

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'create_report',
    default_args=default_args,
    description='Load data from GCS to BQ, create report, and upload back to GCS',
    schedule_interval=None,
    catchup=False,
)

# Sensor: Wait for the file to exist in GCS
wait_for_file = GCSObjectExistenceSensor(
    task_id='wait_for_gcs_file',
    bucket=GCS_BUCKET,
    object=GCS_JSON_FILE,
    dag=dag,
)

# Step 1: Load data from GCS to BigQuery
load_data = GCSToBigQueryOperator(
    task_id='load_data_to_bq',
    bucket=GCS_BUCKET,
    source_objects=[GCS_JSON_FILE],
    destination_project_dataset_table=f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

# Step 2: Create an insight report
def create_report():
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    query = f"""
    SELECT code, AVG(high) as avg_high, AVG(low) as avg_low
    FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
    GROUP BY code
    ORDER BY code
    """
    df = client.query(query).to_dataframe()
    df.to_csv(GCS_REPORT_PATH, index=False)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=create_report,
    dag=dag,
)

# Step 3: Upload the report back to GCS
upload_report = LocalFilesystemToGCSOperator(
    task_id='upload_report_to_gcs',
    src=GCS_REPORT_PATH,
    dst=GCS_REPORT_FILE,
    bucket=GCS_BUCKET,
    dag=dag,
)

wait_for_file >> load_data >> generate_report >> upload_report