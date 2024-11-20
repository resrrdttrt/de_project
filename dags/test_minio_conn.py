from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# {"aws_access_key_id":"", "aws_secret_access_key":"", "endpoint_url":""}

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}


with DAG(
    dag_id='test_minio_conn',
    start_date=days_ago(1),
    schedule_interval=None,
    default_args=default_args
) as dag:
    task1 = S3KeySensor(
        task_id='test_minio_s3',
        bucket_name='mybucket',
        bucket_key='data.csv',
        aws_conn_id='my_minio',
        mode='poke',
        poke_interval=5
    )