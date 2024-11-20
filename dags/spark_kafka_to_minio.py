from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime

MINIO_ENDPOINT = 'http://172.17.0.1:9000'
MINIO_ACCESS_KEY = 'kStHEgiS0L8wSMHBoOq6'
MINIO_SECRET_KEY = '6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV'
PYTHON_JOB_PATH = '/opt/airflow/spark_jobs/kafka_to_minio.py'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}



spark_config = {
    'spark.hadoop.fs.s3a.endpoint': MINIO_ENDPOINT,
    'spark.hadoop.fs.s3a.access.key': MINIO_ACCESS_KEY,
    'spark.hadoop.fs.s3a.secret.key': MINIO_SECRET_KEY,
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.hadoop.fs.s3a.path.style.access': 'true'
}

# Define the DAG
with DAG('spark_kafka_to_minio',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    current_date = datetime.now()
    current_date_format = current_date.strftime('%d-%m-%Y')

    # Define SparkSubmitOperator task
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_minio',
        application=PYTHON_JOB_PATH,  
        conf=spark_config,                       
        application_args=[current_date_format],  # Current date in dd-MM-yyyy format
        name='SparkKafkaMinIO',
        verbose=True,
        driver_memory='1g',
        executor_memory='2g',
        num_executors=1,
        executor_cores=1,
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4',
        conn_id='my_spark',
    )
    spark_submit_task
