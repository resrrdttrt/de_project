from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# Configuration Variables
SPARK_APPLICATION_PATH = './apps/job1.py'
SPARK_CONN_ID = 'spark-default'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'kafka_to_gcs',
    default_args=default_args,
    description='A simple Kafka to GCS DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='spark_submit_job',
        application=SPARK_APPLICATION_PATH,
        conn_id=SPARK_CONN_ID,
        dag=dag,
    )

submit_spark_job
