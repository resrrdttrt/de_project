from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'spark_test_submit_YARN',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark_job = SparkSubmitOperator(
        application='./lib/job1.py',  # Path to your Spark job
        task_id='spark_submit_task',
        conn_id='spark_default',  # Ensure this connection exists in Airflow
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'cluster',
        },
        executor_cores=1,
        executor_memory='1g',
        num_executors=2,
        name='example_spark_job',
    )

    submit_spark_job