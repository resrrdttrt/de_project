from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spark_test_submit_standalone',
    default_args=default_args,
    description='A simple DAG to submit a Spark job using a connection ID',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Define the SparkSubmitOperator task using a connection ID
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/home/resrrdttrt/Tool/spark/spark_apps/job1.py',
        conn_id='standalone-spark-connection',  # Replace with your Spark connection ID
        name='my_spark_job',
        executor_memory='1g',  
        driver_memory='1g',    
        total_executor_cores='1',  
        dag=dag,
    )

    # Set task dependencies if needed (not required for a single task)
    submit_spark_job