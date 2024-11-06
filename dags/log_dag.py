from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function for logging
def task_1():
    print("Task 1 is running")

def task_2():
    print("Task 2 is running")

def task_3():
    print("Task 3 is running")

# Define the DAG
default_args = {
    'start_date': datetime(2024, 10, 4),  # You can adjust the start date as needed
    'retries': 1
}

with DAG(
    'log_dag',
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual execution
    catchup=False
) as dag:

    # Task definitions
    t1 = PythonOperator(
        task_id='log_task_1',
        python_callable=task_1
    )

    t2 = PythonOperator(
        task_id='log_task_2',
        python_callable=task_2
    )

    t3 = PythonOperator(
        task_id='log_task_3',
        python_callable=task_3
    )

    # Task dependencies
    t1 >> t2 >> t3
