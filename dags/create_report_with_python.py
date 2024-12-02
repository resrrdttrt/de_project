from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import s3fs
import os
import fsspec
from ydata_profiling import ProfileReport

# MinIO configuration
MINIO_ENDPOINT = "172.17.0.1:9000"
MINIO_ACCESS_KEY = "kStHEgiS0L8wSMHBoOq6"
MINIO_SECRET_KEY = "6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV"
MINIO_BUCKET = "mybucket"
PARQUET_FOLDER_PATH = f"stock_data/date={datetime.now().strftime('%d-%m-%Y')}"
REPORT_FILE_NAME = f"report_day_{datetime.now().strftime('%d-%m-%Y')}.html"
REPORT_S3_PATH = f"{MINIO_BUCKET}/reports/{REPORT_FILE_NAME}" 


def generate_pandas_profile_report():
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={
            'endpoint_url': f'http://{MINIO_ENDPOINT}',  # Use http if no SSL
        }
    )

    # Construct the s3 folder path 
    s3_folder_path = f'{MINIO_BUCKET}/{PARQUET_FOLDER_PATH}/'

    # List all files in the directory
    files = fs.glob(f'{s3_folder_path}*.parquet')

    # Read all Parquet files from the folder into a single DataFrame
    df = pd.concat([pd.read_parquet(f's3://{file}', filesystem=fs) for file in files])

    # Show the DataFrame
    print(df)


    profile = ProfileReport(df,title=f"Stock Data Report")
    local_report_path = "pandas_profile_report.html"
    # Save the report as an HTML file
    profile.to_file(local_report_path)


    fs.put(local_report_path, REPORT_S3_PATH)  # Pass the file path directly

    print(f"Report successfully uploaded to s3://{REPORT_S3_PATH}")

    if os.path.exists(local_report_path):
        os.remove(local_report_path)
        print(f"Local file '{local_report_path}' has been removed.")
    else:
        print(f"Local file '{local_report_path}' not found.")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

# Define the DAG
dag = DAG(
    'create_report',
    default_args=default_args,
    schedule_interval=None,  # Adjust as needed
    catchup=False,
)

# Define the task
generate_report_task = PythonOperator(
    task_id='generate_pandas_profile_report',
    python_callable=generate_pandas_profile_report,
    dag=dag,
)

generate_report_task