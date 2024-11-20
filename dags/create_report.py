from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# MinIO Configuration
MINIO_ENDPOINT = 'http://172.17.0.1:9000'
MINIO_ACCESS_KEY = 'kStHEgiS0L8wSMHBoOq6'
MINIO_SECRET_KEY = '6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV'
MINIO_BUCKET = 'mybucket'
PYTHON_JOB_PATH = '/opt/airflow/spark_jobs/combine_minio.py'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}



# Function to generate pandas profiling report and save to MinIO
def generate_report(date: str):

    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # If not using SSL/TLS
    )
    # File path in MinIO
    csv_file_path = f"stock_data_combined/{date}/"

    # Get CSV file from MinIO
    objects = minio_client.list_objects(MINIO_BUCKET, prefix=csv_file_path)
    try:
        csv_file = next(objects).object_name  # Assuming only one file in the folder
        print(f"Found CSV file: {csv_file}")

        csv_data = minio_client.get_object(MINIO_BUCKET, csv_file).read()
        df = pd.read_csv(BytesIO(csv_data))
        # Generate pandas profiling report
        profile = df.profile_report(title=f"Stock Data Report - {date}")

        # Save the report as an HTML file
        report_buffer = BytesIO()
        profile.to_file(report_buffer)
        report_buffer.seek(0)

        # Upload the report back to MinIO
        report_path = f"stock_data_reports/{date}/stock_data_report.html"
        minio_client.put_object(
            MINIO_BUCKET, report_path, report_buffer, length=len(report_buffer.getvalue()), content_type='text/html'
        )
        print(f"Report saved to MinIO at {report_path}")

    except StopIteration:
        print("No files found in the specified path.")
    except Exception as e:
        print(f"An error occurred: {e}")



# Define the DAG
with DAG('spark_create_report',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Get the current date in dd-MM-yyyy format
    current_date = datetime.now().strftime('%d-%m-%Y')

    # Spark configuration
    spark_config = {
        'spark.hadoop.fs.s3a.endpoint': MINIO_ENDPOINT,
        'spark.hadoop.fs.s3a.access.key': MINIO_ACCESS_KEY,
        'spark.hadoop.fs.s3a.secret.key': MINIO_SECRET_KEY,
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    }

    # Define SparkSubmitOperator task
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_combine_minio',
        application=PYTHON_JOB_PATH,
        conn_id='my_spark',
        conf=spark_config,
        application_args=[current_date],  # Current date in dd-MM-yyyy format
        name='SparkCombineMinIO',
        verbose=True,
        driver_memory='1g',
        executor_memory='1g',
        num_executors=1,
        executor_cores=1,
        packages='org.apache.hadoop:hadoop-aws:3.3.4'
    )

    # Sensor to wait for the CSV file to be created in MinIO
    wait_for_csv = S3KeySensor(
        task_id='wait_for_csv',
        bucket_name=MINIO_BUCKET,
        bucket_key=f"stock_data_combined/{current_date}/_SUCCESS",  # Airflow execution date in 'YYYY-MM-DD' format
        wildcard_match=True,
        aws_conn_id='my_minio', 
        mode='poke',
        timeout=600,  
        poke_interval=30, 
    )

    # Generate pandas profiling report
    create_report = PythonOperator(
        task_id='create_report',
        python_callable=generate_report,
        op_args=[current_date],  # Pass the execution date as the argument
    )

    # Set the task
    spark_submit_task >> wait_for_csv >> create_report