# Use an Airflow image with Python 3.10
FROM apache/airflow:2.10.2-python3.10

# Add requirements
ADD requirements.txt .

# Install specific Airflow version and dependencies
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
