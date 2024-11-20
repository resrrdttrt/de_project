#!/bin/bash

spark-submit \
--master spark://172.17.0.1:7077 \
--executor-cores 1 \
--executor-memory 2g \
/home/dung01213416738/airflow/spark_jobs/job1.py


spark-submit \
--master spark://172.17.0.1:7077 \
--executor-cores 1 \
--executor-memory 1g \
/opt/airflow/spark_jobs/job1.py