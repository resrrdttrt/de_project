#!/bin/bash

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
--conf spark.hadoop.fs.s3a.access.key=kStHEgiS0L8wSMHBoOq6 \
--conf spark.hadoop.fs.s3a.secret.key=6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV \
--conf "spark.hadoop.fs.s3a.endpoint=http://172.17.0.1:9000" \
--master spark://0.0.0.0:7077 \
--executor-cores 1 \
--executor-memory 2g \
/home/dung01213416738/airflow/spark_jobs/kafka_to_minio.py >> /home/dung01213416738/spark/logs/logs_new_1.log 2>&1 &