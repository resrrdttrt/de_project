#!/bin/bash

spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --master spark://0.0.0.0:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=http://172.17.0.1:9000 \
  --conf spark.hadoop.fs.s3a.access.key=kStHEgiS0L8wSMHBoOq6 \
  --conf spark.hadoop.fs.s3a.secret.key=6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /home/dung01213416738/airflow/spark_jobs/combine_minio.py 19-11-2024 >> /home/dung01213416738/spark/logs/logs_new_2.log 2>&1 &


