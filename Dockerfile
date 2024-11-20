FROM apache/airflow:2.7.3
USER root

# Update package lists and fix missing packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates-java && \
    apt-get install -y openjdk-11-jdk ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Spark
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Add requirements
ADD requirements.txt .

# Install specific Airflow version and dependencies
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt