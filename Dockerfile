FROM apache/airflow:2.7.3

USER root

ENV SPARK_VERSION=3.5.3


RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# RUN apt-get update \
#   && apt-get install -y --no-install-recommends \
#          default-jdk \
#          procps \
#          scala \
#   && apt-get autoremove -yqq --purge \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME
ENV SPARK_HOME=/opt/spark
RUN export SPARK_HOME
ENV PATH=$PATH:$SPARK_HOME/bin
RUN chown -R airflow: ${AIRFLOW_HOME}
# Add requirements
ADD requirements.txt .

# Install specific Airflow version and dependencies
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt