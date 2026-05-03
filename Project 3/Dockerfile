FROM apache/airflow:2.10.5-python3.11

USER root
# Install Java (Required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Install PySpark and Requests
RUN pip install --no-cache-dir pyspark==3.5.1 requests