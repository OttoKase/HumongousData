import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------
# 1. Default Arguments & DAG Configuration
# ---------------------------------------------------------
default_args = {
    "owner": "group-f",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def get_spark_session():
    import os
    from pyspark.sql import SparkSession

    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    os.environ["AWS_REGION"] = "us-east-1"

    spark = (SparkSession.builder
        .appName("CDC-Bronze")
        .config("spark.jars.packages", 
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2," +
                    "org.apache.iceberg:iceberg-aws-bundle:1.5.2," +
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", access_key)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", secret_key)
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

# ---------------------------------------------------------
# 2. Python Callables (Task Logic)
# ---------------------------------------------------------
def check_connector():
    import requests
    import logging
    
    url = "http://connect:8083/connectors/cdc-connector/status"
    logging.info(f"Checking connector status at {url}")
    
    try:
        r = requests.get(url, timeout=10)
        # If it's a 404, the connector hasn't been created yet
        if r.status_code == 404:
            raise Exception("Connector 'cdc-connector' not found. Have you created it yet?")
            
        r.raise_for_status()
        data = r.json()
        status = data["connector"]["state"]
        
        logging.info(f"Connector state: {status}")
        
        if status != "RUNNING":
            # Log the full error message from Debezium if it exists
            worker_error = data.get('tasks', [{}])[0].get('trace', 'No trace available')
            logging.error(f"Connector is failing. Error from worker: {worker_error}")
            raise Exception(f"Connector is {status}, not RUNNING.")
            
    except requests.exceptions.ConnectionError:
        raise Exception("Could not connect to Kafka Connect. Is the 'connect' service running?")

def run_bronze_cdc():
    """Logic for the Bronze layer ingestion."""
    from pyspark.sql import functions as F  # Local Import
    spark = get_spark_session()             # Local Session
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    
    logging.info("Starting Bronze CDC processing...")
    raw = (spark.read
        .format("kafka") 
        .option("kafka.bootstrap.servers", "kafka:9092") 
        .option("subscribe", "dbserver1.public.customers") 
        .option("startingOffsets", "earliest") 
        .load())

    # Filter out tombstone records (null value) first
    raw_filtered = raw.filter(F.col("value").isNotNull())

    bronze_df = raw_filtered.select(
    F.col("topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.get_json_object(F.col("value").cast("string"), "$.payload.op").alias("op"),
    F.get_json_object(F.col("value").cast("string"), "$.payload.ts_ms").cast("long").alias("ts_ms"),
    F.get_json_object(F.col("value").cast("string"), "$.payload.after.id").cast("int").alias("after_id"),
    F.get_json_object(F.col("value").cast("string"), "$.payload.after.name").alias("after_name"),
    F.get_json_object(F.col("value").cast("string"), "$.payload.after.email").alias("after_email"),
    F.get_json_object(F.col("value").cast("string"), "$.payload.after.country").alias("after_country"),
    F.get_json_object(F.col("value").cast("string"), "$.payload.before.id").cast("int").alias("before_id"),
    )
    
    bronze_df.writeTo("lakehouse.cdc.bronze_customers").createOrReplace() # change to append() in actual project
    
    logging.info("Bronze layer complete.")

def run_silver_cdc():
    """Logic for the Silver layer transformation."""
    from pyspark.sql import functions as F, Window # Local Import
    spark = get_spark_session()                   # Re-use/Local Session
    
    bronze_df = spark.table("lakehouse.cdc.bronze_customers")
    
    logging.info("Starting Silver CDC processing...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_customers (
        id INT, name STRING, email STRING, country STRING, last_updated_ms BIGINT
    ) USING iceberg
    """)

    # Use COALESCE because for deletes, after_id is null but before_id has the key
    bronze_with_key = bronze_df.withColumn(
        "entity_id", F.coalesce(F.col("after_id"), F.col("before_id"))
    )

    w = Window.partitionBy("entity_id").orderBy(F.col("ts_ms").desc())

    deduped = bronze_with_key.filter(F.col("op").isNotNull()).withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    
    deduped.createOrReplaceTempView("cdc_batch")

    spark.sql("""

    MERGE INTO lakehouse.cdc.silver_customers AS t
    USING cdc_batch AS s
    ON t.id = s.entity_id
    WHEN MATCHED AND s.op = 'd' THEN DELETE
    WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
        t.name = s.after_name, t.email = s.after_email,
        t.country = s.after_country, t.last_updated_ms = s.ts_ms
    WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
        (id, name, email, country, last_updated_ms)
        VALUES (s.after_id, s.after_name, s.after_email, s.after_country, s.ts_ms)
    """)
    
    logging.info("Silver layer complete.")
    
def run_bronze_taxi():
    """Logic for the Bronze layer taxi ingestion."""
    from pyspark.sql import functions as F
    import os
    
    spark = get_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")
    
    logging.info("Starting Bronze Taxi processing...")
    
    all_files = os.listdir("/opt/airflow/data/")
    logging.info(f"All files in data/: {all_files}")
    
    trip_files = [
        f"file:///opt/airflow/data/{f}" 
        for f in all_files 
        if f.endswith(".parquet") and "zone" not in f.lower()
    ]    
    logging.info(f"Reading trip files: {trip_files}")
    
    df = spark.read.parquet(*trip_files)
    bronze_df = df.withColumn("trip_id", F.monotonically_increasing_id())
    bronze_df.writeTo("lakehouse.taxi.bronze_trips").createOrReplace()
    
    logging.info("Bronze Taxi complete.")


def run_silver_taxi():
    """Logic for the Silver layer taxi transformation."""
    from pyspark.sql import functions as F
    spark = get_spark_session()
    
    logging.info("Starting Silver Taxi processing...")
    
    bronze_df = spark.table("lakehouse.taxi.bronze_trips")
    
    silver_df = (bronze_df
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
    )
    
    silver_df.writeTo("lakehouse.taxi.silver_trips").createOrReplace()
    
    logging.info("Silver Taxi complete.")
    
def run_bronze_drivers():
    from pyspark.sql import functions as F
    spark = get_spark_session()
    
    logging.info("Starting Bronze Drivers CDC processing...")
    
    raw = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "dbserver1.public.drivers")
        .option("startingOffsets", "earliest")
        .load())

    raw_filtered = raw.filter(F.col("value").isNotNull())

    bronze_df = raw_filtered.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.op").alias("op"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.ts_ms").cast("long").alias("ts_ms"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.id").cast("int").alias("after_id"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.name").alias("after_name"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.email").alias("after_email"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.country").alias("after_country"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.before.id").cast("int").alias("before_id"),
    )
    
    bronze_df.writeTo("lakehouse.cdc.bronze_drivers").createOrReplace()
    logging.info("Bronze Drivers complete.")


def run_silver_drivers():
    from pyspark.sql import functions as F, Window
    spark = get_spark_session()
    
    logging.info("Starting Silver Drivers CDC processing...")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_drivers (
            id INT, name STRING, email STRING, country STRING, last_updated_ms BIGINT
        ) USING iceberg
    """)

    bronze_df = spark.table("lakehouse.cdc.bronze_drivers")

    bronze_with_key = bronze_df.withColumn(
        "entity_id", F.coalesce(F.col("after_id"), F.col("before_id"))
    )

    w = Window.partitionBy("entity_id").orderBy(F.col("ts_ms").desc())

    deduped = (bronze_with_key
        .filter(F.col("op").isNotNull())
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    deduped.createOrReplaceTempView("drivers_cdc_batch")

    spark.sql("""
        MERGE INTO lakehouse.cdc.silver_drivers AS t
        USING drivers_cdc_batch AS s
        ON t.id = s.entity_id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
            t.name = s.after_name, t.email = s.after_email, t.country = s.after_country,
            t.last_updated_ms = s.ts_ms
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
            (id, name, country, last_updated_ms)
            VALUES (s.after_id, s.after_name, s.after_country, s.ts_ms)
    """)
    
    logging.info("Silver Drivers complete.")

# ---------------------------------------------------------
# 3. DAG Definition
# ---------------------------------------------------------
with DAG(
    dag_id="project3_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *",   # every 15 minutes
    catchup=False,
    tags=["project3"],
) as dag:

    health_check = PythonOperator(
        task_id="check_debezium_health",
        python_callable=check_connector,
    )

    bronze_cdc = PythonOperator(
        task_id="bronze_cdc",
        python_callable=run_bronze_cdc,
    )

    silver_cdc = PythonOperator(
        task_id="silver_cdc",
        python_callable=run_silver_cdc,
    )

    bronze_taxi = PythonOperator(
        task_id="bronze_taxi",
        python_callable=run_bronze_taxi,
    )

    silver_taxi = PythonOperator(
        task_id="silver_taxi",
        python_callable=run_silver_taxi,
    )
    
    bronze_drivers = PythonOperator(
        task_id="bronze_drivers",
        python_callable=run_bronze_drivers,
    )

    silver_drivers = PythonOperator(
        task_id="silver_drivers",
        python_callable=run_silver_drivers,
    )

    # CDC path
    health_check >> bronze_cdc >> silver_cdc
    health_check >> bronze_drivers >> silver_drivers

    # Taxi path — parallel, independent
    bronze_taxi >> silver_taxi