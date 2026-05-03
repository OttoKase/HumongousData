import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor

def notify_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    log_url = context["task_instance"].log_url
    logging.error(
        f"[ALERT] Task failed: dag={dag_id} task={task_id} log={log_url}"
    )
    
default_args = {
    "owner": "group-f",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure,
}

def get_spark_session():
    import os
    from pyspark.sql import SparkSession

    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()

    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    os.environ["AWS_REGION"] = "us-east-1"

    spark = (SparkSession.builder
        .appName("CDC-Pipeline")
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


def get_starting_offsets(spark, table_name, topic):
    """
    On the first run (when there's an empty table) use 'earliest' to read everything.
    On subsequent runs build a JSON offset map from the max kafka_offset.
    """
    try:
        count = spark.table(table_name).count()
        if count == 0:
            logging.info(f"[offsets] {table_name} is empty — using 'earliest'")
            return "earliest"

        from pyspark.sql import functions as F
        max_offsets = (
            spark.table(table_name)
            .groupBy("kafka_partition")
            .agg((F.max("kafka_offset") + 1).alias("offset"))
            .collect()
        )
        offsets_json = {str(row["kafka_partition"]): row["offset"] for row in max_offsets}
        result = json.dumps({topic: offsets_json})
        logging.info(f"[offsets] {table_name} — resuming from {result}")
        return result

    except Exception as e:
        logging.info(f"[offsets] {table_name} error ({e}) — using 'earliest'")
        return "earliest"


# Path A 
def run_bronze_customers():
    from pyspark.sql import functions as F
    spark = get_spark_session()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.bronze_customers (
            topic           STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            kafka_timestamp TIMESTAMP,
            op              STRING,
            ts_ms           BIGINT,
            after_id        INT,
            after_name      STRING,
            after_email     STRING,
            after_country   STRING,
            before_id       INT,
            source_lsn      BIGINT
        ) USING iceberg
    """)

    offsets = get_starting_offsets(spark, "lakehouse.cdc.bronze_customers", "dbserver1.public.customers")
    logging.info(f"Starting Bronze CDC customers processing (offsets={offsets})...")

    raw = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "dbserver1.public.customers")
        .option("startingOffsets", offsets)
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
        F.get_json_object(F.col("value").cast("string"), "$.payload.source.lsn").cast("long").alias("source_lsn"),
    )

    bronze_df.writeTo("lakehouse.cdc.bronze_customers").append()
    count = spark.table("lakehouse.cdc.bronze_customers").count()
    logging.info(f"Bronze CDC customers complete. Total row count: {count}")
    spark.stop()


def run_silver_customers():
    from pyspark.sql import functions as F, Window
    spark = get_spark_session()

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_customers (
            id              INT,
            name            STRING,
            email           STRING,
            country         STRING,
            last_updated_ms BIGINT
        ) USING iceberg
    """)

    logging.info("Starting Silver CDC customers processing...")

    bronze_df = spark.table("lakehouse.cdc.bronze_customers")

    bronze_with_key = bronze_df.withColumn(
        "entity_id", F.coalesce(F.col("after_id"), F.col("before_id"))
    )

    # Keep only the latest event per entity — ties broken by kafka_offset.
    w = Window.partitionBy("entity_id").orderBy(
        F.col("ts_ms").desc(), F.col("kafka_offset").desc()
    )

    deduped = (bronze_with_key
        .filter(F.col("op").isNotNull())
        .filter(F.col("entity_id").isNotNull())
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    deduped.writeTo("lakehouse.cdc.staging_customers").createOrReplace()

    spark.sql("""
        MERGE INTO lakehouse.cdc.silver_customers AS t
        USING lakehouse.cdc.staging_customers AS s
        ON t.id = s.entity_id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
            t.name = s.after_name, t.email = s.after_email,
            t.country = s.after_country, t.last_updated_ms = s.ts_ms
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
            (id, name, email, country, last_updated_ms)
            VALUES (s.after_id, s.after_name, s.after_email, s.after_country, s.ts_ms)
    """)

    count = spark.table("lakehouse.cdc.silver_customers").count()
    logging.info(f"Silver CDC customers complete. Row count: {count}")
    spark.stop()


def run_bronze_drivers():
    from pyspark.sql import functions as F
    spark = get_spark_session()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.bronze_drivers (
            topic           STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            kafka_timestamp TIMESTAMP,
            op              STRING,
            ts_ms           BIGINT,
            after_id        INT,
            after_name      STRING,
            after_email     STRING,
            after_country   STRING,
            before_id       INT,
            source_lsn      BIGINT
        ) USING iceberg
    """)

    offsets = get_starting_offsets(spark, "lakehouse.cdc.bronze_drivers", "dbserver1.public.drivers")
    logging.info(f"Starting Bronze CDC drivers processing (offsets={offsets})...")

    raw = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "dbserver1.public.drivers")
        .option("startingOffsets", offsets)
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
        F.get_json_object(F.col("value").cast("string"), "$.payload.source.lsn").cast("long").alias("source_lsn")
    )

    bronze_df.writeTo("lakehouse.cdc.bronze_drivers").append()
    count = spark.table("lakehouse.cdc.bronze_drivers").count()
    logging.info(f"Bronze CDC drivers complete. Total row count: {count}")
    spark.stop()


def run_silver_drivers():
    from pyspark.sql import functions as F, Window
    spark = get_spark_session()

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_drivers (
            id              INT,
            name            STRING,
            email           STRING,
            country         STRING,
            last_updated_ms BIGINT
        ) USING iceberg
    """)

    logging.info("Starting Silver CDC drivers processing...")

    bronze_df = spark.table("lakehouse.cdc.bronze_drivers")

    bronze_with_key = bronze_df.withColumn(
        "entity_id", F.coalesce(F.col("after_id"), F.col("before_id"))
    )

    w = Window.partitionBy("entity_id").orderBy(
        F.col("ts_ms").desc(), F.col("kafka_offset").desc()
    )

    deduped = (bronze_with_key
        .filter(F.col("op").isNotNull())
        .filter(F.col("entity_id").isNotNull())
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    deduped.writeTo("lakehouse.cdc.staging_drivers").createOrReplace()

    spark.sql("""
        MERGE INTO lakehouse.cdc.silver_drivers AS t
        USING lakehouse.cdc.staging_drivers AS s
        ON t.id = s.entity_id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
            t.name = s.after_name, t.email = s.after_email,
            t.country = s.after_country, t.last_updated_ms = s.ts_ms
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
            (id, name, email, country, last_updated_ms)
            VALUES (s.after_id, s.after_name, s.after_email, s.after_country, s.ts_ms)
    """)

    count = spark.table("lakehouse.cdc.silver_drivers").count()
    logging.info(f"Silver CDC drivers complete. Row count: {count}")
    spark.stop()


#  Path B 
def run_bronze_taxi():
    from pyspark.sql import functions as F
    spark = get_spark_session()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze_trips (
            kafka_partition       INT,
            kafka_offset          BIGINT,
            kafka_timestamp       TIMESTAMP,
            VendorID              INT,
            tpep_pickup_datetime  STRING,
            tpep_dropoff_datetime STRING,
            passenger_count       DOUBLE,
            trip_distance         DOUBLE,
            RatecodeID            DOUBLE,
            store_and_fwd_flag    STRING,
            PULocationID          INT,
            DOLocationID          INT,
            payment_type          LONG,
            fare_amount           DOUBLE,
            extra                 DOUBLE,
            mta_tax               DOUBLE,
            tip_amount            DOUBLE,
            tolls_amount          DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount          DOUBLE,
            congestion_surcharge  DOUBLE,
            Airport_fee           DOUBLE,
            cbd_congestion_fee    DOUBLE
        ) USING iceberg
    """)

    offsets = get_starting_offsets(spark, "lakehouse.taxi.bronze_trips", "taxi-trips")
    logging.info(f"Starting Bronze Taxi processing (offsets={offsets})...")

    trip_schema = """
        VendorID INT,
        tpep_pickup_datetime STRING,
        tpep_dropoff_datetime STRING,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        RatecodeID DOUBLE,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type LONG,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        Airport_fee DOUBLE,
        cbd_congestion_fee DOUBLE
    """

    raw = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "taxi-trips")
        .option("startingOffsets", offsets)
        .load())

    bronze_df = (raw
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), trip_schema).alias("d"),
        )
        .select("kafka_partition", "kafka_offset", "kafka_timestamp", "d.*")
        .filter(F.col("VendorID").isNotNull())
    )

    bronze_df.writeTo("lakehouse.taxi.bronze_trips").append()

    count = spark.table("lakehouse.taxi.bronze_trips").count()
    logging.info(f"Bronze Taxi complete. Total row count: {count}")
    spark.stop()


def run_silver_taxi():
    from pyspark.sql import functions as F
    import os

    spark = get_spark_session()

    logging.info("Starting Silver Taxi processing...")
    SILVER_TABLE  = "lakehouse.taxi.silver_trips"
    STAGING_TABLE = "lakehouse.taxi.staging_silver"

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            kafka_timestamp       TIMESTAMP,
            VendorID              INT,
            tpep_pickup_datetime  TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count       INT,
            trip_distance         DOUBLE,
            RatecodeID            INT,
            store_and_fwd_flag    BOOLEAN,
            PULocationID          INT,
            DOLocationID          INT,
            payment_type          INT,
            fare_amount           DOUBLE,
            extra                 DOUBLE,
            mta_tax               DOUBLE,
            tip_amount            DOUBLE,
            tolls_amount          DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount          DOUBLE,
            congestion_surcharge  DOUBLE,
            Airport_fee           DOUBLE,
            cbd_congestion_fee    DOUBLE,
            trip_duration_minutes INT,
            avg_speed_kmh         DOUBLE,
            pickup_zone           STRING,
            pickup_borough        STRING,
            dropoff_zone          STRING,
            dropoff_borough       STRING
        ) USING iceberg
    """)

    bronze_df = spark.read.table("lakehouse.taxi.bronze_trips")

    cleaned = (
        bronze_df
        .withColumn("tpep_pickup_datetime",
            F.to_timestamp(F.col("tpep_pickup_datetime")))
        .withColumn("tpep_dropoff_datetime",
            F.to_timestamp(F.col("tpep_dropoff_datetime")))
        .withColumn("passenger_count",    F.col("passenger_count").cast("int"))
        .withColumn("RatecodeID",         F.col("RatecodeID").cast("int"))
        .withColumn("payment_type",       F.col("payment_type").cast("int"))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag") == "Y")
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("passenger_count").isNotNull() & (F.col("passenger_count") > 0))
        .filter(F.col("trip_distance") >= 0)
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        .filter(F.col("RatecodeID").isin([1, 2, 3, 4, 5, 6, 99]))
        .filter(F.col("payment_type").isin([0, 1, 2, 3, 4, 5, 6]))
        .withColumn("trip_duration_minutes",
            ((F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")) / 60).cast("int"))
        .filter(
            (F.col("trip_duration_minutes") > 0) &
            (F.col("trip_duration_minutes") < 1440)
        )
        .withColumn("avg_speed_kmh",
            F.try_divide(
                F.col("trip_distance") * 1.60934,
                F.col("trip_duration_minutes") / 60
            ))
        .filter(
            (F.col("avg_speed_kmh") >= 2) &
            (F.col("avg_speed_kmh") <= 130)
        )
        .dropDuplicates(["VendorID", "tpep_pickup_datetime", "PULocationID"])
    )

    zone_path = "/opt/airflow/data/taxi_zone_lookup.parquet"
    if not os.path.exists(zone_path):
        raise FileNotFoundError(
            f"Zone lookup file not found at {zone_path}. "
            "Ensure taxi_zone_lookup.parquet is placed in the data/ directory."
        )

    zones = (
        spark.read.parquet(zone_path)
        .filter(F.col("LocationID").isNotNull() & (F.col("LocationID") > 0))
        .filter(F.col("Borough").isNotNull()    & (F.col("Borough") != ""))
        .filter(F.col("Zone").isNotNull()       & (F.col("Zone") != ""))
        .dropDuplicates(["LocationID"])
    )

    enriched = (
        cleaned
        .join(
            F.broadcast(zones).select(
                F.col("LocationID").alias("PULocationID"),
                F.col("Zone").alias("pickup_zone"),
                F.col("Borough").alias("pickup_borough"),
            ),
            on="PULocationID", how="left"
        )
        .join(
            F.broadcast(zones).select(
                F.col("LocationID").alias("DOLocationID"),
                F.col("Zone").alias("dropoff_zone"),
                F.col("Borough").alias("dropoff_borough"),
            ),
            on="DOLocationID", how="left"
        )
    )

    enriched.writeTo(STAGING_TABLE).createOrReplace()
    staging_count = spark.read.table(STAGING_TABLE).count()
    logging.info(f"[Silver taxi] Staging rows after cleaning: {staging_count}")

    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS t
        USING {STAGING_TABLE} AS s
        ON  t.VendorID             = s.VendorID
        AND t.tpep_pickup_datetime = s.tpep_pickup_datetime
        AND t.PULocationID         = s.PULocationID
        WHEN NOT MATCHED THEN INSERT *
    """)

    silver_count = spark.read.table(SILVER_TABLE).count()
    bronze_count = spark.read.table("lakehouse.taxi.bronze_trips").count()
    logging.info(f"[Silver taxi] MERGE complete — Silver row count: {silver_count}")
    logging.info(f"[Silver taxi] Bronze rows: {bronze_count} | filtered out: {bronze_count - silver_count}")
    spark.stop()


def run_gold_demand_patterns():
    from pyspark.sql import functions as F
    spark = get_spark_session()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

    silver_df = spark.read.table("lakehouse.taxi.silver_trips")

    hourly_daily = (
        silver_df
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_zone", "pickup_borough", "hour_of_day", "pickup_date")
        .agg(F.count("*").alias("daily_trip_count"))
    )

    stats = (
        hourly_daily
        .groupBy("pickup_zone", "hour_of_day")
        .agg(
            F.avg("daily_trip_count").alias("avg_trip_count"),
            F.stddev("daily_trip_count").alias("stddev_trip_count"),
        )
        .fillna(0, subset=["stddev_trip_count"])
    )

    city_avg = stats.agg(F.avg("avg_trip_count")).collect()[0][0]
    city_std = stats.agg(F.stddev("avg_trip_count")).collect()[0][0]

    demand_patterns = (
        stats
        .withColumn(
            "demand_classification",
            F.when(F.col("avg_trip_count") >= city_avg + city_std, "high demand")
            .when(F.col("avg_trip_count") <= city_avg - city_std, "low demand")
            .otherwise("normal")
        )
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold_demand_patterns (
            pickup_zone           STRING,
            hour_of_day           INT,
            avg_trip_count        DOUBLE,
            stddev_trip_count     DOUBLE,
            demand_classification STRING
        ) USING iceberg
    """)

    demand_patterns.writeTo("lakehouse.taxi.gold_demand_patterns").createOrReplace()

    logging.info(
        f"gold_demand_patterns complete. Rows: "
        f"{spark.read.table('lakehouse.taxi.gold_demand_patterns').count()}"
    )
    spark.stop()


def run_gold_supply_demand_gap():
    from pyspark.sql import functions as F
    spark = get_spark_session()

    total_drivers = spark.read.table("lakehouse.cdc.silver_drivers").count()
    demand = spark.read.table("lakehouse.taxi.gold_demand_patterns")

    citywide_demand = (
        demand
        .groupBy("hour_of_day")
        .agg(F.sum("avg_trip_count").alias("total_avg_trips"))
    )

    gap_df = (
        demand
        .join(citywide_demand, on="hour_of_day", how="left")
        .withColumn("drivers_available", F.lit(float(total_drivers)))
        .withColumn(
            "driver_share",
            F.try_divide(
                F.col("avg_trip_count"),
                F.col("total_avg_trips")
            ) * F.col("drivers_available")
        )
        .withColumn(
            "is_underserved",
            F.col("avg_trip_count") > F.col("driver_share")
        )
        .withColumn(
            "demand_supply_gap",
            F.col("avg_trip_count") - F.col("driver_share")
        )
        .select(
            "pickup_zone",
            "hour_of_day",
            "avg_trip_count",
            "stddev_trip_count",
            F.col("driver_share").alias("drivers_available"),
            "demand_supply_gap",
            "is_underserved"
        )
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold_supply_demand_gap (
            pickup_zone       STRING,
            hour_of_day       INT,
            avg_trip_count    DOUBLE,
            stddev_trip_count DOUBLE,
            drivers_available DOUBLE,
            demand_supply_gap DOUBLE,
            is_underserved    BOOLEAN
        ) USING iceberg
    """)

    gap_df.writeTo("lakehouse.taxi.gold_supply_demand_gap").createOrReplace()
    logging.info(f"[gold] supply-demand gap complete. Total drivers: {total_drivers}")
    spark.stop()


def run_gold_taxi():
    run_gold_demand_patterns()
    run_gold_supply_demand_gap()


def run_validate():
    import psycopg2
    import os

    spark = get_spark_session()

    pg_conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "sourcedb"),
        user=os.environ.get("PG_USER", "cdc_user"),
        password=os.environ.get("PG_PASSWORD", "admin"),
    )

    checks = [
        ("customers", "public.customers", "lakehouse.cdc.silver_customers"),
        ("drivers",   "public.drivers",   "lakehouse.cdc.silver_drivers"),
    ]

    all_passed = True
    with pg_conn.cursor() as cur:
        cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        for label, pg_table, iceberg_table in checks:
            cur.execute(f"SELECT COUNT(*) FROM {pg_table}")
            pg_count = cur.fetchone()[0]

            silver_count = spark.table(iceberg_table).count()

            TOLERANCE = 5
            delta  = abs(pg_count - silver_count)
            status = "PASS" if delta <= TOLERANCE else "FAIL"
            logging.info(
                f"[validate] {label}: PostgreSQL={pg_count}  "
                f"Silver={silver_count}  delta={pg_count - silver_count}  {status}"
            )
            if status == "FAIL":
                all_passed = False

    pg_conn.close()
    spark.stop()

    if not all_passed:
        raise ValueError(
            "Silver CDC row counts do not match PostgreSQL (beyond tolerance). "
            "Stop simulate.py, re-trigger the DAG, and re-check."
        )

    logging.info("[validate] All Silver CDC tables match PostgreSQL.")


with DAG(
    dag_id          = "project3_pipeline",
    default_args    = default_args,
    start_date      = datetime(2026, 4, 1),
    schedule        = None,#"*/15 * * * *",
    catchup         = False,
    dagrun_timeout  = timedelta(hours=1),
    tags            = ["project3"],
) as dag:

    health_check = HttpSensor(
        task_id="connector_health",
        http_conn_id="debezium_connect",
        endpoint="connectors/cdc-connector/status",
        request_params={},
        response_check=lambda response: (
            response.status_code == 200
            and response.json().get("connector", {}).get("state") == "RUNNING"
        ),
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    bronze_customers = PythonOperator(
        task_id="bronze_customers",
        python_callable=run_bronze_customers,
    )

    silver_customers = PythonOperator(
        task_id="silver_customers",
        python_callable=run_silver_customers,
    )

    bronze_drivers = PythonOperator(
        task_id="bronze_drivers",
        python_callable=run_bronze_drivers,
    )

    silver_drivers = PythonOperator(
        task_id="silver_drivers",
        python_callable=run_silver_drivers,
    )

    bronze_taxi = PythonOperator(
        task_id="bronze_taxi",
        python_callable=run_bronze_taxi,
    )

    silver_taxi = PythonOperator(
        task_id="silver_taxi",
        python_callable=run_silver_taxi,
    )

    gold_taxi = PythonOperator(
        task_id="gold_taxi",
        python_callable=run_gold_taxi,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=run_validate,
    )

    health_check >> [bronze_customers, bronze_drivers, bronze_taxi]

    # Path A: CDC
    bronze_customers >> silver_customers
    bronze_drivers >> silver_drivers

    # Path B: Taxi
    bronze_taxi >> silver_taxi

    # Gold depends on both paths
    [silver_taxi, silver_drivers] >> gold_taxi

    # Validate depends on everything
    [silver_customers, gold_taxi] >> validate