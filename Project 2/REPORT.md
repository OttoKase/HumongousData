# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze

```sql
CREATE TABLE lakehouse.taxi.bronze (
    kafka_key        STRING,
    raw_value        STRING,
    topic            STRING,
    partition        INT,
    offset           BIGINT,
    kafka_timestamp  TIMESTAMP
) USING iceberg
```
Bronze stores raw Kafka messages as-is without any parsing or transformation. `raw_value` contains the original JSON string from the producer. `kafka_key` is
the Kafka message key. `topic`, `partition`, and `offset` identify the exact position of the message in Kafka. `kafka_timestamp` is the time the message was written to Kafka. No cleaning is applied here.

### Silver

```sql
CREATE TABLE lakehouse.taxi.silver (
    VendorID                INT,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         INT,
    trip_distance           DOUBLE,
    RatecodeID              INT,
    store_and_fwd_flag      BOOLEAN,
    PULocationID            INT,
    DOLocationID            INT,
    payment_type            INT,
    fare_amount             DOUBLE,
    extra                   DOUBLE,
    mta_tax                 DOUBLE,
    tip_amount              DOUBLE,
    tolls_amount            DOUBLE,
    improvement_surcharge   DOUBLE,
    total_amount            DOUBLE,
    congestion_surcharge    DOUBLE,
    Airport_fee             DOUBLE,
    cbd_congestion_fee      DOUBLE,
    trip_duration_minutes   INT,
    avg_speed_kmh           DOUBLE,
    pickup_zone             STRING,
    pickup_borough          STRING,
    dropoff_zone            STRING,
    dropoff_borough         STRING,
    is_peak_hour            BOOLEAN,
    kafka_timestamp         TIMESTAMP
) USING iceberg
```
Compared to bronze, silver parses the `raw_value` JSON into typed columns. Columns are cleaned (see cleaning rules in section 3). Two derived columns are added: `trip_duration_minutes` and `avg_speed_kmh`. Zone names are joined from the `taxi_zone_lookup` table. Duplicates are removed.

### Gold
```sql
CREATE TABLE lakehouse.taxi.gold (
    day            DATE,
    pickup_zone    STRING,
    trip_count     LONG,
    avg_distance   DOUBLE,
    avg_fare       DOUBLE,
    avg_total      DOUBLE,
    tip_rate_pct   DOUBLE,
    total_revenue  DOUBLE
) USING iceberg
PARTITIONED BY (day)
```
Gold aggregates silver by `(day, pickup_zone)`. Each row represents the aggregated trip statistics for one pickup zone on one day.
- `trip_count` - total number of trips
- `avg_distance` - average trip distance in miles
- `avg_fare` - average base fare
- `avg_total` - average total amount charged
- `tip_rate_pct` - percentage of trips where a tip was given
- `total_revenue` - sum of all `total_amount` values

Partitioned by `day` so date-range queries scan only relevant partitions.
Updated via `MERGE INTO` on `(day, pickup_zone)` so restarts are idempotent.

## 2. Cleaning rules and enrichment

_List each cleaning rule (nulls, invalid values, deduplication key) with a brief justification._
_Describe the enrichment step (zone lookup join)._

## 3. Streaming configuration

_Describe:_
- _Checkpoint path and what it stores._
- _Trigger interval and why you chose it._
- _Output mode (append/update/complete) and why._
- _Watermark (if used) and why._

## 4. Gold table partitioning strategy

_Explain your partitioning choice. Why this column(s)? What query patterns does it optimize?_
_Show the Iceberg snapshot history (query output or screenshot)._

## 5. Restart proof

_Show that stopping and restarting the pipeline does not produce duplicates._
_Include row counts before and after restart._

## 6. Custom scenario

_Explain and/or show how you solved the custom scenario from the GitHub issue._

## 7. How to run

```bash
# Step 1: Start infrastructure
docker compose up -d
# NB! Make sure everything started without errors (except minio-init)

# Step 1.5: Start the kafka topic IF RUNNING IN A NEW CONTAINER (not restarted. If restarted, skip)
docker exec kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
>>   --bootstrap-server localhost:9092 \
>>   --create --topic taxi-trips --partitions 3 --replication-factor 1"

# Step 2: Start the producer
# Simplest in jupyterlab. Open the terminal via launcher. See produce.py for more options
python project/produce.py --rate 10

# Step 3: Run the pipeline

<TODO> # In notebook currently
```

_Add any additional steps or dependencies needed to reproduce your results._

_Include the `.env` values the grader should use to run your project._
