# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze

```sql
CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze (
    kafka_key        STRING,
    raw_value        STRING,
    topic            STRING,
    partition        INT,
    offset           BIGINT,
    kafka_timestamp  TIMESTAMP
) USING iceberg
```

Bronze stores raw Kafka messages exactly as received — no parsing, no validation. 
The full JSON payload is kept in `raw_value` so that no data is lost at ingestion 
time. This allows reprocessing from bronze if cleaning rules change in the future.

### Silver

```sql
CREATE TABLE IF NOT EXISTS lakehouse.taxi.silver (
    VendorID               INT,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        INT,
    trip_distance          DOUBLE,
    RatecodeID             INT,
    store_and_fwd_flag     BOOLEAN,
    PULocationID           INT,
    DOLocationID           INT,
    payment_type           INT,
    fare_amount            DOUBLE,
    extra                  DOUBLE,
    mta_tax                DOUBLE,
    tip_amount             DOUBLE,
    tolls_amount           DOUBLE,
    improvement_surcharge  DOUBLE,
    total_amount           DOUBLE,
    congestion_surcharge   DOUBLE,
    Airport_fee            DOUBLE,
    cbd_congestion_fee     DOUBLE,
    trip_duration_minutes  INT,
    avg_speed_kmh          DOUBLE,
    pickup_zone            STRING,
    pickup_borough         STRING,
    dropoff_zone           STRING,
    dropoff_borough        STRING,
    kafka_timestamp        TIMESTAMP
) USING iceberg
```

Silver parses the raw JSON from bronze, casts types to their correct 
representations, applies cleaning rules, and enriches trips with zone names. 
Two derived columns (`trip_duration_minutes`, `avg_speed_kmh`) are added to 
support cleaning and downstream analysis.

### Gold

_Table DDL or DataFrame schema. Explain the aggregation logic._

---

## 2. Cleaning rules and enrichment

### Type casting

The Kafka JSON payload serializes all numeric fields as floating point (e.g. 
`"passenger_count": 2.0`). Fields are parsed as DOUBLE and then cast to their 
correct types: `passenger_count`, `RatecodeID`, and `payment_type` are cast to 
INT. `store_and_fwd_flag` is cast from `"Y"/"N"` string to BOOLEAN.

### Cleaning rules

| Rule | Filter | Justification |
|------|--------|---------------|
| Null passenger count | `passenger_count IS NOT NULL AND > 0` | A trip with no passengers is invalid |
| Non-negative distance | `trip_distance >= 0` | Negative distance is physically impossible |
| Valid trip direction | `dropoff_datetime > pickup_datetime` | Dropoff must be after pickup |
| Valid RatecodeID | `RatecodeID IN (1,2,3,4,5,6,99)` | Per NYC TLC data dictionary; 99 = unknown |
| Valid payment type | `payment_type IN (0,1,2,3,4,5,6)` | Per NYC TLC data dictionary |
| Positive duration | `trip_duration_minutes > 0` | Zero duration means corrupt timestamps |
| Realistic duration | `trip_duration_minutes < 1440` | NYC yellow taxis do not make multi-day trips |
| Minimum speed | `avg_speed_kmh >= 2` | Below 2 km/h suggests corrupt timestamps |
| Maximum speed | `avg_speed_kmh <= 130` | Above 130 km/h is unrealistic for NYC roads |

### Deduplication

Duplicate records are removed using the key `(VendorID, tpep_pickup_datetime, 
PULocationID)`. The same vendor cannot start two trips from the same location at 
the exact same timestamp — any such duplicates are assumed to be repeated Kafka 
messages rather than distinct trips. Silver is rebuilt from bronze on each run 
(`createOrReplace`) to ensure idempotency.

### Enrichment

Trips are enriched with human-readable zone names by joining the 
`taxi_zone_lookup.parquet` reference table on `PULocationID` and `DOLocationID`. 
This adds `pickup_zone`, `pickup_borough`, `dropoff_zone`, and `dropoff_borough` 
columns. A left join is used so that trips with unknown location IDs are retained 
rather than dropped.

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

Our scenario is as follows: 

_Run produce.py with --rate 1 and again with --rate 50. In REPORT.md, include a Spark UI screenshot for both runs and compare batch processing time, records per batch, and scheduling delay. Explain what happens when the producer is faster than the consumer._

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