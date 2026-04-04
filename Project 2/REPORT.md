# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze

_Table DDL or DataFrame schema. Explain what is stored and why it is kept as-is._

### Silver

_Table DDL or DataFrame schema. Explain what changed compared to bronze and why._

### Gold

_Table DDL or DataFrame schema. Explain the aggregation logic._

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