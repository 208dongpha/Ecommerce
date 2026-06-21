# E-Commerce Data Lakehouse Pipeline

This project is a local e-commerce data lakehouse pipeline. It simulates user activity events, streams them through Kafka, stores Bronze and Silver layers as Delta Lake tables on MinIO, runs a daily Gold aggregation with Airflow, and exposes the Gold table to Superset through Trino.

## Architecture

```text
Airflow daily DAG
    |
    |-- generate_data: produce 1000 events into Kafka
    |-- wait_for_streaming: wait for Bronze/Silver streaming jobs
    |-- gold_job: trigger the Gold batch job through gold-runner

Kafka
    |
    v
bronze-stream
    Kafka -> Bronze Delta
    s3a://ecommerce/bronze/events
    |
    v
silver-stream
    Bronze Delta -> Silver Delta
    s3a://ecommerce/silver/events
    |
    v
gold-runner
    Silver Delta -> Gold Delta
    s3a://ecommerce/gold/sales_metrics
    |
    v
Trino -> Superset dashboard
```

![Pipeline](pipeline.png)

## Tech Stack

- Apache Kafka: event broker for e-commerce activity data.
- Apache Spark 3.5.1: Structured Streaming for Bronze/Silver and batch processing for Gold.
- Delta Lake: table format for Bronze, Silver, and Gold layers.
- MinIO: S3-compatible object storage for the lakehouse.
- Apache Airflow: daily orchestration.
- Trino: SQL query engine over Delta Lake data stored in MinIO.
- Apache Superset: dashboard and BI layer.
- Docker Compose: local multi-service runtime.

## Data Flow

### 1. Producer

Main file:

```text
producer/producer.py
```

The producer generates synthetic e-commerce events with these fields:

- `event_time`
- `user_id`
- `product_id`
- `action`
- `price`
- `revenue`
- `category`

Airflow runs the producer daily:

```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:9092 NUM_EVENTS=1000 python producer.py
```

### 2. Bronze Layer

Main file:

```text
spark/apps/streaming_job.py
```

The `bronze-stream` service runs continuously. It reads the Kafka topic `ecommerce-events`, parses event JSON, and writes the raw stream to Bronze Delta:

```text
s3a://ecommerce/bronze/events
```

Checkpoint path:

```text
/opt/checkpoints/ecommerce-events
```

The checkpoint directory is mounted to the host:

```text
./spark/checkpoints:/opt/checkpoints
```

Because of this checkpoint, the stream can continue from its previous progress after a container or machine restart.

### 3. Silver Layer

Main file:

```text
spark/apps/silver_job.py
```

The `silver-stream` service also runs continuously. It reads Bronze Delta data, cleans and enriches it, then writes Silver Delta.

Current Silver transformations:

- Cast `event_time` to timestamp.
- Filter null `user_id`.
- Filter null `product_id`.
- Drop duplicate rows.
- Add `event_date`.
- Add `event_hour`.
- Add `is_purchase`.
- Add `is_refund`.

Output path:

```text
s3a://ecommerce/silver/events
```

Checkpoint path:

```text
/opt/checkpoints/silver
```

### 4. Gold Layer

Main files:

```text
spark/apps/batch_job.py
spark/apps/gold_runner.sh
```

Gold is a batch layer. It does not run forever like Bronze and Silver. Airflow creates a trigger file, and the `gold-runner` service picks it up and runs the Spark batch job.

Gold reads:

```text
s3a://ecommerce/silver/events
```

Gold writes:

```text
s3a://ecommerce/gold/sales_metrics
```

The current Gold table aggregates by:

- `event_date`
- `category`

Current metrics:

- `total_revenue`
- `total_orders`
- `total_refunds`
- `unique_users`
- `unique_products`
- `avg_price`
- `max_price`
- `min_price`

### 5. Trino and Superset

Trino does not store data. It queries the Gold Delta table directly from MinIO.

Trino catalog config:

```text
trino/catalog/delta.properties
```

After the Gold table exists, register it in Trino:

```sql
CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'sales_metrics',
    table_location => 's3://ecommerce/gold/sales_metrics'
);
```

Test query:

```sql
SELECT *
FROM delta.default.sales_metrics;
```

Superset connects to Trino with a SQLAlchemy URI like:

```text
trino://trino@trino:8080/delta/default
```

The dashboard should use the `delta.default.sales_metrics` dataset.

## Airflow DAG

Main file:

```text
airflow/dags/ecommerce_pipeline.py
```

Current DAG:

```text
generate_data -> wait_for_streaming -> gold_job
```

Tasks:

- `generate_data`: produces 1000 events into Kafka.
- `wait_for_streaming`: waits 60 seconds for Bronze and Silver to process new data.
- `gold_job`: writes `gold.request` and waits for `gold.done` or `gold.failed`.

Airflow does not call `docker exec`. Instead, Airflow communicates with `gold-runner` through this shared folder:

```text
airflow/triggers
```

Trigger files:

- `gold.request`: Airflow requests a Gold batch run.
- `gold.done`: Gold batch completed successfully.
- `gold.failed`: Gold batch failed.

## Docker Services

Main services in `docker-compose.yaml`:

- `zookeeper`
- `kafka`
- `minio`
- `spark-master`
- `spark-worker`
- `bronze-stream`
- `silver-stream`
- `gold-runner`
- `postgres-airflow`
- `airflow-init`
- `airflow-webserver`
- `airflow-scheduler`
- `trino`
- `superset`

`postgres-airflow` is only used as the Airflow metadata database. This project currently does not use PostgreSQL as a data warehouse.

## How To Run

Start the full stack:

```bash
docker compose up -d
```

Check running services:

```bash
docker compose ps
```

Airflow UI:

```text
http://localhost:8081
```

Superset UI:

```text
http://localhost:8088
```

Trino UI:

```text
http://localhost:8085
```

MinIO Console:

```text
http://localhost:9001
```

## Daily Operation

After `docker compose up -d`, these streaming services should keep running:

```text
bronze-stream
silver-stream
```

Airflow runs the `ecommerce_pipeline` DAG daily.

Daily flow:

```text
1. Generate 1000 events into Kafka.
2. Wait for Bronze/Silver streaming jobs to process the events.
3. Trigger the Gold batch job.
4. Update the Gold Delta table.
5. Superset queries the latest Gold data through Trino.
```

## Useful Commands

View Bronze stream logs:

```bash
docker compose logs -f bronze-stream
```

View Silver stream logs:

```bash
docker compose logs -f silver-stream
```

View Gold runner logs:

```bash
docker compose logs -f gold-runner
```

View Airflow scheduler logs:

```bash
docker compose logs -f airflow-scheduler
```

Restart streaming jobs:

```bash
docker compose restart bronze-stream silver-stream
```

Restart Gold runner:

```bash
docker compose restart gold-runner
```

Stop the stack:

```bash
docker compose down
```

## Project Structure

```text
airflow/
  dags/
    ecommerce_pipeline.py
  triggers/
    .gitkeep

producer/
  producer.py

spark/
  apps/
    streaming_job.py
    silver_job.py
    batch_job.py
    gold_runner.sh
  checkpoints/
  ivy/

trino/
  catalog/
    delta.properties

superset/
  Dockerfile

docker-compose.yaml
README.md
```

## Notes

- Bronze and Silver are long-running streaming jobs.
- Gold is a daily batch job triggered by Airflow.
- Trino is a query engine, not a storage layer.
- Superset visualizes the Gold table through Trino.
- If the dashboard does not show fresh data, check Superset cache settings or manually refresh the chart/dashboard.
