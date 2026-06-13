# E-Commerce Real-Time Data Lakehouse Platform

A production-grade, multi-stage Data Lakehouse platform built to handle high-velocity streaming data and robust batch analytical processing for an E-commerce ecosystem. The system ingests synthetic user interaction and transaction streams, stores them securely using a Medallion Architecture on self-hosted object storage, and synchronizes aggregate metrics into a central Data Warehouse for sub-second Business Intelligence (BI) dashboard querying.

---

## System Architecture & Data Pipelines

The platform leverages a hybrid Lambda/Lakehouse design pattern to eliminate data silos, combining the speed of streaming for operational visibility with the data integrity of batch processing for financial reporting. The entire system infrastructure lifecycle and workflows are orchestrated by **Apache Airflow**.

![System Flow](pipeline.png)

### Deep-Dive Data Flow Mechanics:

#### 1. Data Generation (Producer Tier)
* **Mechanism:** A highly concurrent, multi-threaded Python application simulates continuous user activity. It utilizes libraries like `faker` and `json` to mimic realistic production payloads.
* **Stream Types:** * `Clickstream Stream`: High-frequency, low-payload tracking records (`user_id`, `session_id`, `product_id`, `action_type` [view, add_to_cart, search], `device_type`, `ip_address`, `timestamp`).
  * `Transactional Stream`: Lower-frequency, high-integrity records (`order_id`, `user_id`, `items` [list of product IDs, quantities, prices], `total_amount`, `payment_method`, `status` [success, failed, pending], `timestamp`).
* **Ingestion:** Events are immediately buffered and pushed asynchronously into **Apache Kafka** clusters with partition keys mapped to `session_id` and `order_id` to guarantee message ordering per user.

#### 2. Bronze Layer (Raw Storage Tier)
* **Operation:** Written in PySpark using the **Spark Structured Streaming** engine. It establishes a direct connection to Kafka brokers via the `spark-sql-kafka` connector.
* **Processing:** Minimal overhead processing. The application reads raw byte payloads from Kafka topics, casts the key/value data to strings, appends system-level ingestion metadata (`ingestion_time`, `kafka_partition`, `kafka_offset`), and writes the stream to storage.
* **Storage Optimization:** Data is appended to the **MinIO Bronze Bucket** using the **Delta Lake** storage format. It is physically partitioned by `ingestion_date` to prevent the "small file problem" and allow efficient data retention and debugging audits.

#### 3. Silver Layer (Enriched & Modeled Tier)
* **Operation:** A decoupled PySpark Structured Streaming application reads changes directly from the Bronze Delta tables via Delta's transaction log tailing.
* **Transformation & Cleaning Engine:**
  * **Schema Enforcement & Casting:** Parses raw JSON string columns into strongly typed Spark schemas using `from_json()`.
  * **Data Deduplication:** Employs Spark’s `dropDuplicates()` against business keys (`order_id`, `session_id` + `timestamp`) within micro-batches.
  * **Data Quality Checks:** Filters out invalid payloads (e.g., null user IDs, negative transaction totals) and diverts them into a Dead Letter Queue (DLQ) path for isolation.
* **Data Modeling:** Transforms the stream into an optimized **Star Schema** by extracting attributes into Dimension tables (`dim_users`, `dim_products`) and Fact tables (`fact_orders`, `fact_clickstream`).
* **Storage Optimization:** Persisted into the **MinIO Silver Bucket** as Delta tables, partitioned by operational dimensions (e.g., `fact_orders` is partitioned by `order_date`).

#### 4. Gold Layer (Analytical Datamart Tier)
* **Operation:** Scheduled PySpark **Batch** jobs triggered by an upstream scheduler. Unlike the continuous streaming layers, the Gold layer computes complex, cross-table business analytics.
* **Aggregation Logic:** Combines facts and dimensions from the Silver layer to build specialized, business-facing datamarts. Computes rolling window operations and complex KPIs, including:
  * `hourly_conversion_rates`: (Total successful checkouts / Total unique product views) grouped by 1-hour windows.
  * `realtime_revenue_tracker`: Total GMV, average order value (AOV), and payment failure rates.
  * `trending_products`: Top 10 viewed and purchased items over moving time horizons.
* **Storage Optimization:** Written as highly optimized, compacted Parquet files into the **MinIO Gold Bucket**.

#### 5. Data Warehouse & BI Presentation Tier
* **Operation:** To ensure enterprise-grade concurrent querying without placing a processing load on the Object Storage lake tier, data is synchronized down to a relational **PostgreSQL Data Warehouse**.
* **Synchronization Pipeline:** Airflow runs an operational task that reads newly generated partitions from MinIO Gold and loads them into PostgreSQL tables using optimized bulk `COPY` operations or `MERGE INTO` SQL statements.
* **Visualization Interface:** **Apache Superset** connects directly to the PostgreSQL DWH via SQLAlchemy. It hosts analytical dashboards that refresh automatically, exposing low-latency charts for operational monitoring.

---

## Complete Tech Stack & Component Breakdown

* **Programming Language:** Python 3.10+ (Core backend code, event simulator, PySpark automation scripts).
* **Distributed Stream Storage:** Apache Kafka 3.x (Provides a multi-partition distributed commit log acting as the fault-tolerant message backbone).
* **Distributed Processing Cluster:** Apache Spark 3.4+ (Structured Streaming for low-latency Bronze/Silver processing; Spark SQL for Gold batch computations).
* **Cloud-Native Object Storage:** MinIO Server (S3-compatible API, hosting decoupled object storage buckets configured for single-node development setup).
* **Storage Layer Acid Framework:** Delta Lake 2.4+ (Brings ACID transactions, time-travel logging, metadata scaling, and schema evolution to MinIO storage).
* **Workflow Orchestration Engine:** Apache Airflow 2.7+ (DAG-driven control plane managing execution paths, schedules, and alerts).
* **Business Intelligence Engine:** Apache Superset (Cloud-native data exploration and visualization platform displaying business-critical dashboards).
* **Containerization Infrastructure:** Docker Engine & Docker Compose v2 (Containerizes and isolates every standalone component for local development repeatability).

---

## Detailed Role of Apache Airflow (The Orchestrator)

Apache Airflow functions as the automated system operator, governing job lifecycles through declarative Python Directed Acyclic Graphs (DAGs). Airflow is explicitly segregated into two primary functional DAG patterns:

### 1. The Core Analytical & Synchronization DAG (`ecom_gold_sync_pipeline`)
This DAG runs on a cron-based schedule (e.g., hourly) to drive data progression into the consumption layer:
* **Task 1: `spark_batch_gold_transform`** – Submits a PySpark batch application using the `SparkSubmitOperator` to process the latest Silver layer micro-batches and re-compute metrics inside MinIO Gold.
* **Task 2: `postgres_dwh_upsert`** – Triggered immediately upon Task 1 success. It uses the `PostgresOperator` to run an idempotent SQL script that pulls the latest records from MinIO Gold and executes an `INSERT ... ON CONFLICT DO UPDATE` (Upsert) into PostgreSQL.
* **Task 3: `bi_cache_warmup`** – Fires an API webhook call to Apache Superset to clear legacy analytical query caches and force dashboard panels to pull the fresh DWH data.

```text
[Airflow Scheduler]
       │
       ▼ (Hourly Schedule)
┌─────────────────────────────────┐
│ Task 1: spark_batch_gold        │ (Computes Gold Layer in MinIO)
└────────────────┬────────────────┘
                 │
                 ▼ (On Success)
┌─────────────────────────────────┐
│ Task 2: postgres_dwh_upsert     │ (Syncs MinIO Gold ──► PostgreSQL DWH)
└────────────────┬────────────────┘
                 │
                 ▼ (On Success)
┌─────────────────────────────────┐
│ Task 3: bi_cache_warmup         │ (Clears Superset Cache via API)
└─────────────────────────────────┘