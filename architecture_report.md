# Architecture & Technical Design Report: Real-Time E-Commerce Streaming Analytics Platform

## Executive Summary

The **Real-Time E-Commerce Streaming Analytics Platform** is a production-grade, distributed Event-Driven Architecture (EDA) built to simulate and process high-throughput e-commerce interactions. Operating at an ingestion volume of **~100 events/second** (~8.6 million events/day), the platform ingests heterogeneous streams (Orders, Payments, Clicks, Reviews), performs distributed streaming transformations with micro-batch windowing in Apache Spark, persists raw and aggregated data idempotently into PostgreSQL, and presents sub-minute analytical KPIs via an auto-provisioned Grafana monitoring suite.

The platform is designed to operate on a containerized local or cloud footprint (Docker Compose) at **zero software licensing cost**, leveraging battle-tested open-source components configured with enterprise-grade durability and idempotency patterns.

---

## High-Level System Architecture & Component Topology

```mermaid
flowchart TD
    subgraph INGESTION["1. Data Ingestion Layer"]
        PG["Python Event Producer\n(Multi-Threaded Daemon)"]
        FD["Faker Synthetic Generator\n(Orders, Payments, Clicks, Reviews)"]
        FD --> PG
    end

    subgraph BROKER["2. Message Broker Layer (Apache Kafka Cluster)"]
        ZK["ZooKeeper :2181\n(Cluster Coordination)"]
        KB["Kafka Broker :9092 / :29092\n(Confluent v7.5.0)"]
        KUI["Kafka UI :8080\n(Topic Inspection & Monitoring)"]
        KINIT["Kafka Topic Initializer\n(Partition & Topic Setup)"]
        
        ZK --- KB
        KINIT -.->|Provisions Topics| KB
        KUI --- KB
        PG -->|snappy-compressed batches| KB
    end

    subgraph ENGINE["3. Stream Processing Engine (Apache Spark 3.5.1)"]
        SM["Spark Master :7077 / :8081"]
        SW["Spark Worker :8082\n(4GB RAM / 2 Cores)"]
        SM --- SW

        SS["Spark Structured Streaming Job\n(spark_stream.py)"]
        TR["Pure Transformations Library\n(transformations.py)"]
        
        KB -->|Structured Stream Ingest| SS
        SS --> TR
        TR -->|Cleaned Micro-batches| SS
    end

    subgraph STORAGE["4. Persistence & Data Quality Layer (PostgreSQL 15)"]
        PGDB[("PostgreSQL 15-alpine :5432\n(ecommerce_db)")]
        RAW[("Raw Event Tables\n(orders, payments, clicks, reviews)")]
        AGG[("Aggregate Tables\n(analytics_per_minute, revenue_by_country,\ntop_products, payment_method_stats)")]
        VIEWS["Analytical SQL Views\n(v_realtime_summary, v_orders_per_minute,\nv_payment_split, v_top_10_products)"]
        DLQ[("Dead Letter Queue\n(dead_letter_queue)")]

        PGDB --- RAW
        PGDB --- AGG
        PGDB --- VIEWS
        PGDB --- DLQ
        
        SS -->|"psycopg2 ON CONFLICT DO NOTHING (5s)"| RAW
        SS -->|"psycopg2 ON CONFLICT DO UPDATE (30s)"| AGG
        SS -->|"Malformed schema payloads (10s)"| DLQ
    end

    subgraph OBSERVABILITY["5. Visualization & Monitoring (Grafana 10.2.0)"]
        GF["Grafana Server :3000\n(Auto-Provisioned)"]
        DASH["Live E-Commerce Dashboard\n(11 Panels, 5s Auto-Refresh)"]
        GF --- DASH
        VIEWS -->|Direct SQL Queries| GF
        AGG -->|Time-Series Metrics| GF
    end
```

---

## Core Architectural Layers & Component Analysis

### 1. Ingestion Subsystem (`producer/`)

The ingestion engine simulates real-world user activity across an Indian e-commerce catalog with currency, category, and regional variance.

```mermaid
flowchart LR
    subgraph MultiThreadedProducer["producer.py (Multi-Threaded Architecture)"]
        MT[Main Thread & Signal Handler]
        T1["Thread 1: Orders (35 ev/s)"]
        T2["Thread 2: Payments (30 ev/s)"]
        T3["Thread 3: Clicks (25 ev/s)"]
        T4["Thread 4: Reviews (10 ev/s)"]
        MR["Thread 5: Metrics Reporter (10s)"]
        MC["Thread-Safe Metrics Counter"]
    end

    MT --> T1
    MT --> T2
    MT --> T3
    MT --> T4
    MT --> MR
    T1 & T2 & T3 & T4 -->|Atomic Updates| MC
    MR -->|Reads Aggregates| MC
```

#### Engineering Mechanisms
- **Thread-per-Topic Concurrency**: Separate daemon threads generate events independently using precise sleep intervals (`interval = 1.0 / rate - loop_time`) to eliminate thread contention and maintain stable throughput.
- **Producer Configuration & Delivery Guarantees**:
  - `acks=all`: High durability guarantee requiring all in-sync replicas to acknowledge writes.
  - `enable.idempotence=True`: Eliminates producer retries resulting in duplicate broker-side messages.
  - `compression.type=snappy`: CPU-efficient compression minimizing network I/O overhead.
  - `linger.ms=5` and `batch.size=16384`: Batches writes up to 5 milliseconds or 16 KB for maximum network efficiency.
  - `max.in.flight.requests.per.connection=5`: Maintains order consistency without pipeline stalls.
- **Backpressure & Fault Handling**:
  - Consecutive error counter pauses threads for 10 seconds if more than 5 consecutive broker failures occur.
  - Exponential backoff connection probe (`wait_for_kafka`) checks broker readiness up to 20 attempts before initiating traffic.
- **Delivery Callbacks**: Asynchronous `delivery_report` callback processes acks non-blockingly via `producer.poll(0)`.

#### Event Distribution Profile

| Stream | Target Rate | Global % | Partition Key | Primary Payload Attributes |
| :--- | :--- | :--- | :--- | :--- |
| **Orders** | 35 events/sec | 35% | `order_id` | `order_id`, `user_id`, `product_id`, `price`, `quantity`, `total_amount`, `country`, `device`, `browser`, `status` |
| **Payments** | 30 events/sec | 30% | `payment_id` | `payment_id`, `order_id`, `user_id`, `payment_type`, `amount`, `status`, `gateway` |
| **Clicks** | 25 events/sec | 25% | `click_id` | `click_id`, `user_id`, `product_id`, `session_id`, `page`, `action`, `duration_sec`, `device` |
| **Reviews** | 10 events/sec | 10% | `review_id` | `review_id`, `user_id`, `product_id`, `rating` (1-5), `sentiment`, `verified` |

---

### 2. Message Broker Topology (`docker/docker-compose.yml`, `config/kafka_config.py`)

Kafka decouples high-throughput event generation from downstream distributed computing.

```mermaid
flowchart TD
    subgraph KafkaCluster["Kafka Message Topology"]
        subgraph OrdersTopic["Topic: orders (3 Partitions)"]
            OP0["Partition 0"]
            OP1["Partition 1"]
            OP2["Partition 2"]
        end
        subgraph PaymentsTopic["Topic: payments (3 Partitions)"]
            PP0["Partition 0"]
            PP1["Partition 1"]
            PP2["Partition 2"]
        end
        subgraph ClicksTopic["Topic: clicks (3 Partitions)"]
            CP0["Partition 0"]
            CP1["Partition 1"]
            CP2["Partition 2"]
        end
        subgraph ReviewsTopic["Topic: reviews (3 Partitions)"]
            RP0["Partition 0"]
            RP1["Partition 1"]
            RP2["Partition 2"]
        end
        subgraph DLQTopic["Topic: dlq (1 Partition)"]
            DP0["Partition 0"]
        end
    end
```

#### Structural Specifications
- **Topic Configuration**:
  - `orders`, `payments`, `clicks`, `reviews`: 3 partitions, replication factor 1.
  - `dlq`: 1 partition for centralized dead-letter processing.
  - Partitioning strategy: Murmur2 hashing on UUID keys (`order_id`, `payment_id`, etc.) provides uniform load distribution across consumer tasks.
- **Cluster Safeguards**:
  - `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`: Prevents unintended topic creation with default single partitions.
  - `KAFKA_LOG_RETENTION_HOURS: 4`: Limits broker disk consumption to 4 hours of retention, optimal for local developer environments and ephemeral staging.
  - `kafka-init` container: Automated lifecycle script ensures topics exist with designated partition counts before ingestion starts.

---

### 3. Distributed Stream Processing Engine (`spark/`)

Processing is partitioned into two decoupled modules:
1. `transformations.py`: Stateless and stateful pure PySpark transformation functions (fully decoupled from I/O to enable headless local unit testing).
2. `spark_stream.py`: Runtime orchestration managing Kafka streaming sources, micro-batch triggers, checkpoint states, and database sinks.

```mermaid
flowchart TD
    subgraph StreamExecution["Spark Structured Streaming Pipeline Execution"]
        KSRC["Kafka Stream Reader\n(startingOffsets: latest, maxOffsetsPerTrigger: 10000)"]
        PARSE["parse_kafka_stream\n(Schema Validation & JSON Deserialization)"]
        BRANCH{"Data Quality Filter"}
        
        KSRC --> PARSE --> BRANCH
        
        BRANCH -->|Pass Validation| CLEAN["clean_* Functions\n(Deduplication & Type Casting)"]
        BRANCH -->|Fail Validation| DLQW["write_dlq_to_postgres\n(Dead Letter Queue Sink)"]
        
        subgraph MicroBatchQueries["Concurrent Micro-Batch Sinks"]
            CLEAN --> Q1["orders_q (5s trigger)\nAppend Mode -> raw orders"]
            CLEAN --> Q2["analytics_q (30s trigger)\nUpdate Mode -> analytics_per_minute"]
            CLEAN --> Q3["country_q (30s trigger)\nComplete Mode -> revenue_by_country"]
            CLEAN --> Q4["products_q (30s trigger)\nComplete Mode -> top_products"]
            CLEAN --> Q5["payments_q (5s trigger)\nAppend Mode -> raw payments"]
            CLEAN --> Q6["stats_q (30s trigger)\nComplete Mode -> payment_method_stats"]
            CLEAN --> Q7["clicks_q (5s trigger)\nAppend Mode -> raw clicks"]
            CLEAN --> Q8["reviews_q (5s trigger)\nAppend Mode -> raw reviews"]
        end
    end
```

#### Stream Windowing & Algorithmic Optimizations
- **Watermarking Semantics**:
  ```python
  orders_df.withWatermark("event_timestamp", "10 minutes")
  ```
  Enables bounded state memory by dropping events arriving more than 10 minutes past the current event-time horizon.
- **Tumbling Windows**:
  1-minute tumbling windows compute transaction totals, revenue velocity, and distinct users per minute.
- **Memory-Bounded Cardinality Estimation**:
  Uses `approx_count_distinct("user_id")` (HyperLogLog algorithm) instead of standard exact distinct counts. In streaming state stores, exact distinct counts require retaining all historical IDs in memory, leading to memory leaks and executor Out-Of-Memory (OOM) failures under sustained loads.
- **Backpressure Regulation**:
  `maxOffsetsPerTrigger: 10000` limits batch sizing during recovery or broker lag spikes, preventing executor starvation.
- **Stream-Stream Joins**:
  Includes `join_orders_payments()` which matches order events with their corresponding payment events within a 2-minute event-time watermark window using inner join semantics.

---

### 4. Storage & Persistence Architecture (`database/`)

PostgreSQL 15 acts as the centralized analytical store. It is partitioned into three logical tiers: Raw Ingestion, Aggregation Tables, and Query-Accelerating Views.

```mermaid
erDiagram
    orders ||--o{ payments : "order_id"
    orders ||--o{ clicks : "product_id"
    orders ||--o{ reviews : "product_id"

    orders {
        serial id PK
        varchar order_id UK
        varchar user_id
        varchar product_id
        varchar product_name
        varchar category
        decimal price
        int quantity
        decimal total_amount
        varchar country
        varchar device
        varchar browser
        varchar status
        timestamptz event_timestamp
        timestamptz ingested_at
    }

    payments {
        serial id PK
        varchar payment_id UK
        varchar order_id
        varchar user_id
        varchar payment_type
        decimal amount
        varchar status
        varchar gateway
        timestamptz event_timestamp
        timestamptz ingested_at
    }

    clicks {
        serial id PK
        varchar click_id UK
        varchar user_id
        varchar product_id
        varchar session_id
        varchar page
        varchar action
        int duration_sec
        varchar device
        timestamptz event_timestamp
        timestamptz ingested_at
    }

    reviews {
        serial id PK
        varchar review_id UK
        varchar user_id
        varchar product_id
        varchar product_name
        int rating
        varchar sentiment
        boolean verified
        timestamptz event_timestamp
        timestamptz ingested_at
    }

    analytics_per_minute {
        serial id PK
        timestamptz window_start UK
        timestamptz window_end
        int total_orders
        decimal total_revenue
        int unique_users
        varchar top_product
    }

    dead_letter_queue {
        serial id PK
        varchar topic
        text raw_message
        text error_reason
        timestamptz ingested_at
    }
```

#### Idempotency & Database Sink Strategy
Spark's standard JDBC writer does not natively support `ON CONFLICT` semantics. The codebase implements custom writers inside `foreachBatch`:
1. **Raw Event Sinks (Append Streams)**:
   - Evaluated on driver via `psycopg2.extras.execute_batch` with `ON CONFLICT ({conflict_col}) DO NOTHING`.
   - Guarantees that duplicate messages replayed from Kafka during network retries or Spark checkpoint recoveries do not result in duplicate records.
2. **Aggregated Time-Series Sink (`analytics_per_minute`)**:
   - Uses `ON CONFLICT (window_start) DO UPDATE SET total_orders = EXCLUDED.total_orders, total_revenue = EXCLUDED.total_revenue, unique_users = EXCLUDED.unique_users`.
   - Ensures late-arriving records within the watermark window update the corresponding minute interval accurately.
3. **Snapshot Aggregations (`revenue_by_country`, `top_products`, `payment_method_stats`)**:
   - Uses JDBC in `mode("overwrite")` to replace full batch state every 30 seconds.
4. **Dead Letter Queue (`dead_letter_queue`)**:
   - Quarantines records that fail schema validation (null IDs, invalid prices, out-of-bounds ratings). Includes original topic, JSON payload, and failure reason for debugging.

#### Indexing & View Strategy

```sql
-- Indexes for Sub-Second Grafana Query Latency
CREATE INDEX idx_orders_timestamp   ON orders   (event_timestamp DESC);
CREATE INDEX idx_orders_country     ON orders   (country);
CREATE INDEX idx_orders_category    ON orders   (category);
CREATE INDEX idx_payments_timestamp ON payments (event_timestamp DESC);
CREATE INDEX idx_analytics_window   ON analytics_per_minute (window_start DESC);
```

Precomputed Views (`v_realtime_summary`, `v_orders_per_minute`, `v_payment_split`, `v_top_10_products`, `v_revenue_by_country`) encapsulate rolling 1-hour windows so Grafana panels execute fast queries without calculating aggregations over millions of rows on every refresh.

---

### 5. Observability & Dashboard Subsystem (`dashboard/`)

Grafana is provisioned as code via container volume mounts, eliminating manual UI configuration during deployment:
- `datasources/postgres.yml`: Auto-connects PostgreSQL as the default data source with connection pooling (`maxOpenConns: 10`, `maxIdleConns: 5`).
- `dashboards/dashboard.yml`: Auto-discovers JSON dashboards on container startup.
- `dashboards/ecommerce.json`: Comprehensive 11-panel dashboard with a default 5-second auto-refresh rate.

```mermaid
flowchart TD
    subgraph DashboardPanels["Grafana Visual Panels (ecommerce.json)"]
        P1["Total Orders (Stat)"]
        P2["Total Revenue (Stat)"]
        P3["Active Users (Stat)"]
        P4["Average Order Value (Stat)"]
        P5["Orders & Revenue / Minute (Timeseries)"]
        P6["Payment Methods Breakdown (Pie Chart)"]
        P7["Payment Success Rate by Gateway (Bar Chart)"]
        P8["Top 10 Products by Revenue (Horizontal Bar)"]
        P9["Geographic Revenue Distribution (Table / Map)"]
        P10["User Interaction Funnel (Clicks to Orders)"]
        P11["Review Sentiment Analysis (Gauge / Donut)"]
    end
```

---

## End-to-End Data Flow & Interaction Lifecycles

### 1. Happy Path: Order Ingestion to Dashboard Metric

```mermaid
sequenceDiagram
    autonumber
    participant Gen as Fake Data Generator
    participant Prod as Producer Thread
    participant Kafka as Kafka Broker (orders)
    participant Spark as Spark Streaming Engine
    participant DB as PostgreSQL (orders & analytics)
    participant Grafana as Grafana Dashboard

    Gen->>Prod: generate_order_event()
    Prod->>Kafka: produce(topic='orders', key=order_id, snappy payload)
    Kafka-->>Prod: ACK (acks=all)
    Note over Kafka,Spark: Micro-batch trigger interval (5s / 30s)
    Spark->>Kafka: Poll micro-batch records
    Spark->>Spark: parse_kafka_stream() & clean_orders()
    Spark->>DB: execute_batch(INSERT INTO orders ON CONFLICT DO NOTHING)
    Spark->>DB: upsert_analytics(INSERT ... ON CONFLICT DO UPDATE)
    Grafana->>DB: Query v_realtime_summary & analytics_per_minute (every 5s)
    DB-->>Grafana: Return aggregate metrics
    Grafana-->>Grafana: Render updated charts & KPIs
```

### 2. Anomaly & Malformed Record Quarantine Flow

```mermaid
sequenceDiagram
    autonumber
    participant Gen as Malformed Event Producer
    participant Kafka as Kafka Broker
    participant Spark as Spark Engine
    participant DLQ as PostgreSQL (dead_letter_queue)
    participant Engineer as Data Platform Engineer

    Gen->>Kafka: Send invalid event (e.g. price <= 0 or missing user_id)
    Spark->>Kafka: Ingest micro-batch
    Spark->>Spark: clean_orders() splits: valid_df, invalid_df
    Spark->>DLQ: write_dlq_to_postgres(topic, raw_message, error_reason)
    Note over Spark: Stream processing continues without crashing
    Engineer->>DLQ: Inspect quarantined records & error_reasons
```

---

## Reliability, Fault Tolerance & Recovery Architecture

```mermaid
flowchart TD
    subgraph FaultScenarios["Failure Scenarios & Self-Healing Mechanisms"]
        F1["Broker Down / Unreachable"]
        F2["Spark Worker Crash / OOM"]
        F3["Postgres Connection Flap"]
        F4["Duplicate Kafka Delivery"]
    end

    subgraph DefenseMechanisms["Built-in Architectural Mitigations"]
        D1["Producer Exponential Backoff Retry (wait_for_kafka)\nThread sleep backoff up to 30s"]
        D2["Spark Checkpointing (/tmp/spark_checkpoints)\nOffset replay from exact commit point"]
        D3["psycopg2 Exponential Backoff (get_connection)\nRetries=5, Delay=2s, Backoff=2.0x"]
        D4["Database Level Idempotency\nON CONFLICT (order_id) DO NOTHING"]
    end

    F1 --> D1
    F2 --> D2
    F3 --> D3
    F4 --> D4
```

1. **At-Least-Once Delivery to Exactly-Once Processing**:
   - Kafka guarantees at-least-once message delivery.
   - Spark Structured Streaming manages consumer group offsets within transactional checkpoint logs (`/tmp/spark_checkpoints/*`).
   - Idempotent PostgreSQL inserts (`ON CONFLICT DO NOTHING`) ensure duplicate reads resulting from job retries are discarded at zero data corruption risk.
2. **Graceful Shutdown**:
   - Producer traps `SIGINT` and `SIGTERM` signals, triggers an internal `stop_event`, stops worker threads, and calls `producer.flush(timeout=10)` to deliver in-flight buffers.
   - Spark executes with `spark.streaming.stopGracefullyOnShutdown=true`, ensuring existing micro-batches finish writing before container termination.

---

## Deployment Architectures: Local vs Remote

The repository supports dual deployment strategies:

```mermaid
flowchart LR
    subgraph LocalEnv["Local Development (Docker Compose)"]
        DC["docker compose up -d --build"]
        DC -->|Spins up 8 containers| S8["zookeeper, kafka, kafka-init, kafka-ui,\npostgres, grafana, spark-master, spark-worker"]
        LOC_SPARK["Local spark-submit execution"]
    end

    subgraph RemoteEnv["Cloud / Remote VM (deploy_remote.sh)"]
        REM_SH["deploy_remote.sh execution"]
        REM_SH -->|Step 1| K_TOPICS["Provisions Kafka Topics in container"]
        REM_SH -->|Step 2| PIP["Installs psycopg2 on Spark containers"]
        REM_SH -->|Step 3-4| SUBMIT["Deploys nohup spark-submit into spark-master"]
        REM_SH -->|Step 5| RESTART["Restarts Producer container"]
    end
```

### Local vs Production Resource Footprint

| Service | Container Image | Port | Local Profile | Production Scaled Profile |
| :--- | :--- | :--- | :--- | :--- |
| **ZooKeeper** | `confluentinc/cp-zookeeper:7.5.0` | `2181` | 1 Node (512MB) | 3-Node Quorum or KRaft (ZooKeeper-less) |
| **Kafka** | `confluentinc/cp-kafka:7.5.0` | `9092, 29092` | 1 Broker (1GB) | 3+ Broker Cluster with RF=3, Min-ISR=2 |
| **Kafka-UI** | `provectuslabs/kafka-ui:latest` | `8080` | Single Instance | Protected behind OAuth/Ingress |
| **PostgreSQL**| `postgres:15-alpine` | `5432` | 1 Instance (1GB) | Managed RDS/Cloud SQL or TimescaleDB |
| **Spark Master**| `bitnamilegacy/spark:3.5.1` | `7077, 8081` | 1 Master (1GB) | Spark on Kubernetes / Dataproc / EMR |
| **Spark Worker**| `bitnamilegacy/spark:3.5.1` | `8082` | 1 Worker (4GB, 2 cores)| Multi-worker autoscaling pool |
| **Producer** | Custom Python 3.11 Image | N/A | 1 Container (~200MB) | Kubernetes CronJob / Event Bus Ingress |
| **Grafana** | `grafana/grafana:10.2.0` | `3000` | 1 Instance (~300MB) | Enterprise Grafana Cloud / HA Cluster |

---

## Architectural Review, Trade-Offs & Scaling Roadmap

### Identified Architectural Strengths
- **Clean Decoupling**: Business logic in `transformations.py` has no network dependencies and is 100% covered by pytest suite (`tests/test_transformations.py`).
- **Resilient Micro-Batch Ingestion**: Custom idempotent batch writing via psycopg2 resolves the lack of native `ON CONFLICT` support in PySpark JDBC.
- **Controlled Ingestion Backpressure**: Strict bounds on offsets per trigger (`maxOffsetsPerTrigger=10000`) protect against cluster memory exhaustion.

### Recommended Architectural Evolutions

```mermaid
flowchart TD
    subgraph Current["Current Architecture"]
        C1["ZooKeeper + Kafka 7.5"]
        C2["Local /tmp Spark Checkpoints"]
        C3["PostgreSQL Standard RDBMS"]
        C4["Single Spark Worker"]
    end

    subgraph Recommended["Production Scaling Path"]
        R1["Kafka KRaft Mode (ZooKeeper Removal)"]
        R2["Cloud Object Storage Checkpoints (S3 / GCS / MinIO)"]
        R3["Time-Series OLAP (TimescaleDB / ClickHouse / BigQuery)"]
        R4["Spark on Kubernetes (K8s Dynamic Allocation)"]
    end

    C1 -.->|Phase 1: Modernize| R1
    C2 -.->|Phase 2: High Availability| R2
    C3 -.->|Phase 3: Scale Analytics| R3
    C4 -.->|Phase 4: Elasticity| R4
```

1. **Migrate to Kafka KRaft Mode**:
   - *Current*: Dependent on ZooKeeper (`zookeeper:2181`), introducing an additional operational failure domain.
   - *Recommendation*: Migrate to Confluent Kafka KRaft mode to simplify cluster architecture and reduce memory usage by ~25%.
2. **Persistent Remote Checkpoint Storage**:
   - *Current*: Spark checkpoints are stored in `/tmp/spark_checkpoints`, which is ephemeral and wiped upon container rebuild.
   - *Recommendation*: Map checkpoint directory to an external Docker persistent volume or S3/GCS bucket to ensure durability across node redeployments.
3. **Database Scalability for Large Historical Windows**:
   - *Current*: High write volume into a single PostgreSQL container (~100 inserts/sec).
   - *Recommendation*: As row counts exceed 50+ million records, apply automated table partitioning by `event_timestamp` or migrate analytical sinks to **ClickHouse** or **TimescaleDB** for sub-second aggregations over billions of events.
4. **Driver Node Batching vs Executor Direct Writes**:
   - *Current*: In `write_to_postgres`, `batch_df.collect()` brings data into driver memory to execute psycopg2 batch writes.
   - *Recommendation*: While performant for ~500-1,000 rows/micro-batch, at 10,000+ rows/sec this should be refactored using `rdd.foreachPartition` to allow distributed executors to write concurrently into PostgreSQL using a connection pool.
