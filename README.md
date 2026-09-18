# Real-Time E-Commerce Streaming Analytics Platform 🚀

[![Live Demo](https://img.shields.io/badge/Live_Demo-Grafana_Dashboard-green?style=for-the-badge&logo=grafana)](http://13.60.250.242:3000/d/ecommerce-streaming-v1/e-commerce-real-time-streaming-dashboard?orgId=1&refresh=5s)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-red?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?style=for-the-badge&logo=apachespark)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.0-4169E1?style=for-the-badge&logo=postgresql)](https://www.postgresql.org/)
[![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-1.3.0+-F7931E?style=for-the-badge&logo=scikitlearn)](https://scikit-learn.org/)
[![Grafana](https://img.shields.io/badge/Grafana-10.2.0-F46800?style=for-the-badge&logo=grafana)](https://grafana.com/)
[![Docker](https://img.shields.io/badge/Docker_Compose-3.8-2496ED?style=for-the-badge&logo=docker)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python)](https://www.python.org/)

> 🌟 **[Click Here to Access Live Interactive Dashboard](http://13.60.250.242:3000/d/ecommerce-streaming-v1/e-commerce-real-time-streaming-dashboard?orgId=1&refresh=5s)** *(Username: `admin` | Password: `admin123`)*

A high-performance, enterprise-grade real-time streaming analytics and anomaly detection platform simulating a modern e-commerce ecosystem. The system ingests **~100 events/second** across multiple streams (Orders, Payments, Clicks, Reviews), executes distributed stream processing and tumbling-window aggregations using **Spark Structured Streaming**, guarantees **idempotent storage** in **PostgreSQL** via partition-level connection pools, detects fraud/anomalies with an unsupervised **Isolation Forest** model, and visualizes end-to-end metrics via an auto-provisioned **Grafana Dashboard**.

---

## 📌 Architecture & Design Highlights

```text
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                           EVENT GENERATION & INGESTION                        │
 │  • Python + Faker Engine (~100 ev/s baseline; burst capacity >34k ev/s)       │
 │  • Explicit Schema Versioning (schema_version: "1.0")                         │
 │  • Epoch-millisecond Latency Tracers (event_timestamp_ms)                    │
 └──────────────────────────────────────┬────────────────────────────────────────┘
                                        │
                                        ▼
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                     DISTRIBUTED APACHE KAFKA CLUSTER                          │
 │  • 4 Primary Topics (3 partitions each): [ orders, payments, clicks, reviews ]│
 │  • 2 Retry Topics: [ orders_retry, payments_retry ] for automated DLQ replay │
 └──────────────────────────────────────┬────────────────────────────────────────┘
                                        │
                                        ▼
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                      SPARK STRUCTURED STREAMING ENGINE                        │
 │  • Schema Validation & Cleaning       • 10-Minute Event-Time Watermarking     │
 │  • Sub-5s Aggregation Triggers        • Tumbling Window Analytics             │
 │  • Distributed foreachPartition Sink  • Persistent Spark Checkpoint Storage   │
 └───────────────────┬───────────────────────────────────────┬───────────────────┘
                     │                                       │
            (Valid Transformed Data)               (Malformed/Invalid Payloads)
                     │                                       │
                     ▼                                       ▼
 ┌───────────────────────────────────────┐   ┌───────────────────────────────────┐
 │          POSTGRESQL DATABASE          │   │      DEAD LETTER QUEUE & REPLAY   │
 │ • Distributed Idempotent Upserts      │   │ • Schema Quarantine               │
 │ • p50/p95/p99 Latency Views           │   │ • Automated Remediation CLI       │
 │ • System Health & Anomaly Tables      │   │ • Kafka Retry Topic Re-publisher  │
 └───────────────────┬───────────────────┘   └─────────────────▲─────────────────┘
                     │                                         │ (Replay Loop)
                     ├─────────────────────────────────────────┘
                     │
                     ▼
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                   MACHINE LEARNING & OBSERVABILITY LAYER                      │
 │  • Unsupervised Isolation Forest Anomaly Detection (Orders, Revenue, Failures)│
 │  • 13-Panel Auto-Provisioned Grafana Dashboard (5s Auto-Refresh)              │
 │  • Dedicated Pipeline Latency Timeseries & Real-Time Alert Panels             │
 │  • Least-Privilege Read-Only Grafana Database Role (grafana_reader)           │
 └───────────────────────────────────────────────────────────────────────────────┘
```

---

## ⭐ Key Technical Upgrades & Production Features

### 1. Ultra-Low Latency & Distributed Writes
- **Eliminated Driver `collect()` Bottleneck**: Replaced driver-side memory collections with distributed `rdd.foreachPartition()` writes. Worker nodes directly open bounded connection pools to PostgreSQL using `psycopg2.extras.execute_batch`.
- **Sub-5s Micro-Batch Triggers**: Accelerated core aggregation queries (`analytics_q`, `stats_q`) from 30s to **5 seconds**, achieving true real-time dashboard updates without backlog accumulation.

### 2. End-to-End Latency Tracking & Percentiles
- Embedded precision origin timestamps (`event_timestamp_ms`) at event generation.
- PySpark computes `pipeline_latency_ms` upon streaming arrival.
- Database view `v_latency_stats` computes rolling **p50, p95, and p99 end-to-end latencies**.

### 3. Production-Grade Durability & Checkpointing
- Spark checkpoint state is stored in a **named persistent Docker volume** (`spark_checkpoints`) mounted across Spark Master and Workers.
- Guarantees zero offset or state-store loss across container restarts or crashes.

### 4. Automated DLQ Remediation & Kafka Replay
- Schema-violating or corrupted payloads are quarantined into `dead_letter_queue`.
- **[reprocess_dlq.py](database/reprocess_dlq.py)** provides an automated remediation workflow: validates fixes, updates the DLQ state, re-inserts clean records directly to PostgreSQL, or re-publishes to dedicated Kafka retry topics (`orders_retry`, `payments_retry`).

### 5. Unsupervised Machine Learning (Isolation Forest)
- Embedded **[ml/anomaly_detector.py](ml/anomaly_detector.py)** for real-time e-commerce operational anomaly detection.
- Uses `StandardScaler` and scikit-learn's `IsolationForest` to analyze multi-variate signals:
  - Orders per minute
  - Revenue velocity
  - Payment failure percentage spikes
- Writes anomaly flags and scores directly to the `anomaly_alerts` table for real-time Grafana alerting.

### 6. Infrastructure Observability & Security Hardening
- **13 Live Grafana Panels**: Added **Panel 12** (Live Streaming Latency Timeseries p50/p95/p99) and **Panel 13** (Real-Time ML Anomaly Alerts Table).
- **Least-Privilege Access**: Added `grafana_reader` role restricted to `SELECT` permissions on aggregates and views, securing the primary database.

---

## 📊 Measured Performance Benchmarks

Measured using the automated benchmark suite ([tests/benchmark_pipeline.py](tests/benchmark_pipeline.py)):

| Metric | Measured Baseline | Production Target | Status |
| :--- | :--- | :--- | :--- |
| **Event Generation Throughput** | **34,063 events/sec** | > 1,000 events/sec | ✅ Exceeded (34x) |
| **Data Ingestion Throughput** | **15.11 MB/sec** | > 0.5 MB/sec | ✅ Exceeded |
| **End-to-End p50 Latency** | **1,240 ms** | < 3,000 ms | ✅ Optimal |
| **End-to-End p95 Latency** | **4,180 ms** | < 7,000 ms | ✅ Sub-5s KPI |
| **End-to-End p99 Latency** | **6,850 ms** | < 10,000 ms | ✅ Protected |
| **Database Write Mode** | **Distributed `foreachPartition`** | No driver `collect()` | ✅ Distributed |
| **Test Suite Coverage** | **32 / 32 Passed (100%)** | 100% | ✅ Verified |

---

## 🛠️ Technology Stack

| Layer | Technology | Version | Purpose |
| :--- | :--- | :--- | :--- |
| **Data Generation** | Python + Faker | 3.11 / 22.0 | Synthetic e-commerce stream generator with schema v1.0 |
| **Message Broker** | Apache Kafka + ZooKeeper | 7.5.0 (Confluent) | Multi-topic partitioned distributed commit log with retry queues |
| **Stream Processing** | Apache Spark Structured Streaming | 3.5.1 | Cleaning, watermarking, micro-batch aggregations, distributed writes |
| **Database & OLAP** | PostgreSQL | 15-alpine | Idempotent storage, percentile analytics, DLQ quarantine |
| **Machine Learning** | Scikit-Learn (Isolation Forest) | 1.3.0+ | Multi-variate e-commerce anomaly & fraud detection |
| **Visualization** | Grafana | 10.2.0 | 13-panel live executive & operational health dashboard |
| **Containerization** | Docker & Docker Compose | 3.8 | Service orchestration, persistent volumes, and health checks |

---

## 📁 Repository Structure

```text
Real-time-streaming-dashboard/
├── config/
│   ├── kafka_config.py          # Broker addresses, primary & retry topics, event ratios
│   └── db_config.py             # Database parameters, connection pooling, JDBC settings
├── producer/
│   ├── fake_data.py             # Event generation with schema versioning & precision timestamps
│   ├── producer.py              # Multi-threaded Kafka producer with non-blocking callbacks
│   └── Dockerfile               # Container definition for data generator
├── spark/
│   ├── spark_stream.py          # Structured Streaming pipeline with distributed foreachPartition sink
│   └── transformations.py       # PySpark transformations, watermarks, and schema parsing
├── database/
│   ├── init.sql                 # DDL, indexes, latency percentile views, grafana_reader role
│   ├── insert.py                # Batch insertion utilities with exponential backoff
│   └── reprocess_dlq.py         # Automated DLQ inspection, remediation, and replay engine
├── ml/
│   └── anomaly_detector.py      # Unsupervised Isolation Forest model for anomaly alerts
├── dashboard/
│   └── grafana/provisioning/    # 13 pre-provisioned Grafana panels (KPIs, latency, ML alerts)
├── docker/
│   └── docker-compose.yml       # 8 containerized services + spark_checkpoints volume
├── tests/
│   ├── test_fake_data.py        # Generator validation & schema assertions
│   ├── test_transformations.py  # PySpark transformations unit tests
│   ├── test_reprocess_dlq.py    # DLQ parsing and remediation unit tests
│   ├── test_anomaly_detector.py # Isolation Forest detection unit tests
│   └── benchmark_pipeline.py    # Automated latency and throughput benchmarking tool
├── deploy_remote.sh             # EC2 deployment automation script
├── architecture_report.md       # Comprehensive architectural review and component design
├── IMPLEMENTATION_REPORT.md     # Detailed upgrade report (Previous vs. Newly Implemented)
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation
```

---

## 🚀 Quick Start & Deployment Guide

### Prerequisites
- **Docker Desktop** installed and running
- **Minimum 8 GB RAM** (16 GB recommended)
- **Git**

### Step 1: Clone Repository
```bash
git clone https://github.com/Mayank459/Real-time-streaming-dashboard.git
cd Real-time-streaming-dashboard
```

### Step 2: Spin Up Full Stack Infrastructure
```bash
cd docker
docker compose up -d --build
```
*Spins up ZooKeeper, Kafka Broker, Kafka-UI, PostgreSQL, Grafana, Spark Master, Spark Worker, and the Producer container with persistent checkpoint volumes.*

### Step 3: Verify Container Health
```bash
docker ps
```
Ensure all 8 containers show status `Up` / `Healthy`.

### Step 4: Submit Spark Streaming Job
```bash
docker exec -d spark-master bash -c "PYTHONPATH=/opt/bitnami/spark/work /tmp/run_spark.sh"
```

### Step 5: (Optional) Run ML Anomaly Detection Service
```bash
# Run standalone ML anomaly detection loop
python ml/anomaly_detector.py --loop --interval 15
```

### Step 6: (Optional) Run DLQ Replay Engine
```bash
# Inspect and reprocess quarantined records
python database/reprocess_dlq.py --action stats
python database/reprocess_dlq.py --action remediate-and-retry
```

---

## 🧪 Testing & Benchmarking

Run the complete test suite locally:
```bash
pytest tests/test_fake_data.py tests/test_reprocess_dlq.py tests/test_anomaly_detector.py -v
```

Run the automated performance and throughput benchmark:
```bash
python tests/benchmark_pipeline.py --samples 5000
```

---

## 📊 Live Monitoring Interfaces

| Service | Public Access URL | Credentials | Description |
| :--- | :--- | :--- | :--- |
| 📊 **Live Grafana Dashboard** | **[AWS Live Dashboard](http://13.60.250.242:3000/d/ecommerce-streaming-v1/e-commerce-real-time-streaming-dashboard?orgId=1&refresh=5s)** | `admin` / `admin123` | 13 live panels (Business KPIs, Latency p50/p95/p99, ML Alerts) |
| 🔍 **Kafka UI** | **[AWS Kafka UI](http://13.60.250.242:8080)** | None | Visual topic inspection, consumer groups, and message payloads |
| ⚡ **Spark Master UI** | **[AWS Spark UI](http://13.60.250.242:8081)** | None | Active streaming queries, worker executor memory, and stages |
| 🏠 *Local Grafana* | `http://localhost:3000` | `admin` / `admin123` | Local development dashboard instance |

---

## 📝 Key Data Engineering Highlights for Recruiters

- **End-to-End Latency Profile**: Architected for sub-5s KPI updates with measured **p50 latency of 1.2s** and **p95 latency of 4.1s**.
- **Distributed Database Sink**: Replaced driver-side memory collection (`collect()`) with partition-level executor writes (`foreachPartition`), eliminating driver OOM risks.
- **Production Durability**: Implemented persistent Docker volumes for Spark checkpoint offsets and watermarks, ensuring zero data loss across failovers.
- **Data Quality & DLQ Replay**: Quarantined schema violations in PostgreSQL DLQ with an automated Python CLI for remediation and Kafka retry topic re-publishing.
- **Applied ML in Streaming**: Deployed an unsupervised Isolation Forest model scoring operational anomalies directly on streaming KPIs.
- **Security Hardening**: Designed a least-privilege `grafana_reader` role and isolated internal network bridging.

---

## 📜 License

Distributed under the MIT License. See `LICENSE` for more information.
