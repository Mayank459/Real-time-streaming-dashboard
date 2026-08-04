# Real-Time E-Commerce Streaming Analytics Platform 🚀

[![Live Demo](https://img.shields.io/badge/Live_Demo-Grafana_Dashboard-green?style=for-the-badge&logo=grafana)](http://13.60.250.242:3000/d/ecommerce-streaming-v1/e-commerce-real-time-streaming-dashboard?orgId=1&refresh=5s)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-red?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?style=for-the-badge&logo=apachespark)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.0-4169E1?style=for-the-badge&logo=postgresql)](https://www.postgresql.org/)
[![Grafana](https://img.shields.io/badge/Grafana-10.2.0-F46800?style=for-the-badge&logo=grafana)](https://grafana.com/)
[![Docker](https://img.shields.io/badge/Docker_Compose-3.8-2496ED?style=for-the-badge&logo=docker)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python)](https://www.python.org/)

> 🌟 **[Click Here to Access Live Interactive Dashboard](http://13.60.250.242:3000/d/ecommerce-streaming-v1/e-commerce-real-time-streaming-dashboard?orgId=1&refresh=5s)** *(Username: `admin` | Password: `admin123`)*

A production-grade, end-to-end real-time streaming pipeline simulating a high-throughput e-commerce platform. The system ingests **~100 events/second** across multiple streams (Orders, Payments, Clicks, Reviews), performs distributed real-time processing and aggregation using **Spark Structured Streaming**, guarantees **idempotent storage** in **PostgreSQL**, and visualizes live operational metrics via an auto-provisioned **Grafana Dashboard**.

---

## 📌 Executive Summary

Modern e-commerce enterprises require instant visibility into operational metrics—such as order volume, revenue per minute, payment success/failure rates, and regional traffic. Traditional batch architectures introduce latencies of hours or days, delaying critical decision-making. 

This platform demonstrates an enterprise-grade **Event-Driven Architecture (EDA)** built to address latency, idempotency, fault tolerance, and high throughput without data loss.

---

## 🏗️ End-to-End Pipeline Architecture

```text
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                        FAKER DATA GENERATOR                              │
 │   Generates ~100 events/sec across Orders, Payments, Clicks & Reviews    │
 └────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                   APACHE KAFKA BROKER (ZooKeeper Mode)                   │
 │   Topics: [ orders ]  [ payments ]  [ clicks ]  [ reviews ]  [ dlq ]     │
 └────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                     SPARK STRUCTURED STREAMING ENGINE                    │
 │   • Schema Validation & Cleaning   • 10-Min Event-Time Watermarking      │
 │   • Deduplication & Filtering      • Tumbling Window Aggregations        │
 │   • Stream-Stream Joins            • HyperLogLog Approx Count Distinct   │
 └──────────────────┬────────────────────────────────────┬──────────────────┘
                    │                                    │
           (Valid Transformed Data)            (Malformed/Bad Records)
                    │                                    │
                    ▼                                    ▼
 ┌───────────────────────────────────────┐   ┌────────────────────────────────┐
 │         POSTGRESQL DATABASE           │   │       DEAD LETTER QUEUE        │
 │ • Raw Tables (Idempotent ON CONFLICT) │   │ • Quarantine bad payload & err │
 │ • Real-Time Aggregation Views & Tables│   └────────────────────────────────┘
 └──────────────────┬────────────────────┘
                    │
                    ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                         GRAFANA DASHBOARD                                │
 │   • 11 Live Panels  • Auto-refresh 5s  • Pre-provisioned Datasources     │
 └──────────────────────────────────────────────────────────────────────────┘
```

---

## ⭐ Technical Highlights & Engineering Decisions

### 1. High-Throughput Event Generation (Python + Faker)
- Multi-threaded Python producer outputting **100 events/sec** distributed across 4 distinct Kafka topics (`orders`: 35%, `payments`: 30%, `clicks`: 25%, `reviews`: 10%).
- Implements non-blocking delivery callbacks, snappy compression, and retry logic with exponential backoff.

### 2. Stream Processing & Watermarking (PySpark)
- **Watermarking (10 mins)**: Handles late-arriving out-of-order events gracefully in streaming aggregations.
- **Tumbling Windows (1 min)**: Computes real-time sales velocity, revenue per minute, and transaction counts.
- **HyperLogLog (`approx_count_distinct`)**: Replaced standard `COUNT(DISTINCT)` to maintain bounded memory overhead in streaming state stores.

### 3. Idempotent Data Ingestion (`ON CONFLICT DO NOTHING`)
- Prevents duplicate record ingestion upon Spark micro-batch retries or job restarts.
- Custom psycopg2 batch sink guarantees **at-least-once delivery with end-to-end exactly-once processing semantics** at the database layer.

### 4. Dead Letter Queue (DLQ) Strategy
- Schema violations and corrupted messages are quarantined into a dedicated `dead_letter_queue` table alongside error reasons, isolating processing failures without crashing streaming queries.

---

## 🛠️ Technology Stack

| Layer | Tool / Technology | Version | Function |
| :--- | :--- | :--- | :--- |
| **Data Generation** | Python + Faker | 3.11 / 22.0 | Synthetic e-commerce user interaction streaming |
| **Message Broker** | Apache Kafka + ZooKeeper | 7.5.0 (Confluent) | Distributed event log and message queueing |
| **Stream Engine** | Apache Spark Structured Streaming | 3.5.1 | Real-time cleaning, transformations, and windowing |
| **Database** | PostgreSQL | 15-alpine | Primary relational store for raw events and aggregate KPIs |
| **Visualization** | Grafana | 10.2.0 | Live monitoring and executive dashboards |
| **Orchestration** | Docker & Docker Compose | 3.8 | Container management, network bridging, and health checks |

---

## 📁 Repository Structure

```text
Real-time-streaming-dashboard/
├── producer/
│   ├── fake_data.py          # Synthetic event generation engine (Orders, Payments, Clicks, Reviews)
│   ├── producer.py           # Multi-threaded Kafka event producer with metrics & callbacks
│   └── Dockerfile            # Container definition for data producer
├── spark/
│   ├── spark_stream.py       # Main Structured Streaming job (Micro-batch & JDBC ingestion)
│   └── transformations.py    # Pure, testable PySpark transformation functions
├── database/
│   ├── init.sql              # Schema creation, index tuning, and view definitions
│   └── insert.py             # Batch insertion utilities with retry logic
├── dashboard/
│   └── grafana/provisioning/ # Auto-provisioned Grafana datasources and pre-built dashboard JSON
├── docker/
│   └── docker-compose.yml    # Orchestration of all 8 core services
├── config/
│   ├── kafka_config.py       # Broker addresses, topic settings, and distribution rates
│   └── db_config.py          # Database parameters, connection pool, and JDBC settings
├── tests/
│   ├── test_fake_data.py     # Unit tests for event generators and distribution logic
│   └── test_transformations.py # PySpark transformations testing (Local mode)
├── .env                      # Environment variable specifications
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
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

### Step 2: Spin Up Infrastructure
```bash
cd docker
docker compose up -d --build
```
*This starts ZooKeeper, Kafka, Kafka-UI, PostgreSQL, Grafana, Spark Master, Spark Worker, and the Producer container.*

### Step 3: Verify Running Services
```bash
docker ps
```
Ensure all 8 containers are in an `Up` / `Healthy` state.

### Step 4: Submit Spark Streaming Job
```bash
docker exec -d spark-master bash -c "PYTHONPATH=/opt/bitnami/spark/work /tmp/run_spark.sh"
```

---

## 📊 Live Monitoring Interfaces

| Service | Access URL | Credentials | Description |
| :--- | :--- | :--- | :--- |
| 📊 **Live Grafana Dashboard** | **[http://13.60.250.242:3000/...](http://13.60.250.242:3000/d/ecommerce-streaming-v1/e-commerce-real-time-streaming-dashboard?orgId=1&refresh=5s)** | `admin` / `admin123` | Executive KPI dashboard refreshing live every 5s |
| 🔍 **Kafka UI (AWS Cloud)** | **[http://13.60.250.242:8080](http://13.60.250.242:8080)** | None | Visual topic inspection, consumer groups & message view |
| ⚡ **Spark Master UI (AWS Cloud)** | **[http://13.60.250.242:8081](http://13.60.250.242:8081)** | None | Spark cluster state, active streaming queries & workers |
| 🏠 *Local Grafana* | `http://localhost:3000` | `admin` / `admin123` | Local development dashboard instance |

---

## 🧪 Unit Testing

Run unit tests locally using `pytest`:

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## 📝 Key Interview & Resume Highlights

- **Stream Architecture**: Engineered an event-driven data pipeline handling **100+ events/sec** using Apache Kafka and Spark Structured Streaming.
- **Fault Tolerance**: Implemented **10-minute watermarking** for out-of-order records and a **Dead Letter Queue (DLQ)** pattern for schema violations.
- **Data Integrity**: Designed an **idempotent PostgreSQL sink** using `ON CONFLICT DO NOTHING` to guarantee consistency across micro-batch retries.
- **Resource Optimization**: Utilized **HyperLogLog algorithms (`approx_count_distinct`)** to prevent unbounded memory growth during streaming state maintenance.
- **DevOps & Cloud Deployment**: Deployed fully containerized architecture on **AWS EC2** with Docker Compose, automated health checks, and public KPI monitoring.

---

## 📜 License

Distributed under the MIT License. See `LICENSE` for more information.
