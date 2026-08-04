# Real-Time E-Commerce Streaming Analytics Platform

> A production-grade real-time data pipeline that simulates an e-commerce platform where users continuously generate events (orders, payments, clicks, reviews). The pipeline processes millions of events using Apache Kafka and Spark Structured Streaming, stores analytics in PostgreSQL, and visualizes live KPIs on Grafana.

![Architecture](screenshots/architecture.png)

---

## ⭐ Highlights

- **100 events/second** across 4 Kafka topics
- **Spark Structured Streaming** with watermarks, tumbling windows, and stream-stream joins
- **Dead Letter Queue** for bad/invalid events
- **Auto-provisioned Grafana dashboard** with 11 live panels
- **Fully containerized** — one command to start everything
- **ZooKeeper mode** Kafka (production-realistic)

---

## 🏗️ Architecture

```
Users (Python Faker)
       │
  Python Producers (4 threads, 100 events/sec)
       │
┌──────▼──────────────────────────────────────┐
│           Apache Kafka (ZooKeeper Mode)      │
│  orders │ payments │ clicks │ reviews │ dlq  │
└──────┬──────────────────────────────────────┘
       │
  Spark Structured Streaming
  ├─ Clean & Validate
  ├─ Deduplicate
  ├─ Tumbling Window (1 min)
  ├─ Revenue by Country
  ├─ Top Products
  └─ Payment Stats
       │
  PostgreSQL Database
  ├─ orders, payments, clicks, reviews (raw)
  ├─ analytics_per_minute (aggregated)
  ├─ revenue_by_country, top_products
  └─ dead_letter_queue (invalid events)
       │
  Grafana Dashboard (auto-refresh 5s)
  ├─ Revenue / Orders / Users stats
  ├─ Revenue trend (line chart)
  ├─ Orders/min (bar chart)
  ├─ Top 10 Products
  ├─ Payment method split (donut)
  ├─ Revenue by Country (table)
  └─ DLQ error monitor
```

---

## 🛠️ Tech Stack

| Component        | Technology               | Version |
|-----------------|--------------------------|---------|
| Message Broker   | Apache Kafka (ZooKeeper) | 7.5.0   |
| Stream Processing| Apache Spark Structured  | 3.5.0   |
| Database         | PostgreSQL               | 15      |
| Dashboards       | Grafana                  | 10.2.0  |
| Data Generation  | Python + Faker           | 3.11    |
| Containerization | Docker + Compose         | latest  |

---

## 📁 Folder Structure

```
blend360/
├── producer/
│   ├── fake_data.py        # Generates realistic e-commerce events
│   ├── producer.py         # Multi-threaded Kafka producer
│   └── Dockerfile
├── spark/
│   ├── spark_stream.py     # Main Spark Streaming job
│   └── transformations.py  # Pure transformation functions
├── database/
│   ├── init.sql            # PostgreSQL schema (auto-runs on startup)
│   └── insert.py           # Batch insert helpers
├── dashboard/
│   └── grafana/provisioning/
│       ├── datasources/postgres.yml
│       └── dashboards/ecommerce.json
├── docker/
│   └── docker-compose.yml  # All 8 services
├── config/
│   ├── kafka_config.py
│   └── db_config.py
├── tests/
│   ├── test_fake_data.py
│   └── test_transformations.py
├── logs/
├── .env
└── requirements.txt
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (running)
- 16 GB RAM recommended
- Python 3.11+ (for local development)

### Step 1 — Start all containers

```bash
cd d:\blend360\docker
docker compose up -d
```

Wait ~60 seconds for all services to be healthy.

### Step 2 — Verify containers

```bash
docker ps
```

You should see 8 containers running:
- `zookeeper`
- `kafka`
- `kafka-init` (exits after creating topics)
- `kafka-ui`
- `postgres`
- `grafana`
- `spark-master`
- `spark-worker`
- `producer`

### Step 3 — Open Kafka UI

Visit [http://localhost:8080](http://localhost:8080)

You should see 4 topics: `orders`, `payments`, `clicks`, `reviews`

### Step 4 — Submit the Spark Job

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  /opt/bitnami/spark/work/spark_stream.py
```

### Step 5 — Open Grafana Dashboard

Visit [http://localhost:3000](http://localhost:3000)

Login: `admin` / `admin123`

Navigate to **Dashboards → E-Commerce Real-Time Streaming Dashboard**

---

## 🧪 Run Tests

Install dependencies locally:

```bash
pip install -r requirements.txt
```

Run all tests:

```bash
pytest tests/ -v
```

---

## 📊 Grafana Dashboard Panels

| Panel | Type | Description |
|-------|------|-------------|
| Total Revenue | Stat | Running revenue last hour |
| Total Orders | Stat | Order count last hour |
| Unique Users | Stat | Distinct users last hour |
| Events/min | Stat | Real-time event rate |
| Revenue Trend | Time Series | Per-minute revenue line |
| Orders/Minute | Bar Chart | Order volume bars |
| Top 10 Products | Bar Chart | Horizontal revenue bars |
| Payment Methods | Donut Chart | UPI/Card/COD split |
| Revenue by Country | Table | Country leaderboard |
| Payment Success/Fail | Time Series | Success vs failure trend |
| Dead Letter Queue | Table | Invalid events monitor |

---

## 🔧 Configuration

All configuration is in `.env`:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
EVENTS_PER_SECOND=100
DB_HOST=localhost
DB_PASSWORD=admin123
```

---

## 🐞 Troubleshooting

| Issue | Solution |
|-------|----------|
| Kafka not starting | Wait 60s, check `docker logs kafka` |
| Topics not created | Re-run `docker compose up kafka-init` |
| No data in Grafana | Ensure producer is running (`docker logs producer`) |
| Spark job fails | Check JDBC driver version matches PostgreSQL |
| Port conflict | Change ports in `docker-compose.yml` |

---

## 📝 Resume Points

- Designed and implemented a real-time streaming pipeline processing **100+ events/sec** using **Apache Kafka** (ZooKeeper mode) and **Spark Structured Streaming**
- Built a multi-threaded Python **Kafka producer** simulating realistic e-commerce events across 4 topics
- Implemented **tumbling window aggregations**, **watermarking** for late data, and **stream-stream joins** in PySpark
- Deployed a **Dead Letter Queue** pattern for invalid/malformed events
- Containerized all 8 services with **Docker Compose** including health checks and dependency ordering
- Created an auto-provisioned **Grafana dashboard** with 11 live KPI panels refreshing every 5 seconds

---

## 📄 License

MIT License — Free to use for learning and portfolio purposes.
