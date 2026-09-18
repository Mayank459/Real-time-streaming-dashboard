#!/bin/bash
set -e

echo "=== 1. Creating Kafka Topics ==="
sudo docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic orders || true
sudo docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic payments || true
sudo docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic clicks || true
sudo docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic reviews || true
sudo docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic dlq || true
sudo docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic orders_retry || true
sudo docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic payments_retry || true

echo "=== 2. Installing Python dependencies on Spark containers ==="
sudo docker exec spark-master pip install psycopg2-binary scikit-learn --quiet || true
sudo docker exec spark-worker pip install psycopg2-binary scikit-learn --quiet || true

echo "=== 3. Setting up Spark Script on spark-master ==="
sudo docker exec spark-master bash -c 'cat << "EOF" > /tmp/run_spark.sh
#!/bin/bash
export PYTHONPATH=/opt/bitnami/spark/work
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export DB_HOST=postgres

spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /opt/bitnami/spark/work/spark/spark_stream.py \
  > /tmp/spark_job.log 2>&1
EOF
chmod +x /tmp/run_spark.sh
'

echo "=== 4. Launching Spark Streaming Job ==="
sudo docker exec spark-master bash -c "nohup /tmp/run_spark.sh > /tmp/spark_nohup.log 2>&1 &"

echo "=== 5. Restarting Producer ==="
sudo docker restart producer

echo "=== Deployment script completed successfully! ==="
