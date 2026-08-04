FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
# Context is project root, so paths are relative to d:\blend360\
RUN pip install --no-cache-dir \
    faker==22.0.0 \
    confluent-kafka==2.3.0 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.0

# Copy producer source code
COPY producer/ /app/producer/

# Copy shared config
COPY config/ /app/config/

# Create logs directory
RUN mkdir -p /app/logs

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "producer.producer"]
