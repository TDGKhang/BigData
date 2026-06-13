FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre netcat-traditional && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.1

WORKDIR /app

COPY streaming_ingestion.py .

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "streaming_ingestion.py"]