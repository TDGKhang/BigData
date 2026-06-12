# 1. Sử dụng nền Python 3.10
FROM python:3.10-slim

# 2. Spark hoạt động trên lõi Java. Ta cần cài đặt Java JRE cho môi trường Linux này.
# 2. Cài đặt Java và Netcat (để ping mạng)
RUN apt-get update && \
    apt-get install -y default-jre netcat-traditional && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Cài đặt thư viện PySpark bản ổn định nhất (3.5.1)
RUN pip install --no-cache-dir pyspark==3.5.1

# 4. Thiết lập thư mục làm việc
WORKDIR /app

# 5. Copy file code xử lý luồng vào Container
COPY streaming_ingestion.py .

# 6. Khởi chạy hệ thống. Tự động gắn kèm gói Kafka tương thích chuẩn xác với Spark 3.5.1
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "streaming_ingestion.py"]