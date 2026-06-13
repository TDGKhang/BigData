FROM python:3.9-slim

# Cài đặt các công cụ hệ thống cần thiết (git có thể cần để tải dbt packages nếu có)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc là /usr/app giống như trong docker-compose.yml volume mount
WORKDIR /usr/app

# Cài đặt dbt-postgres
RUN pip install --no-cache-dir dbt-postgres

# Sao chép các tệp cấu hình vào container
COPY . .

# Giữ container chạy ngầm để có thể exec vào chạy lệnh dbt
CMD ["tail", "-f", "/dev/null"]
