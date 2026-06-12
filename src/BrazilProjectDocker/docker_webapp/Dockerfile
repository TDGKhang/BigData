# 1. Sử dụng môi trường Python bản nhẹ (slim) để tối ưu dung lượng
FROM python:3.9-slim

# 2. Tạo thư mục làm việc ảo bên trong Container tên là /app
WORKDIR /app

# 3. Cài đặt các thư viện cần thiết trực tiếp thông qua pip
RUN pip install --no-cache-dir flask kafka-python

# 4. Copy toàn bộ mã nguồn (app.py và thư mục templates) vào trong /app
COPY . .

# 5. Khai báo mở cổng 5000 cho Website
EXPOSE 5000

# 6. Lệnh sẽ được thực thi khi Container khởi động
CMD ["python", "app.py"]