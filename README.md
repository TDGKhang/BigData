# Tiểu luận Big Data - Nhóm 04

Đồ án sử dụng bộ dữ liệu **Brazilian E-Commerce Public Dataset by Olist** (Kaggle), bao gồm các đơn hàng thực tế từ năm 2016-2018 trên sàn thương mại điện tử Olist (Brazil). Dữ liệu gồm nhiều bảng liên quan (orders, order_items, products, customers, sellers, payments...), tổng số mẫu tin và thuộc tính đáp ứng yêu cầu tối thiểu 100.000 dòng và 10 thuộc tính.

## Cấu trúc thư mục

```
Nhom_04/
├── README.md              # Hướng dẫn chạy code, tổng quan dự án
├── docs/                  # Báo cáo Word, Slide thuyết trình
├── images/                # Ảnh minh chứng 
├── data/                  # Dữ liệu mẫu (raw/processed data)
└── src/
    ├── eda/               # Khám phá và tiền xử lý dữ liệu ngắt quãng (Batch)
    │   └── eda.ipynb
    │
    ├── spark_sql/         # Các câu truy vấn Spark SQL (10+ câu)
    │   └── queries.ipynb
    │
    ├── mllib/             # Xây dựng và đánh giá 3 mô hình MLlib
    │   ├── late_delivery_prediction.ipynb   # Phân lớp - GBT Classifier
    │   ├── freight_value_prediction.ipynb  # Hồi quy - Random Forest Regressor
    │   └── customer_clustering.ipynb       # Phân cụm - KMeans RFM
    │
    └── streaming/        
```

## Hướng dẫn chạy
---
### Bước 1: Chuẩn bị môi trường
1. Đảm bảo bạn đã cài đặt và đang bật **Docker Desktop** trên máy tính.
2. Đảm bảo thư mục dữ liệu `c:\bigdata\Data` đã chứa đầy đủ các file dữ liệu Olist CSV gốc (như `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, v.v.) vì Docker sẽ mount thư mục này vào các container Spark và Producer.
---

### Bước 2: Khởi chạy dự án
Mở **PowerShell** hoặc **Command Prompt** của Windows, di chuyển vào thư mục dự án và chạy Docker Compose:

```powershell
# 1. Di chuyển vào thư mục chứa cấu hình Docker của dự án
cd c:\bigdata\src\streaming

# 2. Xây dựng (build) các image và khởi chạy toàn bộ 11 container dưới nền (detached mode)
docker compose up --build -d
```

### Bước 3: Kiểm tra trạng thái và theo dõi Logs

```powershell
# Kiểm tra danh sách và trạng thái hoạt động của các dịch vụ
docker compose ps

# Theo dõi log thời gian thực của toàn bộ hệ thống
docker compose logs -f

# Hoặc theo dõi riêng log của một dịch vụ cụ thể (Ví dụ: dịch vụ Spark Streaming hoặc dbt)
docker compose logs -f spark_streaming
docker compose logs -f spark_batch
docker compose logs -f dbt
```

---

## Địa Chỉ Truy Cập Các Dịch Vụ (Web UI)

### Bước 4: Sử dụng và kiểm tra các dịch vụ
**Giả lập giao dịch:** Truy cập trình duyệt tại địa chỉ [http://localhost:5000](http://localhost:5000) để nhập đơn hàng giả lập qua giao diện Flask Web App. Khi submit, Flask sẽ gửi dữ liệu vào Kafka.
**Hadoop HDFS Web UI:** Truy cập [http://localhost:9870](http://localhost:9870) để theo dõi hệ thống tệp tin phân tán và các thư mục Parquet được Spark ghi xuống.
**Theo dõi và phân tích:** Truy cập Grafana tại [http://localhost:3000](http://localhost:3000) để xem các dashboard báo cáo thời gian thực.

---

## Dừng Hệ Thống

### Bước 5: Dừng hệ thống
Khi muốn dừng dự án để giải phóng tài nguyên máy tính, sử dụng các lệnh sau:

```powershell
# Dừng và xóa toàn bộ các container đang chạy
docker compose down

# Nếu muốn xóa sạch toàn bộ dữ liệu cũ trong Database và HDFS để chạy lại từ đầu:
docker compose down -v
```
---
## Phân chia công việc

| Thành viên | MSSV | Công việc phụ trách |
|---|---|---|
| Phan Anh Tài | 31231026535 | Viết Spark SQL, thiết kế pipeline và dashboard streaming |
| Đỗ Thái Gia Hy | 31231021575 |  Thiết kế, cấu hình Kafka và Web UI  |
| Trương Đoàn Gia Khang | ... | Cài đặt Hadoop/HDFS, Cài đặt Spark, mô hình Hồi quy chi phí vận chuyển |
| Lê Thủy Tiên | 31231020076 | Mô hình Phân cụm khách hàng (K-Means RFM), mô hình Phân lớp giao trễ (GBT)|
| (Tất cả) | | Tổng hợp báo cáo Word, làm slide thuyết trình, chuẩn bị Q&A |

