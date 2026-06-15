# Tiểu luận Big Data - Nhóm 04

Đồ án sử dụng bộ dữ liệu **Brazilian E-Commerce Public Dataset by Olist** (Kaggle), bao gồm các đơn hàng thực tế từ năm 2016-2018 trên sàn thương mại điện tử Olist (Brazil). Dữ liệu gồm nhiều bảng liên quan (orders, order_items, products, customers, sellers, payments...), tổng số mẫu tin và thuộc tính đáp ứng yêu cầu tối thiểu 100.000 dòng và 10 thuộc tính.

## Cấu trúc thư mục

```
Nhom_04/
├── README.md
├── docs/                  # Báo cáo Word, Slide thuyết trình
├── images/                # Ảnh minh chứng (config, kết quả query, mô hình)
└── src/
    ├── 01_setup/          # File cấu hình Hadoop, Spark
    ├── 02_data_ingestion/ # Đưa dataset vào HDFS
    ├── 03_eda/            # Khám phá dữ liệu
    ├── 04_spark_sql/      # 10+ câu Spark SQL
    └── 05_mllib/          # 3 mô hình MLlib
        ├── late_delivery_prediction.ipynb     (Phân lớp - GBT)
        ├── freight_value_prediction.ipynb     (Hồi quy - Random Forest)
        └── customer_clustering.ipynb          (Phân cụm - KMeans RFM)
```


## Hướng dẫn

1. Khởi động Hadoop & Spark (xem `src/01_setup/`).
2. Upload dataset vào HDFS: `src/02_data_ingestion/upload_to_hdfs.sh`
3. Chạy các notebook trong `src/04_spark_sql/` và `src/05_mllib/` bằng Jupyter (PySpark kernel).

## Phân chia công việc

| Thành viên | MSSV | Công việc phụ trách |
|---|---|---|
| Phan Anh Tài | ... | Viết Spark SQL, thiết kế pipeline và dashboard streaming |
| Đỗ Thái Gia Hy | 31231021575 |  Thiết kế, cấu hình Kafka và Web UI  |
| Trương Đoàn Gia Khang | ... | Cài đặt Hadoop/HDFS, Cài đặt Spark, mô hình Hồi quy chi phí vận chuyển |
| Lê Thủy Tiên | 31231020076 | Mô hình Phân cụm khách hàng (K-Means RFM), mô hình Phân lớp giao trễ (GBT)|
| (Tất cả) | | Tổng hợp báo cáo Word, làm slide thuyết trình, chuẩn bị Q&A |

