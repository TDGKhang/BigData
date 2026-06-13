from flask import Flask, render_template, request
from kafka import KafkaProducer
import json
import uuid
from datetime import datetime, timedelta
import time

app = Flask(__name__)

# Khởi tạo Kafka Producer
def get_kafka_producer():
    max_retries = 10
    for i in range(max_retries):
        try:
            print(f"⏳ Đang thử kết nối Kafka... Lần {i+1}")
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'], # Đảm bảo tên là kafka:9092
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Đã kết nối Kafka thành công!")
            return producer
        except Exception as e:
            print(f"Kafka chưa sẵn sàng, đợi 3 giây... (Lỗi: {e})")
            time.sleep(3)
            
    raise Exception("❌ Đã thử 10 lần nhưng không thể kết nối Kafka. Hệ thống dừng lại!")

# Khởi tạo Kafka bằng hàm vừa viết
producer = get_kafka_producer()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit_transaction():
    # 1. Thu thập dữ liệu khách hàng nhập trên Web
    customer_id = request.form['customer_id']
    product_id = request.form['product_id']
    price = float(request.form['price'])
    freight_value = float(request.form['freight_value'])
    payment_type = request.form['payment_type']
    payment_installments = int(request.form['payment_installments'])
    review_score = int(request.form['review_score'])
    review_comment = request.form.get('review_comment', '')

    # 2. Tự động sinh các trường hệ thống
    order_id = str(uuid.uuid4()).replace("-", "")
    now = datetime.now()
    
    # --- TẠO DỮ LIỆU BẢNG 1: ORDERS ---
    order_data = {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": "delivered",
        "order_purchase_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "order_approved_at": (now + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S"),
        "order_delivered_carrier_date": (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "order_delivered_customer_date": (now + timedelta(days=4)).strftime("%Y-%m-%d %H:%M:%S"),
        "order_estimated_delivery_date": (now + timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    }

    # --- TẠO DỮ LIỆU BẢNG 2: ITEMS ---
    item_data = {
        "order_id": order_id,
        "order_item_id": 1,
        "product_id": product_id,
        "seller_id": "1554a68530182680ad5c8b042c3ab563", # Fix cứng 1 seller cho demo
        "shipping_limit_date": (now + timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S"),
        "price": price,
        "freight_value": freight_value
    }

    # --- TẠO DỮ LIỆU BẢNG 3: PAYMENTS ---
    payment_data = {
        "order_id": order_id,
        "payment_sequential": 1,
        "payment_type": payment_type,
        "payment_installments": payment_installments,
        "payment_value": round(price + freight_value, 2)
    }

    # --- TẠO DỮ LIỆU BẢNG 4: REVIEWS ---
    review_data = {
        "review_id": str(uuid.uuid4()).replace("-", ""),
        "order_id": order_id,
        "review_score": review_score,
        "review_comment_title": "Từ Web App",
        "review_comment_message": review_comment if review_comment else None,
        "review_creation_date": (now + timedelta(days=5)).strftime("%Y-%m-%d 00:00:00"),
        "review_answer_timestamp": (now + timedelta(days=6)).strftime("%Y-%m-%d %H:%M:%S")
    }

    # 3. Bắn đồng loạt vào 4 Topic Kafka
    producer.send('live_orders', order_data)
    producer.send('live_items', item_data)
    producer.send('live_payments', payment_data)
    producer.send('live_reviews', review_data)
    producer.flush() # Ép gửi ngay lập tức

    # 4. Trả về màn hình thành công
    return f"""
    <div style="font-family: Arial; text-align: center; margin-top: 50px;">
        <h2 style="color: #198754;">✔ Đặt hàng thành công! (Mã Đơn: {order_id[:8]}...)</h2>
        <p style="color: #6c757d; font-size: 18px;">Hệ thống đã phân rã sự kiện và đẩy vào 4 Topic Kafka riêng biệt.</p>
        <p>Vui lòng kiểm tra màn hình Spark Streaming để xem luồng dữ liệu đang chạy về HDFS.</p>
        <br><br>
        <a href="/" style="padding: 12px 24px; background: #0d6efd; color: white; text-decoration: none; border-radius: 8px; font-weight: bold;">Tạo Giao Dịch Mới</a>
    </div>
    """

if __name__ == '__main__':
    print("🚀 Website đã sẵn sàng tại: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000)