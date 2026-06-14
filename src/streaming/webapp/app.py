import json
import time
import uuid
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template
from kafka import KafkaProducer

app = Flask(__name__)

# CẤU HÌNH KAFKA
def get_kafka_producer():
    max_retries = 10
    for i in range(max_retries):
        try:
            print(f"Đang thử kết nối Kafka... Lần {i+1}")
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Đã kết nối Kafka thành công")
            return producer
        except Exception as e:
            time.sleep(3)
    raise Exception("Không thể kết nối Kafka!")

producer = get_kafka_producer()

def get_time_offset(days=0):
    return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

# TRANG CHỦ CỬA HÀNG DEMO
@app.route('/')
def index():
    return render_template('index.html')

# API CHECKOUT TỪ WEB APP
@app.route('/api/checkout', methods=['POST'])
def api_checkout():
    data = request.json
    product_id = data.get('product_id', 'unknown_product')
    price = float(data.get('price', 0))
    payment_type = data.get('payment_type', 'credit_card')
    payment_installments = int(data.get('payment_installments', 1))
    
    order_id = str(uuid.uuid4()).replace("-", "")
    now = datetime.now()
    
    # 1. Gửi sang live_orders
    order_data = {
        "order_id": order_id,
        "customer_id": "guest_customer",
        "order_status": "delivered",
        "order_purchase_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "order_approved_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "order_delivered_carrier_date": (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "order_delivered_customer_date": (now + timedelta(days=4)).strftime("%Y-%m-%d %H:%M:%S"),
        "order_estimated_delivery_date": (now + timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    }
    producer.send('live_orders', order_data)
    
    # 2. Gửi sang live_items
    item_data = {
        "order_id": order_id,
        "order_item_id": 1,
        "product_id": product_id,
        "seller_id": "my_shopify_store",
        "shipping_limit_date": (now + timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S"),
        "price": price,
        "freight_value": 15.0
    }
    producer.send('live_items', item_data)
    
    # 3. Gửi sang live_payments
    payment_data = {
        "order_id": order_id,
        "payment_sequential": 1,
        "payment_type": payment_type,
        "payment_installments": payment_installments,
        "payment_value": price + 15.0
    }
    producer.send('live_payments', payment_data)
    
    producer.flush()
    print(f"Checkout Web App: Đơn hàng [{order_id}] đã gửi vào Kafka")
    return jsonify({
        "status": "success",
        "order_id": order_id,
        "total": price + 15.0
    }), 200

# API REVIEW TỪ WEB APP
@app.route('/api/review', methods=['POST'])
def api_review():
    data = request.json
    order_id = data.get('order_id')
    review_score = int(data.get('review_score', 5))
    review_comment = data.get('review_comment_message', '')
    
    now = datetime.now()
    review_id = str(uuid.uuid4()).replace("-", "")
    
    review_data = {
        "review_id": review_id,
        "order_id": order_id,
        "review_score": review_score,
        "review_comment_title": "Từ Web App",
        "review_comment_message": review_comment if review_comment else None,
        "review_creation_date": now.strftime("%Y-%m-%d 00:00:00"),
        "review_answer_timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send('live_reviews', review_data)
    producer.flush()
    print(f"Review Web App: Đơn [{order_id}] đã gửi đánh giá vào Kafka")
    return jsonify({"status": "success"}), 200

# TRẠM THU NHẬN WEBHOOK TỪ SHOPIFY
@app.route('/webhook/order_created', methods=['POST'])
def shopify_webhook():
    shopify_data = request.json
    print("1. Cổng Gateway:", shopify_data.get('gateway'), flush=True)
    print("2. Tên Gateway Names:", shopify_data.get('payment_gateway_names'), flush=True)
    print("----------------------------\n", flush=True)
    order_id = str(shopify_data.get('id', 'test_order'))
    
    # 1. BÓC TÁCH BẢNG ORDERS (Đơn hàng)
    order_data = {
        "order_id": order_id,
        "customer_id": str(shopify_data.get('customer', {}).get('id', 'guest')),
        "order_status": "approved",
        "order_purchase_timestamp": shopify_data.get('created_at', get_time_offset(0)),
        "order_approved_at": shopify_data.get('updated_at', get_time_offset(0)),
        "order_estimated_delivery_date": get_time_offset(5) 
    }
    producer.send('live_orders', order_data)

    # 2. BÓC TÁCH BẢNG ITEMS (Chi tiết Sản phẩm)
    line_items = shopify_data.get('line_items', [])
    for index, item in enumerate(line_items):
        item_data = {
            "order_id": order_id,
            "order_item_id": str(index + 1),
            "product_id": str(item.get('product_id', 'unknown_product')),
            "seller_id": "my_shopify_store",
            "shipping_limit_date": get_time_offset(2),
            "price": str(item.get('price', '0')),
            "freight_value": "15.0"
        }
        producer.send('live_items', item_data)

    # 3. BẢNG PAYMENTS 
    shopify_gateway = shopify_data.get('gateway') or ""
    gateway_names = shopify_data.get('payment_gateway_names') or []
    
    payment_info = (str(shopify_gateway) + " " + " ".join(gateway_names)).lower()

    if 'cod' in payment_info or 'cash' in payment_info or 'manual' in payment_info:
        payment_type = 'COD'
    elif 'gift_card' in payment_info or 'discount' in payment_info:
        payment_type = 'voucher'
    elif 'debit' in payment_info:
        payment_type = 'debit_card'
    else:
        payment_type = 'credit_card'

    total_value = float(shopify_data.get('total_price', 0))
    if payment_type == 'credit_card' and total_value > 100.0:
        installments = "3"
    else:
        installments = "1"

    payment_data = {
        "order_id": order_id,
        "payment_sequential": "1",
        "payment_type": payment_type, 
        "payment_installments": installments,
        "payment_value": str(total_value)
    }
    producer.send('live_payments', payment_data)
    producer.flush()
    print(f"Đã bóc tách & đẩy thành công Đơn hàng [{order_id}] vào 3 Topic Kafka\n")
    return jsonify({"status": "success", "message": "Đã ghi nhận Webhook"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5000)
