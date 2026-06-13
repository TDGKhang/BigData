import os
import time
import json
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from datetime import datetime

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

# Helper function to convert NaNs or null values to None for JSON serialization
def sanitize_dict(d):
    return {k: (None if pd.isna(v) else v) for k, v in d.items()}

# Helper functions for date shifting
def parse_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def shift_date_string(date_str, delta):
    if not date_str or not isinstance(date_str, str):
        return date_str
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return (dt + delta).strftime(fmt)
        except ValueError:
            continue
    return date_str

# 1. Config paths & bootstrap servers
if os.path.exists('/Data'):
    data_dir = '/Data'
elif os.path.exists('../../Data'):
    data_dir = '../../Data'
elif os.path.exists('./Data'):
    data_dir = './Data'
else:
    data_dir = 'c:/bigdata/Data'

bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')

print(f"Loading data from: {data_dir}")
print(f"Connecting to Kafka at: {bootstrap_servers}")

# Load datasets
orders_df = pd.read_csv(os.path.join(data_dir, 'olist_orders_dataset.csv'))
items_df = pd.read_csv(os.path.join(data_dir, 'olist_order_items_dataset.csv'))
payments_df = pd.read_csv(os.path.join(data_dir, 'olist_order_payments_dataset.csv'))
reviews_df = pd.read_csv(os.path.join(data_dir, 'olist_order_reviews_dataset.csv'))

# Group items, payments, and reviews by order_id for efficient lookup
items_by_order = {}
for item in items_df.to_dict(orient='records'):
    sanitized_item = sanitize_dict(item)
    order_id = sanitized_item['order_id']
    if order_id not in items_by_order:
        items_by_order[order_id] = []
    items_by_order[order_id].append(sanitized_item)

payments_by_order = {}
for payment in payments_df.to_dict(orient='records'):
    sanitized_payment = sanitize_dict(payment)
    order_id = sanitized_payment['order_id']
    if order_id not in payments_by_order:
        payments_by_order[order_id] = []
    payments_by_order[order_id].append(sanitized_payment)

reviews_by_order = {}
for review in reviews_df.to_dict(orient='records'):
    sanitized_review = sanitize_dict(review)
    order_id = sanitized_review['order_id']
    if order_id not in reviews_by_order:
        reviews_by_order[order_id] = []
    reviews_by_order[order_id].append(sanitized_review)

def get_producer():
    max_retries = 20
    for i in range(max_retries):
        try:
            print(f"⏳ Connecting to Kafka... Try {i+1}")
            prod = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, cls=NumpyEncoder).encode('utf-8')
            )
            return prod
        except Exception as e:
            print(f"Kafka not ready, waiting 3 seconds... (Error: {e})")
            time.sleep(3)
    raise Exception("Could not connect to Kafka after multiple retries.")

producer = get_producer()
print("Kafka Producer connected successfully!")

# 4. Stream events at 100 events/second
target_rate = 10  # events per second
start_time = time.time()
events_sent = 0

orders_list = [sanitize_dict(r) for r in orders_df.to_dict(orient='records')]

# Calculate global delta once, aligning latest purchase time to current time
latest_purchase_time = None
for order in orders_list:
    purchase_time_str = order.get('order_purchase_timestamp')
    purchase_time = parse_date(purchase_time_str)
    if purchase_time:
        if latest_purchase_time is None or purchase_time > latest_purchase_time:
            latest_purchase_time = purchase_time

if latest_purchase_time:
    delta = datetime.now() - latest_purchase_time
    print(f"Calculated date shift delta: {delta} based on latest purchase timestamp {latest_purchase_time}")
else:
    delta = None
    print("Warning: Could not determine latest purchase timestamp. Date shifting is disabled.")

print("Starting to stream events...")
while True:
    for order in orders_list:
        order_id = order['order_id']
        shifted_order = order.copy()
        if delta:
            for field in ['order_purchase_timestamp', 'order_approved_at', 
                          'order_delivered_carrier_date', 'order_delivered_customer_date', 
                          'order_estimated_delivery_date']:
                if shifted_order.get(field):
                    shifted_order[field] = shift_date_string(shifted_order[field], delta)
                    
        # Stream order
        producer.send('live_orders', shifted_order)
        events_sent += 1
        
        # Stream items
        if order_id in items_by_order:
            for item in items_by_order[order_id]:
                shifted_item = item.copy()
                if delta and shifted_item.get('shipping_limit_date'):
                    shifted_item['shipping_limit_date'] = shift_date_string(shifted_item['shipping_limit_date'], delta)
                producer.send('live_items', shifted_item)
                events_sent += 1
                
        # Stream payments
        if order_id in payments_by_order:
            for payment in payments_by_order[order_id]:
                producer.send('live_payments', payment)
                events_sent += 1
                
        # Stream reviews
        if order_id in reviews_by_order:
            for review in reviews_by_order[order_id]:
                shifted_review = review.copy()
                if delta:
                    for field in ['review_creation_date', 'review_answer_timestamp']:
                        if shifted_review.get(field):
                            shifted_review[field] = shift_date_string(shifted_review[field], delta)
                producer.send('live_reviews', shifted_review)
                events_sent += 1
                
        # Rate limit calculation
        elapsed = time.time() - start_time
        expected = events_sent / target_rate
        sleep_time = expected - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
            
        # Optional logging
        if events_sent % 1000 == 0:
            print(f"Sent {events_sent} events... Current speed: ~{events_sent / (time.time() - start_time):.1f} events/sec")
