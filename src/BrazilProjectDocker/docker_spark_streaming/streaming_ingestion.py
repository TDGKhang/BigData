from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("Olist_Streaming_Ingestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 1. ĐỊNH NGHĨA SCHEMA CHÍNH XÁC THEO CSV OLIST
# ==========================================
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True)
])

items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])

payments_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])

reviews_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True)
])

# ==========================================
# 2. HÀM TẠO LUỒNG KẾT NỐI VÀ LƯU XUỐNG HDFS
# ==========================================
def create_stream(topic_name, schema, folder_name):
    print(f"-> Khoi tao luong tiep nhan cho: {topic_name}")
    
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()
        
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
        

    hdfs_path = f"hdfs://namenode:8020/user/hadoop/olist_datalake/{folder_name}/"
    chk_path = f"hdfs://namenode:8020/user/hadoop/olist_checkpoints/{folder_name}/"
    
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", hdfs_path) \
        .option("checkpointLocation", chk_path) \
        .start()
        
    return query

# ==========================================
# 3. CHẠY ĐỒNG THỜI 4 LUỒNG DỮ LIỆU
# ==========================================
q1 = create_stream("live_orders", orders_schema, "orders")
q2 = create_stream("live_items", items_schema, "items")
q3 = create_stream("live_payments", payments_schema, "payments")
q4 = create_stream("live_reviews", reviews_schema, "reviews")

print("Hoan tat. Spark dang cho du lieu tu Kafka...")
spark.streams.awaitAnyTermination()