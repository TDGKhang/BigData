from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

print("Starting Spark Session for Data Lake loader...")
spark = SparkSession.builder \
    .appName("Olist_Datalake_to_Postgres_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create schema 'raw' using JDBC via PySpark JVM Gateway
try:
    print("Creating database schema 'raw' in PostgreSQL...")
    gw = spark.sparkContext._gateway
    gw.jvm.Class.forName("org.postgresql.Driver")
    conn = gw.jvm.java.sql.DriverManager.getConnection("jdbc:postgresql://postgres:5432/olist", "postgres", "postgres")
    stmt = conn.createStatement()
    stmt.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    stmt.close()
    conn.close()
    print("Schema 'raw' verified/created successfully.")
except Exception as e:
    print(f"Warning: Could not create schema 'raw' via JDBC direct call: {e}")

# Define target PostgreSQL connection
pg_url = "jdbc:postgresql://postgres:5432/olist"
pg_properties_overwrite = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
    "truncate": "true"
}
pg_properties_append = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Define schemas for HDFS tables
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

# Define paths and schemas mapping
hdfs_tables = {
    "orders": ("hdfs://namenode:8020/user/hadoop/olist_datalake/orders/", orders_schema),
    "items": ("hdfs://namenode:8020/user/hadoop/olist_datalake/items/", items_schema),
    "payments": ("hdfs://namenode:8020/user/hadoop/olist_datalake/payments/", payments_schema),
    "reviews": ("hdfs://namenode:8020/user/hadoop/olist_datalake/reviews/", reviews_schema)
}

# Pre-create HDFS paths if they don't exist to prevent readStream from failing
try:
    print("Checking HDFS directories for stream sources...")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(conf)
    for table_name, (hdfs_path, _) in hdfs_tables.items():
        path = Path(hdfs_path)
        if not fs.exists(path):
            print(f"Creating empty HDFS directory: {hdfs_path}")
            fs.mkdirs(path)
except Exception as e:
    print(f"Warning: Could not verify/create HDFS directories: {e}")

# 1. Static CSV Reference Data Ingestion (Reference Datasets - One-off batch load)
csv_tables = {
    "customers": "/Data/olist_customers_dataset.csv",
    "products": "/Data/olist_products_dataset.csv",
    "sellers": "/Data/olist_sellers_dataset.csv",
    "category_translation": "/Data/product_category_name_translation.csv",
    "geolocation": "/Data/olist_geolocation_dataset.csv"
}

print("\n--- INGESTING STATIC CSV TABLES ---")
for table_name, csv_path in csv_tables.items():
    print(f"Reading CSV table: {table_name} from {csv_path}")
    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        record_count = df.count()
        print(f"Writing {table_name} to PostgreSQL raw.{table_name} ({record_count} records)...")
        df.write.jdbc(url=pg_url, table=f"raw.{table_name}", mode="overwrite", properties=pg_properties_overwrite)
        print(f"Successfully loaded {table_name}!")
    except Exception as e:
        print(f"Error: Could not load static CSV table {table_name} from {csv_path}: {e}")

# 2. HDFS Parquet Data Ingestion (Streaming Datalake to Postgres)
print("\n--- STARTING HDFS TO POSTGRES STREAMING ---")

def write_to_postgres(batch_df, batch_id, t_name):
    print(f"Writing streaming batch {batch_id} to PostgreSQL raw.{t_name}...")
    batch_df.write.jdbc(
        url=pg_url,
        table=f"raw.{t_name}",
        mode="append",
        properties=pg_properties_append
    )

streams = []
for table_name, (hdfs_path, schema) in hdfs_tables.items():
    print(f"Initializing stream for HDFS table: {table_name} from {hdfs_path}")
    try:
        # Read stream from HDFS
        df = spark.readStream.schema(schema).parquet(hdfs_path)
        
        # Write stream using foreachBatch to Postgres
        chk_path = f"hdfs://namenode:8020/user/hadoop/postgres_checkpoints/{table_name}/"
        
        query = df.writeStream \
            .foreachBatch(lambda batch_df, batch_id, t_name=table_name: write_to_postgres(batch_df, batch_id, t_name)) \
            .option("checkpointLocation", chk_path) \
            .trigger(processingTime='10 seconds') \
            .start()
            
        streams.append(query)
        print(f"✅ Stream started for {table_name} → PostgreSQL raw.{table_name}")
    except Exception as e:
        print(f"Error starting stream for {table_name}: {e}")

if streams:
    print("\nAll HDFS -> Postgres streams started. Spark is awaiting termination...")
    spark.streams.awaitAnyTermination()
else:
    print("\nNo streams were started.")
    spark.stop()
