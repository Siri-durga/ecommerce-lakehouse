import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)
from delta.tables import DeltaTable

# -------------------------
# Spark Session
# -------------------------
spark = (
    SparkSession.builder
    .appName("Ecommerce Lakehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# -------------------------
# MinIO Client
# -------------------------
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)

bucket_name = "data"

if bucket_name not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket=bucket_name)

# -------------------------
# Upload raw files
# -------------------------
s3.upload_file("/data/products.csv", bucket_name, "raw/products.csv")
s3.upload_file("/data/customers.csv", bucket_name, "raw/customers.csv")
s3.upload_file("/data/updates.csv", bucket_name, "raw/updates.csv")
s3.upload_file("/data/sales.csv", bucket_name, "raw/sales.csv")

# -------------------------
# PRODUCTS → DELTA
# -------------------------
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("stock_quantity", IntegerType(), False),
])

products_df = (
    spark.read
    .schema(products_schema)
    .option("header", "true")
    .csv("s3a://data/raw/products.csv")
)

products_path = "s3a://data/warehouse/products"

(
    products_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("category")
    .save(products_path)
)

# -------------------------
# CUSTOMERS → DELTA + MERGE
# -------------------------
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("last_updated_date", StringType(), False),
])

customers_df = (
    spark.read
    .schema(customers_schema)
    .option("header", "true")
    .csv("s3a://data/raw/customers.csv")
)

customers_path = "s3a://data/warehouse/customers"

customers_df.write.format("delta").mode("overwrite").save(customers_path)

updates_df = (
    spark.read
    .schema(customers_schema)
    .option("header", "true")
    .csv("s3a://data/raw/updates.csv")
)

customers_delta = DeltaTable.forPath(spark, customers_path)

(
    customers_delta.alias("t")
    .merge(
        updates_df.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdate(set={
        "email": "s.email",
        "last_updated_date": "s.last_updated_date"
    })
    .whenNotMatchedInsert(values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "email": "s.email",
        "last_updated_date": "s.last_updated_date"
    })
    .execute()
)

# -------------------------
# SALES → STRUCTURED STREAMING
# -------------------------
sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("sale_amount", DoubleType(), False),
    StructField("sale_timestamp", StringType(), False),
])

sales_path = "s3a://data/warehouse/sales"
checkpoint_path = "s3a://data/checkpoints/sales_stream"

sales_stream_df = (
    spark.readStream
    .schema(sales_schema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .csv("s3a://data/raw/sales/")
)

sales_query = (
    sales_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(sales_path)
)

sales_query.awaitTermination()

spark.stop()
