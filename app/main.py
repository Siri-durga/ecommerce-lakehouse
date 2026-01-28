import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

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
# MinIO bucket creation
# -------------------------
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)

bucket_name = "data"

existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if bucket_name not in existing:
    s3.create_bucket(Bucket=bucket_name)

# -------------------------
# Upload raw CSV files
# -------------------------
s3.upload_file("/data/products.csv", bucket_name, "raw/products.csv")
s3.upload_file("/data/customers.csv", bucket_name, "raw/customers.csv")

# -------------------------
# Enforced schema for products
# -------------------------
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("stock_quantity", IntegerType(), False),
])

# -------------------------
# Batch ingestion → Delta
# -------------------------
products_df = (
    spark.read
    .schema(products_schema)
    .option("header", "true")
    .csv("s3a://data/raw/products.csv")
)

(
    products_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("category")
    .save("s3a://data/warehouse/products")
)

spark.stop()
