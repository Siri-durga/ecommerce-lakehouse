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

# -------------------------
# PRODUCTS → DELTA (Batch)
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
# CUSTOMERS → INITIAL LOAD
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

# -------------------------
# UPDATES → MERGE (UPSERT)
# -------------------------
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
# TIME TRAVEL VERIFICATION
# -------------------------
print("===== CURRENT VERSION (AFTER MERGE) =====")
spark.read.format("delta") \
    .load(customers_path) \
    .filter("customer_id = 5") \
    .select("customer_id", "email") \
    .show(truncate=False)

print("===== VERSION 0 (BEFORE MERGE) =====")
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(customers_path) \
    .filter("customer_id = 5") \
    .select("customer_id", "email") \
    .show(truncate=False)

# -------------------------
# OPTIMIZE PRODUCTS (ZORDER)
# -------------------------
spark.sql(f"""
OPTIMIZE delta.`{products_path}`
ZORDER BY (product_id)
""")

# -------------------------
# VACUUM CUSTOMERS
# -------------------------
spark.conf.set(
    "spark.databricks.delta.retentionDurationCheck.enabled", "false"
)

spark.sql(f"""
VACUUM delta.`{customers_path}` RETAIN 0 HOURS
""")

spark.stop()
