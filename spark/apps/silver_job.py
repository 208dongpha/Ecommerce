from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# SparkSession
spark = SparkSession.builder \
    .appName("SilverLayer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# MinIO Config
# =========================
hadoopConf = spark._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin123")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# =========================
# Bronze Schema
# =========================
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("category", StringType(), True)
])

# =========================
# Read Bronze Stream
# =========================
bronze_df = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load("s3a://ecommerce/bronze/events")

# =========================
# Silver Transformations
# =========================
silver_df = bronze_df \
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"))) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .dropDuplicates() \
    .withColumn(
        "event_date",
        to_date(col("event_time"))) \
    .withColumn(
        "event_hour",
        hour(col("event_time"))) \
    .withColumn(
        "is_purchase",
        when(col("action") == "purchase", 1).otherwise(0)) \
    .withColumn(
        "is_refund",
        when(col("action") == "return_refund", 1).otherwise(0))

# =========================
# Write Silver Layer
# =========================
query = silver_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://ecommerce/silver/events") \
    .option("checkpointLocation", "/tmp/checkpoints/silver") \
    .outputMode("append") \
    .start()

query.awaitTermination()