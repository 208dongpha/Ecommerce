from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Spark Session
spark = SparkSession.builder \
    .appName("GoldLayer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# MinIO Config
hadoopConf = spark._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin123")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Silver Schema
schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_hour", IntegerType(), True),
    StructField("is_purchase", IntegerType(), True),
    StructField("is_refund", IntegerType(), True)
])

# Read silver stream
silver_df = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load("s3a://ecommerce/silver/events")

# Gold Aggregations
gold_df = silver_df.groupBy("event_date, category").agg(sum("revenue").alias("total_revenue"),
    sum("is_purchase").alias("total_orders"),
    sum("is_refund").alias("total_refunds"),
    avg("price").alias("avg_price"))

# Write Gold layer
query = gold_df.writeStream \
    .format("parquet") \
    .outputMode("complete") \
    .option("path", "s3a://ecommerce/gold/sales_metrics") \
    .option("checkpointLocation", "/tmp/checkpoints/gold") \
    .start()

query.awaitTermination()