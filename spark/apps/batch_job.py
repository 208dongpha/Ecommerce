from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("GoldBatch") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# MinIO config
hadoopConf = spark._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin123")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

silver_df = spark.read.parquet(
    "s3a://ecommerce/silver/events"
)

gold_df = silver_df.groupBy(
    "event_date",
    "category"
).agg(
    sum("revenue").alias("total_revenue"),
    sum("is_purchase").alias("total_orders"),
    sum("is_refund").alias("total_refunds"),
    avg("price").alias("avg_price")
)

gold_df.write \
    .mode("overwrite") \
    .parquet("s3a://ecommerce/gold/sales_metrics")

spark.stop()