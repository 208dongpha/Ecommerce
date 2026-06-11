from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("GoldBatch") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# MinIO config
hadoopConf = spark._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin123")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

silver_df = spark.read.format("delta").load(
    "s3a://ecommerce/silver/events"
)

gold_df = silver_df.groupBy(
    "event_date",
    "category"
).agg(
    sum("revenue").alias("total_revenue"),

    sum("is_purchase").alias("total_orders"),

    sum("is_refund").alias("total_refunds"),

    countDistinct("user_id").alias("unique_users"),

    countDistinct("product_id").alias("unique_products"),

    avg("price").alias("avg_price"),

    max("price").alias("max_price"),

    min("price").alias("min_price")
)

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://ecommerce/gold/sales_metrics")

spark.stop()