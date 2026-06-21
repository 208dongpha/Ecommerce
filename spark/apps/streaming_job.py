from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# =========================
# SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# MINIO CONFIG
# =========================

hadoopConf = spark._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin123")

hadoopConf.set("fs.s3a.path.style.access", "true")

hadoopConf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

# =========================
# SCHEMA
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
# READ KAFKA STREAM
# =========================

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# =========================
# PARSE JSON
# =========================

json_df = df.selectExpr("CAST(value AS STRING) as json")

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")

# =========================
# WRITE TO MINIO
# =========================

query = parsed_df.writeStream \
    .format("delta") \
    .option(
        "path",
        "s3a://ecommerce/bronze/events"
    ) \
    .option(
        "checkpointLocation",
        "/opt/checkpoints/ecommerce-events"
    ) \
    .outputMode("append") \
    .start()

query.awaitTermination()
