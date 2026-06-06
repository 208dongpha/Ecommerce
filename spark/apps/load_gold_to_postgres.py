from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadGoldToPostgres") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# MinIO
hadoopConf = spark._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin123")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

gold_df = spark.read.parquet(
    "s3a://ecommerce/gold/sales_metrics"
)

gold_df.write \
    .format("jdbc") \
    .option(
        "url",
        "jdbc:postgresql://postgres-dwh:5432/ecommerce"
    ) \
    .option("dbtable", "sales_metrics") \
    .option("user", "postgres") \
    .option("password", "postgres123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()