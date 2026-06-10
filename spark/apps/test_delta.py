from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaTest") \
    .master("spark://spark-master:7077") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()

hadoopConf = spark._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin123")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.createDataFrame([
    (1, "phone"),
    (2, "laptop")
], ["id", "product"])

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://ecommerce/test_delta")

print("DELTA WRITE SUCCESS")

spark.stop()