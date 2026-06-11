from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckSilver") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key","minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key","minioadmin123")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint","http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access","true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.format("delta").load(
    "s3a://ecommerce/silver/events"
)

print("TOTAL ROWS =", df.count())

spark.stop()