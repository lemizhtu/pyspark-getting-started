from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hello_world") \
    .master("local[*]") \
    .getOrCreate()

print(spark.version)

spark.stop()
