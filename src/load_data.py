from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("load_data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .csv("../data/AAPL.csv")

df.show()

spark.stop()
