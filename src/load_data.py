from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("load_data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .csv("../data/AAPL.csv")

df.show()

spark.stop()
