from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("load_data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("../data/AAPL.csv")

df.show()
df.printSchema()

column1 = df.Close
column2 = df["Close"]
column3 = col("Close")

df.select(column1, column2, column3).show()

df.select("Date", "Close", "Open").show()

spark.stop()
