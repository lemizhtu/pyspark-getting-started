from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

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

date_as_string = df["Date"].cast(StringType()).alias("date")
df.select(df["Date"], date_as_string).show()

spark.stop()
