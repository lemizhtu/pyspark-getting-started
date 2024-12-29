from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, DateType, DoubleType

spark = SparkSession.builder \
    .appName("load_data") \
    .master("local[*]") \
    .getOrCreate()


def load_stock_data(symbol: str) -> DataFrame:
    df = spark.read \
        .option("header", True) \
        .csv(f"../data/{symbol}.csv")

    return df.select(
        df["Date"].cast(DateType()).alias("date"),
        df["Open"].cast(DoubleType()).alias("open"),
        df["Close"].cast(DoubleType()).alias("close"),
        df["High"].cast(DoubleType()).alias("high"),
        df["Low"].cast(DoubleType()).alias("low"),
    )


aapl_df = load_stock_data("AAPL")

window = Window.partitionBy().orderBy(aapl_df["date"])
aapl_df.withColumn("previous_date_close", lag(aapl_df["close"], 1, 0.0).over(window)) \
    .withColumn("diff_prev_close", aapl_df["open"] - col("previous_date_close")) \
    .show()

spark.stop()
