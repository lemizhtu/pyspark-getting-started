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

window = Window.partitionBy(year(aapl_df["date"])).orderBy(aapl_df["close"].desc(), aapl_df["date"])
aapl_df.withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .show()

spark.stop()
