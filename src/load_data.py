from pyspark.sql import SparkSession
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

aapl_df.groupby("date").max("close").show()
aapl_df.groupby(
    year(aapl_df["date"]).alias("year"),
    month(aapl_df["date"], ).alias("month"),
).agg(
    max("close").alias("max_close"),
    avg("close").alias("avg_close"),
    sum("open").alias("sum_open"),
).sort(col("max_close").desc()).show()

spark.stop()
