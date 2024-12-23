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
aapl_df.show()

date_plus_2 = date_add(aapl_df["date"], 2)
date_of_string = date_plus_2.cast(StringType())
concat_column = concat(date_of_string, lit(" Hello, World!"))

aapl_df.select("date", concat_column.alias("transformed_date")).show(truncate=False)

aapl_df.createOrReplaceTempView("df")

spark.sql("SELECT date, concat(cast(date_add(date, 2) as string), ' Hello, World!') as transformed_date FROM df").show(truncate=False)

spark.stop()
