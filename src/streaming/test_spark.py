# ./streaming/test_spark.py
import os
import sys

# Windows Spark Setup
if platform.system() == "Windows":
    current_dir = os.getcwd()
    hadoop_home = os.path.join(current_dir, "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, element_at, to_timestamp
from schemas import binance_schema

spark = SparkSession.builder \
    .appName("TestNormalization") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read Stream
raw_stream = spark.readStream \
    .schema(binance_schema) \
    .json("data/bronze/binance/")

# Transform 
# We use 'bids' and 'asks' now, and use receipt_timestamp as event_time
normalized_stream = raw_stream.select(
    (col("receipt_timestamp") / 1000).cast("timestamp").alias("event_time"),
    lit("Binance").alias("exchange"),
    lit("BTCUSDT").alias("symbol"), 
    element_at(col("bids"), 1)[0].cast("double").alias("bid_price"),
    element_at(col("asks"), 1)[0].cast("double").alias("ask_price")
)

# Sink to Console
query = normalized_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()