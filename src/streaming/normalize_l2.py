# ./streaming/normalize_l2.py
import os
import platform
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, element_at, when, window, max, min
from schemas import binance_schema, coinbase_schema

# Only apply Hadoop/winutils setup on Windows
if platform.system() == "Windows":
    hadoop_home = os.path.join(os.getcwd(), "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

spark = SparkSession.builder \
    .appName("CryptoArbNormalization") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw streams
binance_raw = spark.readStream.schema(binance_schema).json("data/bronze/binance/")
coinbase_raw = spark.readStream.schema(coinbase_schema).json("data/bronze/coinbase/")

# Biance and Coinbase normalization to a common schema
binance_silver = binance_raw.select(
    (col("receipt_timestamp") / 1000).cast("timestamp").alias("event_time"),
    lit("Binance").alias("exchange"),
    lit("BTC-USD").alias("symbol"),
    element_at(col("bids"), 1)[0].cast("double").alias("bid_price"),
    element_at(col("asks"), 1)[0].cast("double").alias("ask_price")
).withWatermark("event_time", "10 seconds")


coinbase_silver = coinbase_raw.select(
    (col("receipt_timestamp") / 1000).cast("timestamp").alias("event_time"),
    lit("Coinbase").alias("exchange"),
    lit("BTC-USD").alias("symbol"), # Ensure exact match
    when(element_at(col("changes"), 1)[0] == "buy", 
         element_at(col("changes"), 1)[1].cast("double")).alias("bid_price"),
    when(element_at(col("changes"), 1)[0] == "sell", 
         element_at(col("changes"), 1)[1].cast("double")).alias("ask_price")
).filter(col("bid_price").isNotNull() | col("ask_price").isNotNull()) \
 .withWatermark("event_time", "10 seconds") # FIX: Watermark applied before union


unified_stream = binance_silver.unionByName(coinbase_silver)

# Arbitrage calcuation
arbitrage_stream = unified_stream \
    .groupBy(
        window(col("event_time"), "1 second"),
        col("symbol")
    ) \
    .agg(
        max(when(col("exchange") == "Binance", col("bid_price"))).alias("Binance_bid"),
        min(when(col("exchange") == "Binance", col("ask_price"))).alias("Binance_ask"),
        max(when(col("exchange") == "Coinbase", col("bid_price"))).alias("Coinbase_bid"),
        min(when(col("exchange") == "Coinbase", col("ask_price"))).alias("Coinbase_ask")
    ) \
    .select(
        col("window.start").alias("time"),
        col("symbol"),
        "Binance_bid", "Binance_ask",
        "Coinbase_bid", "Coinbase_ask",
        (col("Binance_bid") - col("Coinbase_ask")).alias("spread_bn_bid_cb_ask"),
        (col("Coinbase_bid") - col("Binance_ask")).alias("spread_cb_bid_bn_ask")
    )


# SINK TO PARQUET
# Spark will wait for the 10-second watermark to pass before writing the file to guarantee no late data is missed.
query = arbitrage_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/gold/arbitrage_spreads/") \
    .option("checkpointLocation", "checkpoint/gold_arbitrage/") \
    .trigger(processingTime='10 seconds') \
    .start()

print("Streaming to Parquet started. Waiting for watermark to close the first windows (approx 15 seconds)...")

query.awaitTermination()