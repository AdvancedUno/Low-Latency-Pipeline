# src/streaming/schemas.py

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType

# Binance depth5 snapshot schema
binance_schema = StructType([
    StructField("lastUpdateId", LongType(), True),
    StructField("bids", ArrayType(ArrayType(StringType())), True),
    StructField("asks", ArrayType(ArrayType(StringType())), True),
    StructField("receipt_timestamp", DoubleType(), True)
])

# Coinbase level2_batch schema
coinbase_schema = StructType([
    StructField("type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("changes", ArrayType(ArrayType(StringType())), True),
    StructField("time", StringType(), True),
    StructField("receipt_timestamp", DoubleType(), True)
])