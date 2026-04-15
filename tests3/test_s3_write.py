"""
from pyspark.sql import SparkSession
import os
os.environ["HADOOP_OPTS"] = "-Dhadoop.native.lib=false"

spark = (
    SparkSession.builder
    .appName("S3SmokeTest")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.io.native.lib.available", "false")

    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")

    
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")   # 24h
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000")       # 5m
    .config("spark.hadoop.fs.s3a.retry.interval", "500")          # 500ms
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100") # 100ms

    .getOrCreate()
)

conf = spark.sparkContext._jsc.hadoopConfiguration()
it = conf.iterator()

df = spark.createDataFrame(
    [(1, "ok"), (2, "s3-test")],
    ["id", "status"]
)

out = "s3a://crypto-arb-gold-yimeng/test/smoke/"
df.coalesce(1).write.mode("overwrite").parquet(out)

print(f"Wrote to {out}")
spark.stop()
"""
from pyspark.sql import SparkSession


def main() -> None:
    out = "s3a://crypto-arb-gold-yimeng/test/smoke/"

    spark = (
        SparkSession.builder
        .appName("S3SmokeTest")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")
        .getOrCreate()
    )

    try:
        df = spark.range(10).coalesce(1)

        print(f"Writing to {out} ...")
        df.write.mode("overwrite").parquet(out)
        print("SUCCESS: write completed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()