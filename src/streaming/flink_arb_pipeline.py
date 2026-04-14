#src/streaming/flink_arb_pipeline.py
import json
import os
import math
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.table import StreamTableEnvironment
from pyflink.common.time import Time
from pyflink.common import WatermarkStrategy, Types, Duration, Row

KAFKA_BOOTSTRAP = "localhost:9092"
S3_PATH = "s3a://crypto-arb-gold-yimeng/gold/arbitrage_spreads/" # update this to local if you don't have S3 access



#  Parsers 
def parse_binance(raw: str):
    try:
        d = json.loads(raw)

        bid = float(d["bids"][0][0]) if d.get("bids") else math.nan
        ask = float(d["asks"][0][0]) if d.get("asks") else math.nan
        ts  = int(d["receipt_timestamp"])
        return (ts, "Binance", bid, ask)
    except Exception:
        return None

def parse_coinbase(raw: str):
    try:
        d = json.loads(raw)
        # Initialize with math.nan
        bid, ask = math.nan, math.nan
        for change in d.get("changes", []):
            side, price = change[0], float(change[1])
            if side == "buy":
                bid = price
            elif side == "sell":
                ask = price
        ts = int(d["receipt_timestamp"])
        
        # If both are NaN, drop the record entirely
        if math.isnan(bid) and math.isnan(ask):
            return None
            
        return (ts, "Coinbase", bid, ask)
    except Exception:
        return None

#  Window Function
class ArbitrageWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        binance_bids, binance_asks   = [], []
        coinbase_bids, coinbase_asks = [], []

        for (ts, exchange, bid, ask) in elements:
            if exchange == "Binance":
                if not math.isnan(bid): binance_bids.append(bid)
                if not math.isnan(ask): binance_asks.append(ask)
            else:
                if not math.isnan(bid): coinbase_bids.append(bid)
                if not math.isnan(ask): coinbase_asks.append(ask)

        if not (binance_bids and binance_asks and coinbase_bids and coinbase_asks):
            return

        bn_bid, bn_ask = max(binance_bids), min(binance_asks)
        cb_bid, cb_ask = max(coinbase_bids), min(coinbase_asks)

        spread_bn_cb = bn_bid - cb_ask   # buy on Coinbase, sell on Binance
        spread_cb_bn = cb_bid - bn_ask   # buy on Binance,  sell on Coinbase

        result = {
            "window_start_ms":      context.window().start,
            "Binance_bid":          bn_bid,
            "Binance_ask":          bn_ask,
            "Coinbase_bid":         cb_bid,
            "Coinbase_ask":         cb_ask,
            "spread_bn_bid_cb_ask": spread_bn_cb,
            "spread_cb_bid_bn_ask": spread_cb_bn,
            "arb_open":             spread_bn_cb > 0 or spread_cb_bn > 0,
        }
        yield json.dumps(result)

#  Timestamp Assigner 
class TupleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[0])

#  Pipeline 
def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    #  Kafka Sources 
    def make_kafka_source(topic: str) -> KafkaSource:
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_topics(topic)
            .set_group_id("flink-arb-group")
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

    binance_source  = make_kafka_source("binance-raw")
    coinbase_source = make_kafka_source("coinbase-raw")

    #  Watermark Strategy 
    wm_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(2))
        .with_timestamp_assigner(TupleTimestampAssigner())
    )

    tuple_type = Types.TUPLE([Types.LONG(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()])

    #  DataStreams 
    binance_stream = (
        env.from_source(binance_source, WatermarkStrategy.no_watermarks(), "Binance Kafka")
        .map(parse_binance, output_type=tuple_type)
        .filter(lambda x: x is not None)
        .assign_timestamps_and_watermarks(wm_strategy)
    )

    coinbase_stream = (
        env.from_source(coinbase_source, WatermarkStrategy.no_watermarks(), "Coinbase Kafka")
        .map(parse_coinbase, output_type=tuple_type)
        .filter(lambda x: x is not None)
        .assign_timestamps_and_watermarks(wm_strategy)
    )

    #  Arbitrage Window 
    arb_stream = (
        binance_stream
        .union(coinbase_stream)
        .key_by(lambda x: "BTC-USD")
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .process(ArbitrageWindowFunction(), output_type=Types.STRING())
    )

    #  Table API: convert Row stream 
    t_env = StreamTableEnvironment.create(env)

    gold_schema = Types.ROW_NAMED(
        [
            "window_start_ms", "Binance_bid", "Binance_ask",
            "Coinbase_bid",    "Coinbase_ask",
            "spread_bn_bid_cb_ask", "spread_cb_bid_bn_ask", "arb_open",
        ],
        [
            Types.LONG(),   Types.DOUBLE(), Types.DOUBLE(),
            Types.DOUBLE(), Types.DOUBLE(),
            Types.DOUBLE(), Types.DOUBLE(), Types.BOOLEAN(),
        ]
    )

    def json_to_row(json_str):
        d = json.loads(json_str)
        # Use Flink's Row object instead of a Python tuple
        return Row(
            window_start_ms=d["window_start_ms"],
            Binance_bid=d["Binance_bid"],
            Binance_ask=d["Binance_ask"],
            Coinbase_bid=d["Coinbase_bid"],
            Coinbase_ask=d["Coinbase_ask"],
            spread_bn_bid_cb_ask=d["spread_bn_bid_cb_ask"],
            spread_cb_bid_bn_ask=d["spread_cb_bid_bn_ask"],
            arb_open=d["arb_open"]
        )

    row_stream = arb_stream.map(json_to_row, output_type=gold_schema)
    table      = t_env.from_data_stream(row_stream)

    #  Sink DDL: S3 Parquet
    t_env.execute_sql(f"""
        CREATE TABLE arbitrage_gold_s3 (
            window_start_ms       BIGINT,
            Binance_bid           DOUBLE,
            Binance_ask           DOUBLE,
            Coinbase_bid          DOUBLE,
            Coinbase_ask          DOUBLE,
            spread_bn_bid_cb_ask  DOUBLE,
            spread_cb_bid_bn_ask  DOUBLE,
            arb_open              BOOLEAN
        ) WITH (
            'connector'                          = 'filesystem',
            'path'                               = '{S3_PATH}',
            'format'                             = 'parquet',
            'sink.rolling-policy.file-size'      = '128MB',
            'sink.rolling-policy.rollover-interval' = '1 min'
        )
    """)

    #  Sink DDL: Kafka JSON 
    t_env.execute_sql(f"""
        CREATE TABLE arbitrage_gold_kafka (
            window_start_ms       BIGINT,
            Binance_bid           DOUBLE,
            Binance_ask           DOUBLE,
            Coinbase_bid          DOUBLE,
            Coinbase_ask          DOUBLE,
            spread_bn_bid_cb_ask  DOUBLE,
            spread_cb_bid_bn_ask  DOUBLE,
            arb_open              BOOLEAN
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'arb-results',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format'                       = 'json'
        )
    """)

    #  Execute both sinks simultaneously 
    statement_set = t_env.create_statement_set()
    statement_set.add_insert("arbitrage_gold_s3",    table)
    statement_set.add_insert("arbitrage_gold_kafka", table)

    print("Submitting Flink job (S3 Parquet + Kafka JSON sinks)...")
    statement_set.execute().wait()


if __name__ == "__main__":
    build_pipeline()