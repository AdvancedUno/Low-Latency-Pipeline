# ./streaming/flink_arb_pipeline.py
import json
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
)

from pyflink.common import WatermarkStrategy, Types, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col

KAFKA_BOOTSTRAP = "localhost:9092"

def parse_binance(raw: str):
    """
    Normalize a Binance depth5 message (event_time_ms, exchange, bid, ask)
    """
    
    try:
        d = json.loads(raw)

        # get the highest bid and lowest ask
        bid = float(d["bids"][0][0]) if d.get("bids") else None
        ask = float(d["asks"][0][0]) if d.get("asks") else None
        
        # we put this during the ingestion
        ts = int(d["receipt_timestamp"])

        return (ts, "Binance", bid, ask)
    except Exception:
        return None

def parse_coinbase(raw: str):
    """
    Normalize a Coinbase l2update message (event_time_ms, exchange, bid, ask)
    """
    
    try:
        d = json.loads(raw)
        bid, ask = None, None

        # Coinbase sends changes as a list, we need to loop through to find buys and sells
        for change in d.get("changes", []):
            side, price = change[0], float(change[1])

            if side == "buy":
                bid = price
            elif side == "sell":
                ask = price

        ts = int(d["receipt_timestamp"])

        # if a message doesn't contain pricing data just skip it
        if bid is None and ask is None:
            return None
        
        return (ts, "Coinbase", bid, ask)
    except Exception:
        return None

class ArbitrageWindowFunction(ProcessWindowFunction):
    """
    For each 1 second tumbling window, compute arbitrage spreads
    """
    
    def process(self, key, context, elements):
        # Temporary lists to hold all prices seen in this 1 second window
        binance_bids, binance_asks = [], []
        coinbase_bids, coinbase_asks = [], []
        

        # just seperate the elements into corresponding lists
        for (ts, exchange, bid, ask) in elements:
            if exchange == "Binance":
                if bid: 
                    binance_bids.append(bid)

                if ask: 
                    binance_asks.append(ask)

            else:
                if bid: 
                    coinbase_bids.append(bid)
                if ask: 
                    coinbase_asks.append(ask)
                
        # Skip if lack data for a full comparison in this 1 second window
        if not (binance_bids and binance_asks and coinbase_bids and coinbase_asks):
            return  
            

        # find the absolute best prices across the entire 1-second window
        bn_bid = max(binance_bids)
        bn_ask = min(binance_asks)
        cb_bid = max(coinbase_bids)
        cb_ask = min(coinbase_asks)

        
        # Calculate Spread
        # buy low on Coinbase, sell high on Binance
        spread_bn_cb = bn_bid - cb_ask

        # buy low on Binance, sell high on Coinbase
        spread_cb_bn = cb_bid - bn_ask  

        # get the start time of this specific window
        window_start = context.window().start
        
        # arb_open is true if either spread is positive, 
        # which mean there is an opportunity to buy low on one exchange and sell high on the other
        result = {
            "window_start_ms": window_start,
            "Binance_bid": bn_bid,
            "Binance_ask": bn_ask,
            "Coinbase_bid": cb_bid,
            "Coinbase_ask": cb_ask,
            "spread_bn_bid_cb_ask": spread_bn_cb,
            "spread_cb_bid_bn_ask": spread_cb_bn,
            "arb_open": spread_bn_cb > 0 or spread_cb_bn > 0,
        }

        yield json.dumps(result)

def build_pipeline():
    # initialize the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Load the Kafka Connector JAR required by Flink to talk to Kafka
    jar_path = f"file://{os.getcwd()}/flink-sql-connector-kafka-3.0.1-1.18.jar"
    env.add_jars(jar_path)

    # Define the input source for Binance data from Kafka
    binance_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("binance-raw")
        .set_group_id("flink-arb-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    
    # Define the input source for Coinbase data from Kafka
    coinbase_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("coinbase-raw")
        .set_group_id("flink-arb-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # allow data to be up to 2 seconds late before discarding it.
    wm_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(2))
        .with_timestamp_assigner(TimestampAssigner.of(lambda record, _: record[0]))
    )

    # process raw Binance stream. Here I just do map (parse), filter (remove nulls), and assign watermarks
    binance_stream = (
        env.from_source(binance_source, WatermarkStrategy.no_watermarks(), "Binance Kafka")
        .map(parse_binance, output_type=Types.TUPLE([Types.LONG(), Types.STRING(), Types.FLOAT(), Types.FLOAT()]))
        .filter(lambda x: x is not None)
        .assign_timestamps_and_watermarks(wm_strategy)
    )
    
    # same here for Coinbase stream
    coinbase_stream = (
        env.from_source(coinbase_source, WatermarkStrategy.no_watermarks(), "Coinbase Kafka")
        .map(parse_coinbase, output_type=Types.TUPLE([Types.LONG(), Types.STRING(), Types.FLOAT(), Types.FLOAT()]))
        .filter(lambda x: x is not None)
        .assign_timestamps_and_watermarks(wm_strategy)
    )


    # combine streams, group into 1-second chunks
    unified = binance_stream.union(coinbase_stream)
    
    # this is where we need to aggregate
    # group all data under a single key ("BTC-USD") since we are only tracking one pair
    arb_stream = (
        unified
        .key_by(lambda x: "BTC-USD")  
        
        # chop the infinite stream into discrete 1-second chunks. 
        # uno: I think instead of sliding window we can just use the tumbling window
        .window(TumblingEventTimeWindows.of(Duration.of_seconds(1)))
        .process(ArbitrageWindowFunction(), output_type=Types.STRING())
    )


    # define the output destination to push calculated results back to Kafka
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("arb-results")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        # uno: just not to drop calculated results or you guys can change
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    
    arb_stream.sink_to(sink)
    env.execute("Bitcoin Arbitrage Pipeline")

    
    t_env = StreamTableEnvironment.create(env)
    
    # Define the schema for your Gold layer
    gold_schema = Types.ROW_NAMED(
        ["window_start_ms", "Binance_bid", "Binance_ask", "Coinbase_bid", "Coinbase_ask", "spread_bn_bid_cb_ask", "spread_cb_bid_bn_ask", "arb_open"],
        [Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.BOOLEAN()]
    )
    
    # Convert the string stream to a Row stream
    def json_to_row(json_str):
        d = json.loads(json_str)
        return (d["window_start_ms"], d["Binance_bid"], d["Binance_ask"], d["Coinbase_bid"], d["Coinbase_ask"], d["spread_bn_bid_cb_ask"], d["spread_cb_bid_bn_ask"], d["arb_open"])
        
    row_stream = arb_stream.map(json_to_row, output_type=gold_schema)
    
    # Convert DataStream to Table
    table = t_env.from_data_stream(row_stream)

    
    # Create the Parquet Sink Table Definition for the s3 
    # uno: please check it for me and we need to discuss what we want to keep for the s3 data and later snowflake
    t_env.execute_sql("""
        CREATE TABLE arbitrage_gold (
            window_start_ms BIGINT,
            Binance_bid DOUBLE,
            Binance_ask DOUBLE,
            Coinbase_bid DOUBLE,
            Coinbase_ask DOUBLE,
            spread_bn_bid_cb_ask DOUBLE,
            spread_cb_bid_bn_ask DOUBLE,
            arb_open BOOLEAN
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://crypto-arb-gold-yimeng/gold/arbitrage_spreads/',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '1 min'
        )
    """)
    
    # Insert the streaming table into the Parquet sink
    table.execute_insert("arbitrage_gold")

    env.execute("Bitcoin Arbitrage Pipeline")

if __name__ == "__main__":
    build_pipeline()