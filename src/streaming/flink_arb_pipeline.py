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


KAFKA_BOOTSTRAP = "localhost:9092"

def parse_binance(raw: str):
    """
    Normalize a Binance depth5 message (event_time_ms, exchange, bid, ask)
    """
    
    try:
        d = json.loads(raw)
        bid = float(d["bids"][0][0]) if d.get("bids") else None
        ask = float(d["asks"][0][0]) if d.get("asks") else None
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
        for change in d.get("changes", []):
            side, price = change[0], float(change[1])
            if side == "buy":
                bid = price
            elif side == "sell":
                ask = price
        ts = int(d["receipt_timestamp"])
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
        binance_bids, binance_asks = [], []
        coinbase_bids, coinbase_asks = [], []
        
        for (ts, exchange, bid, ask) in elements:
            if exchange == "Binance":
                if bid: binance_bids.append(bid)
                if ask: binance_asks.append(ask)
            else:
                if bid: coinbase_bids.append(bid)
                if ask: coinbase_asks.append(ask)
                
        # Skip if we lack data for a full comparison in this 1-second window
        if not (binance_bids and binance_asks and coinbase_bids and coinbase_asks):
            return  
            
        bn_bid = max(binance_bids)
        bn_ask = min(binance_asks)
        cb_bid = max(coinbase_bids)
        cb_ask = min(coinbase_asks)
        
        # Calculate Spread
        spread_bn_cb = bn_bid - cb_ask   # profit buying on CB, selling on BN
        spread_cb_bn = cb_bid - bn_ask   # profit buying on BN, selling on CB
        window_start = context.window().start
        
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
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Required for Kafka integration in Flink
    jar_path = f"file://{os.getcwd()}/flink-sql-connector-kafka-3.0.1-1.18.jar"
    env.add_jars(jar_path)

    # WHAT CHANGED: Reading from Kafka instead of local Bronze JSON directories.
    binance_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("binance-raw")
        .set_group_id("flink-arb-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    
    coinbase_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("coinbase-raw")
        .set_group_id("flink-arb-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Handle out-of-order data up to 2 seconds late
    wm_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(2))
        .with_timestamp_assigner(TimestampAssigner.of(lambda record, _: record[0]))
    )

    binance_stream = (
        env.from_source(binance_source, WatermarkStrategy.no_watermarks(), "Binance Kafka")
        .map(parse_binance, output_type=Types.TUPLE([Types.LONG(), Types.STRING(), Types.FLOAT(), Types.FLOAT()]))
        .filter(lambda x: x is not None)
        .assign_timestamps_and_watermarks(wm_strategy)
    )
    
    coinbase_stream = (
        env.from_source(coinbase_source, WatermarkStrategy.no_watermarks(), "Coinbase Kafka")
        .map(parse_coinbase, output_type=Types.TUPLE([Types.LONG(), Types.STRING(), Types.FLOAT(), Types.FLOAT()]))
        .filter(lambda x: x is not None)
        .assign_timestamps_and_watermarks(wm_strategy)
    )

    # Combine streams, group into 1-second chunks, and apply the logic
    unified = binance_stream.union(coinbase_stream)
    
    arb_stream = (
        unified
        .key_by(lambda x: "BTC-USD")  
        .window(TumblingEventTimeWindows.of(Duration.of_seconds(1)))
        .process(ArbitrageWindowFunction(), output_type=Types.STRING())
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("arb-results")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    
    arb_stream.sink_to(sink)
    env.execute("Bitcoin Arbitrage Pipeline")

if __name__ == "__main__":
    build_pipeline()