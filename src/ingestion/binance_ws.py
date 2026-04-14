# src/ingestion/binance_ws.py

import asyncio
import websockets
import json
import time
import os
import ssl
import certifi
import argparse
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "binance-raw"
WS_URL = "wss://stream.binance.us:9443/ws/btcusdt@depth5"
RECONNECT_DELAY = 5


async def stream_binance():

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    end_time = None

    while True:
        try:
            print(f"Connecting to Binance...")
            async with websockets.connect(WS_URL, ssl=ssl_context) as ws:
                print("Connected!")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    # receipt time for Flink watermarking later
                    data["receipt_timestamp"] = time.time() * 1000
                    
                    
                    # push immediately to Kafka with no batching, zero latency
                    producer.send(TOPIC, value=data)
                    
        except Exception as e:
            print(f"Binance error: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
 

if __name__ == "__main__":

    asyncio.run(stream_binance())