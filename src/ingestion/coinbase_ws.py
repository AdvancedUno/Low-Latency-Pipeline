# ./ingestion/coinbase_ws.py

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
TOPIC = "coinbase-raw"
WS_URL = "wss://ws-feed.exchange.coinbase.com"
RECONNECT_DELAY = 5

SUBSCRIBE_MSG = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channels": ["level2_batch"]
}



async def stream_coinbase(max_runtime_seconds=None):
    
    ssl_context = ssl.create_default_context(cafile=certifi.where())


    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


    while True:
        try:
            print(f"Connecting to Coinbase at {WS_URL}...")
            async with websockets.connect(WS_URL, ssl=ssl_context, ping_interval=30) as ws:
                await ws.send(json.dumps(SUBSCRIBE_MSG))
                print("Connected and subscribed to Coinbase!")
                
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    if data.get("type") == "l2update":
                        data["receipt_timestamp"] = time.time() * 1000
                        producer.send(TOPIC, value=data)
        except Exception as e:
            print(f"Coinbase error/disconnect: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-runtime-seconds", type=int, default=None)
    args = parser.parse_args()

    asyncio.run(stream_coinbase(args.max_runtime_seconds))