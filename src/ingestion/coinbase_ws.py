# ./ingestion/coinbase_ws.py

import asyncio
import websockets
import json
import time
import os
import ssl
import certifi

OUTPUT_DIR = "data/bronze/coinbase"
WS_URL = "wss://ws-feed.exchange.coinbase.com"
BATCH_SIZE = 10
RECONNECT_DELAY = 5

SUBSCRIBE_MSG = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channels": ["level2_batch"]
}

def write_buffer(buffer):
    """Write buffered messages to a newline-delimited JSON file."""
    if not buffer:
        return

    ts = int(time.time() * 1000)
    filename = os.path.join(OUTPUT_DIR, f"cb_{ts}.json")

    with open(filename, "w") as f:
        for item in buffer:
            f.write(json.dumps(item) + "\n")

    print(f"Wrote {len(buffer)} records to {filename}")

async def stream_coinbase():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    while True:
        buffer = []

        try:
            print(f"Connecting to {WS_URL}...")
            async with websockets.connect(
                WS_URL,
                ssl=ssl_context,
                max_size=None,
                ping_interval=30
            ) as ws:
                print("Connected to Coinbase successfully!")
                await ws.send(json.dumps(SUBSCRIBE_MSG))
                print("Subscription message sent.")

                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)

                    if data.get("type") == "l2update":
                        data["receipt_timestamp"] = time.time() * 1000
                        buffer.append(data)

                        if len(buffer) >= BATCH_SIZE:
                            write_buffer(buffer)
                            buffer = []

        except KeyboardInterrupt:
            print("Stopping Coinbase stream...")
            write_buffer(buffer)
            raise

        except Exception as e:
            print(f"Coinbase error: {e}")
            write_buffer(buffer)
            print(f"Reconnect attempt in {RECONNECT_DELAY} seconds...")
            await asyncio.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    asyncio.run(stream_coinbase())