# ./ingestion/binance_ws.py

import asyncio
import websockets
import json
import time
import os
import ssl
import certifi
import argparse

OUTPUT_DIR = "data/bronze/binance"
WS_URL = "wss://stream.binance.us:9443/ws/btcusdt@depth5"
BATCH_SIZE = 10
RECONNECT_DELAY = 5

def write_buffer(buffer):
    if not buffer:
        return

    ts = int(time.time() * 1000)
    filename = os.path.join(OUTPUT_DIR, f"bn_{ts}.json")

    with open(filename, "w") as f:
        for item in buffer:
            f.write(json.dumps(item) + "\n")

    print(f"Wrote {len(buffer)} records to {filename}")

async def stream_binance(max_runtime_seconds=None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    end_time = None
    if max_runtime_seconds is not None:
        end_time = time.time() + max_runtime_seconds

    while True:
        buffer = []

        try:
            print(f"Connecting to {WS_URL}...")
            async with websockets.connect(WS_URL, ssl=ssl_context) as ws:
                print("Connected to Binance.US successfully!")

                while True:
                    # stop cleanly if time limit reached
                    if end_time is not None and time.time() >= end_time:
                        print("Reached max runtime. Flushing remaining Binance data and stopping.")
                        write_buffer(buffer)
                        return

                    msg = await ws.recv()
                    data = json.loads(msg)
                    data["receipt_timestamp"] = time.time() * 1000
                    buffer.append(data)

                    if len(buffer) >= BATCH_SIZE:
                        write_buffer(buffer)
                        buffer = []

        except Exception as e:
            print(f"Binance error: {e}")
            write_buffer(buffer)

            if end_time is not None and time.time() >= end_time:
                print("Reached max runtime during recovery. Stopping Binance stream.")
                return

            print(f"Reconnect attempt in {RECONNECT_DELAY} seconds...")
            await asyncio.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-runtime-seconds", type=int, default=None)
    args = parser.parse_args()

    asyncio.run(stream_binance(args.max_runtime_seconds))