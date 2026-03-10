# ./ingestion/binance_ws.py
import asyncio
import websockets
import json
import time
import os

async def stream_binance():
    url = "wss://stream.binance.us:9443/ws/btcusdt@depth5" 
    os.makedirs("data/bronze/binance", exist_ok=True)
    print(f"Connecting to {url}...")
    
    buffer = []
    try:
        async with websockets.connect(url) as ws:
            print("Connected to Binance.US successfully!")
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                data['receipt_timestamp'] = time.time() * 1000 
                buffer.append(data)
                
                # Write every 10 messages to a new file to optimize Spark I/O
                if len(buffer) >= 10:
                    ts = int(time.time() * 1000)
                    filename = f"data/bronze/binance/bn_{ts}.json"
                    with open(filename, "w") as f:
                        for item in buffer:
                            f.write(json.dumps(item) + "\n")
                    buffer = [] 
    except Exception as e:
        print(f"Binance Error: {e}")

if __name__ == "__main__":
    asyncio.run(stream_binance())