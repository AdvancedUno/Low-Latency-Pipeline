# # ./ingestion/coinbase_ws.py
# import asyncio
# import websockets
# import json
# import time
# import os

# async def stream_coinbase():
#     url = "wss://ws-feed.exchange.coinbase.com"
#     subscribe_msg = {
#         "type": "subscribe",
#         "product_ids": ["BTC-USD"],
#         "channels": ["level2_batch"]
#     }
    
#     os.makedirs("data/bronze/coinbase", exist_ok=True)
#     print(f"Connecting to {url}...")

#     buffer = []
#     try:
#         async with websockets.connect(url, max_size=None, ping_interval=30) as ws:
#             print("Connected to Coinbase successfully!")
#             await ws.send(json.dumps(subscribe_msg))
            
#             while True:
#                 msg = await ws.recv()
#                 data = json.loads(msg)
                
#                 if data.get('type') == 'l2update':
#                     data['receipt_timestamp'] = time.time() * 1000 
#                     buffer.append(data)
                    
#                     if len(buffer) >= 10:
#                         ts = int(time.time() * 1000)
#                         filename = f"data/bronze/coinbase/cb_{ts}.json"
#                         with open(filename, "w") as f:
#                             for item in buffer:
#                                 f.write(json.dumps(item) + "\n")
#                         buffer = []
                        
#     except Exception as e:
#         print(f"Coinbase Error: {e}")

# if __name__ == "__main__":
#     asyncio.run(stream_coinbase())

# ./ingestion/coinbase_ws.py
import asyncio
import websockets
import json
import time
import os
import ssl
import certifi

async def stream_coinbase():
    url = "wss://ws-feed.exchange.coinbase.com"
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["level2_batch"]
    }

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    os.makedirs("data/bronze/coinbase", exist_ok=True)
    print(f"Connecting to {url}...")

    buffer = []
    try:
        async with websockets.connect(
            url,
            ssl=ssl_context,
            max_size=None,
            ping_interval=30
        ) as ws:
            print("Connected to Coinbase successfully!")
            await ws.send(json.dumps(subscribe_msg))

            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get("type") == "l2update":
                    data["receipt_timestamp"] = time.time() * 1000
                    buffer.append(data)

                    if len(buffer) >= 10:
                        ts = int(time.time() * 1000)
                        filename = f"data/bronze/coinbase/cb_{ts}.json"
                        with open(filename, "w") as f:
                            for item in buffer:
                                f.write(json.dumps(item) + "\n")
                        buffer = []

    except Exception as e:
        print(f"Coinbase Error: {e}")

if __name__ == "__main__":
    asyncio.run(stream_coinbase())