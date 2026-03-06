import websocket
import json

def on_open(ws):
    subscribe_message = {
        "event": "subscribe",
        "pair": ["XBT/USD"],
        "subscription": {
            "name": "book",
            "depth": 10
        }
    }
    ws.send(json.dumps(subscribe_message))
    print("Subscription message sent to Kraken.")

def on_message(ws, message):
    data = json.loads(message)
    if isinstance(data, list):
        print("KRAKEN L2 UPDATE:\n", json.dumps(data, indent=2))

def on_error(ws, error):
    print(f"Error: {error}")

if __name__ == "__main__":
    ws_url = "wss://ws.kraken.com"
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error)
    print("Connecting to Kraken...")
    ws.run_forever()