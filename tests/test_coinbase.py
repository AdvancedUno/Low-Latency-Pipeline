
import websocket
import json

def on_open(ws):
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["level2"]
    }
    ws.send(json.dumps(subscribe_message))
    print("Subscription message sent to Coinbase.")

def on_message(ws, message):
    data = json.loads(message)
    print("COINBASE L2 UPDATE:\n", json.dumps(data, indent=2))

def on_error(ws, error):
    print(f"Error: {error}")

if __name__ == "__main__":
    ws_url = "wss://ws-feed.exchange.coinbase.com"
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error)
    print("Connecting to Coinbase...")
    ws.run_forever()