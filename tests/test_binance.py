#
import websocket
import json

def on_message(ws, message):
    data = json.loads(message)
    print("BINANCE L2 UPDATE:\n", json.dumps(data, indent=2))

def on_error(ws, error):
    print(f"Error: {error}")

if __name__ == "__main__":
    ws_url = "wss://stream.binance.us:9443/ws/btcusdt@depth5@100ms"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error)
    print("Connecting to Binance...")
    ws.run_forever()