# analytics/live_dashboard.py
import json
from kafka import KafkaConsumer
from collections import deque
import statistics

consumer = KafkaConsumer(
    "arb-results",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="analytics-group"
)

# Rolling window for stats (last 60 seconds of 1s windows = 60 records)
WINDOW = 60
spreads_bn_cb = deque(maxlen=WINDOW)
spreads_cb_bn = deque(maxlen=WINDOW)
arb_open_count = 0
total_windows = 0

print("Live Arbitrage Dashboard — streaming from Flink output\n")
print(f"{'Time':>14} | {'BN Bid':>10} | {'CB Ask':>10} | {'Spread (BN→CB)':>15} | {'Spread (CB→BN)':>15} | {'ARB?':>5}")
print("-" * 80)

for msg in consumer:
    r = msg.value
    total_windows += 1
    s1 = r["spread_bn_bid_cb_ask"]
    s2 = r["spread_cb_bid_bn_ask"]
    spreads_bn_cb.append(s1)
    spreads_cb_bn.append(s2)
    if r["arb_open"]:
        arb_open_count += 1

    from datetime import datetime
    ts = datetime.fromtimestamp(r["window_start_ms"] / 1000).strftime("%H:%M:%S.%f")[:12]
    arb_flag = "OK" if r["arb_open"] else " "

    print(f"{ts:>14} | {r['Binance_bid']:>10.2f} | {r['Coinbase_ask']:>10.2f} | "
          f"{s1:>+15.4f} | {s2:>+15.4f} | {arb_flag:>5}")

    # Print rolling stats every 10 windows
    if total_windows % 10 == 0 and len(spreads_bn_cb) > 1:
        print(f"\n  === Rolling Stats (last {len(spreads_bn_cb)} windows) ===")
        print(f"  Spread BN→CB: mean={statistics.mean(spreads_bn_cb):+.4f}, "
              f"stdev={statistics.stdev(spreads_bn_cb):.4f}, "
              f"max={max(spreads_bn_cb):+.4f}")
        print(f"  Spread CB→BN: mean={statistics.mean(spreads_cb_bn):+.4f}, "
              f"stdev={statistics.stdev(spreads_cb_bn):.4f}, "
              f"max={max(spreads_cb_bn):+.4f}")
        print(f"  Arb open rate: {arb_open_count}/{total_windows} windows "
              f"({100*arb_open_count/total_windows:.1f}%)\n")