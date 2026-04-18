import json
from collections import deque

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:9092"
TOPIC = "arb-results"
MAX_ROWS = 500

st.set_page_config(page_title="Crypto Arbitrage Dashboard", layout="wide")
st.title("Live Crypto Arbitrage Dashboard")

# Auto-refresh every 2 seconds
st.autorefresh(interval=2000, key="dashboard_refresh")

# Keep recent rows across reruns
if "rows" not in st.session_state:
    st.session_state.rows = deque(maxlen=MAX_ROWS)

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    consumer_timeout_ms=1000,
)

# Pull available messages into session state
for msg in consumer:
    st.session_state.rows.append(msg.value)

# If no data yet, stop early
if not st.session_state.rows:
    st.info("Waiting for live arbitrage data...")
    st.stop()

# Build DataFrame
df = pd.DataFrame(list(st.session_state.rows))
df["window_start"] = pd.to_datetime(df["window_start_ms"], unit="ms")

latest = df.iloc[-1]

# Compute extra metrics
recent_open_count = int(df["arb_open"].tail(50).sum()) if "arb_open" in df else 0
max_spread_bn_cb = df["spread_bn_bid_cb_ask"].tail(50).max()
max_spread_cb_bn = df["spread_cb_bid_bn_ask"].tail(50).max()

# Status banner
if latest["arb_open"]:
    st.success("Arbitrage opportunity currently open")
else:
    st.warning("No arbitrage opportunity currently open")

# Top metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("BN Bid - CB Ask", f"{latest['spread_bn_bid_cb_ask']:.4f}")
c2.metric("CB Bid - BN Ask", f"{latest['spread_cb_bid_bn_ask']:.4f}")
c3.metric("Arb Open", "Yes" if latest["arb_open"] else "No")
c4.metric("Open Windows (last 50)", f"{recent_open_count}")

# Exchange prices
st.subheader("Latest Exchange Prices")
p1, p2, p3, p4 = st.columns(4)
p1.metric("Binance Bid", f"{latest['Binance_bid']:.4f}")
p2.metric("Binance Ask", f"{latest['Binance_ask']:.4f}")
p3.metric("Coinbase Bid", f"{latest['Coinbase_bid']:.4f}")
p4.metric("Coinbase Ask", f"{latest['Coinbase_ask']:.4f}")

# Spread chart
st.subheader("Recent Spread History")
chart_df = df[["window_start", "spread_bn_bid_cb_ask", "spread_cb_bid_bn_ask"]].set_index("window_start")
st.line_chart(chart_df, use_container_width=True)

# Extra summary metrics
st.subheader("Recent Summary")
s1, s2 = st.columns(2)
s1.metric("Max BN→CB Spread (last 50)", f"{max_spread_bn_cb:.4f}")
s2.metric("Max CB→BN Spread (last 50)", f"{max_spread_cb_bn:.4f}")

# Recent records table
st.subheader("Latest Records")
st.dataframe(
    df.tail(20)[[
        "window_start",
        "Binance_bid",
        "Binance_ask",
        "Coinbase_bid",
        "Coinbase_ask",
        "spread_bn_bid_cb_ask",
        "spread_cb_bid_bn_ask",
        "arb_open"
    ]],
    use_container_width=True
)