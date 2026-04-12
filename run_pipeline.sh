#!/bin/bash

echo "Starting Cryptocurrency Arbitrage Pipeline..."

# Start Ingestion streams in the background
python src/ingestion/binance_ws.py &
BINANCE_PID=$!
echo "Binance ingestion started (PID: $BINANCE_PID)"


python src/ingestion/coinbase_ws.py &
COINBASE_PID=$!
echo "Coinbase ingestion started (PID: $COINBASE_PID)"

# just few seconds to connect and buffer initial data to Kafka
sleep 5

# start the Apache Flink Streaming Job
echo "Starting Flink Normalization and Analytics Job..."
python src/streaming/flink_arb_pipeline.py

# Cleanup on exit
trap "kill $BINANCE_PID $COINBASE_PID; exit" INT TERM