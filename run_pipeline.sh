#!/bin/bash

# Start docker compose
echo "Starting Kafka and Zookeeper..."
docker-compose up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Run all pipeline components in parallel
echo "Starting pipeline..."

python src/ingestion/binance_ws.py &
BINANCE_PID=$!

python src/ingestion/coinbase_ws.py &
COINBASE_PID=$!

python src/streaming/flink_arb_pipeline.py &
FLINK_PID=$!

python src/analytics/live_dashboard.py &
DASHBOARD_PID=$!

echo "Pipeline running. Press Ctrl+C to stop."

# On Ctrl+C, kill all background processes
trap "echo 'Stopping pipeline...'; kill $BINANCE_PID $COINBASE_PID $FLINK_PID $DASHBOARD_PID; docker-compose down; exit 0" SIGINT

# Wait forever until Ctrl+C
wait