# Low-Latency-Pipeline

## 1. What Has Been Completed

The foundational data engineering pipeline is now fully operational. We are successfully capturing live market data from two distinct exchanges, normalizing their disparate JSON schemas, time-aligning the data, and calculating the arbitrage spread in real-time.

### System Architecture Implemented:
* **Bronze Layer (Ingestion):** * Built asynchronous Python WebSocket producers (`binance_ws.py`, `coinbase_ws.py`).
  * Optimized disk I/O by batch-writing 10 messages per JSON file to prevent Spark file-listener bottlenecking.
  * Successfully bypassed regional API blocks by routing through `stream.binance.us`.

* **Silver Layer (Normalization):**
  * Defined strict PySpark `StructType` schemas for both exchanges.
  * Mapped Binance's snapshot data and Coinbase's delta updates into a single unified stream.
  * Standardized trading pairs (e.g., mapping `BTCUSDT` and `BTC-USD` to a unified `BTC-USD` key).

* **Gold Layer (Arbitrage Aggregation):**
  * Implemented a **10-second watermark** to handle late-arriving data and network jitter.
  * Grouped data into **1-second tumbling windows**.
  * Used conditional aggregation (streaming pivot) to place Binance and Coinbase prices on the exact same row.
  * Calculated the real-time spread: `Spread = Binance_Bid - Coinbase_Ask` (and vice versa).
  
* **Storage Sink:**
  * The final Gold table is continuously written to high-performance **Parquet** files in the `data/gold/arbitrage_spreads/` directory using Spark's `append` mode.

---

## 2. How to Test & Run the Code Locally

To see the pipeline run end-to-end on your machine, you will need to open three separate terminal windows. 

### Prerequisites
Ensure your virtual environment is active and dependencies are installed (`pip install -r requirements.txt`). 
*Note for Windows users: Ensure the `setup_windows_spark.py` script has been run to configure the Hadoop binaries.*

### Step 1: Clean the Slate
Before starting, clear out any old data to prevent Spark from lagging as it tries to process historical test files alongside live data.
**Remove the data folder if it is already exist for testing**


### Step 2: Start the Ingestion Producers
These scripts will connect to the exchanges and begin dropping JSON batches into the Bronze folder.

Terminal 1: python src/ingestion/binance_ws.py

Terminal 2: python src/ingestion/coinbase_ws.py

### Step 3: Start the Spark Consumer
This script reads the Bronze JSONs, calculates the spreads, and writes the final Parquet files to the Gold folder.

Terminal 3: python src/streaming/normalize_l2.py

Note: Because we are using a 10-second watermark, it will take about 15-20 seconds for the first .parquet files to appear in data/gold/arbitrage_spreads/.

