from dotenv import load_dotenv
import os
import streamlit as st
import pandas as pd
import snowflake.connector

load_dotenv()

st.set_page_config(page_title="Daily Arbitrage Report", layout="wide")

# ---------------------------
# Snowflake connection
# ---------------------------
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="CRYPTO_DB",
        schema="PUBLIC"
    )

def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

# ---------------------------
# Title
# ---------------------------
st.title("Daily Arbitrage Report")
st.caption("Summary of arbitrage activity for yesterday")

# ---------------------------
# KPI queries
# ---------------------------
kpi_query = """
SELECT
    COUNT(*) AS num_events,
    AVG(duration_seconds) AS avg_duration_seconds,
    MAX(peak_spread) AS max_peak_spread,
    AVG(peak_spread) AS avg_peak_spread
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
"""

direction_query = """
SELECT
    arb_direction,
    COUNT(*) AS event_count,
    AVG(duration_seconds) AS avg_duration_seconds,
    AVG(peak_spread) AS avg_peak_spread
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
GROUP BY 1
ORDER BY event_count DESC
"""

hourly_query = """
SELECT
    DATE_PART(hour, event_start_ts) AS hour,
    COUNT(*) AS event_count
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
GROUP BY 1
ORDER BY 1
"""

duration_bucket_query = """
SELECT
    CASE
        WHEN duration_seconds < 2 THEN '<2s'
        WHEN duration_seconds < 5 THEN '2-5s'
        WHEN duration_seconds < 10 THEN '5-10s'
        ELSE '10s+'
    END AS bucket,
    COUNT(*) AS count
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
GROUP BY 1
ORDER BY 1
"""

spread_bucket_query = """
SELECT
    CASE
        WHEN peak_spread < 5 THEN '<5'
        WHEN peak_spread < 15 THEN '5-15'
        WHEN peak_spread < 30 THEN '15-30'
        ELSE '30+'
    END AS bucket,
    COUNT(*) AS count
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
GROUP BY 1
ORDER BY 1
"""

top_events_query = """
SELECT
    event_start_ts,
    event_end_ts,
    duration_seconds,
    arb_direction,
    peak_spread,
    avg_spread,
    num_windows
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
ORDER BY peak_spread DESC
LIMIT 10
"""

# Optional fee-adjusted query
fee_adjusted_query = """
SELECT
    COUNT(*) AS profitable_events_after_fees
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
  AND peak_spread > 15
"""

# Average events per second
avg_events_per_sec = """
WITH per_second AS (
    SELECT
        DATE_TRUNC('second', event_start_ts) AS ts_sec,
        COUNT(*) AS events_per_sec
    FROM btc_quotes
    WHERE DATE(event_start_ts) = CURRENT_DATE - 1
    GROUP BY 1
)
SELECT AVG(events_per_sec) AS avg_events_per_sec
FROM per_second;"""

# Peak ingestion rate
peak_ingestion_rate = """
WITH per_second AS (
    SELECT
        DATE_TRUNC('second', event_start_ts) AS ts_sec,
        COUNT(*) AS events_per_sec
    FROM btc_quotes
    WHERE DATE(event_start_ts) = CURRENT_DATE - 1
    GROUP BY 1
)
SELECT MAX(events_per_sec) AS peak_events_per_sec
FROM per_second;"""

# Top 10 ingestion spikes
top_ingestion_spikes = """
SELECT *
FROM per_second
ORDER BY events_per_sec DESC
LIMIT 10;"""

# Total daily volume (events)
daily_volume = """
SELECT COUNT(*) AS total_events
FROM btc_quotes
WHERE DATE(event_ts) = CURRENT_DATE - 1;"""

# Data per day
daily_data = """
SELECT
    SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) AS total_bytes
FROM btc_quotes
WHERE DATE(event_ts) = CURRENT_DATE - 1;"""

# Events per hour
hourly_events = """
SELECT
    DATE_TRUNC('hour', event_ts) AS hour,
    COUNT(*) AS events_per_hour
FROM btc_quotes
WHERE DATE(event_ts) = CURRENT_DATE - 1
GROUP BY 1
ORDER BY 1;"""

# ---------------------------
# Load data
# ---------------------------
kpi_df = run_query(kpi_query)
direction_df = run_query(direction_query)
hourly_df = run_query(hourly_query)
duration_df = run_query(duration_bucket_query)
spread_df = run_query(spread_bucket_query)
top_events_df = run_query(top_events_query)
fee_df = run_query(fee_adjusted_query)

# ---------------------------
# KPIs
# ---------------------------
if not kpi_df.empty:
    row = kpi_df.iloc[0]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Events Yesterday", int(row["NUM_EVENTS"]) if pd.notna(row["NUM_EVENTS"]) else 0)
    c2.metric("Avg Duration (s)", f'{row["AVG_DURATION_SECONDS"]:.2f}' if pd.notna(row["AVG_DURATION_SECONDS"]) else "0.00")
    c3.metric("Max Peak Spread", f'{row["MAX_PEAK_SPREAD"]:.2f}' if pd.notna(row["MAX_PEAK_SPREAD"]) else "0.00")
    c4.metric("Avg Peak Spread", f'{row["AVG_PEAK_SPREAD"]:.2f}' if pd.notna(row["AVG_PEAK_SPREAD"]) else "0.00")
    c5.metric(
        "Profitable After Fees",
        int(fee_df.iloc[0]["PROFITABLE_EVENTS_AFTER_FEES"]) if not fee_df.empty and pd.notna(fee_df.iloc[0]["PROFITABLE_EVENTS_AFTER_FEES"]) else 0
    )

# ---------------------------
# Charts
# ---------------------------
left, right = st.columns(2)

with left:
    st.subheader("Events by Hour")
    if not hourly_df.empty:
        st.bar_chart(hourly_df.set_index("HOUR"))

    st.subheader("Duration Distribution")
    if not duration_df.empty:
        st.bar_chart(duration_df.set_index("BUCKET"))

with right:
    st.subheader("Spread Distribution")
    if not spread_df.empty:
        st.bar_chart(spread_df.set_index("BUCKET"))

    st.subheader("Direction Breakdown")
    if not direction_df.empty:
        st.bar_chart(direction_df.set_index("ARB_DIRECTION")[["EVENT_COUNT"]])

# ---------------------------
# Tables
# ---------------------------
st.subheader("Direction Summary")
st.dataframe(direction_df, use_container_width=True)

st.subheader("Top 10 Largest Events Yesterday")
st.dataframe(top_events_df, use_container_width=True)