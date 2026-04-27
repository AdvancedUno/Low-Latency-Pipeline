from dotenv import load_dotenv
import os
import streamlit as st
import pandas as pd
import snowflake.connector

load_dotenv()

st.set_page_config(page_title="Daily Arbitrage Report", layout="wide")


# Snowflake connection
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

# Title

st.title("Daily Arbitrage Report")
st.caption("Summary of arbitrage activity for yesterday")


# KPI queries
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
    COUNT(*) AS count,
    CASE
        WHEN duration_seconds < 2 THEN 1
        WHEN duration_seconds < 5 THEN 2
        WHEN duration_seconds < 10 THEN 3
        ELSE 4
    END AS bucket_order
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
GROUP BY 1, 3
ORDER BY bucket_order
"""

spread_bucket_query = """
SELECT
    CASE
        WHEN peak_spread < 5 THEN '<5'
        WHEN peak_spread < 15 THEN '5-15'
        WHEN peak_spread < 30 THEN '15-30'
        ELSE '30+'
    END AS bucket,
    COUNT(*) AS count,
    CASE
        WHEN peak_spread < 5 THEN 1
        WHEN peak_spread < 15 THEN 2
        WHEN peak_spread < 30 THEN 3
        ELSE 4
    END AS bucket_order
FROM arb_opportunity_events
WHERE DATE(event_start_ts) = CURRENT_DATE - 1
GROUP BY 1, 3
ORDER BY bucket_order
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

fee_adjusted_query = """
WITH fee_windows AS (
    SELECT
        ts,
        arb_direction,
        max_positive_spread,

        CASE
            WHEN arb_direction = 'BUY_BINANCE_SELL_COINBASE'
                THEN binance_ask
            WHEN arb_direction = 'BUY_COINBASE_SELL_BINANCE'
                THEN coinbase_ask
        END AS buy_price,

        max_positive_spread -
        (
            CASE
                WHEN arb_direction = 'BUY_BINANCE_SELL_COINBASE'
                    THEN binance_ask
                WHEN arb_direction = 'BUY_COINBASE_SELL_BINANCE'
                    THEN coinbase_ask
            END * 0.002 * 0.01
        ) AS net_spread_after_fees
    FROM arb_clean
    WHERE DATE(ts) = CURRENT_DATE - 1
      AND arb_open = TRUE
),
flagged AS (
    SELECT
        *,
        LAG(ts) OVER (ORDER BY ts) AS prev_ts,
        LAG(arb_direction) OVER (ORDER BY ts) AS prev_direction
    FROM fee_windows
),
event_flags AS (
    SELECT
        *,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN DATEDIFF('second', prev_ts, ts) > 1 THEN 1
            WHEN arb_direction <> prev_direction THEN 1
            ELSE 0
        END AS is_new_event
    FROM flagged
),
event_ids AS (
    SELECT
        *,
        SUM(is_new_event) OVER (
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS event_id
    FROM event_flags
),
event_profitability AS (
    SELECT
        event_id,
        MIN(ts) AS event_start_ts,
        MAX(ts) AS event_end_ts,
        MAX(max_positive_spread) AS peak_raw_spread,
        MAX(net_spread_after_fees) AS peak_net_spread_after_fees,
        AVG(net_spread_after_fees) AS avg_net_spread_after_fees
    FROM event_ids
    GROUP BY event_id
)
SELECT
    COUNT(*) AS profitable_events_after_fees
FROM event_profitability
WHERE peak_net_spread_after_fees > 0;
"""

# Average events per second
avg_events_per_sec = """
WITH per_second AS (
    SELECT
        DATE_TRUNC('second', ts) AS ts_sec,
        COUNT(*) AS events_per_sec
    FROM arb_clean
    WHERE DATE(ts) = CURRENT_DATE - 1
    GROUP BY 1
)
SELECT AVG(events_per_sec) AS avg_events_per_sec
FROM per_second;"""

# Peak ingestion rate
peak_ingestion_rate = """
WITH per_second AS (
    SELECT
        DATE_TRUNC('second', ts) AS ts_sec,
        COUNT(*) AS events_per_sec
    FROM arb_clean
    WHERE DATE(ts) = CURRENT_DATE - 1
    GROUP BY 1
)
SELECT MAX(events_per_sec) AS peak_events_per_sec
FROM per_second;"""

# Top 10 ingestion spikes
top_ingestion_spikes = """
WITH per_second AS (
    SELECT
        DATE_TRUNC('second', ts) AS ts_sec,
        COUNT(*) AS events_per_sec
    FROM arb_clean
    WHERE DATE(ts) = CURRENT_DATE - 1
    GROUP BY 1
)
SELECT *
FROM per_second
ORDER BY events_per_sec DESC
LIMIT 10;"""

# Total daily volume (events)
daily_volume = """
SELECT COUNT(*) AS total_events
FROM arb_clean
WHERE DATE(ts) = CURRENT_DATE - 1;"""

# Data per day
daily_data = """
SELECT
    SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) AS total_bytes
FROM arb_clean
WHERE DATE(ts) = CURRENT_DATE - 1;"""

# Events per hour
hourly_events = """
SELECT
    DATE_TRUNC('hour', ts) AS hour,
    COUNT(*) AS events_per_hour
FROM arb_clean
WHERE DATE(ts) = CURRENT_DATE - 1
GROUP BY 1
ORDER BY 1;"""

realistic_profitable_query = """
WITH fee_windows AS (
    SELECT
        ts,
        arb_direction,
        max_positive_spread,
        CASE
            WHEN arb_direction = 'BUY_BINANCE_SELL_COINBASE'
                THEN binance_ask
            WHEN arb_direction = 'BUY_COINBASE_SELL_BINANCE'
                THEN coinbase_ask
        END AS buy_price,
        max_positive_spread -
        (
            CASE
                WHEN arb_direction = 'BUY_BINANCE_SELL_COINBASE'
                    THEN binance_ask
                WHEN arb_direction = 'BUY_COINBASE_SELL_BINANCE'
                    THEN coinbase_ask
            END * 0.002 * 0.01
        ) AS net_spread_after_fees
    FROM arb_clean
    WHERE DATE(ts) = CURRENT_DATE - 1
      AND arb_open = TRUE
),
flagged AS (
    SELECT
        *,
        LAG(ts) OVER (ORDER BY ts) AS prev_ts,
        LAG(arb_direction) OVER (ORDER BY ts) AS prev_direction
    FROM fee_windows
),
event_flags AS (
    SELECT
        *,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN DATEDIFF('second', prev_ts, ts) > 1 THEN 1
            WHEN arb_direction <> prev_direction THEN 1
            ELSE 0
        END AS is_new_event
    FROM flagged
),
event_ids AS (
    SELECT
        *,
        SUM(is_new_event) OVER (
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS event_id
    FROM event_flags
),
event_profitability AS (
    SELECT
        event_id,
        MIN(ts) AS event_start_ts,
        MAX(ts) AS event_end_ts,
        DATEDIFF('second', MIN(ts), MAX(ts)) + 1 AS duration_seconds,
        MAX(net_spread_after_fees) AS peak_net_spread_after_fees
    FROM event_ids
    GROUP BY event_id
)
SELECT
    COUNT(*) AS realistic_profitable_events
FROM event_profitability
WHERE peak_net_spread_after_fees > 0
  AND duration_seconds >= 5;
"""

net_spread_query = """
WITH fee_windows AS (
    SELECT
        ts,
        arb_direction,
        max_positive_spread,
        CASE
            WHEN arb_direction = 'BUY_BINANCE_SELL_COINBASE'
                THEN binance_ask
            WHEN arb_direction = 'BUY_COINBASE_SELL_BINANCE'
                THEN coinbase_ask
        END AS buy_price,
        max_positive_spread -
        (
            CASE
                WHEN arb_direction = 'BUY_BINANCE_SELL_COINBASE'
                    THEN binance_ask
                WHEN arb_direction = 'BUY_COINBASE_SELL_BINANCE'
                    THEN coinbase_ask
            END * 0.002 * 0.01
        ) AS net_spread_after_fees
    FROM arb_clean
    WHERE DATE(ts) = CURRENT_DATE - 1
      AND arb_open = TRUE
),
flagged AS (
    SELECT
        *,
        LAG(ts) OVER (ORDER BY ts) AS prev_ts,
        LAG(arb_direction) OVER (ORDER BY ts) AS prev_direction
    FROM fee_windows
),
event_flags AS (
    SELECT
        *,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN DATEDIFF('second', prev_ts, ts) > 1 THEN 1
            WHEN arb_direction <> prev_direction THEN 1
            ELSE 0
        END AS is_new_event
    FROM flagged
),
event_ids AS (
    SELECT
        *,
        SUM(is_new_event) OVER (
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS event_id
    FROM event_flags
),
event_profitability AS (
    SELECT
        event_id,
        MIN(ts) AS event_start_ts,
        MAX(ts) AS event_end_ts,
        DATEDIFF('second', MIN(ts), MAX(ts)) + 1 AS duration_seconds,
        MAX(net_spread_after_fees) AS peak_net_spread_after_fees
    FROM event_ids
    GROUP BY event_id
)
SELECT
    SUM(peak_net_spread_after_fees) AS total_profit_after_fees_5s_plus
FROM event_profitability
WHERE peak_net_spread_after_fees > 0
  AND duration_seconds >= 5;"""


# Load data
kpi_df = run_query(kpi_query)
direction_df = run_query(direction_query)
hourly_df = run_query(hourly_query)
duration_df = run_query(duration_bucket_query)
spread_df = run_query(spread_bucket_query)
top_events_df = run_query(top_events_query)
fee_df = run_query(fee_adjusted_query)
avg_eps_df = run_query(avg_events_per_sec)
peak_eps_df = run_query(peak_ingestion_rate)
daily_vol_df = run_query(daily_volume)
top_spikes_df = run_query(top_ingestion_spikes)
daily_data_df = run_query(daily_data)
hourly_events_df = run_query(hourly_events)
realistic_df = run_query(realistic_profitable_query)
net_spread_query_df = run_query(net_spread_query)


# KPIs
if not kpi_df.empty:
    row = kpi_df.iloc[0]

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Events Yesterday", int(row["NUM_EVENTS"]) if pd.notna(row["NUM_EVENTS"]) else 0)
    c2.metric("Avg Duration (s)", f'{row["AVG_DURATION_SECONDS"]:.2f}' if pd.notna(row["AVG_DURATION_SECONDS"]) else "0.00")
    c3.metric("Max Peak Spread", f'{row["MAX_PEAK_SPREAD"]:.2f}' if pd.notna(row["MAX_PEAK_SPREAD"]) else "0.00")
    c4.metric("Avg Peak Spread", f'{row["AVG_PEAK_SPREAD"]:.2f}' if pd.notna(row["AVG_PEAK_SPREAD"]) else "0.00")
    c5.metric(
        "Profitable After Fees",
        int(fee_df.iloc[0]["PROFITABLE_EVENTS_AFTER_FEES"]) if not fee_df.empty and pd.notna(fee_df.iloc[0]["PROFITABLE_EVENTS_AFTER_FEES"]) else 0
    )
    c6.metric(
        "Profitable + ≥5s",
        int(realistic_df.iloc[0]["REALISTIC_PROFITABLE_EVENTS"])
        if not realistic_df.empty and pd.notna(realistic_df.iloc[0]["REALISTIC_PROFITABLE_EVENTS"])
        else 0
    )
    c7.metric(
        "Net Spread ≥5s",
        f"{net_spread_query_df.iloc[0]['TOTAL_PROFIT_AFTER_FEES_5S_PLUS']:.2f}"
        if not net_spread_query_df.empty and pd.notna(net_spread_query_df.iloc[0]["TOTAL_PROFIT_AFTER_FEES_5S_PLUS"])
        else "0.00"
    )


# Pipeline Scale Metrics
st.subheader("Pipeline Scale Metrics")

if (
    not avg_eps_df.empty
    and not peak_eps_df.empty
    and not daily_vol_df.empty
    and pd.notna(avg_eps_df.iloc[0]["AVG_EVENTS_PER_SEC"])
):
    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Avg Events/sec",
        f'{avg_eps_df.iloc[0]["AVG_EVENTS_PER_SEC"]:.2f}'
    )

    c2.metric(
        "Peak Events/sec",
        f"{int(peak_eps_df.iloc[0]['PEAK_EVENTS_PER_SEC']):,}"
    )

    c3.metric(
        "Daily Events",
        f"{int(daily_vol_df.iloc[0]['TOTAL_EVENTS']):,}"
    )


# Charts
left, right = st.columns(2)

with left:
    st.subheader("Events by Hour")
    if not hourly_df.empty:
        st.bar_chart(hourly_df.set_index("HOUR"))

    st.subheader("Duration Distribution")
    if not duration_df.empty:
        duration_df = duration_df.sort_values("BUCKET_ORDER")
        duration_df = duration_df.drop(columns=["BUCKET_ORDER"])
        st.bar_chart(duration_df.set_index("BUCKET"))

with right:
    st.subheader("Spread Distribution")
    if not spread_df.empty:
        spread_df = spread_df.sort_values("BUCKET_ORDER")
        spread_df = spread_df.drop(columns=["BUCKET_ORDER"])
        st.bar_chart(spread_df.set_index("BUCKET"))

    st.subheader("Direction Breakdown")
    if not direction_df.empty:
        st.bar_chart(direction_df.set_index("ARB_DIRECTION")[["EVENT_COUNT"]])


# Tables
st.subheader("Direction Summary")
st.dataframe(direction_df, use_container_width=True)

st.subheader("Top 10 Largest Events Yesterday")
st.dataframe(top_events_df, use_container_width=True)


# Debug stuff
with st.expander("Advanced Metrics (System + Diagnostics)"):

    st.subheader("Estimated Data Volume")

    if not daily_data_df.empty and pd.notna(daily_data_df.iloc[0][0]):
        bytes_val = daily_data_df.iloc[0][0]
        gb_val = bytes_val / (1024**3)
        st.metric("Daily Data (GB)", f"{gb_val:.2f}")
    else:
        st.info("No daily data volume available.")

    st.subheader("Top Ingestion Spikes")

    if not top_spikes_df.empty:
        st.dataframe(top_spikes_df, use_container_width=True)
    else:
        st.info("No ingestion spike data available.")

    st.subheader("Raw Events by Hour")

    if not hourly_events_df.empty:
        st.bar_chart(hourly_events_df.set_index("HOUR"))
    else:
        st.info("No hourly raw event data available.")