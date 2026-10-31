import streamlit as st
import pandas as pd
import sqlite3
import os
import time
import threading

# Import the background continuous pipeline functions
from data_generator.stream import stream_data
from pipeline.etl import run_continuous_etl

# Start background data generator and ETL loop once per app deployment
@st.cache_resource
def start_background_pipeline():
    thread_stream = threading.Thread(target=stream_data, daemon=True)
    thread_etl = threading.Thread(target=run_continuous_etl, daemon=True)
    
    thread_stream.start()
    thread_etl.start()

start_background_pipeline()

DB_PATH = 'database/financial_data.db'

st.set_page_config(page_title="MS Real-Time Financial Dashboard", layout="wide")

# Auto-refresh logic
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

st.title("📊 Real-Time Financial Data Dashboard")
st.markdown("### Trading Volume & Trade Count per Minute")

def get_data():
    if not os.path.exists(DB_PATH):
        return None, None, None, None
    conn = sqlite3.connect(DB_PATH)
    try:
        aggregates = pd.read_sql("SELECT * FROM minute_aggregates", conn)
        trades = pd.read_sql("SELECT * FROM cleaned_trades ORDER BY timestamp DESC LIMIT 100", conn)
        
        # DLQ Stats
        dlq_count = pd.read_sql("SELECT COUNT(*) as count FROM dead_letter_queue", conn)['count'][0]
        valid_count = pd.read_sql("SELECT COUNT(*) as count FROM cleaned_trades", conn)['count'][0]
    except Exception:
        return None, None, 0, 0
    finally:
        conn.close()
    return aggregates, trades, dlq_count, valid_count

aggregates, trades, dlq_count, valid_count = get_data()

# DLQ Warning
total_records = dlq_count + valid_count
if total_records > 0:
    error_rate = (dlq_count / total_records) * 100
    if error_rate > 5:
        st.error(f"⚠️ HIGH ERROR RATE: {error_rate:.2f}% of records are malformed (DLQ: {dlq_count})")
    else:
        st.info(f"Data Quality: {100-error_rate:.2f}% healthy records")

if aggregates is not None and not aggregates.empty:
    aggregates['timestamp'] = pd.to_datetime(aggregates['timestamp'])
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Total Trading Volume & 5m Rolling Avg")
        # Multi-line chart for volume and rolling average
        chart_data = aggregates.set_index('timestamp')[['total_value', 'rolling_avg_5m']]
        st.line_chart(chart_data)
        
    with col2:
        st.subheader("Trade Count per Minute")
        st.bar_chart(aggregates.set_index('timestamp')['trade_count'])

    st.subheader("Recent Cleaned Trades")
    st.dataframe(trades, use_container_width=True)
else:
    st.warning("No data found in database. Please run the ETL pipeline first.")

# Manual refresh button
if st.button('Manual Refresh'):
    st.rerun()

# Auto-refresh every 2 seconds
time.sleep(2)
st.rerun()