import sys
import os

# Add project root directory to Python path for Streamlit Cloud
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
import sqlite3
import time
import threading

# Ensure database directory exists
os.makedirs('database', exist_ok=True)

# Import background continuous pipeline functions
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

# Configure page tab title and icon
st.set_page_config(
    page_title="Real-Time Financial Analytics",
    page_icon="📈",
    layout="wide"
)

# Header layout with elegant logo and typography
col_logo, col_header = st.columns([0.06, 0.94])

with col_logo:
    # Uses assets/logo.svg if created, fallback to clean vector
    if os.path.exists("assets/logo.svg"):
        st.image("assets/logo.svg", width=52)
    else:
        st.markdown("## 📈")

with col_header:
    st.markdown("""
        <h1 style='font-weight: 300; letter-spacing: -0.5px; margin-bottom: 0px;'>
            Real-Time Financial Analytics
        </h1>
        <p style='color: #808495; font-size: 0.95rem; margin-top: -5px;'>
            High-Frequency Stream Ingestion &bull; Pydantic DLQ Validation &bull; 5m Rolling Metrics
        </p>
    """, unsafe_allow_html=True)

st.divider()

# Auto-refresh logic
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

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

# DLQ Warning Banner
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
    st.warning("No data found in database. Initializing pipeline...")

# Manual refresh button
if st.button('Manual Refresh'):
    st.rerun()

# Auto-refresh loop
time.sleep(2)
st.rerun()