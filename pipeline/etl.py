import pandas as pd
import sqlite3
import json
import os
import time
from datetime import datetime
from pydantic import BaseModel, ValidationError, Field
from typing import Optional, Union

DB_PATH = 'database/financial_data.db'
STREAM_FILE = 'data_generator/raw_stream.jsonl'

class Trade(BaseModel):
    trade_id: int
    symbol: str
    side: str
    price: float
    quantity: int
    timestamp: datetime

def process_incremental_data(last_position):
    if not os.path.exists(STREAM_FILE):
        return last_position

    valid_records = []
    malformed_records = []
    current_position = last_position

    with open(STREAM_FILE, 'r') as f:
        f.seek(last_position)
        lines = f.readlines()
        current_position = f.tell()

    if not lines:
        return current_position

    for line in lines:
        if not line.strip():
            continue
        try:
            raw_record = json.loads(line)
            # Pydantic validation
            trade = Trade(**raw_record)
            valid_records.append(trade.model_dump())
        except (json.JSONDecodeError, ValidationError) as e:
            malformed_records.append({
                "raw_data": line.strip(),
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })

    # Handle DLQ
    conn = sqlite3.connect(DB_PATH)
    if malformed_records:
        df_dlq = pd.DataFrame(malformed_records)
        df_dlq.to_sql('dead_letter_queue', conn, if_exists='append', index=False)

    if not valid_records:
        conn.close()
        return current_position

    df = pd.DataFrame(valid_records)
    
    # 2. Clean (Additional cleaning like duplicates)
    initial_count = len(df)
    df = df.drop_duplicates(subset=['trade_id'], keep='last')
    
    if df.empty:
        conn.close()
        return current_position

    # 3. Transform
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['total_value'] = df['price'] * df['quantity']
    
    # Append new cleaned trades
    df.to_sql('cleaned_trades', conn, if_exists='append', index=False)
    
    # Aggregate per minute
    df_indexed = df.set_index('timestamp')
    minute_summary = df_indexed.resample('1min').agg({
        'total_value': 'sum',
        'trade_id': 'count'
    }).rename(columns={'trade_id': 'trade_count'})
    
    # 4. Update/Merge aggregates and calculate rolling average
    try:
        existing_aggregates = pd.read_sql("SELECT timestamp, total_value, trade_count FROM minute_aggregates", conn, parse_dates=['timestamp'])
        existing_aggregates.set_index('timestamp', inplace=True)
        
        # Combine and re-aggregate
        combined = pd.concat([existing_aggregates, minute_summary])
        final_summary = combined.groupby(level=0).agg({
            'total_value': 'sum',
            'trade_count': 'sum'
        })
    except Exception:
        final_summary = minute_summary

    # Calculate 5-minute rolling average
    final_summary = final_summary.sort_index()
    final_summary['rolling_avg_5m'] = final_summary['total_value'].rolling(window='5min').mean()

    final_summary.to_sql('minute_aggregates', conn, if_exists='replace', index=True)
    conn.close()
    
    print(f"[{datetime.now().strftime('%H:%M:%S' )}] ETL Batch Complete. Valid: {len(df)}, Malformed: {len(malformed_records)}.")
    return current_position

def run_etl():
    """Compatibility wrapper for tests and one-off execution."""
    process_incremental_data(0)

def run_continuous_etl():
    print("Starting continuous ETL pipeline... (Press Ctrl+C to stop)")
    last_position = 0
    
    while True:
        try:
            last_position = process_incremental_data(last_position)
        except Exception as e:
            print(f"Error in ETL loop: {e}")
        
        time.sleep(5)

if __name__ == "__main__":
    run_continuous_etl()
