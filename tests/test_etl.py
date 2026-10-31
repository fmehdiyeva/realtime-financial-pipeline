import pytest
import pandas as pd
import json
import os
import sqlite3
from pipeline.etl import run_etl

@pytest.fixture
def mock_raw_data(tmp_path):
    # Create a temporary raw stream file with intentional issues
    stream_dir = tmp_path / "data_generator"
    stream_dir.mkdir()
    stream_file = stream_dir / "raw_stream.jsonl"
    
    data = [
        # Valid trades across different minutes to test rolling avg
        {"trade_id": 1, "symbol": "MS", "side": "BUY", "price": 100, "quantity": 10, "timestamp": "2026-10-30T12:00:00"},
        {"trade_id": 2, "symbol": "GS", "side": "SELL", "price": 200, "quantity": 5, "timestamp": "2026-10-30T12:01:00"},
        {"trade_id": 3, "symbol": "JPM", "side": "BUY", "price": 300, "quantity": 2, "timestamp": "2026-10-30T12:02:00"},
        # Malformed: Missing price (should go to DLQ)
        {"trade_id": 4, "symbol": "AAPL", "side": "SELL", "price": None, "quantity": 10, "timestamp": "2026-10-30T12:03:00"},
        # Malformed: Invalid quantity type (should go to DLQ)
        {"trade_id": 5, "symbol": "GOOGL", "side": "BUY", "price": 150, "quantity": "N/A", "timestamp": "2026-10-30T12:04:00"},
        # Duplicate trade_id (should keep last)
        {"trade_id": 1, "symbol": "MS", "side": "BUY", "price": 110, "quantity": 10, "timestamp": "2026-10-30T12:00:00"}
    ]
    
    with open(stream_file, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")
            
    return str(stream_file)

def test_etl_upgraded_features(mock_raw_data, monkeypatch, tmp_path):
    # Mock paths in etl.py to use temporary test paths
    db_path = tmp_path / "test_financial.db"
    monkeypatch.setattr("pipeline.etl.STREAM_FILE", mock_raw_data)
    monkeypatch.setattr("pipeline.etl.DB_PATH", str(db_path))
    
    # Run ETL
    run_etl()
    
    conn = sqlite3.connect(str(db_path))
    
    # 1. Verify Cleaned Trades
    df_cleaned = pd.read_sql("SELECT * FROM cleaned_trades", conn)
    # Trade 1 (last one), Trade 2, Trade 3 are valid.
    assert len(df_cleaned) == 3
    assert 1 in df_cleaned['trade_id'].values
    assert 110 in df_cleaned['price'].values # Last duplicate
    
    # 2. Verify DLQ
    df_dlq = pd.read_sql("SELECT * FROM dead_letter_queue", conn)
    assert len(df_dlq) == 2
    # Check for specific error content or just that error field is populated
    assert "validation error" in df_dlq.iloc[0]['error'].lower()
    
    # 3. Verify Rolling Average
    df_agg = pd.read_sql("SELECT * FROM minute_aggregates", conn)
    # Sorting to ensure correct calculation check
    df_agg = df_agg.sort_values('timestamp')
    
    # Total values: 
    # 12:00: 110 * 10 = 1100
    # 12:01: 200 * 5 = 1000
    # 12:02: 300 * 2 = 600
    
    # Rolling avg at 12:02 (3 min window: 00, 01, 02)
    # (1100 + 1000 + 600) / 3 = 2700 / 3 = 900
    # Note: resample/rolling might include 12:03 and 12:04 if they exist in index but are 0
    # Our ETL only includes minutes with data in final_summary if it's the first run
    
    last_rolling_avg = df_agg.iloc[-1]['rolling_avg_5m']
    expected_avg = (1100 + 1000 + 600) / 3 
    assert last_rolling_avg == pytest.approx(expected_avg)
    
    conn.close()
