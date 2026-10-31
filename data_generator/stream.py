import json
import random
import time
from datetime import datetime

SYMBOLS = ['MS', 'GS', 'JPM', 'AAPL', 'GOOGL']
TRADE_TYPES = ['BUY', 'SELL']

def generate_messy_trade():
    trade_id = random.randint(1000, 1005) # Intentional low range for duplicates
    symbol = random.choice(SYMBOLS)
    side = random.choice(TRADE_TYPES)
    
    # Introduce messy data
    price = random.uniform(100, 500) if random.random() > 0.1 else None # 10% missing price
    quantity = random.randint(1, 100) if random.random() > 0.1 else "N/A" # 10% string instead of int
    
    timestamp = datetime.now().isoformat()
    
    return {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side,
        "price": price,
        "quantity": quantity,
        "timestamp": timestamp
    }

def stream_data():
    print("Starting data stream... (Ctrl+C to stop)")
    while True:
        trade = generate_messy_trade()
        print(json.dumps(trade))
        # Appending to a local file to simulate a stream/log
        with open('data_generator/raw_stream.jsonl', 'a') as f:
            f.write(json.dumps(trade) + '\n')
        time.sleep(random.uniform(0.1, 0.5))

if __name__ == "__main__":
    stream_data()
