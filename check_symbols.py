"""Quick check to see which symbols are in the database"""
import os
from dotenv import load_dotenv
from supabase import create_client
import pandas as pd

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

# Check each symbol count individually
print("Checking stock data by symbol...")
symbols = ['AAPL', 'AMZN', 'MSFT', 'GOOGL', 'TSLA']

total = 0
for symbol in symbols:
    response = supabase.table("fact_stock_prices").select("*", count="exact").eq("symbol", symbol).limit(1).execute()
    count = response.count
    total += count
    print(f"  {symbol}: {count} records")

print(f"\nTotal stock records: {total}")

# Get sample of recent data
print("\nSample of recent stock data:")
recent = supabase.table("fact_stock_prices").select("symbol, date_id, close_price").order("date_id", desc=True).limit(10).execute()
for row in recent.data:
    print(f"  {row['symbol']} | Date ID: {row['date_id']} | Close: ${row['close_price']}")
