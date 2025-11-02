import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

print("\n=== Market Intelligence Data Verification ===\n")

stock_count = supabase.table("fact_stock_prices").select("*", count="exact").execute()
print(f"✅ Stock Prices: {stock_count.count} records")

if stock_count.count > 0:
    sample_stock = supabase.table("fact_stock_prices").select("*").limit(3).execute()
    print("\nSample Stock Data:")
    for record in sample_stock.data:
        print(f"  - {record.get('symbol')} | Date ID: {record.get('date_id')} | Close: ${record.get('close_price')}")

news_count = supabase.table("fact_market_news").select("*", count="exact").execute()
print(f"\n✅ Market News: {news_count.count} records")

if news_count.count > 0:
    sample_news = supabase.table("fact_market_news").select("*").limit(3).execute()
    print("\nSample News Data:")
    for record in sample_news.data:
        sentiment = record.get('sentiment_score', 0)
        sentiment_label = "Positive" if sentiment > 0 else "Negative" if sentiment < 0 else "Neutral"
        print(f"  - {record.get('source_name')} | Sentiment: {sentiment:.3f} ({sentiment_label})")
        print(f"    Title: {record.get('title')[:80]}...")

date_count = supabase.table("dim_date").select("*", count="exact").execute()
print(f"\n✅ Dates in Dimension: {date_count.count} records")

print("\n=== Verification Complete ===\n")
