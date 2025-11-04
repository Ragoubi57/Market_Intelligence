"""Test the enhanced news fetching without loading to database"""
from dotenv import load_dotenv
load_dotenv()

from etl_pipeline.market_extract import fetch_news_for_symbols
from datetime import datetime, timedelta

# Test fetching news from past 7 days
to_date = datetime.now().strftime("%Y-%m-%d")
from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

symbols = ["AAPL", "TSLA"]
print(f"Fetching news from {from_date} to {to_date}")

news_data = fetch_news_for_symbols(symbols, from_date=from_date, to_date=to_date)

print(f"\n✅ Total articles fetched: {len(news_data)}")
print("\nSample articles:")
for i, article in enumerate(news_data[:5]):
    print(f"\n{i+1}. {article['title']}")
    print(f"   Source: {article['source_name']}")
    print(f"   Published: {article['published_at']}")
    print(f"   Description: {article['description'][:100] if article.get('description') else 'N/A'}...")
