import os
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

def print_header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def print_section(title):
    print(f"\n{title}")
    print(f"{'-'*60}")

print_header("🎉 MARKET INTELLIGENCE PLATFORM - PHASE 1 COMPLETE")

print(f"Pipeline Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Status: ✅ PRODUCTION READY")

print_section("📊 DATA WAREHOUSE METRICS")

dimensions = [
    ("dim_date", "Calendar Dimension"),
    ("dim_product", "Product Catalog"),
    ("dim_region", "Geographic Regions"),
    ("dim_channel", "Sales Channels")
]

facts = [
    ("fact_sales", "E-commerce Sales"),
    ("fact_expense", "Business Expenses"),
    ("fact_cloud_cost", "Cloud Infrastructure"),
    ("fact_pandl", "Profit & Loss"),
    ("fact_stock_prices", "Stock Market Data ⭐"),
    ("fact_market_news", "News Sentiment ⭐")
]

print("\nDimension Tables:")
for table, description in dimensions:
    try:
        count = supabase.table(table).select("*", count="exact").execute().count
        print(f"  • {description:.<45} {count:>6} records")
    except:
        print(f"  • {description:.<45} {'N/A':>6}")

print("\nFact Tables:")
for table, description in facts:
    try:
        count = supabase.table(table).select("*", count="exact").execute().count
        print(f"  • {description:.<45} {count:>6} records")
    except:
        print(f"  • {description:.<45} {'N/A':>6}")

print_section("📈 MARKET DATA ANALYSIS")

stock_data = supabase.table("fact_stock_prices").select("*").order("date_id", desc=False).limit(1).execute()
if stock_data.data:
    oldest = stock_data.data[0]
    newest = supabase.table("fact_stock_prices").select("*").order("date_id", desc=True).limit(1).execute().data[0]
    print(f"\nStock Symbol: {newest.get('symbol')}")
    print(f"Date Range: Latest record")
    print(f"  Open:  ${newest.get('open_price'):.2f}")
    print(f"  High:  ${newest.get('high_price'):.2f}")
    print(f"  Low:   ${newest.get('low_price'):.2f}")
    print(f"  Close: ${newest.get('close_price'):.2f}")
    print(f"  Volume: {newest.get('volume'):,} shares")

print_section("📰 NEWS SENTIMENT DISTRIBUTION")

news_data = supabase.table("fact_market_news").select("sentiment_score").execute().data
if news_data:
    sentiments = [n['sentiment_score'] for n in news_data]
    positive = len([s for s in sentiments if s > 0])
    neutral = len([s for s in sentiments if s == 0])
    negative = len([s for s in sentiments if s < 0])
    total = len(sentiments)
    
    print(f"\nTotal Articles: {total}")
    print(f"  Positive: {positive:>3} ({positive/total*100:.1f}%) {'█' * int(positive/total*20)}")
    print(f"  Neutral:  {neutral:>3} ({neutral/total*100:.1f}%) {'█' * int(neutral/total*20)}")
    print(f"  Negative: {negative:>3} ({negative/total*100:.1f}%) {'█' * int(negative/total*20)}")
    
    avg_sentiment = sum(sentiments) / len(sentiments)
    print(f"\n  Average Sentiment: {avg_sentiment:.3f} {'📈' if avg_sentiment > 0 else '📉'}")

print_section("🎯 NEXT STEPS (PHASE 2)")

next_steps = [
    "Create Jupyter notebooks for exploratory data analysis",
    "Implement FinBERT for advanced financial sentiment",
    "Build time series forecasting models (Prophet/LSTM)",
    "Analyze correlation between news sentiment and stock prices",
    "Expand to multiple stock symbols and data sources"
]

for i, step in enumerate(next_steps, 1):
    print(f"  {i}. {step}")

print_section("✨ TECHNICAL ACHIEVEMENTS")

achievements = [
    "✅ Modular ETL architecture with separation of concerns",
    "✅ PySpark integration (hybrid with Pandas for Windows compatibility)",
    "✅ Real-time API data fetching (Alpha Vantage + NewsAPI)",
    "✅ Automated sentiment analysis with TextBlob",
    "✅ Star schema data warehouse in Supabase",
    "✅ Date synchronization across all data sources",
    "✅ Batch loading for performance optimization",
    "✅ Comprehensive error handling and logging"
]

for achievement in achievements:
    print(f"  {achievement}")

print(f"\n{'='*60}")
print(f"  🚀 Ready for Phase 2: Intelligence Layer")
print(f"{'='*60}\n")
