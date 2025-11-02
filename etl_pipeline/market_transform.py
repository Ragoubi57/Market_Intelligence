# etl_pipeline/market_transform.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.types import FloatType
from textblob import TextBlob
import pandas as pd
from supabase import Client

def _calculate_sentiment(text: str) -> float:
    """Calculates the sentiment polarity of a text string."""
    if text is None:
        return 0.0
    return TextBlob(text).sentiment.polarity

def transform_market_data(
    spark: SparkSession,
    supabase: Client,
    stock_data: list[dict],
    news_data: list[dict]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Transforms raw stock and news data using Spark and Pandas."""
    print("\n[STEP 2] Transforming data with PySpark...")

    print("  - Synchronizing dates with dim_date table...")
    all_dates = set()
    for item in stock_data:
        all_dates.add(item['date'])
    for item in news_data:
        all_dates.add(item['published_at'][:10])
    
    if all_dates:
        dates_to_upsert = pd.DataFrame({'date': list(all_dates)})
        dates_to_upsert['date'] = pd.to_datetime(dates_to_upsert['date'])
        dates_to_upsert['year'] = dates_to_upsert['date'].dt.year
        dates_to_upsert['month'] = dates_to_upsert['date'].dt.month
        dates_to_upsert['day'] = dates_to_upsert['date'].dt.day
        dates_to_upsert['quarter'] = dates_to_upsert['date'].dt.quarter
        dates_to_upsert['date'] = dates_to_upsert['date'].dt.strftime('%Y-%m-%d')
        supabase.table("dim_date").upsert(
            dates_to_upsert.to_dict(orient="records"), 
            on_conflict='date'
        ).execute()
        print(f"  - Ensured {len(all_dates)} dates exist in dim_date.")
    
    print("  - Fetching refreshed dim_date from Supabase.")
    dim_date_pd = pd.DataFrame(supabase.table("dim_date").select("date_id, date").execute().data)
    dim_date_pd['date'] = pd.to_datetime(dim_date_pd['date']).dt.date
    print(f"  - Loaded {len(dim_date_pd)} date records.")

    final_news_df = pd.DataFrame()
    if news_data:
        print("  - Processing news data with Pandas...")
        news_pd = pd.DataFrame(news_data)
        news_pd['date'] = pd.to_datetime(news_pd['published_at']).dt.date
        news_pd['sentiment_score'] = news_pd['title'].apply(_calculate_sentiment)
        
        news_merged = news_pd.merge(dim_date_pd, on='date', how='inner')
        final_news_df = news_merged[['date_id', 'source_name', 'title', 'sentiment_score']]
        print(f"  - News transformation complete. {len(final_news_df)} articles linked.")

    final_stock_df = pd.DataFrame()
    if stock_data:
        print("  - Processing stock data with Pandas...")
        stock_pd = pd.DataFrame(stock_data)
        stock_pd['date'] = pd.to_datetime(stock_pd['date']).dt.date
        
        stock_merged = stock_pd.merge(dim_date_pd, on='date', how='inner')
        final_stock_df = stock_merged.rename(columns={
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price'
        })[['date_id', 'symbol', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
        print(f"  - Stock transformation complete. {len(final_stock_df)} records linked.")
    
    return final_stock_df, final_news_df