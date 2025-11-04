# etl_pipeline/market_transform.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.types import FloatType
from textblob import TextBlob
import pandas as pd
from supabase import Client
from etl_pipeline.sentiment_transformer import sentiment_transformer

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
    # Fetch all date records (no limit)
    all_dates_response = []
    page_size = 1000
    offset = 0
    while True:
        batch = supabase.table("dim_date").select("date_id, date").range(offset, offset + page_size - 1).execute().data
        if not batch:
            break
        all_dates_response.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    
    dim_date_pd = pd.DataFrame(all_dates_response)
    dim_date_pd['date'] = pd.to_datetime(dim_date_pd['date']).dt.date
    print(f"  - Loaded {len(dim_date_pd)} date records.")

    final_news_df = pd.DataFrame()
    if news_data:
        print("  - Processing news data with Pandas...")
        
        # Sentiment analysis for news articles using FinBERT
        print("  - Performing sentiment analysis on news articles with FinBERT...")
        news_texts = [article.get("title", "") for article in news_data]
        sentiment_results = sentiment_transformer(news_texts)

        for i, result in enumerate(sentiment_results):
            news_data[i]["sentiment_label"] = result["label"]
            news_data[i]["sentiment_score"] = result["polarity"]
        
        print(f"  - FinBERT sentiment analysis complete for {len(sentiment_results)} articles.")
        
        news_pd = pd.DataFrame(news_data)
        news_pd['date'] = pd.to_datetime(news_pd['published_at']).dt.date
        
        news_merged = news_pd.merge(dim_date_pd, on='date', how='inner')
        # Include additional fields: description, content, url
        final_news_df = news_merged[['date_id', 'source_name', 'title', 'description', 'content', 'url', 'sentiment_score']]
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