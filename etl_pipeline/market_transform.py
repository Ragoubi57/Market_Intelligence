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
    """Transforms raw stock and news data using Spark."""
    print("\n[STEP 2] Transforming data with PySpark...")

    # --- NEW LOGIC: Ensure dates from our new data exist in dim_date ---
    print("  - Synchronizing dates with dim_date table...")
    all_dates = set()
    for item in stock_data:
        all_dates.add(item['date'])
    for item in news_data:
        # Extract just the date part from the timestamp
        all_dates.add(item['published_at'][:10])
    
    # Create a DataFrame of the unique dates we need
    if all_dates:
        dates_to_upsert = pd.DataFrame({'date': list(all_dates)})
        dates_to_upsert['date'] = pd.to_datetime(dates_to_upsert['date'])
        dates_to_upsert['year'] = dates_to_upsert['date'].dt.year
        dates_to_upsert['month'] = dates_to_upsert['date'].dt.month
        dates_to_upsert['day'] = dates_to_upsert['date'].dt.day
        dates_to_upsert['quarter'] = dates_to_upsert['date'].dt.quarter
        dates_to_upsert['date'] = dates_to_upsert['date'].dt.strftime('%Y-%m-%d')
        # Upsert these dates into the dimension table
        supabase.table("dim_date").upsert(
            dates_to_upsert.to_dict(orient="records"), 
            on_conflict='date'
        ).execute()
        print(f"  - Ensured {len(all_dates)} dates exist in dim_date.")
    
    # Now, proceed with Spark using the updated dim_date table
    stock_df = spark.createDataFrame(stock_data) if stock_data else None
    news_df = spark.createDataFrame(news_data) if news_data else None
    print("  - Spark DataFrames created.")

    print("  - Fetching refreshed dim_date from Supabase.")
    dim_date_pd = pd.DataFrame(supabase.table("dim_date").select("date_id, date").execute().data)
    dim_date_pd['date'] = pd.to_datetime(dim_date_pd['date']).dt.date
    dim_date_spark_df = spark.createDataFrame(dim_date_pd)
    print(f"  - Loaded {dim_date_spark_df.count()} date records into Spark.")

    final_news_df = pd.DataFrame()
    if news_df is not None:
        print("  - Processing news data.")
        # CORRECT ORDER: Apply UDF before joining
        sentiment_udf = udf(_calculate_sentiment, FloatType())
        news_with_sentiment = news_df.withColumn("sentiment_score", sentiment_udf(col("title")))
        news_with_sentiment = news_with_sentiment.withColumn("date", to_date(col("published_at")))
        
        linked_news_df = news_with_sentiment.join(dim_date_spark_df, on="date", how="inner")
        final_news_spark_df = linked_news_df.select(
            "date_id", "source_name", "title", "sentiment_score"
        )
        print(f"  - News transformation complete. {final_news_spark_df.count()} articles linked.")
        if final_news_spark_df.count() > 0:
            final_news_df = final_news_spark_df.toPandas()

    final_stock_df = pd.DataFrame()
    if stock_df is not None:
        print("  - Processing stock data.")
        linked_stock_df = stock_df.join(dim_date_spark_df, on="date", how="inner")
        final_stock_spark_df = linked_stock_df.select(
            col("date_id"), "symbol",
            col("open").alias("open_price"), col("high").alias("high_price"),
            col("low").alias("low_price"), col("close").alias("close_price"),
            "volume"
        )
        print(f"  - Stock transformation complete. {final_stock_spark_df.count()} records linked.")
        if final_stock_spark_df.count() > 0:
            final_stock_df = final_stock_spark_df.toPandas()
    
    return final_stock_df, final_news_df