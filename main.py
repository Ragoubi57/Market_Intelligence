import os
import sys 
from supabase import create_client, Client
from dotenv import load_dotenv
from etl_pipeline import config
from etl_pipeline.extract import extract_csv
from etl_pipeline.transform import unify_raw_data, create_star_schema, create_cloud_cost_df, create_expense_df, create_pandl_df
from etl_pipeline.load import load_star_schema, load_fact_table, load_simple_fact_table 
from etl_pipeline.market_extract import fetch_stock_data, fetch_news_data
from etl_pipeline.market_transform import transform_market_data
from etl_pipeline.market_load import load_market_data
from pyspark.sql import SparkSession

load_dotenv()

ALPHA_VANTAGE_API_KEY=os.getenv("ALPHA_VANTAGE_API_KEY")
NEWS_API_KEY=os.getenv("NEWS_API_KEY")
def run_sales_etl_pipeline(supabase: Client):
    """Runs the end-to-end ETL pipeline for sales-related data."""
    print("\nStarting Market Intelligence ETL Pipeline for Sales Data...")
    print("\n[STEP 1] Extracting raw data from CSV files...")
    raw_dataframes = {}
    sales_sources = {
        "amazon": config.RAW_DATA_SOURCES.get("amazon"), 
        "international": config.RAW_DATA_SOURCES.get("international")
    }
    for source_name, file_path in sales_sources.items():
        if file_path:
            try:
                raw_df = extract_csv(file_path)
                raw_dataframes[source_name] = raw_df
            except (KeyError, FileNotFoundError):
                print(f"[WARN] File not found for source '{source_name}'. Skipping.")
        else:
            print(f"[WARN] No path configured for source '{source_name}'. Skipping.")

    if not raw_dataframes:
        print("\nNo sales data extracted. Halting sales pipeline.")
        return

    print("\n[STEP 2] Transforming raw data into a Star Schema...")
    unified_df = unify_raw_data(raw_dataframes)
    star_schema_dataframes = create_star_schema(unified_df)
    for name, df in star_schema_dataframes.items():
        print(f"  - Created DataFrame '{name}' with {df.shape[0]} rows.")

    print("\n[STEP 3] Loading Star Schema into Supabase...")
    load_star_schema(supabase, star_schema_dataframes)
    print("\nETL pipeline for sales data completed successfully!")

def run_cloud_cost_etl_pipeline(supabase: Client):
    """Runs the ETL pipeline for cloud cost data."""
    print("\nStarting ETL Pipeline for Cloud Cost Data...")
    try:
        source_name = "cloud_cost"
        file_path = config.RAW_DATA_SOURCES[source_name]
        print(f"\n[STEP 1] Extracting {file_path}...")
        raw_df = extract_csv(file_path)
    except (KeyError, FileNotFoundError):
        print(f"[WARN] Cloud cost data source not found. Skipping.")
        return

    print("\n[STEP 2] Transforming raw data...")
    transformed_df = create_cloud_cost_df(raw_df)

    print("\n[STEP 3] Loading transformed data into Supabase...")
    load_simple_fact_table(supabase, transformed_df, "fact_cloud_cost", "cloud_id")
    print("\nETL pipeline for cloud cost data completed successfully!")

def run_expense_etl_pipeline(supabase: Client):
    """Runs the ETL pipeline for expense data."""
    print("\nStarting ETL Pipeline for Expense Data...")
    try:
        source_name = "expense"
        file_path = config.RAW_DATA_SOURCES[source_name]
        print(f"\n[STEP 1] Extracting {file_path}...")
        raw_df = extract_csv(file_path)
    except (KeyError, FileNotFoundError):
        print(f"[WARN] Expense data source not found. Skipping.")
        return

    print("\n[STEP 2] Transforming raw data...")
    transformed_df = create_expense_df(raw_df)

    if transformed_df.empty:
        print("No expense data transformed. Skipping load.")
        return

    print("\n[STEP 3] Loading transformed data into Supabase...")
    dimension_links = {
        "dim_date": {"fk_col": "date_id", "lookup_col": "date"}
    }
    load_fact_table(
        supabase=supabase,
        df=transformed_df.drop(columns=['description']),
        table_name="fact_expense",
        pkey_col="expense_id",
        dimension_links=dimension_links
    )
    print("\nETL pipeline for expense data completed successfully!")

def run_pandl_etl_pipeline(supabase: Client):
    """Runs the ETL pipeline for Profit & Loss data."""
    print("\nStarting ETL Pipeline for P&L Data...")
    print("\n[STEP 1] Extracting raw P&L data from CSV files...")
    raw_data = {}
    pandl_sources = ["pandl_march", "pandl_may"]
    for source in pandl_sources:
        try:
            file_path = config.RAW_DATA_SOURCES[source]
            raw_data[source] = extract_csv(file_path)
        except (KeyError, FileNotFoundError):
            print(f"[WARN] P&L data source '{source}' not found. Skipping.")
    
    if not raw_data:
        print("\nNo P&L data extracted. Halting P&L pipeline.")
        return

    print("\n[STEP 2] Transforming P&L data...")
    transformed_df = create_pandl_df(raw_data)

    if transformed_df.empty:
        print("No P&L data transformed. Skipping load.")
        return

    print("\n[STEP 3] Loading transformed P&L data into Supabase...")
    dimension_links = {
        "dim_date": {"fk_col": "date_id", "lookup_col": "date"}
    }
    load_fact_table(
        supabase=supabase,
        df=transformed_df,
        table_name="fact_pandl",
        pkey_col="pandl_id",
        dimension_links=dimension_links
    )
    print("\nETL pipeline for P&L data completed successfully!")

def run_market_data_etl_pipeline(supabase: Client):
    """Orchestrates the ETL pipeline for external market data."""
    print("\n--- Starting ETL Pipeline for External Market Data ---")

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .appName("MarketIntelligenceSpark") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

    print("\n[STEP 1] Extracting data from Market APIs...")
    stock_data = fetch_stock_data("AMZN")
    news_data = fetch_news_data("ecommerce OR retail")

    if not stock_data and not news_data:
        print("\nNo market data was extracted. Halting pipeline.")
        spark.stop()
        return

    transformed_stock_df, transformed_news_df = transform_market_data(
        spark=spark,
        supabase=supabase,
        stock_data=stock_data,
        news_data=news_data
    )

    print("\n[STEP 3] Loading transformed data into Supabase...")
    load_market_data(supabase, transformed_stock_df, "fact_stock_prices")
    load_market_data(supabase, transformed_news_df, "fact_market_news")

    spark.stop()
    print("\n--- Market Data ETL Pipeline Finished ---")


if __name__ == "__main__":
    load_dotenv()
    url: str = os.getenv("SUPABASE_URL")
    key: str = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    #run_cloud_cost_etl_pipeline(supabase)
    #run_sales_etl_pipeline(supabase)
    #run_expense_etl_pipeline(supabase)
    #run_pandl_etl_pipeline(supabase)

    run_market_data_etl_pipeline(supabase)
