import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def clean_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans a DataFrame to make it JSON serializable."""
    df_copy = df.copy()
    df_copy = df_copy.astype(object).where(pd.notna(df_copy), None)
    df_copy.replace([np.inf, -np.inf], None, inplace=True)
    return df_copy

def load_star_schema(schema_dfs: dict):
    """Orchestrates the loading of the full star schema into Supabase."""
    print("\n--- Starting Star Schema Load ---")

    print("Loading dimension: dim_date...")
    dim_date_to_load = schema_dfs["dim_date"].copy()
    dim_date_to_load['date'] = dim_date_to_load['date'].dt.strftime('%Y-%m-%d')
    supabase.table("dim_date").upsert(dim_date_to_load.to_dict(orient="records"), on_conflict="date").execute()

    print("Loading dimension: dim_product...")
    dim_product_to_load = clean_for_json(schema_dfs["dim_product"])
    supabase.table("dim_product").upsert(dim_product_to_load.to_dict(orient="records"), on_conflict="sku").execute()
    
    print("Loading dimension: dim_region...")
    dim_region_to_load = clean_for_json(schema_dfs["dim_region"])
    supabase.table("dim_region").upsert(dim_region_to_load.to_dict(orient="records"), on_conflict="country").execute()

    print("Loading dimension: dim_channel...")
    dim_channel_to_load = clean_for_json(schema_dfs["dim_channel"])
    supabase.table("dim_channel").upsert(dim_channel_to_load.to_dict(orient="records"), on_conflict="channel_name").execute()
    
    print("Dimensions loaded successfully.")

    print("Fetching dimension keys from Supabase for mapping...")
    dates = pd.DataFrame(supabase.table("dim_date").select("date_id, date").execute().data)
    dates['date'] = pd.to_datetime(dates['date']).dt.date
    products = pd.DataFrame(supabase.table("dim_product").select("product_id, sku").execute().data)
    regions = pd.DataFrame(supabase.table("dim_region").select("region_id, country").execute().data)
    channels = pd.DataFrame(supabase.table("dim_channel").select("channel_id, channel_name").execute().data)

    print("Preparing fact_sales table with foreign keys...")
    df_fact_sales = schema_dfs["fact_sales"].copy()
    df_fact_sales['date'] = pd.to_datetime(df_fact_sales['date']).dt.date
    
    df_fact_sales = pd.merge(df_fact_sales, dates, on="date", how="left")
    df_fact_sales = pd.merge(df_fact_sales, products, on="sku", how="left")
    df_fact_sales = pd.merge(df_fact_sales, regions, on="country", how="left")
    df_fact_sales = pd.merge(df_fact_sales, channels, left_on="channel", right_on="channel_name", how="left")

    final_fact_sales = df_fact_sales[["date_id", "product_id", "region_id", "channel_id", "quantity", "unit_price", "total_amount"]]
    final_fact_sales = final_fact_sales.dropna()
    
    if not final_fact_sales.empty:
        int_columns = ["date_id", "product_id", "region_id", "channel_id", "quantity"]
        for col in int_columns:
            final_fact_sales[col] = final_fact_sales[col].astype(int)

    if final_fact_sales.empty:
        print("No valid fact rows to load after merging. Check for mismatches in keys.")
    else:
        print(f"Loading {final_fact_sales.shape[0]} rows into fact_sales...")
        final_fact_sales_to_load = clean_for_json(final_fact_sales)
        supabase.table("fact_sales").insert(final_fact_sales_to_load.to_dict(orient="records")).execute()

    print("Fact table loaded successfully.")
    print("--- Star Schema Load Complete ---")