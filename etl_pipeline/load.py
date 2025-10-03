import os
import pandas as pd
import numpy as np
from supabase import Client
from dotenv import load_dotenv

load_dotenv()

def clean_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans a DataFrame to make it JSON serializable."""
    df_copy = df.copy()
    df_copy = df_copy.astype(object).where(pd.notna(df_copy), None)
    df_copy.replace([np.inf, -np.inf], None, inplace=True)
    return df_copy

def load_simple_fact_table(supabase: Client, df: pd.DataFrame, table_name: str, pkey_col: str):
    """Performs a full refresh (delete and insert) for a simple fact table."""
    print(f"Performing full refresh for table: {table_name}...")
    df_to_load = clean_for_json(df)
    supabase.table(table_name).delete().neq(pkey_col, 0).execute()
    response = supabase.table(table_name).insert(df_to_load.to_dict(orient="records")).execute()
    if len(response.data) > 0:
        print(f"Successfully loaded {len(response.data)} rows into {table_name}.")
    else:
        print(f"Load operation to {table_name} reported 0 rows loaded.")

def load_fact_table(supabase: Client, df: pd.DataFrame, table_name: str, pkey_col: str, dimension_links: dict):
    """
    Loads data into a fact table, linking to multiple dimension tables.
    """
    print(f"Loading data into fact table: {table_name}...")
    df_merged = df.copy()

    if "dim_date" in dimension_links:
        print("  - Ensuring all dates exist in dim_date...")
        date_lookup_col = dimension_links["dim_date"]["lookup_col"]
        unique_dates = df[[date_lookup_col]].drop_duplicates().copy()
        unique_dates.rename(columns={date_lookup_col: 'date'}, inplace=True)
        unique_dates['date'] = pd.to_datetime(unique_dates['date'])
        
        unique_dates['year'] = unique_dates['date'].dt.year
        unique_dates['month'] = unique_dates['date'].dt.month
        unique_dates['day'] = unique_dates['date'].dt.day
        unique_dates['quarter'] = unique_dates['date'].dt.quarter
        unique_dates['date'] = unique_dates['date'].dt.strftime('%Y-%m-%d')
        
        supabase.table("dim_date").upsert(
            unique_dates.to_dict(orient="records"), 
            on_conflict='date'
        ).execute()

    for dim_table, link_info in dimension_links.items():
        print(f"  - Fetching keys from dimension: {dim_table}")
        fk_col, lookup_col = link_info['fk_col'], link_info['lookup_col']
        dim_df = pd.DataFrame(supabase.table(dim_table).select(f"{fk_col}, {lookup_col}").execute().data)
        if 'date' in lookup_col:
            df_merged[lookup_col] = pd.to_datetime(df_merged[lookup_col]).dt.date
            dim_df[lookup_col] = pd.to_datetime(dim_df[lookup_col]).dt.date
        df_merged = pd.merge(df_merged, dim_df, on=lookup_col, how="left")

    fact_table_cols = [link['fk_col'] for link in dimension_links.values()]
    measure_cols = [col for col in df.columns if col not in [link['lookup_col'] for link in dimension_links.values()]]
    final_df = df_merged[fact_table_cols + measure_cols].copy()
    final_df.dropna(subset=fact_table_cols, inplace=True)

    if final_df.empty:
        print("No valid rows to load after linking with dimensions.")
        return
        
    for fk_col in fact_table_cols:
        final_df[fk_col] = final_df[fk_col].astype(int)

    print(f"Loading {final_df.shape[0]} rows into {table_name}...")
    supabase.table(table_name).delete().neq(pkey_col, 0).execute()
    
    BATCH_SIZE = 1000
    list_of_chunks = np.array_split(final_df, len(final_df) // BATCH_SIZE + 1)
    
    for chunk in filter(lambda c: not c.empty, list_of_chunks):
        supabase.table(table_name).insert(clean_for_json(chunk).to_dict(orient="records")).execute()

    print(f"Successfully loaded data into {table_name}.")

def load_star_schema(supabase: Client, schema_dfs: dict):
    """Orchestrates the loading of the full star schema into Supabase."""
    print("\n--- Starting Star Schema Load ---")
    # (Dimension loading remains the same)
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

    # (Key fetching and merging remains the same)
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

    # --- BATCHING LOGIC STARTS HERE ---
    if final_fact_sales.empty:
        print("No valid fact rows to load after merging.")
    else:
        print(f"Loading {final_fact_sales.shape[0]} rows into fact_sales in batches...")
        
        BATCH_SIZE = 1000 # We'll send 1000 rows at a time
        total_rows = len(final_fact_sales)
        
        # Use numpy to split the dataframe into chunks
        list_of_chunks = np.array_split(final_fact_sales, total_rows // BATCH_SIZE + 1)
        
        for i, chunk in enumerate(list_of_chunks):
            print(f"  - Loading batch {i+1} of {len(list_of_chunks)}...")
            chunk_to_load = clean_for_json(chunk)
            supabase.table("fact_sales").insert(chunk_to_load.to_dict(orient="records")).execute()

    print("Fact table loaded successfully.")
    print("--- Star Schema Load Complete ---")