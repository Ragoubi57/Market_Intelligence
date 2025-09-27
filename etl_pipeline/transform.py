import pandas as pd
from . import config

def unify_raw_data(raw_dfs: dict) -> pd.DataFrame:
    """
    Unifies multiple raw dataframes into a single dataframe based on config mappings.
    """
    unified_frames = []
    
    for source_name, df in raw_dfs.items():
        print(f"Processing source: {source_name}...")
        df_copy = df.copy()
        mappings = config.COLUMN_MAPPINGS.get(source_name)
        if not mappings:
            print(f"[WARN] No column mappings found for source '{source_name}'. Skipping.")
            continue
            
        df_copy.rename(columns=mappings, inplace=True)
        unified_columns = list(mappings.values())
        df_copy = df_copy[[col for col in unified_columns if col in df_copy.columns]]
        df_copy['data_source'] = source_name
        unified_frames.append(df_copy)
        
    if not unified_frames:
        raise ValueError("No data was unified. Check your sources and mappings.")
        
    unified_df = pd.concat(unified_frames, ignore_index=True)
    print(f"Unified data created with {unified_df.shape[0]} total rows.")
    return unified_df

def create_star_schema(unified_df: pd.DataFrame) -> dict:
    """
    Takes the unified dataframe and generates the dimension and fact tables for the star schema.
    """
    unified_df['order_date'] = pd.to_datetime(unified_df['order_date'], errors='coerce')
    unified_df['quantity'] = pd.to_numeric(unified_df['quantity'], errors='coerce')
    unified_df['total_amount'] = pd.to_numeric(unified_df['total_amount'], errors='coerce')
    unified_df.dropna(subset=['order_date', 'product_sku', 'quantity', 'total_amount'], inplace=True)

    country_map = {'IN': 'India'}
    unified_df['country'] = unified_df['country_code'].map(country_map).fillna('Unknown')

    df_date = unified_df[['order_date']].copy()
    df_date.drop_duplicates(inplace=True)
    df_date.rename(columns={'order_date': 'date'}, inplace=True)
    df_date['year'] = df_date['date'].dt.year
    df_date['month'] = df_date['date'].dt.month
    df_date['day'] = df_date['date'].dt.day
    df_date['quarter'] = df_date['date'].dt.quarter
    
    df_product = unified_df[['product_sku', 'product_name', 'product_category']].copy()
    df_product.drop_duplicates(subset=['product_sku'], inplace=True)
    df_product.rename(columns={'product_sku': 'sku', 'product_name': 'product_name', 'product_category': 'category'}, inplace=True)

    df_region = unified_df[['country', 'currency']].copy()
    df_region.drop_duplicates(subset=['country'], inplace=True)
    
    df_channel = unified_df[['data_source']].copy()
    df_channel.rename(columns={'data_source': 'channel_name'}, inplace=True)
    df_channel.drop_duplicates(inplace=True)

    fact_cols = ['order_date', 'product_sku', 'country', 'data_source', 'quantity', 'total_amount']
    df_fact_sales = unified_df[fact_cols].copy()
    
    df_fact_sales.rename(columns={'order_date': 'date', 'product_sku': 'sku', 'data_source': 'channel'}, inplace=True)
    
    df_fact_sales['unit_price'] = df_fact_sales['total_amount'] / df_fact_sales['quantity']
    df_fact_sales['unit_price'].fillna(0, inplace=True)

    print("Star schema DataFrames created.")
    
    return {
        "dim_date": df_date, "dim_product": df_product, "dim_region": df_region,
        "dim_channel": df_channel, "fact_sales": df_fact_sales
    }