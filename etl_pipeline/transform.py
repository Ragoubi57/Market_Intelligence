import pandas as pd
from . import config
import re 


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

def create_cloud_cost_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the wide cloud cost comparison chart into a long format for the database.
    """
    # The first row contains headers/metrics.
    metrics = df.iloc[0].values
    
    # The actual data starts from the second row.
    df_data = df.iloc[1:].copy()
    
    # Use the first column as our service column, the rest are providers
    service_col = df.columns[1] # 'Shiprocket' column contains the service names
    provider_cols = df.columns[2:] # 'Unnamed: 1' and 'INCREFF' are the provider costs
    
    # Rename columns explicitly for clarity before melting
    # The service name is in the 'Shiprocket' column in the source file
    df_data = df_data.rename(columns={service_col: 'service'})
    
    # Melt the dataframe to unpivot
    df_long = pd.melt(
        df_data,
        id_vars=['service'],
        value_vars=provider_cols,
        var_name='provider_temp',
        value_name='value'
    )
    
    # Map the temporary provider names ('Unnamed: 1', 'INCREFF') to clean names
    # We can infer the provider from the metric row
    provider_map = {
        'Unnamed: 1': metrics[1], # The header for the Shiprocket values
        'INCREFF': 'INCREFF'
    }
    df_long['provider'] = df_long['provider_temp'].replace(provider_map)
    
    # Data Cleaning
    df_long['value'] = df_long['value'].astype(str).str.replace(r'[^\d.]', '', regex=True)
    df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
    df_long.dropna(subset=['value', 'service'], inplace=True)
    df_long = df_long[df_long['service'].str.contains('SCOPE OF WORK') == False] # Remove informational rows

    # Add constant columns
    df_long['metric'] = 'Price (Per Unit)'
    df_long['unit'] = 'INR'
    
    # Final column selection
    final_df = df_long[['provider', 'service', 'metric', 'value', 'unit']]
    
    print(f"  - Created DataFrame 'fact_cloud_cost' with {final_df.shape[0]} rows.")
    return final_df

def create_expense_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the poorly structured expense CSV into a clean, long-format DataFrame.
    """
    df1 = df.iloc[:, [0, 1]].copy()
    df1.columns = ['description', 'amount']

    df2 = df.iloc[:, [2, 3]].copy()
    df2.columns = ['description', 'amount']

    df_combined = pd.concat([df1, df2], ignore_index=True)
    
    df_combined['amount'] = df_combined['amount'].astype(str).str.replace(r'[^\d.]', '', regex=True)
    df_combined['amount'] = pd.to_numeric(df_combined['amount'], errors='coerce')
    df_combined.dropna(subset=['amount', 'description'], inplace=True)
    
    # Add this line to ensure the 'description' column is treated as a string
    df_combined['description'] = df_combined['description'].astype(str)
    
    df_combined = df_combined[~df_combined['description'].str.contains('total', case=False, na=False)]
    
    df_combined['date'] = pd.to_datetime('2021-03-31')
    df_combined['category'] = df_combined['description']

    final_df = df_combined[['date', 'category', 'amount', 'description']]
    
    print(f"  - Created DataFrame 'fact_expense' with {final_df.shape[0]} rows.")
    return final_df


def create_pandl_df(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Combines, cleans, and transforms multiple P&L dataframes into a single,
    aggregated monthly dataframe. It derives dates from filenames.
    """
    print("Combining and transforming P&L data...")
    monthly_pandl = []

    for source_name, df in raw_data.items():
        # --- 1. Derive date from the filename (e.g., 'pandl_march', 'pandl_may') ---
        # We will create a date for the last day of that month.
        if 'march' in source_name.lower():
            month_date = pd.to_datetime('2021-03-31')
        elif 'may' in source_name.lower():
            month_date = pd.to_datetime('2022-05-31')
        else:
            print(f"[WARN] Could not determine date for source '{source_name}'. Skipping.")
            continue
        
        df_copy = df.copy()

        # --- 2. Rename columns based on actual CSV headers ---
        # Handle variations like 'TP' vs 'TP 1'
        rename_map = {
            'Sku': 'sku',
            'Final MRP Old': 'revenue' # Using 'Final MRP Old' as the revenue source
        }
        if 'TP 1' in df_copy.columns:
            rename_map['TP 1'] = 'cost_of_goods'
        elif 'TP' in df_copy.columns:
            rename_map['TP'] = 'cost_of_goods'
            
        df_copy.rename(columns=rename_map, inplace=True)

        # --- 3. Clean and convert data types ---
        if 'revenue' not in df_copy.columns or 'cost_of_goods' not in df_copy.columns:
            print(f"[WARN] Revenue or Cost columns not found in '{source_name}'. Skipping.")
            continue
            
        df_copy['revenue'] = pd.to_numeric(df_copy['revenue'], errors='coerce')
        df_copy['cost_of_goods'] = pd.to_numeric(df_copy['cost_of_goods'], errors='coerce')
        df_copy.dropna(subset=['revenue', 'cost_of_goods'], inplace=True)

        # --- 4. Aggregate the data for the month ---
        total_revenue = df_copy['revenue'].sum()
        total_cost = df_copy['cost_of_goods'].sum()

        monthly_pandl.append({
            'date': month_date,
            'revenue': total_revenue,
            'cost_of_goods': total_cost
        })

    if not monthly_pandl:
        return pd.DataFrame()

    # --- 5. Create final DataFrame and calculate profit ---
    final_df = pd.DataFrame(monthly_pandl)
    final_df['gross_profit'] = final_df['revenue'] - final_df['cost_of_goods']
    
    print(f"  - Created aggregated DataFrame 'fact_pandl' with {final_df.shape[0]} rows.")
    return final_df