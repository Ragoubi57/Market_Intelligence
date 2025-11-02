# etl_pipeline/market_load.py

import pandas as pd
import numpy as np
from supabase import Client

def _clean_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares a DataFrame for JSON serialization before loading.
    This function handles None/NaN values and infinite numbers.
    """
    df_copy = df.copy()
    df_copy = df_copy.astype(object).where(pd.notna(df_copy), None)
    df_copy.replace([np.inf, -np.inf], None, inplace=True)
    return df_copy

def load_market_data(supabase: Client, df: pd.DataFrame, table_name: str):
    """
    Performs a full refresh load (delete and insert) for a given market data table.
    
    Args:
        supabase: The Supabase client instance.
        df: The transformed Pandas DataFrame to load.
        table_name: The name of the target table in Supabase.
    """
    if df.empty:
        print(f"  - No data to load into '{table_name}'. Skipping.")
        return

    print(f"  - Loading {len(df)} rows into '{table_name}'...")
    
    df_to_load = _clean_for_json(df)
    
    supabase.table(table_name).delete().gt("date_id", 0).execute()
    
    # Insert the new data in batches to handle potentially large volumes and avoid timeouts.
    BATCH_SIZE = 500
    total_rows = len(df_to_load)
    
    # Calculate the number of chunks needed for the batch insert.
    # We add 1 to the integer division to ensure the last partial chunk is included.
    list_of_chunks = np.array_split(df_to_load, (total_rows // BATCH_SIZE) + 1)
    
    for i, chunk in enumerate(list_of_chunks):
        # Ensure the chunk is not empty before trying to insert.
        if not chunk.empty:
            print(f"    - Loading batch {i+1} of {len(list_of_chunks)}...")
            supabase.table(table_name).insert(chunk.to_dict(orient="records")).execute()

    print(f"  - Successfully loaded data into '{table_name}'.")