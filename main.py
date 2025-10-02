import os
from supabase import create_client, Client
from dotenv import load_dotenv
from etl_pipeline import config
from etl_pipeline.extract import extract_csv
from etl_pipeline.transform import unify_raw_data, create_star_schema, create_cloud_cost_df, create_expense_df
from etl_pipeline.load import load_star_schema,load_linked_fact_table, load_simple_fact_table

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
    load_linked_fact_table(
        supabase=supabase,
        df=transformed_df,
        table_name="fact_expense",
        pkey_col="expense_id",
        link_col="date",
        dim_table="dim_date",
        dim_key="date_id",
        dim_link_col="date"
    )
    print("\nETL pipeline for expense data completed successfully!")

if __name__ == "__main__":
    load_dotenv()
    url: str = os.getenv("SUPABASE_URL")
    key: str = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    run_cloud_cost_etl_pipeline(supabase)
    run_sales_etl_pipeline(supabase)
    run_expense_etl_pipeline(supabase)