from dotenv import load_dotenv
from etl_pipeline import config
from etl_pipeline.extract import extract_csv
from etl_pipeline.transform import unify_raw_data, create_star_schema, create_cloud_cost_df
from etl_pipeline.load import load_star_schema, load_simple_fact_table

def run_sales_etl_pipeline():
    """Runs the end-to-end ETL pipeline for sales-related data."""
    load_dotenv()
    print("Starting Market Intelligence ETL Pipeline for Sales Data...")

    print("\n[STEP 1] Extracting raw data from CSV files...")
    raw_dataframes = {}
    for source_name, file_path in config.RAW_DATA_SOURCES.items():
        try:
            raw_df = extract_csv(file_path)
            raw_dataframes[source_name] = raw_df
        except FileNotFoundError:
            print(f"[WARN] File not found at {file_path}. Skipping source '{source_name}'.")

    if not raw_dataframes:
        print("\nNo data extracted. Halting pipeline.")
        return

    print("\n[STEP 2] Transforming raw data into a Star Schema...")
    unified_df = unify_raw_data(raw_dataframes)
    star_schema_dataframes = create_star_schema(unified_df)
    
    for name, df in star_schema_dataframes.items():
        print(f"  - Created DataFrame '{name}' with {df.shape[0]} rows.")

    print("\n[STEP 3] Loading Star Schema into Supabase...")
    load_star_schema(star_schema_dataframes)

    print("\nETL pipeline for sales data completed successfully!")

def run_cloud_cost_etl_pipeline():
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
    load_simple_fact_table(transformed_df, "fact_cloud_cost", "cloud_id")
    
    print("\nETL pipeline for cloud cost data completed successfully!")
    
if __name__ == "__main__":
    run_sales_etl_pipeline()