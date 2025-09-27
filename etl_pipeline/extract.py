import pandas as pd
from pathlib import Path

def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Extract data from a CSV file into a Pandas DataFrame.
    """
    file = Path(file_path)
    if not file.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        df = pd.read_csv(file)
        print(f"  - Successfully extracted {file.name} with {df.shape[0]} rows.")
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to extract {file_path}: {e}")