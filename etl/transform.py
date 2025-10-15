import pandas as pd
import numpy as np
import janitor

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize uploaded data
    """
    try:
        df = df.clean_names()
    except Exception:
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.dropna(how="all")
    num_cols = df.select_dtypes(include=np.number).columns
    df[num_cols] = df[num_cols].fillna(0)
    df.columns = [c.replace("-", "_").replace(".", "_") for c in df.columns]
    return df
