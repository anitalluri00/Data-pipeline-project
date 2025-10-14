import pandas as pd
import numpy as np
import janitor

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform uploaded/extracted data
    """
    df = df.clean_names()
    df = df.dropna(how="all")
    df[df.select_dtypes(include=np.number).columns] = df.select_dtypes(include=np.number).fillna(0)
    return df
