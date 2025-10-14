import pandas as pd
import numpy as np
import janitor

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.clean_names()
    df[df.select_dtypes(include=np.number).columns] = df.select_dtypes(include=np.number).fillna(0)
    return df