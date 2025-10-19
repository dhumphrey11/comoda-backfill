from __future__ import annotations
import pandas as pd


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df = df.replace({"": None})
    return df
