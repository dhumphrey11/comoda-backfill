from __future__ import annotations
import pandas as pd


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure consistent column casing and types
    df.columns = [c.lower() for c in df.columns]
    # Example: enforce dtypes for numeric columns if present
    for c in ["open_price", "high_price", "low_price", "close_price", "volume", "value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df
