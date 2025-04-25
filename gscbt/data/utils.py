import pandas as pd

def add_back_adjusted_diff(
    df : pd.DataFrame,
    diff: float 
): 
    if "open" in df.columns:
        df["open"] = df["open"] + diff
    if "close" in df.columns:
        df["close"] = df["close"] + diff
    if "high" in df.columns:
        df["high"] = df["high"] + diff
    if "low" in df.columns:
        df["low"] = df["low"] + diff