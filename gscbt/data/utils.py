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

def drop_ohlcv(df : pd.DataFrame,keep_ohlcv : str)-> pd.DataFrame:
    drop_list = []
    if "o" not in keep_ohlcv and "open" in df.columns:
        drop_list.append("open")
    if "h" not in keep_ohlcv and "high" in df.columns:
        drop_list.append("high")
    if "l" not in keep_ohlcv and "low" in df.columns:
        drop_list.append("low")
    if "c" not in keep_ohlcv and "close" in df.columns:
        drop_list.append("close")
    if "v" not in keep_ohlcv and "volume" in df.columns:
        drop_list.append("volume")

    df.drop(drop_list, axis=1, inplace=True)
    return df
        
        

def df_apply_operation_to_given_columns(
    df : pd.DataFrame,
    value : float,
    columns : list[str] = ["open", "high", "low", "close"],
    op : str = "add",
)-> pd.DataFrame:
    
    for col in columns:
        if col in df.columns:
            if op == "add":
                df[col] += value
            elif op == 'sub':
                df[col] -= value
            elif op == 'mul':
                df[col] *= value
            elif op == 'div':
                df[col] /= value
            else:
                raise ValueError(f"Unsupported operation: {op}")
            
    return df


def df2df_apply_operation_to_given_columns(
    df1 : pd.DataFrame,
    df2 : pd.DataFrame,
    columns : list[str] = ["open", "high", "low", "close"],
    op : str = "add",
) -> pd.DataFrame:
    
    for col in columns:
        if col in df1.columns and col in df2.columns:
            if op == "add":
                df1[col] += df2[col]
            elif op == "sub":
                df1[col] -= df2[col]
            elif op == "mul":
                df1[col] *= df2[col]
            elif op == "div":
                df1[col] /= df2[col]
            else:
                raise ValueError(f"Unsupported operation: {op}")
    
    return df1

def get_full_year(
    two_digit_year : int,
    pivot : int = 50,
) -> int:
    if two_digit_year < pivot:
        return 2000 + two_digit_year
    else: 
        return 1900 + two_digit_year