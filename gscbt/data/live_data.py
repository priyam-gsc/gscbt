from datetime import date

import pandas as pd 

from gscbt.utils import (
    req_wrapper,
    bytes_to_df,
    API,
)


def get_live_data(
    symbol : str,
    ohlcv : str,
) -> tuple[bool, pd.DataFrame]:

    today = date.today()
    end_date = today.strftime('%Y-%m-%d')

    params = {
        "symbols" : symbol,
        "from" : "1950-01-01",
        "to" : end_date
    }

    status_code, content = req_wrapper(API.GET_MARKET_DATA, params)

    if status_code != 200:
        return False, pd.DataFrame()

    df = bytes_to_df(content)
    df.columns = df.columns.str.lower()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    column_drop_list = ["sym", "open_int"]
    if "o" not in ohlcv:
        column_drop_list.append("open")
    if "h" not in ohlcv: 
        column_drop_list.append("high")
    if "l" not in ohlcv:
        column_drop_list.append("low")
    if "c" not in ohlcv:
        column_drop_list.append("close")
    if "v" not in ohlcv:
        column_drop_list.append("volume")

    df.drop(column_drop_list, axis=1, inplace=True)
    df.set_index(["timestamp"], inplace=True)

    return True, df
