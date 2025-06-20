from datetime import datetime

import pandas as pd

from gscbt.utils import Dotdict

def adjuste_eod_timezone_n_price(df : pd.Series, ticker : Dotdict) -> pd.Series:

    df2 = df.copy()

    assert type(ticker.settlement_time) != datetime.time, "[-] iqfeed settlement time ValueError"
    
    fixed_time = pd.to_timedelta(str(ticker.settlement_time))
    df2.index = df2.index.tz_convert(None).tz_localize(str(ticker.settlement_timezone))
    df2.index = df2.index.normalize() + fixed_time

    df2.index = df2.index.tz_convert("US/Eastern")

    date_strs = df2.index.normalize().strftime("%Y-%m-%d")
    df2[:] = pd.Series(date_strs).map(df).values

    df2.index = df2.index.tz_convert("UTC")

    return df2

def avg_price_calculation(
    prev_price : int,
    prev_pos : int,
    curr_price : int, 
    curr_pos : int,
) -> tuple[float, bool]:
    # second return value will be true when some position are cut
    # i.e. when prev and curr position are at opposite side(buy, sell)

    if prev_pos == 0:
        return curr_price, False
    elif (prev_pos < 0 and curr_pos < 0) or (prev_pos >= 0 and curr_pos >= 0):
        return ((prev_pos * prev_price) + (curr_pos * curr_price))/(prev_pos + curr_pos), False
    else:
        if prev_pos + curr_pos == 0:
            return 0, True
        elif abs(prev_pos) > abs(curr_pos):
            return prev_price, True
        else:
            return curr_price, True