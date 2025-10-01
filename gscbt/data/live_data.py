from readerwriterlock import rwlock
from datetime import date, datetime, timedelta
import pytz
import pandas as pd 
import json

from gscbt.utils import (
    req_wrapper,
    bytes_to_df,
    API,
)

cache = {}
rw_lock = rwlock.RWLockFair()

def get_next_market_expiry() -> datetime:
    ist = pytz.timezone("Asia/Kolkata")

    now = datetime.now(ist)
    expiry = now.replace(hour=9, minute=15, second=0, microsecond=0)

    if now >= expiry:
        # If it's already past 9:15 today, set for tomorrow
        expiry += timedelta(days=1)
    return expiry

def get_live_data(
    symbol : str,
    ohlcv : str,
) -> tuple[bool, pd.DataFrame]:

    today = date.today()
    end_date = today.strftime('%Y-%m-%d')

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    r_lock = rw_lock.gen_rlock()
    with r_lock:
        if symbol in cache and now < cache["expiry"]:
            return True, cache[symbol]

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

    w_lock = rw_lock.gen_wlock()
    with w_lock:
        if len(cache) == 0 or symbol in cache:
            cache.clear()
            cache["expiry"] = get_next_market_expiry()

        cache[symbol] = df

    return True, df

def get_tick_n_eod_combine_data(
    symbol : str,
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

    column_drop_list = ["sym", "open_int", "open", "high", "low", "volume"]

    df.drop(column_drop_list, axis=1, inplace=True)
    df.set_index(["timestamp"], inplace=True)

    status_code, tick_data = req_wrapper("http://192.168.0.155:24503/latest", {"symbol":symbol})
    if status_code == 200:
        res = json.loads(tick_data)
        dt_utc = pd.to_datetime(res["timestamp"], utc=True)
        df.loc[dt_utc.normalize()] = res["price"]

    return True, df