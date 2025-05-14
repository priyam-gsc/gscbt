import pandas as pd

from gscbt.cache import Cache
from gscbt.expression_utils import extract_sym_month_year_from_contract
from gscbt.utils import Dotdict, PATH

def get_outright(
    ticker : Dotdict,
    contract : str ,
    ohlcv : str,
    interval : str = "1d",
    cache_mode: Cache.Mode = Cache.Mode.hdb_n_market_api,
) -> pd.DataFrame:

    df = pd.DataFrame()
    try:
        _, month, year = extract_sym_month_year_from_contract(contract)
        
        path = PATH.CACHE / ticker.exchange / ticker.symbol / ticker.type / interval
        path /= contract + ".parquet"
        
        if not path.exists():
            isCache = Cache.cache(
                ticker,
                interval,
                Cache.Datatype.outright,
                Cache.Metadata.create_outright(month, year),
                cache_mode
            )

            if not isCache:
                return df, False

        column_list = ["timeutc"]
        if "o" in ohlcv:
            column_list.append("open")
        if "h" in ohlcv:
            column_list.append("high")
        if "l" in ohlcv:
            column_list.append("low")
        if "c" in ohlcv:
            column_list.append("close")
        if "v" in ohlcv:
            column_list.append("volume")

        df = pd.read_parquet(path, columns=column_list)
        df.rename(columns={"timeutc" : "timestamp"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index(["timestamp"], inplace=True)

        return df, True
    
    except Exception as e:
        raise Exception("[-] DataPipeline.get_outright : ERROR") from e