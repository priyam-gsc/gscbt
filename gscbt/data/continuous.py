
import pandas as pd
import pyarrow.parquet as pq

from gscbt.cache import Cache
from gscbt.utils import Dotdict, PATH

from .utils import add_back_adjusted_diff

def get(
    tickers : list[Dotdict],
    ohlcv : str = "c",
    back_adjusted : bool = True,
    start : str = None,
    end : str = None,
    interval : str = "1d",
    cache_mode : Cache.Mode = Cache.Mode.market_api,
):
    df = pd.DataFrame()

    for ticker in tickers:
        df_contract = get_continuous(
            ticker, ohlcv, back_adjusted, start, end, interval, cache_mode
        )
        if df.empty:
            df = df_contract
        else:
            df = pd.merge(
                df, df_contract, how="outer", left_index=True, right_index=True
            )

    # convert df into
    # Close          Low
    # CL    RC      CL    RC
    df = df.swaplevel(axis=1).sort_index(axis=1)
    return df

def get_continuous(
    ticker : Dotdict,
    ohlcv : str = "c",
    back_adjusted : bool = True,
    start : str = None,
    end : str = None,
    interval : str = "1d",
    cache_mode : Cache.Mode = Cache.Mode.market_api,
):
    cache_datatype = Cache.Datatype.underlying
    cache_metadata = Cache.Metadata.create_underlying()

    if back_adjusted:
        cache_datatype = Cache.Datatype.back_adjusted
        cache_metadata = Cache.Metadata.create_back_adjusted()
    
    contract_type = "cd1" if back_adjusted else "c1"
    file_type = ".parquet"
    ticker.filename = ticker.symbol + contract_type + file_type

    file_path = PATH.CACHE / ticker.exchange / ticker.symbol / ticker.type / interval
    path = file_path / ticker.filename

    if not path.exists():
        Cache.cache(
            ticker,
            interval,
            cache_datatype,
            cache_metadata,
            cache_mode
        )

    column_list = ["timeutc"]
    if "o" in ohlcv:
        column_list.append("open")
    if "h" in ohlcv:
        column_list.append("high")
    if "c" in ohlcv:
        column_list.append("close")
    if "l" in ohlcv:
        column_list.append("low")
    if "v" in ohlcv:
        column_list.append("volume")

    # this will read only rows which requrie + some over head
    # pf : parquet file
    pf = pq.ParquetFile(path)

    # first we fetch start and end row gorud id from stored parquet format
    st_row_group_idx = 0
    end_row_group_idx = pf.num_row_groups - 1

    if start != None:
        st_row_group_idx = row_group_finder(pf, column_list[0], start)

    if end != None:
        end_row_group_idx = row_group_finder(pf, column_list[0], end)

    # join all row groups from start to end
    # convert it into pandas dataframe
    list_of_row_groups = list(range(st_row_group_idx, end_row_group_idx + 1))
    df = pf.read_row_groups(list_of_row_groups, columns=column_list).to_pandas()

    df = df.rename(columns={"timeutc": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df.set_index(["timestamp"], inplace=True)

    # bcz we are accessing row groups we have some unneccessary entries at start and end
    # which requrie us to cut it
    df = df.loc[start:end]

    # re-backadjust the data from the end date
    if back_adjusted and end != None:

        # getting last row of underlying for finding diff
        underlying_contract_type = "c1"
        back_adjusted_contract_type = "cd1"

        underlying_filename = ticker.symbol + underlying_contract_type + file_type
        underlying_path = file_path / underlying_filename

        back_adjusted_filename = (
            ticker.symbol + back_adjusted_contract_type + file_type
        )
        back_adjusted_path = file_path / back_adjusted_filename

        # there is case where we believe that underlying data is already cache
        # but there is possibility that underlying don't exists
        if not underlying_path.exists():
            Cache.cache(
                ticker,
                interval,
                Cache.Datatype.underlying,
                Cache.Metadata.create_underlying(),
                cache_mode
            )

        u_pf = pq.ParquetFile(underlying_path)
        u_row_group_idx = row_group_finder(u_pf, column_list[0], end)
        u_df = u_pf.read_row_group(
            u_row_group_idx, columns=[column_list[0], "close"]
        ).to_pandas()

        b_pf = pq.ParquetFile(back_adjusted_path)
        b_row_group_idx = row_group_finder(b_pf, column_list[0], end)
        b_df = b_pf.read_row_group(
            b_row_group_idx, columns=[column_list[0], "close"]
        ).to_pandas()

        u_df = u_df.rename(columns={"timeutc": "timestamp"})
        b_df = b_df.rename(columns={"timeutc": "timestamp"})

        u_df["timestamp"] = pd.to_datetime(u_df["timestamp"], utc=True)
        u_df.set_index(["timestamp"], inplace=True)

        b_df["timestamp"] = pd.to_datetime(b_df["timestamp"], utc=True)
        b_df.set_index(["timestamp"], inplace=True)

        u_df = u_df.loc[:end]
        b_df = b_df.loc[:end]

        u_df = u_df.reindex(b_df.index)

        diff = u_df.iloc[-1]["close"] - b_df.iloc[-1]["close"]

        # eleminate diff but not from volume column
        add_back_adjusted_diff(df, diff)

    # Create MultiIndex for column
    columns = pd.MultiIndex.from_tuples(
        [
            (ticker.symbol, i)
            for i in column_list
            if (i != "timestamp" and i != "timeutc")
        ]
    )
    df.columns = columns

    df = df.sort_index()
    return df

def row_group_finder(pf, col_name, target):
    left = 0
    right = pf.num_row_groups - 1

    while left <= right:
        mid = left + (right - left) // 2

        temp_res = is_timestamp_in_row_group_bs(pf, mid, col_name, target)
        if temp_res == 0:
            return mid
        elif temp_res > 0:
            left = mid + 1
        else:
            right = mid - 1

    return -1

# # bs : binary search
# # target string should be able to get converted into timestamp_format
def is_timestamp_in_row_group_bs(pf, mid, col_name, target):

    table = pf.read_row_group(mid, columns=[col_name])

    st = table[0][0].as_py()
    end = table[0][-1].as_py()

    st = pd.to_datetime(st, utc=True)
    end = pd.to_datetime(end, utc=True)
    target = pd.to_datetime(target, utc=True)

    if target < st:
        return -1
    if end < target:
        return 1
    return 0