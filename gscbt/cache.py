from datetime import datetime
from pathlib import Path
from enum import Enum, auto

from .hdb_client import (
    prep_hdb_key,
    hdb_download,
)

from .utils import (
    DEFAULT,
    PATH, 
    API, 
    Dotdict, 
    Interval,
    download_file, 
    json_to_parquet, 
    remove_file,
)

def is_year_historical(year : int) -> bool:
    if(24 < year and year < 50):
        return False
    
    return True

class Cache:
    class Datatype(Enum):
        underlying = auto()
        back_adjusted = auto()
        outright = auto()

    class Mode(Enum):
        hdb = auto()
        hdb_n_market_api = auto()
        market_api = auto()
        direct_iqfeed = auto()

    class Metadata():
        def __init__(
            self, 
            symbol_suffix : str, 
            filename_suffix : str,
            month_code : str = None, 
            year : str = None,
        ):
            self.symbol_suffix = symbol_suffix
            self.filename_suffix = filename_suffix
            self.month_code = month_code
            self.year = year
        
        def __repr__(self):
            return f"Cache.Metadata({self.symbol_suffix}, {self.month_code}, {self.year})"
        
        @staticmethod
        def create_underlying():
            return Cache.Metadata("#", "c1")
        
        @staticmethod
        def create_back_adjusted():
            return Cache.Metadata("#C", "cd1")
        
        @staticmethod
        def create_outright(month : str, year : str):
            return Cache.Metadata(month+year, month+year, month, year)
        
    def cache(
        ticker : Dotdict,
        interval : str, 
        cache_datatype : Datatype, 
        cache_metadata : Metadata,
        cache_mode: Mode,
    ) -> bool:
        file_path = PATH.CACHE / ticker.exchange / ticker.symbol / ticker.type
        file_path /=  interval
        
        if cache_datatype == Cache.Datatype.outright:    
            if(cache_metadata.month_code == None or cache_metadata.year == None):
                raise ValueError(f"[-] Cache.cache Invalid cache_metadata {cache_metadata} \
                                 for given cache_datatype {cache_datatype}")
            
        Path(file_path).mkdir(parents=True, exist_ok=True)
        filename = ticker.symbol + cache_metadata.filename_suffix

        path = file_path / Path(filename).with_suffix(".parquet")
        if path.exists():
            return True

        hdb_flag = None      
        if(
            cache_datatype == Cache.Datatype.outright and
            (cache_mode == Cache.Mode.hdb or
            cache_mode == Cache.Mode.hdb_n_market_api) and
            is_year_historical(int(cache_metadata.year)) 
        ):       
            # only historical data from hdb (data upto 31-12-2024 for any data)
            hdb_key = prep_hdb_key(
                interval, 
                ticker.iqfeed_symbol + cache_metadata.symbol_suffix,
                ticker.symbol + cache_metadata.symbol_suffix,
            )
            hdb_flag = hdb_download(
                API.HDB_IP_PORT,
                hdb_key,
                path
            )

        if(
            cache_mode == Cache.Mode.market_api or
            (cache_mode == Cache.Mode.hdb_n_market_api and hdb_flag != 1)
        ):
            path = file_path / Path(filename).with_suffix(".json")
            url = API.GET_IQFEED_DATA
            params = {
                "symbols": ticker.iqfeed_symbol + cache_metadata.symbol_suffix,
                "start_date": DEFAULT.START_DATE,
                "end_date": datetime.today().strftime("%Y-%m-%d"),
                "type": "eod" if interval == "1d" else "ohlcv",
                "duration": Interval.str_to_second(interval),
            }

            status_code = download_file(url, path, params)
            if status_code == 200:
                json_to_parquet(path)
                remove_file(path)

        elif cache_mode == Cache.Mode.direct_iqfeed:
            url = API.DIRECT_IQFEED_APIS
            params = {
                "symbols": ticker.iqfeed_symbol + cache_metadata.symbol_suffix,
                "start_date":DEFAULT.START_DATE,
                "end_date": datetime.today().strftime("%Y-%m-%d"),
                "type": "eod" if interval == "1d" else "ohlcv",
                "duration": Interval.str_to_second(interval),
            }
            download_file(url, path, params)
        else:
            pass

        if path.exists():
            return True
        else:
            return False 

    def outrights(
        tickers : list[Dotdict],
        intervals : list[str],
        start_year : int = None,
        end_year : int = None,
        cache_mode: Mode = Mode.hdb_n_market_api,
        verbose : bool = False
    ):
        isStartYearNone = False
        if start_year == None:
            isStartYearNone = True

        if end_year == None:
            end_year = datetime.today().year


        for ticker in tickers:
            rev_valid_months = ticker.contract_months.replace("-", "")[::-1]
    
            if isStartYearNone:
                start_year = DEFAULT.START_YEAR
            
            for interval in intervals:
                isCached = False
                for year in range(end_year, start_year-1, -1):
                    for month in rev_valid_months:
                        isCached = Cache.cache(
                            ticker,
                            interval,
                            Cache.Datatype.outright,
                            Cache.Metadata.create_outright(month, f"{year%100:02}"),
                            cache_mode
                        )

                        if not isCached:
                            break
                    if not isCached:
                        break    

            if verbose:
                print(f"{ticker.symbol} for {interval} cached.")        

    def continuous(
        tickers : list[Dotdict],
        intervals : list[str],
        cache_mode: Mode = Mode.hdb_n_market_api,
        verbose : bool = False
    ):
        for ticker in tickers:
            for interval in intervals:
                Cache.cache(
                    ticker,
                    interval,
                    Cache.Datatype.underlying,
                    Cache.Metadata.create_underlying(),
                    cache_mode
                )

                Cache.cache(
                    ticker,
                    interval,
                    Cache.Datatype.underlying,
                    Cache.Metadata.create_back_adjusted(),
                    cache_mode
                )

            if verbose:
                print(f"{ticker.symbol} cached.")   