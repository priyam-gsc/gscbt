from datetime import datetime
from pathlib import Path
from enum import Enum, auto
from concurrent.futures import ThreadPoolExecutor

from gscbt.utils import (
    PATH, 
    API, 
    Dotdict, 
    Interval,
    download_file, 
    json_to_parquet, 
    remove_file,
)

class Cache:
    class Datatype(Enum):
        underlying = auto()
        back_adjusted = auto()
        outright = auto()

    class Mode(Enum):
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
    ):
        file_path = PATH.CACHE / ticker.exchange / ticker.symbol / ticker.type
        file_path /=  interval
        
        if cache_datatype == Cache.Datatype.outright:    
            if(cache_metadata.month_code == None or cache_metadata.year == None):
                raise ValueError(f"[-] Cache.cache Invalid cache_metadata {cache_metadata} \
                                 for given cache_datatype {cache_datatype}")
            
        Path(file_path).mkdir(parents=True, exist_ok=True)
        filename = ticker.symbol + cache_metadata.filename_suffix

        url = API.GET_IQFEED_DATA
        params = {
            "symbols": ticker.iqfeed_symbol + cache_metadata.symbol_suffix,
            "start_date": ticker.data_from_date.strftime("%Y-%m-%d"),
            "end_date": datetime.today().strftime("%Y-%m-%d"),
            "type": "eod" if interval == "1d" else "ohlcv",
            "duration": Interval.str_to_second(interval),
        }

        path = file_path / Path(filename).with_suffix(".parquet")
        if not path.exists():
            if cache_mode == Cache.Mode.direct_iqfeed:
                url = API.DIRECT_IQFEED_APIS
                download_file(url, path, params)

            else:
                path = file_path / Path(filename).with_suffix(".json")
                download_file(url, path, params)
                json_to_parquet(path)
                remove_file(path)

    def outrights(
        tickers : list[Dotdict],
        intervals : list[str],
        start_year : int = None,
        end_year : int = None,
        cache_mode: Mode = Mode.market_api,
        max_workers : int = 1,
        verbose : bool = False
    ):
        if cache_mode == Cache.Mode.market_api:
            max_workers = 1

        isStartYearNone = False
        if start_year == None:
            isStartYearNone = True

        if end_year == None:
            end_year = datetime.today().year

        for ticker in tickers:
            valid_months = ticker.contract_months.replace("-", "")
    
            if isStartYearNone:
                start_year = ticker.data_from_date.year
            
            for interval in intervals:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for year in range(start_year, end_year+1):
                        for month in valid_months:
                            executor.submit(
                                Cache.cache,
                                ticker,
                                interval,
                                Cache.Datatype.outright,
                                Cache.Metadata.create_outright(month, f"{year%100:02}"),
                                cache_mode
                            )

                if verbose:
                    print(f"{ticker.symbol} for {interval} cached.")        

    def continuous(
        tickers : list[Dotdict],
        intervals : list[str],
        cache_mode: Mode = Mode.market_api,
        max_workers : int = 1,
        verbose : bool = False
    ):
        if cache_mode == Cache.Mode.market_api:
            max_workers = 1

        for ticker in tickers:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for interval in intervals:
                    executor.submit(
                        Cache.cache,
                        ticker,
                        interval,
                        Cache.Datatype.underlying,
                        Cache.Metadata.create_underlying(),
                        cache_mode
                    )

                    executor.submit(
                        Cache.cache,
                        ticker,
                        interval,
                        Cache.Datatype.underlying,
                        Cache.Metadata.create_back_adjusted(),
                        cache_mode
                    )

            if verbose:
                print(f"{ticker.symbol} cached.")   