from datetime import datetime
from pathlib import Path
from io import BytesIO
import os

import requests
import pandas as pd
import polars as pl
from dotenv import load_dotenv, dotenv_values, set_key

class DEFAULT:
    START_YEAR = 1950
    START_DATE = "1950-01-01"

class PATH:
    LOCAL_STORAGE = Path.home() / ".gscbt"
    ENV = LOCAL_STORAGE / ".env"
    PACKAGE_DIR = Path(__file__).parent
    LOCAL_DATA = PACKAGE_DIR / "config"
    CACHE = LOCAL_STORAGE / "cache"
    IQFEED_EXCEL = LOCAL_STORAGE / "iqfeed_data.xlsx"
    IQFEED_EXCEL_LOCAL = LOCAL_DATA / "iqfeed_data.xlsx"

class API:
    REQUIRED_KEYS = [
        "SERVER_IP_PORT",
        "LOCAL_WIN_DIRECT_IQFEED_IP_PORT",
        "HDB_IP_PORT"
    ]
    if PATH.ENV.exists():
        env_values = dotenv_values(dotenv_path=PATH.ENV)
        for key in REQUIRED_KEYS:
            if not env_values.get(key):
                PATH.ENV.unlink()
                break

    if not PATH.ENV.exists():
        Path(PATH.LOCAL_STORAGE).mkdir(parents=True, exist_ok=True)
        
        print("format 'ipv4:port' for example = 123.0.0.1:8080")

        for key in REQUIRED_KEYS:
            value = input(f"{key} = ").strip()
            
            while not value:
                print(f"{key} cannot be empty.")
                value = input(f"{key} = ").strip()
            
            set_key(str(PATH.ENV), key, value)

    
    load_dotenv(dotenv_path=PATH.ENV)
    SERVER_IP_PORT = os.getenv("SERVER_IP_PORT") 
    LOCAL_WIN_DIRECT_IQFEED_IP_PORT = os.getenv("LOCAL_WIN_DIRECT_IQFEED_IP_PORT")# replace  "your_ip:5675"
    HDB_IP_PORT = os.getenv("HDB_IP_PORT")
    
    GET_USD_CONVERSION = f"http://{SERVER_IP_PORT}/api/v1/data/dollarequivalent"
    DOWNLOAD_MARKET_DATA = f"http://{SERVER_IP_PORT}/api/v1/data/download"
    # GET_IQFEED_DATA = f"http://{SERVER_IP_PORT}/api/v2/data/iqfeed"
    GET_IQFEED_DATA = f"http://192.168.0.155:24504/data"
    GET_MARKET_DATA = f"http://{SERVER_IP_PORT}/api/v1/data/ohlcv"
    QUANT_APIS = f"http://{SERVER_IP_PORT}/api/v1/quant/data/ohlcv"
    DIRECT_IQFEED_APIS = f"http://{LOCAL_WIN_DIRECT_IQFEED_IP_PORT}/api/v1/data_parquet/iqfeed"


class Interval:
    INTERVALS = "smhd"
    SECONDS = [60, 3600, 86_400, -1]

    if len(INTERVALS) != len(SECONDS):
        raise ValueError(f"[-] Interval : static member length don't match \
                            \n\t len(INTERVALS) : {len(INTERVALS)} \
                            \n\t len(SECONDS) : {len(SECONDS)}"
        )

    @staticmethod
    def second_to_str(interval : int) -> str:
        try:
            if interval <= 0:
                raise
            for itr in range(len(Interval.INTERVALS)):
                if(Interval.SECONDS[itr] == -1 or interval < Interval.SECONDS[itr]):
                    if itr == 0:
                        return str(interval) + Interval.INTERVALS[itr]
                    else:
                        if interval % Interval.SECONDS[itr-1] != 0:
                            raise
                        
                        mul = str(interval // Interval.SECONDS[itr-1])
                        return mul + Interval.INTERVALS[itr]

        except:
            raise ValueError(f"[-] Interval.second_to_str : \
                               Invalid interval integer {interval}"
            )

    @staticmethod
    def str_to_second(interval : str) -> int:
        try:
            mul = int(interval[:-1])    
            idx = Interval.INTERVALS.find(interval[-1])
            if idx == -1:
                raise
            return mul * (Interval.SECONDS[idx-1] if idx>0 else 1)
            
        except:
            raise ValueError(f"[-] Interval.str_to_second : \
                               Invalid interval string {interval}"
            )


class Dotdict:
    def __init__(self, data):
        for key, value in data.items():
            setattr(self, key, Dotdict(value) if isinstance(value, dict) else value)

    def __getattr__(self, item):
        raise AttributeError(f"[-] Dotdict object has no attribute {item}")


class MonthMap:
    months = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

    if len(months) != 12:
        raise ValueError(f"[-] MonthMap : static member length don't match \
                            \n\t len(months) : {len(months)}"
        )

    @staticmethod
    def month(m):
        if isinstance(m, int):
            if 1<=m and m<=12:
                return MonthMap.months[m-1]
        
        elif isinstance(m, str):
            idx = -1
            for itr in range(len(MonthMap.months)):
                if MonthMap.months[itr] == m:
                    idx = itr
                    break

            if idx != -1:
                return idx + 1
        
        raise ValueError(f"[-] MonthMaps.month Invalid value {m} and \
                            value type {type(m)}")
            

    @staticmethod
    def min(m1 : str, m2 : str):
        try:
            if (type(m1) != type(m2) and isinstance(m1, str)):
                raise ValueError

            mn1 = MonthMap.month(m1)
            mn2 = MonthMap.month(m2)

            return mn1 if mn1 < mn2 else mn2
        except ValueError:
            raise ValueError(f"[-] MonthMaps.min Invalid value {m1} and {m2}")
            

def download_file(
        url : str, 
        filename_with_path : Path, 
        params : dict = None, 
        timeout: int = 30, 
        allow_redirect: bool = False
    ) -> int:
    response = requests.get(
        url,
        params=params,
        stream=True,
        timeout=timeout,
        allow_redirects=allow_redirect,
    )

    if response.status_code == 200:
        with open(filename_with_path, "wb") as file:
            file.write(response.content)
    
    return response.status_code

def json_to_parquet(json_path : Path, parquet_path : Path = None):
    if parquet_path is None:
        parquet_path = json_path.with_suffix(".parquet")

    if not json_path.exists():
        raise FileNotFoundError(f"[-] json_to_parquet : Path {json_path} not exists.")
    
    df = pl.read_json(json_path)
    df.write_parquet(parquet_path)

def remove_file(path: Path):
    if path.exists():
        path.unlink()

def req_wrapper(
    url : str,
    params : dict = None,
    timeout : int = 30,
) -> tuple[int, bytes]:

    res = requests.get(
        url,
        params = params,
        stream = True,
        timeout = timeout,
        allow_redirects = False,
    )

    return res.status_code, res.content

def bytes_to_df(
    content : bytes,
) -> pd.DataFrame:
    
    return pd.read_json(BytesIO(content))

if __name__ == "__main__":
    pass
