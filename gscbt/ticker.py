from collections import defaultdict
import shutil 

import pandas as pd

from gscbt.utils import PATH, Dotdict

# this function is couple with iqfeed_data.excel(which is maintain by dev team for internal usage)
# any change in that file leads to change in this function
# this function return ticker with it's metadata in dictionary format

class Ticker:
    def _mv_iqfeed_excel_to_local_storage():
        if not PATH.IQFEED_EXCEL.exists():
            PATH.LOCAL_STORAGE.mkdir(parents=True, exist_ok=True)
            shutil.copy(PATH.IQFEED_EXCEL_LOCAL, PATH.IQFEED_EXCEL)

    def _parse_iqfeed_excel_to_Ticker():

        # Load Excel file into a DataFrame
        # skip first row "IQ FEED SYMBOL MAPPING"
        try:
            df = pd.read_excel(PATH.IQFEED_EXCEL, engine="calamine", skiprows=1)
        except FileNotFoundError:
            raise FileNotFoundError(f"[-] Ticker : Excel file don't exists")
        except Exception as e:
            raise Exception(f"[-] Ticker : ERROR") from e

        tickers = defaultdict(
            lambda: defaultdict(dict)
        ) 
        symbols = {}
        symbols_dict = {}

        column_list = [
            col.strip().replace(" ", "_").lower()
            for col in df.columns.tolist()
        ]

        if "exchange" not in column_list or "symbol" not in column_list:
            raise ValueError(f"[-] Ticker : Excel don't have Exchange and Symbol columns")

        for row in df.itertuples(index=False):
            data = {"type": "futures"}
            for column, val in zip(column_list, row):
                data[column] = val

            exchange = data["exchange"].lower()
            symbol = data["symbol"].lower()

            tickers[exchange][symbol]["f"] = data

            # reverse map
            symbols[data["symbol"]] = Dotdict(data)
            symbols_dict[data["symbol"]] = data    

        tickers = Dotdict(dict(tickers))
        return tickers, symbols, symbols_dict
    
    _mv_iqfeed_excel_to_local_storage()
    TICKERS, SYMBOLS, SYMBOLS_DICT = _parse_iqfeed_excel_to_Ticker()


def get_instrument_contract_months(instrument : str) -> str:
    months = Ticker.SYMBOLS[instrument].contract_months
    months = months.replace('-', '')
    return months


if __name__ == "__main__":
    pass
