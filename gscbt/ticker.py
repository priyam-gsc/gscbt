from collections import defaultdict

import pandas as pd

from gscbt.utils import PATH, Dotdict

# this function is couple with iqfeed_data.excel(which is maintain by dev team for internal usage)
# any change in that file leads to change in this function
# this function return ticker with it's metadata in dictionary format

class Ticker:
    def _parse_iqfeed_excel_to_Ticker():

        COLUMNS = [
            "Product",
            "Symbol",
            "IQFeed symbol",
            "Exchange",
            "Data From Date",
            "Asset Class",
            "Product Group",
            "Repeat",
            "Currency Multiplier",
            "Currency",
            "Exchange rate",
            "Dollar equivalent",
            "Contract Months",
            "Last Contract",
            "Currency Tick Value",
            "Cost in Ticks",
            "Commission Cost",
            "Trading Hours",
            "Roll Offset",
        ]

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

        # Iterate through the rows of the DataFrame
        for _, row in df.iterrows():
            # you will be using class.
            exchange = row.get("Exchange", None).lower()
            symbol = row.get("Symbol", None).lower()

            if exchange == None or symbol == None:
                raise ValueError(f"[-] Ticker : Excel don't have Exchange and Symbol columns")

            # Construct the dictionary structure for "Future" data
            # Make all column a field
            data = {
                "type": "futures",  # custom added not part of excel file
            }

            for column in COLUMNS:
                key = column.lower().replace(" ", "_")
                data[key] = row.get(column, None)
            # Populate the nested dictionary with exchange and symbol
            # "f" is used for futures
            tickers[exchange][symbol]["f"] = data

            # reverse map
            symbols[row.get("Symbol")] = Dotdict(data)
            symbols_dict[row.get("Symbol")] = data

        # Convert defaultdict to normal dict before returning
        # and normal dict to dotaccessdict(DotDict)

        tickers = Dotdict(dict(tickers))
        return tickers, symbols, symbols_dict
    
    TICKERS, SYMBOLS, SYMBOLS_DICT = _parse_iqfeed_excel_to_Ticker()

if __name__ == "__main__":
    pass
