from datetime import datetime
import json

import pandas as pd

from .live_data import get_live_data
from .spread import (
    offset_roll
)
from gscbt.utils import (
    req_wrapper,
)

from gscbt.expression_utils import (
    move_contracts_to_prev_year,
    extract_contracts_multipliers,

)
    


def get_config():
    # getting json data
    CONFIG_DATA_API = "http://127.0.0.1:24502"

    status_code, content = req_wrapper(
        CONFIG_DATA_API,
    )
    json_data = json.loads(content)

    result = {}
    for product in json_data["productContract"]:
        symbol = product["symbol"]

        result[symbol] = product
        for contract in product["contracts"]:
            contract_code = contract["contractCode"]
            result[f"{symbol}{contract_code}"] = contract
            
    return result
        
def get_live_synthetic_contractwise(
    expression : str,
    ohlcv : str,
    isBackAdjusted : bool,
    start_year : int,
    interval : str,
    offset : int,
    max_lookahead : int,
    mode : str = "normal"
) -> pd.DataFrame:
    
    contracts, multipliers = extract_contracts_multipliers(expression)
    contract_count = len(contracts)
    META = get_config()

    synthetic_df_list = []
    synthetic_roll_list = []

    end_year = datetime.today().year
    itr_contracts = contracts

    for itr_year in range(end_year, start_year-1, -1):
        itr_synthetic_df = pd.DataFrame()
        roll_date = None
        isAllLegFound = True

        for itr in range(contract_count):
            itr_contract = itr_contracts[itr]

            ok, df = get_live_data(
                    symbol = itr_contract,
                   ohlcv = ohlcv,
            ) 
            if not ok:
                isAllLegFound = False
                print(f"Data for contract {itr_contract} not available so stop at year {itr_year}")
                break
            
            df = df * multipliers[itr]
            df = df * META[itr_contract[:-3]]["currencyMultiplier"]

            itr_expiry_date = None
            if itr_contract in META:
                itr_expiry_date = META[itr_contract]["expiry"]
                itr_expiry_date = pd.to_datetime(itr_expiry_date)
            else:                
                itr_expiry_date = df.index[-1]

            if itr_synthetic_df.empty:
                itr_synthetic_df = df.copy()
                roll_date = itr_expiry_date
            else:
                itr_synthetic_df += df
                roll_date = min(roll_date, itr_expiry_date)

        if not isAllLegFound:
            break

        synthetic_df_list.append(itr_synthetic_df)

        roll_date -= pd.DateOffset(days=offset)
        synthetic_roll_list.append(roll_date)

        itr_contracts = move_contracts_to_prev_year(itr_contracts)

    synthetic_df_list = synthetic_df_list[::-1]
    synthetic_roll_list = synthetic_roll_list[::-1]

    res_df = offset_roll(
        synthetic_df_list = synthetic_df_list,
        synthetic_roll_list = synthetic_roll_list,
        interval = interval,
        isBackAdjusted = isBackAdjusted,
        max_lookahead = max_lookahead,
        mode=mode,
    )

    return res_df

def get_live_synthetic(
    expression : str,
    start : str,
    offset : int,
    ohlcv : str  = "c",
    isBackAdjusted : bool = True,
    interval : str = "1d",
    roll_method : str = "contractwise",
    max_lookahead : int | None = None,
    mode : str = "normal"
) -> pd.DataFrame:
    
    if isBackAdjusted and max_lookahead == None:
        raise ValueError(f"[-] In backadjust mode max_lookahead can't be None value")

    start_date = pd.to_datetime(start)

    if roll_method == "contractwise":
        res_df = get_live_synthetic_contractwise(
            expression = expression,
            ohlcv = ohlcv,
            isBackAdjusted = isBackAdjusted,
            start_year = start_date.year - 1,
            interval = interval,
            offset = offset,
            max_lookahead = max_lookahead,
            mode = mode,
        )

        res_df["days_to_roll"] = res_df.roll_date - res_df.index
        return res_df.loc[start:]
    

    return pd.DataFrame()



def get_live_synthetic_stack(
    expression : str,
    start_year : int,
    interval : str = "1d",
) -> list[pd.DataFrame]:
    contracts, multipliers = extract_contracts_multipliers(expression)
    contract_count = len(contracts)
    META = get_config()

    synthetic_df_list = []

    end_year = datetime.today().year
    itr_contracts = contracts

    for itr_year in range(end_year, start_year-1, -1):
        itr_synthetic_df = pd.DataFrame()
        roll_date = None
        isAllLegFound = True

        for itr in range(contract_count):
            itr_contract = itr_contracts[itr]

            ok, df = get_live_data(
                    symbol = itr_contract,
                   ohlcv = "c",
            )
            if not ok:
                isAllLegFound = False
                print(f"Data for contract {itr_contract} not available so stop at year {itr_year}")
                break

            df = df * multipliers[itr]
            df = df * META[itr_contract[:-3]]["currencyMultiplier"]

            itr_expiry_date = None
            if itr_contract in META:
                itr_expiry_date = META[itr_contract]["expiry"]
                itr_expiry_date = pd.to_datetime(itr_expiry_date)
            else:
                itr_expiry_date = df.index[-1]

            if itr_synthetic_df.empty:
                itr_synthetic_df = df.copy()
                roll_date = itr_expiry_date
            else:
                itr_synthetic_df += df
                roll_date = min(roll_date, itr_expiry_date)

        if not isAllLegFound:
            break

        itr_synthetic_df.rename(columns={"close": str(roll_date.year)}, inplace=True)
        synthetic_df_list.append(itr_synthetic_df)

        itr_contracts = move_contracts_to_prev_year(itr_contracts)

    synthetic_df_list = synthetic_df_list[::-1]

    return synthetic_df_list