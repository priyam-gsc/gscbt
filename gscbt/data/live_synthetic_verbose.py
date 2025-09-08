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
    create_expression_from_contracts_multipliers,
)

from .live_synthetic import get_config
    

def get_live_synthetic_contractwise_with_cache(
    expression : str,
    ohlcv : str,
    isBackAdjusted : bool,
    start_date : str,
    start_year : int,
    interval : str,
    offset : int,
    max_lookahead : int,
    mode : str = "normal",
    cache : dict = {}
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

            if itr_contract in cache:
                df = cache[itr_contract]
                ok = True
            else:
                ok, df = get_live_data(
                    symbol = itr_contract,
                    ohlcv = ohlcv,
                )
                if ok:
                    cache[itr_contract] = df

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

    res_df["days_to_roll"] = res_df.roll_date - res_df.index
    return res_df.loc[start_date:]

def get_live_synthetic_verbose(
    expression : str,
    start : str,
    offset : int,
    isBackAdjusted : bool = True,
    max_lookahead : int | None = None,
    mode : str = "normal"
) -> pd.DataFrame:
    
    if isBackAdjusted and max_lookahead == None:
        raise ValueError(f"[-] In backadjust mode max_lookahead can't be None value")

    start_date = pd.to_datetime(start)
    local_cache = {}
    
    contracts, multipliers = extract_contracts_multipliers(expression)
    df_list = []
    for i in range(len(contracts)):
        new_multipliers = [0]*len(contracts)
        new_multipliers[i] = 1
        a = create_expression_from_contracts_multipliers(contracts, new_multipliers)
        df = get_live_synthetic_contractwise_with_cache(
            expression = a,
            ohlcv  = "c",
            isBackAdjusted = isBackAdjusted,
            start_date = start,
            start_year = start_date.year - 1,
            interval = "1d",
            offset  = offset,
            max_lookahead = max_lookahead,
            mode = mode,
            cache = local_cache
        )
        df = df.drop(columns=["roll_date", "days_to_roll"])
        df = df.rename(columns={"close": contracts[i]})
        df_list.append(df)
 
    df = get_live_synthetic_contractwise_with_cache(
        expression = expression,
        ohlcv  = "c",
        isBackAdjusted = isBackAdjusted,
        start_date = start,
        start_year = start_date.year - 1,
        interval = "1d",
        offset  = offset,
        max_lookahead = max_lookahead,
        mode = mode,
        cache = local_cache
    )

    df_list.append(df)
    res_df = pd.concat(df_list, axis=1)
    return res_df