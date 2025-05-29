from enum import Enum, auto
from datetime import datetime

import numpy as np 
import pandas as pd

from gscbt.ticker import Ticker
from gscbt.cache import Cache
from gscbt.expression_utils import (
    extract_contracts_multipliers_operators,
    extract_year_offset,
    convert_contracts_to_offset_contracts,
    extract_min_contracts_from_contracts,
    convert_offset_contracts_to_given_year,
    move_contracts_to_prev_year,
    move_contracts_to_prev_valid_month,
)
from gscbt.utils import DEFAULT

from .outright import get_outright
from .utils import (
    add_back_adjusted_diff, 
)

class RollMethod(Enum):
    offset = auto()
    volume = auto()
    open_interest = auto()

    contractwise = auto()
    spreadwise = auto()




def apply_multipliers_operators_to_df_contract_list(
    contracts : list[str],
    df_contract_list : list[pd.DataFrame],
    multipliers : list[int],
    operators : list[str]
) -> pd.DataFrame:
    
    if (len(contracts) != len(df_contract_list) or
        len(contracts) != len(multipliers) or
        len(contracts) != len(operators)):

        raise ValueError(
            f"[-] DataPipeline.apply_multipliers_operators_to_df_contract_list \n"
            "length of contracts, df_contract_list, multipliers and operators don't match \n"
        )

    final_df = pd.DataFrame()
    
    for df_itr in range(len(df_contract_list)):
        sym = contracts[df_itr][:-3]

        df_contract = df_contract_list[df_itr]
        df_contract *= multipliers[df_itr] 
        df_contract *= Ticker.SYMBOLS[sym].currency_multiplier

        if(operators[df_itr] == '-'):
            df_contract *= -1

        if final_df.empty:
            final_df = df_contract
        else:
            final_df += df_contract
    
    return final_df


def get_spread(
    expression : str,
    ohlcv : str  = "c",
    back_adjusted : bool = True,
    start : str = None,
    end : str = None,
    interval : str = "1d",
    roll_method : RollMethod = RollMethod.contractwise,
    cache_mode : Cache.Mode = Cache.Mode.hdb_n_market_api,
    max_lookback_for_back_adjust : int = 1,
    verbose : bool = False
) -> pd.DataFrame:
    
    df = pd.DataFrame()
    
    expression = expression.replace(" ", "")
    contracts, multipliers, operators = extract_contracts_multipliers_operators(expression)
    year_offset = extract_year_offset(contracts)
    offset_contracts = convert_contracts_to_offset_contracts(contracts)
    min_offset_contracts = extract_min_contracts_from_contracts(offset_contracts)

    start_year =  DEFAULT.START_YEAR
    end_year = datetime.today().year
    if start != None:
        start_year = pd.to_datetime(start).year
    if end != None:
        end_year = pd.to_datetime(end).year


    isFirstEmptyFound = False
    df_spread_list = []
    df_contract_list = []

    if roll_method == RollMethod.contractwise:
        for itr_year in range(end_year, start_year-1, -1):
            df_contract_list.clear()
            itr_contracts = convert_offset_contracts_to_given_year(
                offset_contracts, 
                f"{itr_year%100:02}"
            )
            
            for contract in itr_contracts:
                sym = contract[:-3]
                
                # +"c" is work around for back_adjusted at any end date with no close in ohlcv
                df_contract, ok =  get_outright(
                    Ticker.SYMBOLS[sym], 
                    contract, 
                    ohlcv+"c", 
                    interval, 
                    cache_mode
                )

                if not ok:
                    isFirstEmptyFound = True
                    break                 
                else:
                    df_contract_list.append(df_contract)


            if isFirstEmptyFound:
                break

            final_df = apply_multipliers_operators_to_df_contract_list(
                itr_contracts,
                df_contract_list,
                multipliers,
                operators
            )

            # finding min date of roll
            min_contracts = convert_offset_contracts_to_given_year(
                min_offset_contracts, 
                f"{(itr_year-year_offset)%100:02}"
            )

            roll_date = None
            for contract in min_contracts:
                sym = contract[:-3]
                ticker = Ticker.SYMBOLS[sym]
                df_contract, ok = get_outright(ticker, contract, "c", interval, cache_mode)

                if not ok:
                    new_temp_min_contract = f"{contract[:-2]}{(itr_year+year_offset)%100:02}"

                    if verbose:
                        print(f"new_temp_min_contract : {new_temp_min_contract}\n")

                    df_contract, ok = get_outright(ticker, new_temp_min_contract, "c", interval, cache_mode)
                    
                    if len(df_contract.index.normalize().unique()) < ticker.roll_offset:
                        raise Exception(
                            "[-] DataPipeline contract don't have enough data to"
                            f"roll over given roll_offset {ticker.roll_offset}"
                        )
                    temp_date = df_contract.index.normalize().unique()[-ticker.roll_offset]
                    temp_date -= pd.DateOffset(years=year_offset)
                    if roll_date == None:
                        roll_date = temp_date
                    elif roll_date > temp_date:
                        roll_date = temp_date
                else:
                    if len(df_contract.index.normalize().unique()) < ticker.roll_offset:
                        raise Exception(
                            "[-] DataPipeline contract don't have enough data to"
                            f"roll over given roll_offset {ticker.roll_offset}"
                        )
                    temp_date = df_contract.index.normalize().unique()[-ticker.roll_offset]
                    if roll_date == None:
                        roll_date = temp_date
                    elif roll_date > temp_date:
                        roll_date = temp_date

            # start_crop = None
            # if(len(df_spread_list) == 0):
            #     start_crop = roll_date - pd.DateOffset(years=1)
            #     final_df = final_df.loc[start_crop:]

            df_spread_list.append(final_df.loc[:roll_date])

        # perform join to yearwise spread
        itr = None

        for df_spread in df_spread_list[::-1]:
            diff = None
            df_trimmed = None

            if df.empty:
                df = df_spread.copy()
            else:
                df_trimmed = df_spread[df_spread.index > df.index[-1]]

                if back_adjusted:
                    itr  = -1
                    while (
                        -itr <= max_lookback_for_back_adjust and
                        -itr <= len(df.index) and
                        df.index[itr] >= df_spread.index[0] and 
                        (
                            df.index[itr] not in df_spread.index or 
                            pd.isna(df_spread.loc[df.index[itr]]["close"]) or
                            pd.isna(df.loc[df.index[itr]]["close"])
                        )
                    ):
                        df.loc[df.index[itr], "close"] = np.nan 
                        itr -= 1

                    if -itr > max_lookback_for_back_adjust:
                        raise Exception(
                            "[-] DataPipeline.get_spread unable to back adjust "
                            "in given max_lookback_for_back_adjust : "
                            f"{max_lookback_for_back_adjust}"
                        )
                    
                    if (
                        -itr > len(df.index) or 
                        df.index[itr] < df_spread.index[0] or 
                        (
                            pd.isna(df_spread.loc[df.index[itr]]["close"]) and
                            pd.isna(df.loc[df.index[itr]]["close"])
                        )
                    ):
                        raise Exception(
                            "[-] DataPipeline.get_spread unable to back adjust"
                        )

                    diff = df_spread.loc[df.index[itr]]["close"] - df.loc[df.index[itr]]["close"]
                    add_back_adjusted_diff(df, diff)

                df = pd.concat([df, df_trimmed])

    # # handle continuous 
    elif roll_method == RollMethod.spreadwise:     
        temp_itr = 1
        while temp_itr < len(contracts):
            sym1 = contracts[temp_itr - 1][:-3]
            sym2 = contracts[temp_itr - 2][:-3]

            month1 = Ticker.SYMBOLS[sym1].contract_months.replace("-", "")
            month2 = Ticker.SYMBOLS[sym2].contract_months.replace("-", "")

            if len(month1) != len(month2):
                raise Exception(
                    "[-] DataPipeline.get_spread spreadwise"
                    "spread contain contract which don't have same valid month count\n"
                    f"{temp_itr} : \n"
                    f"{sym1} :: {month1} :: {len(month1)} \n"
                    f"{sym2} :: {month2} :: {len(month2)} \n"
                )
            temp_itr += 1

        start_contracts = convert_offset_contracts_to_given_year(offset_contracts, f"{start_year%100:02}")
        end_contracts  = convert_offset_contracts_to_given_year(offset_contracts, f"{end_year%100:02}")
        itr_contracts = end_contracts

        continue_itr = True
        while continue_itr:

            if itr_contracts == start_contracts:
                continue_itr = False

            df_contract_list.clear()

            for contract in itr_contracts:
                sym = contract[:-3]

                # +"c" is work around for back_adjusted at any end date with no close in ohlcv
                df_contract, ok = get_outright(
                    Ticker.SYMBOLS[sym], 
                    contract, 
                    ohlcv+"c", 
                    interval, 
                    cache_mode
                )

                if not ok:
                    isFirstEmptyFound = True
                    break
                else:
                    df_contract_list.append(df_contract)

            if isFirstEmptyFound:
                break

            final_df = apply_multipliers_operators_to_df_contract_list(
                itr_contracts,
                df_contract_list,
                multipliers,
                operators
            )

            min_contracts = extract_min_contracts_from_contracts(itr_contracts)
            for _ in range(year_offset):
                min_contracts = move_contracts_to_prev_year(min_contracts)

            roll_date = None
            for contract in min_contracts:
                sym = contract[:-3]
                ticker = Ticker.SYMBOLS[sym]
                df_contract, ok = get_outright(ticker, contract, "c", interval, cache_mode)

                if not ok:
                    new_temp_min_contract = f"{contract[:-2]}{int(contract[-2:])+year_offset:02}"

                    df_contract, ok = get_outright(ticker, new_temp_min_contract, "c", interval, cache_mode)

                    if len(df_contract.index.normalize().unique()) < ticker.roll_offset:
                        raise Exception(
                            "[-] DataPipeline contract don't have enough data to"
                            f"roll over given roll_offset {ticker.roll_offset}"
                        )
                    temp_date = df_contract.index.normalize().unique()[-ticker.roll_offset]
                    temp_date -= pd.DateOffset(years=year_offset)
                    if roll_date == None:
                        roll_date = temp_date
                    elif roll_date > temp_date:
                        roll_date = temp_date
                else:
                    if len(df_contract.index.normalize().unique()) < ticker.roll_offset:
                        raise Exception(
                            "[-] DataPipeline contract don't have enough data to"
                            f"roll over given roll_offset {ticker.roll_offset}"
                        )
                    temp_date = df_contract.index.normalize().unique()[-ticker.roll_offset]
                    if roll_date == None:
                        roll_date = temp_date
                    elif roll_date > temp_date:
                        roll_date = temp_date

            # start_crop = None
            # if(len(df_spread_list) == 0):
            #     start_crop = roll_date - pd.DateOffset(months=1)
            #     final_df = final_df.loc[start_crop:]

            df_spread_list.append(final_df.loc[:roll_date])

            itr_contracts = move_contracts_to_prev_valid_month(itr_contracts)
        

        itr = None
        for df_spread in df_spread_list[::-1]:
            diff = None
            df_trimmed = None

            if df.empty:
                df = df_spread.copy()
            else:
                df_trimmed = df_spread[df_spread.index > df.index[-1]]

                if back_adjusted:
                    itr  = -1
                    while (
                        -itr <= max_lookback_for_back_adjust and
                        -itr <= len(df.index) and
                        df.index[itr] >= df_spread.index[0] and 
                        (
                            df.index[itr] not in df_spread.index or 
                            pd.isna(df_spread.loc[df.index[itr]]["close"]) or
                            pd.isna(df.loc[df.index[itr]]["close"])
                        )
                    ):
                        df.loc[df.index[itr], "close"] = np.nan 
                        itr -= 1

                    if -itr > max_lookback_for_back_adjust:
                        raise Exception(
                            "[-] DataPipeline.get_spread unable to back adjust "
                            "in given max_lookback_for_back_adjust : "
                            f"{max_lookback_for_back_adjust}"
                        )
                    
                    if (
                        -itr > len(df.index) or 
                        df.index[itr] < df_spread.index[0] or 
                        (
                            pd.isna(df_spread.loc[df.index[itr]]["close"]) and
                            pd.isna(df.loc[df.index[itr]]["close"])
                        )
                    ):
                        raise Exception(
                            "[-] DataPipeline.get_spread unable to back adjust"
                        )

                    diff = df_spread.loc[df.index[itr]]["close"] - df.loc[df.index[itr]]["close"]
                    add_back_adjusted_diff(df, diff)

                df = pd.concat([df, df_trimmed])    

    else:
        pass

    df = df.loc[start:end]

    # back_adjust data to end date
    # if back_adjusted:
    #     udf = get_spread(
    #         expression, 
    #         back_adjusted = False,
    #         start = start,
    #         end = end,
    #         interval = interval,
    #         roll_method = roll_method,
    #         cache_mode = cache_mode
    #     )

    #     diff = udf.iloc[-1]["close"] - df.iloc[-1]["close"]
    #     add_back_adjusted_diff(df, diff)

    if "c" not in ohlcv:
        df = df.drop(columns=["close"])

    return df

