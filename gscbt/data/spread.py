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
    move_contracts_to_next_valid_month,
)

from .outright import get_outright
from .utils import add_back_adjusted_diff

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
    cache_mode : Cache.Mode = Cache.Mode.market_api,
    max_lookback_for_back_adjust : int = 1,
    verbose : bool = False
) -> pd.DataFrame:
    
    if verbose:
        print(
            "------------------------------------------ \n"
            "PARAMS :: \n"
            "------------------------------------------ \n"
            f"expression : {expression} \n"
            f"ohlcv : {ohlcv}  \n"
            f"back_adjusted : {back_adjusted}  \n"
            f"start : {start}  \n"
            f"end : {end}  \n"
            f"interval : {interval}  \n"
            f"roll_method : {roll_method}  \n"
            f"cache_mode : {cache_mode}  \n"
            f"max_lookback_for_back_adjust : {max_lookback_for_back_adjust} \n"
            "------------------------------------------> \n\n"
        ) 

    df = pd.DataFrame()
    
    expression = expression.replace(" ", "")
    contracts, multipliers, operators = extract_contracts_multipliers_operators(expression)
    year_offset = extract_year_offset(contracts)
    offset_contracts = convert_contracts_to_offset_contracts(contracts)
    min_offset_contracts = extract_min_contracts_from_contracts(offset_contracts)

    start_year =  1975
    end_year = datetime.today().year
    if start != None:
        start_year = pd.to_datetime(start).year - 1
    if end != None:
        end_year = pd.to_datetime(end).year

    if verbose:
        print(
            "------------------------------------------ \n"
            " 1 :: \n"
            "------------------------------------------ \n"
            f"expression : {expression} \n"
            f"contracts : {contracts} \n"
            f"multipliers : {multipliers} \n"
            f"operators : {operators} \n"
            f"year_offset : {year_offset} \n"
            f"offset_contracts : {offset_contracts} \n"
            f"min_offset_contracts : {min_offset_contracts} \n"
            f"start_year : {start_year} \n"
            f"end_year : {end_year} \n"
            "------------------------------------------> \n\n"
        )

    isFirstCompleteFound = False

    df_spread_list = []
    df_contract_list = []

    if roll_method == RollMethod.contractwise:
        if verbose:
            print(
                "------------------------------------------ \n"
                " 2 ::  \n"
                "------------------------------------------ \n"
            )

        for itr_year in range(start_year, end_year+1):
            if verbose:
                print("==========================================\n")

            df_contract_list.clear()
            itr_contracts = convert_offset_contracts_to_given_year(
                offset_contracts, 
                f"{itr_year%100:02}"
            )
            
            v_missing_contract_list = []
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
                    if verbose:
                        v_missing_contract_list.append({
                            "itr_year" : itr_year,
                            "contract" : contract,
                            "contracts" : contracts,
                            "isFirstCompleteFound" : isFirstCompleteFound,
                        })

                    if not isFirstCompleteFound:
                        break
                    else:
                        raise Exception("[-] DataPipeline.get_spread outright data missing in between")
                    
                else:
                    df_contract_list.append(df_contract)

            if verbose:
                print(
                    f"itr_year : {itr_year} \n"
                    f"itr_contracts : {itr_contracts} \n"
                    f"missing_contracts : {v_missing_contract_list} \n\n"
                    f"[[ df_contract_list ]] : \n {df_contract_list} \n\n"
                )
                    
            if len(df_contract_list) == len(itr_contracts):
                isFirstCompleteFound = True
            else:
                continue

            final_df = apply_multipliers_operators_to_df_contract_list(
                itr_contracts,
                df_contract_list,
                multipliers,
                operators
            )

            if verbose:
                print(
                    f"[[ final_df ]] : \n {final_df} \n\n"
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

            start_crop = None
            if(len(df_spread_list) == 0):
                start_crop = roll_date - pd.DateOffset(years=1)
                final_df = final_df.loc[start_crop:]

            df_spread_list.append(final_df.loc[:roll_date])

            if verbose:
                print(
                    f"min_contracts : {min_contracts} \n"
                    f"start_crop : {start_crop} \n"
                    f"roll_date : {roll_date} \n\n"
                    f"[[ final_df[:roll_date] ]] : \n {df_spread_list[-1]} \n\n"
                    "==========================================>\n\n"
                )

        if verbose:
            print("------------------------------------------> \n\n")
            print(
                "------------------------------------------ \n"
                " 3 ::  \n"
                "------------------------------------------ \n"
            )


        # perform join to yearwise spread
        df_last_date = None
        itr = None
        df_spread_last_date_value = None
        df_last_date_value = None
        df_spread_itr_value = None
        df_itr_value = None

        for df_spread in df_spread_list:
            diff = None
            df_trimmed = None

            if verbose:
                if not df.empty:
                    df_last_date = df.index[-1]
                    if df_last_date in df_spread.index:
                        df_spread_last_date_value = df_spread.loc[df_last_date]["close"]
                    else:
                        df_spread_last_date_value = None

                    df_last_date_value = df.loc[df_last_date]["close"]

            if df.empty:
                df = df_spread
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

                    if verbose:
                        df_spread_itr_value = df_spread.loc[df.index[itr]]
                        df_itr_value = df.loc[df.index[itr]]

                    diff = df_spread.loc[df.index[itr]]["close"] - df.loc[df.index[itr]]["close"]
                    add_back_adjusted_diff(df, diff)

                df = pd.concat([df, df_trimmed])

            if verbose:
                print(
                    "==========================================\n"
                    f"df_last_date : {df_last_date} \n"
                    f"itr_value : {itr} \n"
                    f"max_lookback_for_back_adjust : {max_lookback_for_back_adjust}\n"
                    f"df_spread_last_date_value : {df_spread_last_date_value}\n"
                    f"df_last_date_value : {df_last_date_value}\n\n"
                    f"df_spread_itr_value : {df_spread_itr_value}\n"
                    f"df_itr_value : {df_itr_value}\n\n"
                    f"diff : {diff} \n\n"

                    f"[[ df_spread ]] : \n {df_spread} \n\n"
                    f"[[ df_trimmed ]] : \n {df_trimmed} \n\n"
                    f"[[ df ]] : \n {df} \n"
                    "==========================================>\n\n"
                )    

        if verbose:
            print(
                "------------------------------------------> \n\n"
            )

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

        itr_contracts = convert_offset_contracts_to_given_year(offset_contracts, f"{start_year%100:02}")
        end_contracts  = convert_offset_contracts_to_given_year(offset_contracts, f"{end_year%100:02}")

        if verbose:
            print(
                "------------------------------------------ \n"
                " 2 ::  \n"
                "------------------------------------------ \n"
                f"itr_contracts : {itr_contracts} \n"
                f"end_contracts : {end_contracts} \n"
            )

        continue_itr = True
        while continue_itr:
            if verbose:
                print("==========================================\n")

            if itr_contracts == end_contracts:
                continue_itr = False

            df_contract_list.clear()
            v_missing_contract_list = []

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
                    if verbose:
                        v_missing_contract_list.append({
                            "contract" : contract,
                            "isFirstCompleteFound" : isFirstCompleteFound,
                        })

                    if not isFirstCompleteFound:
                        break
                    else:
                        return pd.DataFrame()
                else:
                    df_contract_list.append(df_contract)

            if verbose:
                print(
                    f"itr_contracts : {itr_contracts} \n"
                    f"missing_contracts : {v_missing_contract_list} \n\n"
                    f"[[ df_contract_list ]] : \n {df_contract_list} \n\n"
                )

            if len(df_contract_list) == len(itr_contracts):
                isFirstCompleteFound = True
            else:
                itr_contracts = move_contracts_to_next_valid_month(itr_contracts)
                continue

            final_df = apply_multipliers_operators_to_df_contract_list(
                itr_contracts,
                df_contract_list,
                multipliers,
                operators
            )

            if verbose:
                print(
                    f"[[ final_df ]] : \n {final_df} \n\n"
                )

            min_contracts = extract_min_contracts_from_contracts(itr_contracts)
            for _ in range(year_offset):
                # contract = move_contracts_to_prev_year(contract)
                min_contracts = move_contracts_to_prev_year(min_contracts)

            roll_date = None
            for contract in min_contracts:
                sym = contract[:-3]
                ticker = Ticker.SYMBOLS[sym]
                df_contract, ok = get_outright(ticker, contract, "c", interval, cache_mode)

                if not ok:
                    new_temp_min_contract = f"{contract[:-2]}{int(contract[-2:])+year_offset:02}"

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

            start_crop = None
            if(len(df_spread_list) == 0):
                start_crop = roll_date - pd.DateOffset(months=1)
                final_df = final_df.loc[start_crop:]

            df_spread_list.append(final_df.loc[:roll_date])

            if verbose:
                print(
                    f"min_contracts : {min_contracts} \n"
                    f"start_crop : {start_crop} \n"
                    f"roll_date : {roll_date} \n\n"
                    f"[[ final_df[:roll_date] ]] : \n {df_spread_list[-1]} \n\n"
                    "==========================================>\n\n"
                )

            itr_contracts = move_contracts_to_next_valid_month(itr_contracts)
        

        if verbose:
            print("------------------------------------------> \n\n")
            print(
                "------------------------------------------ \n"
                " 3 ::  \n"
                "------------------------------------------ \n"
            )

        df_last_date = None
        itr = None
        df_spread_last_date_value = None
        df_last_date_value = None
        df_spread_itr_value = None
        df_itr_value = None

        for df_spread in df_spread_list:
            diff = None
            df_trimmed = None

            if verbose:
                if not df.empty:
                    df_last_date = df.index[-1]
                    if df_last_date in df_spread.index:
                        df_spread_last_date_value = df_spread.loc[df_last_date]["close"]
                    else:
                        df_spread_last_date_value = None

                    df_last_date_value = df.loc[df_last_date]["close"]

            if df.empty:
                df = df_spread
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

                    if verbose:
                        df_spread_itr_value = df_spread.loc[df.index[itr]]
                        df_itr_value = df.loc[df.index[itr]]

                    diff = df_spread.loc[df.index[itr]]["close"] - df.loc[df.index[itr]]["close"]
                    add_back_adjusted_diff(df, diff)

                df = pd.concat([df, df_trimmed])    

            if verbose:
                print(
                    "==========================================\n"
                    f"df_last_date : {df_last_date} \n"
                    f"itr_value : {itr} \n"
                    f"max_lookback_for_back_adjust : {max_lookback_for_back_adjust}\n"
                    f"df_spread_last_date_value : {df_spread_last_date_value}\n"
                    f"df_last_date_value : {df_last_date_value}\n\n"
                    f"df_spread_itr_value : {df_spread_itr_value}\n"
                    f"df_itr_value : {df_itr_value}\n\n"
                    f"diff : {diff} \n\n"

                    f"[[ df_spread ]] : \n {df_spread} \n\n"
                    f"[[ df_trimmed ]] : \n {df_trimmed} \n\n"
                    f"[[ df ]] : \n {df} \n"
                    "==========================================>\n\n"
                )    

        if verbose:
            print(
                "------------------------------------------> \n\n"
            )

    else:
        pass

    df =df.loc[start:end]

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



