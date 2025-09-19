import pandas as pd

from gscbt.ticker import Ticker
from gscbt.expression_utils import(
    extract_contracts_multipliers,
    move_contracts_to_given_year_from_min,
    extract_full_min_year_from_contracts,
    move_contracts_to_prev_valid_month,
)
from gscbt.utils import Interval

from .outright import get_outright
from .utils import df_apply_operation_to_given_columns


def offset_roll(
    synthetic_df_list : list[pd.DataFrame],
    synthetic_roll_list : list[pd.Timestamp],
    interval : str,
    isBackAdjusted : bool,
    max_lookahead : int,
    mode : str =  "normal"
) -> pd.DataFrame:
    
    if len(synthetic_df_list) != len(synthetic_roll_list):
        raise ValueError(f"[-] Length of df_list and roll_list don't match")

    res_df = pd.DataFrame()
    synthetic_count = len(synthetic_df_list)

    interval_in_sec = Interval.str_to_second(interval)
    interval_offset = pd.Timedelta(seconds=interval_in_sec)

    for itr in range(synthetic_count):
        itr_synthetic = synthetic_df_list[itr]

        if res_df.empty:
            res_df = itr_synthetic.copy()
            continue

        roll_date = synthetic_roll_list[itr-1]

        if isBackAdjusted:
            diff = None

            if mode == "normal":
                for _ in range(max_lookahead+1):
                    if roll_date in res_df.index and roll_date in itr_synthetic.index:
                        d1 = res_df["close"].loc[roll_date]
                        d2 = itr_synthetic["close"].loc[roll_date]

                        if not pd.isna(d1) and not pd.isna(d2):
                            diff = d2 - d1
                            break

                    roll_date += interval_offset
            elif mode == "force":
                d1 = res_df["close"].dropna().loc[:roll_date].iloc[-1]
                d2 = itr_synthetic[itr_synthetic.index >= roll_date]["close"].dropna().iloc[0]
                if not pd.isna(d1) and not pd.isna(d2):
                    diff = d2 - d1

            if diff == None:
                raise Exception("[-] Fail to backadjust in given max_lookahead.")
            
            res_df = df_apply_operation_to_given_columns(
                df= res_df,
                value= diff,
                columns= ["open", "high", "low", "close"],
                op= "add",
            )

        res_df = res_df.loc[:roll_date]

        if "roll_date" not in res_df.columns:
            res_df["roll_date"] = roll_date
        else:
            res_df["roll_date"] = res_df["roll_date"].fillna(roll_date)

        trimmed = itr_synthetic[itr_synthetic.index > roll_date]
        res_df = pd.concat([res_df, trimmed])
    
    # For cropping last synthetic_df
    roll_date = synthetic_roll_list[-1]
    res_df = res_df.loc[:roll_date]

    if "roll_date" not in res_df.columns:
        res_df["roll_date"] = roll_date
    else:
        res_df["roll_date"] = res_df["roll_date"].fillna(roll_date)
    
    return res_df

def get_synthetic_contractwise(
    expression : str,
    ohlcv : str,
    isBackAdjusted : bool,
    start_year : int,
    end_year : int, 
    interval : str,
    offset : int ,
    max_lookahead : int,
):
    contracts, multipliers = extract_contracts_multipliers(expression)    
    contract_count = len(contracts)

    synthetic_df_list = []
    synthetic_roll_list = []

    # step : 1
    # getting data and ideal roll date
    for itr_year in range(end_year, start_year-1, -1):
        itr_contracts = move_contracts_to_given_year_from_min(contracts, itr_year)

        itr_synthetic_df = pd.DataFrame()
        roll_date = None
        isAllLegFound = True

        for itr in range(contract_count):
            itr_contract = itr_contracts[itr]

            temp_ticker = Ticker.SYMBOLS[itr_contract[:-3]] 

            df, ok = get_outright(
                ticker = temp_ticker,
                contract = itr_contract,
                ohlcv = ohlcv,
                interval = interval,
            )

            if not ok:
                isAllLegFound = False
                print(f"Data for contract {itr_contract} not available so stop at year {itr_year}")
                break

            df = df * multipliers[itr] 
            df = df * temp_ticker.currency_multiplier

            if itr_synthetic_df.empty:
                itr_synthetic_df = df.copy()
                roll_date = df.index[-1]
            else:
                itr_synthetic_df += df
                roll_date = min(roll_date, df.index[-1])

        # if not sufficient contract to create synthetic then stop 
        if not isAllLegFound:
            break

        synthetic_df_list.append(itr_synthetic_df)

        roll_date -= pd.DateOffset(days=offset)
        synthetic_roll_list.append(roll_date)
            
    # step : 2 
    # performing roll &| back_adjust
    synthetic_df_list = synthetic_df_list[::-1]
    synthetic_roll_list = synthetic_roll_list[::-1]

    res_df = offset_roll(
        synthetic_df_list = synthetic_df_list,
        synthetic_roll_list = synthetic_roll_list,
        interval = interval,
        isBackAdjusted = isBackAdjusted,
        max_lookahead = max_lookahead
    )

    # res_df start uncropped
    return res_df
    
def get_synthetic_spreadwise(
    expression : str,
    ohlcv : str,
    isBackAdjusted : bool,
    start_year : int,
    end_year : int, 
    interval : str,
    offset : int,
    max_lookahead : int,
):
    contracts, multipliers = extract_contracts_multipliers(expression)    
    contract_count = len(contracts)

    synthetic_df_list = []
    synthetic_roll_list = []

    # step : 1
    # getting data and ideal roll date

    itr_contracts = move_contracts_to_given_year_from_min(contracts, end_year)

    while True:
        itr_min_year = extract_full_min_year_from_contracts(itr_contracts)
        if itr_min_year <= start_year - 1 :
            break

        itr_synthetic_df = pd.DataFrame()
        roll_date = None
        isAllLegFound = True

        for itr in range(contract_count):
            itr_contract = itr_contracts[itr]

            temp_ticker = Ticker.SYMBOLS[itr_contract[:-3]] 

            df, ok = get_outright(
                ticker = temp_ticker,
                contract = itr_contract,
                ohlcv = ohlcv,
                interval = interval,
            )

            if not ok:
                isAllLegFound = False
                print(f"Data for contract {itr_contract} not available so stop at {itr_contracts}")
                break

            df = df * multipliers[itr] 
            df = df * temp_ticker.currency_multiplier

            if itr_synthetic_df.empty:
                itr_synthetic_df = df.copy()
                roll_date = df.index[-1]
            else:
                itr_synthetic_df += df
                roll_date = min(roll_date, df.index[-1])

        
        # if not sufficient contract to create synthetic then stop 
        if not isAllLegFound:
            break

        synthetic_df_list.append(itr_synthetic_df)

        roll_date -= pd.DateOffset(days=offset)
        synthetic_roll_list.append(roll_date)

        itr_contracts = move_contracts_to_prev_valid_month(itr_contracts)


    # step : 2 
    # performing roll &| back_adjust

    synthetic_df_list = synthetic_df_list[::-1]
    synthetic_roll_list = synthetic_roll_list[::-1]

    res_df = offset_roll(
        synthetic_df_list = synthetic_df_list,
        synthetic_roll_list = synthetic_roll_list,
        interval = interval,
        isBackAdjusted = isBackAdjusted,
        max_lookahead = max_lookahead
    )

    # res_df start uncropped
    return res_df

def get_spread(
    expression : str,
    start : str,
    end : str,
    offset : int,
    ohlcv : str  = "c",
    isBackAdjusted : bool = True,
    interval : str = "1d",
    roll_method : str = "contractwise",
    max_lookahead : int | None = None,
) -> pd.DataFrame:

    if isBackAdjusted and max_lookahead == None:
        raise ValueError(f"[-] In backadjust mode max_lookahead can't be None value")

    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)

    if roll_method == "contractwise":
        res_df = get_synthetic_contractwise(
            expression = expression,
            ohlcv = ohlcv,
            isBackAdjusted = isBackAdjusted,
            start_year = start_date.year - 1,
            end_year = end_date.year, 
            interval = interval,
            offset = offset,
            max_lookahead = max_lookahead,
        )

        res_df["days_to_roll"] = res_df.roll_date - res_df.index
        return res_df.loc[start:]
    

    if roll_method == "spreadwise":
        res_df = get_synthetic_spreadwise(
            expression = expression,
            ohlcv = ohlcv,
            isBackAdjusted = isBackAdjusted,
            start_year = start_date.year - 1,
            end_year = end_date.year, 
            interval = interval,
            offset =  offset,
            max_lookahead = max_lookahead,
        )

        res_df["days_to_roll"] = res_df.roll_date - res_df.index
        return res_df.loc[start:]
    
    raise ValueError(f"[-] A roll_method allowed values are (1) contractwise (2) spreadwise")