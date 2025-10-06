from datetime import datetime
import json

import pandas as pd

from .live_data import (
    get_live_data,
    get_tick_n_eod_combine_data
)
from .spread import (
    offset_roll
)
from gscbt.utils import (
    req_wrapper,
)

from gscbt.expression_utils import (
    move_contracts_to_prev_year,
    extract_contracts_multipliers,
    extract_full_min_year_from_contracts,
    create_expression_from_contracts_multipliers,
    move_contracts_to_given_prev_month,
    get_full_year
)
    


def get_config():
    # getting json data
    CONFIG_DATA_API = "http://192.168.0.155:24502"

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
    mode : str = "normal",
    isTickNEod : bool = False,
) -> pd.DataFrame:
    
    cache = {}

    if isTickNEod and ohlcv != 'c':
        raise ValueError(f"[-] In TickNEod mode value of ohlcv must be 'c'.")

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
                ok = True
                df = cache[itr_contract]
            else:
                if isTickNEod and itr_contract in META:
                    ok, df = get_tick_n_eod_combine_data(itr_contract)
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

    return res_df


def get_live_synthetic_custom(
    expression : str,
    ohlcv : str,
    isBackAdjusted : bool,
    start_year : int,
    interval : str,
    offset : int,
    max_lookahead : int,
    month_map : dict,
    mode : str = "normal",
    isTickNEod : bool = False,
) -> pd.DataFrame:

    if isTickNEod and ohlcv != 'c':
        raise ValueError(f"[-] In TickNEod mode value of ohlcv must be 'c'.")

    contracts, multipliers = extract_contracts_multipliers(expression)
    META = get_config()

    synthetic_df_list = []
    synthetic_roll_list = []

    contract_count = len(contracts)
    itr_contracts = contracts

    while True:
        itr_min_year = extract_full_min_year_from_contracts(itr_contracts)
        if itr_min_year < start_year:
            break

        itr_synthetic_df = pd.DataFrame()
        roll_date = None
        isAllLegFound = True

        for itr in range(contract_count):
            itr_contract = itr_contracts[itr]

            if isTickNEod and itr_contract in META:
                ok, df = get_tick_n_eod_combine_data(itr_contract)
            else:
                ok, df = get_live_data(
                    symbol = itr_contract,
                    ohlcv = ohlcv,
                )

            if not ok:
                isAllLegFound = False
                print(f"Data for contract {itr_contract} not available so stop at {itr_contracts}")
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

        # if not sufficient contract to create synthetic then stop
        if not isAllLegFound:
            break

        synthetic_df_list.append(itr_synthetic_df)

        roll_date -= pd.DateOffset(days=offset)
        synthetic_roll_list.append(roll_date)

        itr_contracts = move_contracts_to_given_prev_month(itr_contracts, month_map)

    # ===========================================================

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


def get_live_synthetic_spreadwise(
    expression : str,
    ohlcv : str,
    isBackAdjusted : bool,
    start_year : int,
    interval : str,
    offset : int,
    max_lookahead : int,
    mode : str = "normal",
    isTickNEod : bool = False
) -> pd.DataFrame:

    contracts, multipliers = extract_contracts_multipliers(expression)
    META = get_config()

    month_map = {}

    for contract in contracts:
        sym = contract[:-3]
        month_map[sym] = META[sym][constractMonths]

    res_df = get_live_synthetic_custom(
        expression = expression,
        ohlcv = ohlcv,
        isBackAdjusted = isBackAdjusted,
        start_year = start_year,
        interval = interval,
        offset = offset,
        max_lookahead = max_lookahead,
        month_map = month_map,
        mode = mode,
        isTickNEod = isTickNEod,
    )

    return res_df


def get_live_synthetic_generic(
    expression : str,
    ohlcv : str,
    isBackAdjusted : bool,
    start_year : int,
    interval : str,
    offset : int,
    max_lookahead : int,
    mode : str = "normal",
    itr_months : str | None = None,
    isTickNEod : bool = False
) -> pd.DataFrame:

    # get config
    META = get_config()

    # step : 0 : check expression validness
    contracts, multipliers = extract_contracts_multipliers(expression)

    if(len(contracts) == 0):
        raise ValueError(f"[-] Invalid {expression=}.")

    instrument = contracts[0][:-3]
    for contract in contracts:
        if instrument != contract[:-3]:
            raise ValueError(f"[-] Invalid expression contain multilple instruments || ERROR : {contract}")
        if contract not in META:
            raise ValueError(f"[-] Invalid expression should contain only active contracts || ERROR : {contract}")

    if itr_months == None:
        itr_months = META[instrument]["contractMonths"]

    for month in itr_months:
        if month not in META[instrument]["contractMonths"]:
            raise ValueError(f"[-] Invalid expression invalid {month=} found for {instrument=} found")

    for contract in contracts:
        if contract[-3] not in itr_months:
            raise ValueError(f"[-] Invalid expression {month=} which is not part of {itr_months}")


    # step : 1 : add rolling trigger in expression if not exist
    smallest_contract_suffix = None
    for active_contract_suffix_dict in META[instrument]["contracts"]:
        active_contract_suffix = active_contract_suffix_dict["contractCode"]

        current_month = active_contract_suffix[0]
        current_year = get_full_year(int(active_contract_suffix[1:]))

        if current_month in itr_months:
            if smallest_contract_suffix is None:
                update_smallest = True
            else:
                smallest_year = get_full_year(int(smallest_contract_suffix[1:]))
                smallest_month = smallest_contract_suffix[0]

                if smallest_year > current_year:
                    update_smallest = True
                elif smallest_year == current_year and smallest_month > current_month:
                    update_smallest = True
                else:
                    update_smallest = False

            if update_smallest:
                smallest_contract_suffix = active_contract_suffix

    trigger_contract = instrument + smallest_contract_suffix
    isTriggerExist = False
    for contract in contracts:
        if contract == trigger_contract:
            isTriggerExist = True
            break

    if not isTriggerExist:
        contracts.append(trigger_contract)
        multipliers.append(0)


    # step : 2 :
    new_expression = create_expression_from_contracts_multipliers(
        contracts = contracts,
        multipliers = multipliers
    )

    month_map = {}
    month_map[instrument] = itr_months

    res_df = get_live_synthetic_custom(
        expression = new_expression,
        ohlcv = ohlcv,
        isBackAdjusted = isBackAdjusted,
        start_year = start_year,
        interval = interval,
        offset = offset,
        max_lookahead = max_lookahead,
        month_map = month_map,
        mode = mode,
        isTickNEod = isTickNEod
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
    mode : str = "normal",
    isTickNEod : bool = False,
    itr_months : str | None = None,
) -> pd.DataFrame:

    expression = expression.replace(" ", "")
    interval = "1d"

    if isBackAdjusted and max_lookahead == None:
        raise ValueError(f"[-] In backadjust mode max_lookahead can't be None value")

    if start == "":
        start = "1950-01-01"

    start_date = pd.to_datetime(start)
    start_year = start_date.year - 1

    if roll_method == "contractwise":
        res_df = get_live_synthetic_contractwise(
            expression = expression,
            ohlcv = ohlcv,
            isBackAdjusted = isBackAdjusted,
            start_year = start_year,
            interval = interval,
            offset = offset,
            max_lookahead = max_lookahead,
            mode = mode,
            isTickNEod = isTickNEod,
        )

        res_df["days_to_roll"] = res_df.roll_date - res_df.index
        return res_df.loc[start:]

    elif roll_method == "spreadwise":
        res_df = get_live_synthetic_spreadwise(
            expression = expression,
            ohlcv = ohlcv,
            isBackAdjusted = isBackAdjusted,
            start_year = start_year,
            interval = interval,
            offset = offset,
            max_lookahead = max_lookahead,
            mode = mode,
            isTickNEod = isTickNEod,
        )

        res_df["days_to_roll"] = res_df.roll_date - res_df.index
        return res_df.loc[start:]

    elif roll_method == "generic":
        res_df = get_live_synthetic_generic(
            expression = expression,
            ohlcv = ohlcv,
            isBackAdjusted = isBackAdjusted,
            start_year = start_year,
            interval = interval,
            offset = offset,
            max_lookahead = max_lookahead,
            mode = mode,
            itr_months = itr_months,
            isTickNEod = isTickNEod
        )

        res_df["days_to_roll"] = res_df.roll_date - res_df.index
        return res_df.loc[start:]

    else:
        raise ValueError(f"[-] Invalid roll_method for get_live_synthetic choose from [contractwise, spreadwise, generic]")

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