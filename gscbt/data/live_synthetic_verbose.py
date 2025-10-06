import pandas as pd
from .live_synthetic import get_live_synthetic

from gscbt.expression_utils import (
    extract_contracts_multipliers,
    create_expression_from_contracts_multipliers,
)

    
def get_live_synthetic_verbose(
    expression : str,
    start : str,
    offset : int,
    isBackAdjusted : bool = True,
    roll_method : str = "contractwise",
    max_lookahead : int | None = None,
    mode : str = "normal",
    month_map : dict = {}
) -> pd.DataFrame:
    
    if isBackAdjusted and max_lookahead == None:
        raise ValueError(f"[-] In backadjust mode max_lookahead can't be None value")

    processed_contracts = set()

    contracts, multipliers = extract_contracts_multipliers(expression)

    itr_months = None
    instrument = contracts[0][:-3]
    if instrument in month_map:
        itr_months = month_map[instrument]

    df_list = []
    for i in range(len(contracts)):
        if contracts[i] in processed_contracts:
            continue

        processed_contracts.add(contracts[i])

        new_multipliers = [0]*len(contracts)
        new_multipliers[i] = 1
        a = create_expression_from_contracts_multipliers(contracts, new_multipliers)
        df = get_live_synthetic(
            expression = a,
            start = start,
            offset  = offset,
            ohlcv  = "c",
            isBackAdjusted = isBackAdjusted,
            interval = "1d",
            roll_method = roll_method,
            max_lookahead = max_lookahead,
            mode = mode,
            itr_months = itr_months
        )
        df = df.drop(columns=["roll_date", "days_to_roll"])
        df = df.rename(columns={"close": contracts[i]})
        df_list.append(df)
 
    df = get_live_synthetic(
        expression = expression,
        start = start,
        offset  = offset,
        ohlcv  = "c",
        isBackAdjusted = isBackAdjusted,
        interval = "1d",
        roll_method = roll_method,
        max_lookahead = max_lookahead,
        mode = mode,
        itr_months = itr_months
    )

    df_list.append(df)
    res_df = pd.concat(df_list, axis=1)
    return res_df