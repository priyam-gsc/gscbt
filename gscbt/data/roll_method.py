
import pandas as pd

from .contract_spec import DataType
from .utils import df_apply_operation_to_given_columns

def roll_offset(
    contract_df_list : list[pd.DataFrame],
    rt_expiry_date_list : list[pd.Timestamp],
    offset : int,
    interval_offset : pd.Timedelta,
    data_type : DataType,
    max_lookahead : int,
    extra_columns : list[str],
)-> pd.DataFrame:

    res_df = pd.DataFrame()

    if len(contract_df_list) != len(rt_expiry_date_list):
        raise ValueError("Length of contract_df_list and rt_date_list should be same.")

    rt_date_list = []
    for itr in range(len(rt_expiry_date_list)):
        rt_date_list.append(rt_expiry_date_list[itr] - pd.offsets.Day(offset))

        if "ideal_roll_date" in extra_columns:
            contract_df_list[itr]["ideal_roll_date"] = rt_date_list[-1]

    if data_type != DataType.CONTINUOUS:
        for itr in range(len(contract_df_list)):
            isRollDateFound = False
            roll_date = rt_date_list[itr]

            for _ in range(max_lookahead + 1):
                if(
                    roll_date in contract_df_list[itr].index and 
                    roll_date in contract_df_list[itr].index
                ):
                    rt_date_list[itr] = roll_date
                    isRollDateFound = True

                    if "actual_roll_date" in extra_columns:
                        contract_df_list[itr]["actual_roll_date"] = rt_date_list[itr]
                    
                    break
                else:
                    roll_date += interval_offset

            if not isRollDateFound:
                raise Exception(f"fail to adjust data in given {max_lookahead=}")


    for idx, contract_df in enumerate(contract_df_list):
        if res_df.empty:
            res_df = contract_df.loc[:rt_date_list[idx]].copy()
            continue

        trimmed = contract_df[contract_df.index > rt_date_list[idx-1]]
        trimmed = trimmed.loc[:rt_date_list[idx]]

        if data_type in [DataType.BACKADJUSTED, DataType.FORWARDADJUSTED]:
            try :
                diff = contract_df.loc[rt_date_list[idx-1]]["close"] 
                diff = diff - res_df.loc[rt_date_list[idx-1]]["close"]                
            except:
                raise Exception(
                    "Fail to calculate the diff for same timestamp "
                    "to performe data adjustment(i.e. BACKADJUSTED | FORWARDADJUSTED)"
                )

            if data_type == DataType.BACKADJUSTED:
                res_df = df_apply_operation_to_given_columns(
                    df= res_df,
                    value= diff,
                    columns= ["open", "high", "low", "close"],
                    op= "add",
                )
            elif data_type == DataType.FORWARDADJUSTED:
                trimmed = df_apply_operation_to_given_columns(
                    df= trimmed,
                    value= diff,
                    columns= ["open", "high", "low", "close"],
                    op= "sub",
                )

        res_df = pd.concat([res_df, trimmed])

    return res_df