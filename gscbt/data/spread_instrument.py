from enum import Enum, auto

import numpy as np
import pandas as pd 

from gscbt.ticker import Ticker
from gscbt.cache import Cache
from gscbt.expression_utils import (
    move_contract_to_given_prev_valid_month,
)

from .outright import get_outright
from .spread import RollMethod



class DataType(Enum):
    continuous = auto()
    backadjusted =auto()

class SpreadInstrument:
    def __init__(
        self,
        contract : str,
        contract_itr_months : str,
        roll : str,
        roll_itr_months : str,
        multiplier : int,
        roll_method : RollMethod,
        offset : int,
        data_type : DataType, 
        max_lookback : int = None,
        interval : str = "1d",
    ):
        if (
            (data_type == DataType.backadjusted and max_lookback == None) or
            len(contract_itr_months) != len(roll_itr_months)
        ):
            raise

        self.contract = contract
        self.contract_itr_months = contract_itr_months
        self.roll = roll
        self.roll_itr_months = roll_itr_months
        self.multiplier = multiplier
        self.roll_method = roll_method
        self.offset = offset
        self.data_type = data_type
        self.max_lookback = max_lookback
        self.interval = interval
        self.df = pd.DataFrame()

    def get(
        self,
    ) -> pd.DataFrame:
        if self.df.empty:
            contract_sym = self.contract[:-3]
            roll_sym =  self.roll[:-3]

            contract_ticker = Ticker.SYMBOLS[contract_sym]
            roll_ticker = Ticker.SYMBOLS[roll_sym]

            contract = self.contract
            roll = self.roll

            contract_df_list = []
            roll_df_list = []

            contract_df = None
            roll_df = None
            while True:
                try:
                    contract_df, ok = get_outright(
                        contract_ticker,
                        contract,
                        "ohlc",
                        self.interval,
                    )

                    if not ok:
                        break

                    roll_df, ok = get_outright(
                        roll_ticker,
                        roll,
                        "c",
                        self.interval,
                    )

                    if not ok:
                        break
                
                except:
                    break

                contract_df_list.append(contract_df)
                roll_df_list.append(roll_df)
                
                contract = move_contract_to_given_prev_valid_month(
                    contract,
                    self.contract_itr_months,
                )

                roll = move_contract_to_given_prev_valid_month(
                    roll,
                    self.roll_itr_months,
                )

            if len(contract_df_list) != len(roll_df_list):
                raise

            if self.roll_method == RollMethod.offset:
                n = len(contract_df_list)
                
                rolled_df_list = []

                for itr in range(n):
                    day_df = roll_df_list[itr].index.normalize().unique()
                    offset = self.offset

                    if len(day_df) < offset:
                        raise

                    roll_date = day_df[-offset]   
                    rolled_df_list.append(contract_df_list[itr].loc[:roll_date])

                # isFirstBackadjusteSucceed = False
                for itr in range(1, n+1):
                    if self.df.empty:
                        self.df = rolled_df_list[-itr].copy()

                    else:
                        temp = rolled_df_list[-itr]
                        trimmed = temp[temp.index > self.df.index[-1]]

                        if self.data_type == DataType.backadjusted:
                            lookback_itr = 1
                            diff = None
                            
                            if len(self.df.index) < self.max_lookback:
                                lookback_itr = self.max_lookback + 1

                            while self.max_lookback >= lookback_itr:
                                diff_date = self.df.index[-lookback_itr]

                                if (
                                    diff_date in rolled_df_list[-itr].index and
                                    not pd.isna(self.df.loc[diff_date].close) and
                                    not pd.isna(rolled_df_list[-itr].loc[diff_date].close)
                                ):
                                    diff = rolled_df_list[-itr].loc[diff_date].close
                                    diff -= self.df.loc[diff_date].close
                                    break

                                else:
                                    self.df.loc[diff_date, "open "] = np.nan
                                    self.df.loc[diff_date, "high "] = np.nan
                                    self.df.loc[diff_date, "low "] = np.nan
                                    self.df.loc[diff_date, "close "] = np.nan

                                lookback_itr += 1

                            if self.max_lookback < lookback_itr:
                                # if isFirstBackadjusteSucceed or itr == n:
                                #     print(isFirstBackadjusteSucceed)
                                #     print(itr)
                                #     print(lookback_itr)
                                #     raise
                                # else:
                                self.df = pd.DataFrame()
                                continue     

                            # isFirstBackadjusteSucceed = True
                            self.df = self.df + diff

                        self.df = pd.concat([self.df, trimmed])

            self.df = self.df * self.multiplier 
            self.df *= contract_ticker.currency_multiplier

        return self.df