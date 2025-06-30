import pandas as pd 

from gscbt.ticker import Ticker
from gscbt.expression_utils import move_contract_to_given_prev_valid_month
from gscbt.utils import Interval

from .outright import get_outright
from .roll_method import roll_offset
from .utils import (
    df_apply_operation_to_given_columns,
    get_full_year,
    drop_ohlcv,
)
from .contract_spec import (
    ContractSpec,
    DataType,
    ValuationType,
    RollMethod,
)

class SyntheticLeg:
    def __init__(
        self,
        contract : str,
        contract_roll_months : str,
        rt_contract : str,             # roll_trigger_contract
        rt_contract_roll_months : str,
        multiplier : int,
        contract_spec : ContractSpec,
        start_rt_contract : str,
        ohlcv : str = "c",
        interval : str = "1d",
        extra_columns : list[str] = []
    ):
        self.contract = contract
        self.contract_roll_months = contract_roll_months
        self.rt_contract = rt_contract
        self.rt_contract_roll_months = rt_contract_roll_months
        self.multiplier = multiplier
        self.contract_spec = contract_spec
        self.start_rt_contract = start_rt_contract
        self.ohlcv = ohlcv
        self.interval = interval
        self.extra_columns = extra_columns
        self._df : pd.DataFrame = pd.DataFrame()

    def get(self) -> pd.DataFrame:
        if self._df.empty:
            self.create()

        return self._df

    def create(self) -> None:

        if not self._df.empty:
            return 
        
        contract_ticker = Ticker.SYMBOLS[self.rt_contract[:-3]]
        rt_contract_ticker = Ticker.SYMBOLS[self.rt_contract[:-3]]

        contract = self.contract
        rt_contract = self.rt_contract

        contract_df_list = []
        rt_contract_df_list = []

        contract_df = None
        rt_contract_df = None

        while True:
            year = get_full_year(int(rt_contract[-2:]))
            src_year = get_full_year(int(self.start_rt_contract[-2:])) # src = start_rt_contract
            month = contract[-3]
            src_month = self.start_rt_contract[-3]
            
            
            if(year < src_year or (month<src_month and year == src_year)):
                break

            try:
                contract_df, ok = get_outright(
                    contract_ticker,
                    contract,
                    self.ohlcv,
                    self.interval,
                )

                if not ok:
                    break

                rt_contract_df, ok = get_outright(
                    rt_contract_ticker,
                    rt_contract,
                    "c",
                    self.interval,
                )

                if not ok:
                    break
            
            except:
                break

            if "sym" in self.extra_columns:
                contract_df["sym"] = contract
            
            if "contract_expiry_date" in self.extra_columns:
                contract_df["contract_expiry_date"] = contract_df.index[-1]

            contract_df_list.append(contract_df)
            rt_contract_df_list.append(rt_contract_df)
            
            contract = move_contract_to_given_prev_valid_month(
                contract,
                self.contract_roll_months,
            )

            rt_contract = move_contract_to_given_prev_valid_month(
                rt_contract,
                self.rt_contract_roll_months,
            )

        if len(contract_df_list) != len(rt_contract_df_list):
            raise Exception(
                f"length of contract data and roll trigger data don't match \
                try again by setting the data start point to a closer one. "
            )

        contract_df_list = contract_df_list[::-1]
        rt_contract_df_list = rt_contract_df_list[::-1]


        if self.contract_spec.roll_method == RollMethod.OFFSET:    

            rt_expiry_date_list = []
            for rt_contract_df in rt_contract_df_list:
                rt_expiry_date_list.append(rt_contract_df.index[-1])

            interval_in_sec = Interval.str_to_second(self.interval)
            interval_offset = pd.Timedelta(seconds=interval_in_sec)

            self._df = roll_offset(
                contract_df_list= contract_df_list,
                rt_expiry_date_list= rt_expiry_date_list,
                offset= self.contract_spec.roll_params.offset,
                interval_offset= interval_offset,
                data_type= self.contract_spec.data_type,
                max_lookahead= self.contract_spec.roll_params.max_lookahead,
                extra_columns= self.extra_columns,
            )
        

        self._df = df_apply_operation_to_given_columns(
            df = self._df,
            value= self.multiplier,
            op="mul"
        )

        if self.contract_spec.valuation_type == ValuationType.DOLLAR_EQUIVALENT:
            self._df = df_apply_operation_to_given_columns(
                df = self._df,
                value= contract_ticker.currency_multiplier,
                op="mul"
            )

        self._df = drop_ohlcv(self._df, self.ohlcv)