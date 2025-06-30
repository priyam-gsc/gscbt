import pandas as pd

from .contract_spec import ContractSpec
from .synthetic_leg import SyntheticLeg
from .utils import df2df_apply_operation_to_given_columns

class SyntheticBuilder:

    def create_leg(
        contract : str,
        contract_roll_months : str,
        rt_contract : str,
        rt_contract_roll_months : str,
        start_rt_contract : str,
        multiplier : int,
        contract_spec : ContractSpec,
        interval : str,
    ) -> dict:

        leg = {}
        leg["contract"] = contract
        leg["contract_roll_months"] = contract_roll_months
        leg["rt_contract"] = rt_contract
        leg["rt_contract_roll_months"] = rt_contract_roll_months
        leg["start_rt_contract"] = start_rt_contract
        leg["multiplier"] = multiplier
        leg["contract_spec"] = contract_spec
        leg["interval"] = interval

        return leg

    def __init__(
        self,
        legs : list,
    ):
        self.legs = legs
        self._df : pd.DataFrame = pd.DataFrame()

    def get(self) -> pd.DataFrame:
        if self._df.empty:
            self.create()

        return self._df

    def create(self):
        if not self._df.empty:
            return 
        
        leg_list = []

        for leg in self.legs:
            curr_leg = SyntheticLeg(
                contract = leg["contract"],
                contract_roll_months = leg["contract_roll_months"],
                rt_contract = leg["rt_contract"],
                rt_contract_roll_months = leg["rt_contract_roll_months"],
                start_rt_contract = leg["start_rt_contract"],
                multiplier = leg["multiplier"],
                contract_spec = leg["contract_spec"],
                interval = leg["interval"],
                ohlcv = "c",
                extra_columns = [],
            )
            curr_leg.create()
            leg_list.append(curr_leg.get())


        for leg in leg_list:
            if self._df.empty:
                self._df = leg.copy()
            else:
                self._df = df2df_apply_operation_to_given_columns(
                    df1= self._df,
                    df2= leg,
                    columns= ["open", "high", "low", "close"],
                    op= "add",
                )