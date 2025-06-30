from datetime import datetime
import json
import tomllib
import io

import pandas as pd
import requests

from gscbt.ticker import get_instrument_contract_months
from gscbt.expression_utils import (
    extract_contracts_multipliers,
    move_contracts_to_prev_valid_month,
)

from .synthetic_builder import SyntheticLeg, SyntheticBuilder
from .contract_spec import (
    ContractSpec,
    DataType,
    ValuationType,
    RollMethod,
    RollParams,
)
from .contract_spec_dict import (
    DATATYPEDICT,
    VALUATIONTYPEDICT,
    ROLLMETHODDICT,
)
from .utils import get_full_year

def get_contract_expiry(contract : str) -> pd.Timestamp:
    response = requests.get(
        "http://192.168.0.25:8080/api/v1/data/contract_dates_bulk",
        params={
            "symbols" : contract,
        },
    )

    if response.status_code == 200:
        res = json.loads(response.content)
        date_str = res[0]["last_date"]
        if date_str != "":
            try:
                return pd.to_datetime(date_str)
            except:
                pass

    raise ValueError(f"contract {contract} can't find expiry for it")

def move_contracts_to_give_year_from_min(contracts: list[str], year: int) -> list[str]:
    min_year = None
    for contract in contracts:
        if min_year == None or min_year > get_full_year(int(contract[-2:])):
            min_year = get_full_year(int(contract[-2:]))

    diff = year - min_year
    updated_contracts = []
    for contract in contracts:
        updated_year = get_full_year(int(contract[-2:])) + diff
        
        updated_contracts.append(f"{contract[:-2]}{updated_year%100}")

    return updated_contracts


def sbw_get_contractwise(
    expression : str,
    roll_offset : int,
    roll_max_lookahead : int,
    start_year : int,
    end_year : int | None = None,
    isForwardAdjusted : bool = True,
    interval : str = "1d",
)->pd.DataFrame:
    
    # move expression to end year
    if end_year == None or end_year > datetime.now().year - 2:
        end_year = datetime.now().year - 2

    contracts, multipliers = extract_contracts_multipliers(expression)
    contracts = move_contracts_to_give_year_from_min(contracts, end_year)

    min_contract = None
    min_expiry = None
    for contract in contracts:
        contract_expiry = get_contract_expiry(contract)
        
        if min_contract == None or min_expiry > contract_expiry:
            min_contract = contract
            min_expiry = contract_expiry


    data_type = DataType.CONTINUOUS
    if isForwardAdjusted:
        data_type = DataType.FORWARDADJUSTED

    contract_spec = ContractSpec(
        data_type = data_type,
        valuation_type = ValuationType.DOLLAR_EQUIVALENT,
        roll_method = RollMethod.OFFSET,
        roll_params = RollParams(
            offset = roll_offset,
            max_lookahead = roll_max_lookahead,
        )
    )

    legs = []
    for itr in range(len(contracts)):
        leg = SyntheticBuilder.create_leg(
            contract = contracts[itr],
            contract_roll_months = contracts[itr][-3],
            rt_contract = min_contract,
            rt_contract_roll_months = min_contract[-3],
            start_rt_contract = min_contract[:-2] + str(start_year%100),
            multiplier =  multipliers[itr],
            contract_spec = contract_spec,
            interval = interval,
        )

        legs.append(leg)

    sb = SyntheticBuilder(legs)
    df1 = sb.get()

    # for extracting ideal roll date
    sl = SyntheticLeg(
        contract = min_contract,
        contract_roll_months = min_contract[-3],
        rt_contract = min_contract,
        rt_contract_roll_months = min_contract[-3],
        multiplier = 1,
        start_rt_contract = min_contract[:-2] + str(start_year%100),
        contract_spec = contract_spec,
        ohlcv = "c",
        interval = interval,
        extra_columns = ["ideal_roll_date"],
    )
    df2 = sl.get()
    df1["ideal_roll_date"] = df2["ideal_roll_date"]

    # cropping first df
    approx_start_date = pd.to_datetime(df2["ideal_roll_date"].iloc[0])
    approx_start_date -= pd.DateOffset(years=1)

    return df1.loc[approx_start_date:]

def sbw_get_spreadwise(
    expression : str,
    roll_offset : int,
    roll_max_lookahead : int,
    start_year : int,
    end_year : int | None = None,
    isForwardAdjusted : bool = True,
    interval : str = "1d",
)->pd.DataFrame:

    # move expression to end year
    if end_year == None or end_year > datetime.now().year - 2:
        end_year = datetime.now().year - 2

    contracts, multipliers = extract_contracts_multipliers(expression)
    contracts = move_contracts_to_give_year_from_min(contracts, end_year)
    
    # for cropping first df
    contracts = move_contracts_to_prev_valid_month(contracts)

    min_contract = None
    min_expiry = None
    for contract in contracts:
        contract_expiry = get_contract_expiry(contract)
        
        if min_contract == None or min_expiry > contract_expiry:
            min_contract = contract
            min_expiry = contract_expiry

    data_type = DataType.CONTINUOUS
    if isForwardAdjusted:
        data_type = DataType.FORWARDADJUSTED

    contract_spec = ContractSpec(
        data_type = data_type,
        valuation_type = ValuationType.DOLLAR_EQUIVALENT,
        roll_method = RollMethod.OFFSET,
        roll_params = RollParams(
            offset = roll_offset,
            max_lookahead = roll_max_lookahead,
        )
    )

    legs = []
    for itr in range(len(contracts)):
        contract = contracts[itr]

        contract_roll_months = get_instrument_contract_months(contract[:-3])
        rt_contract_roll_months = get_instrument_contract_months(min_contract[:-3])

        if len(contract_roll_months) != len(rt_contract_roll_months):
            raise ValueError(
                f"contract {contract} and roll trigger contract {min_contract} \
                months length don't match {contract_roll_months} & {rt_contract_roll_months}"
            )

        leg = SyntheticBuilder.create_leg(
            contract = contract,
            contract_roll_months = contract_roll_months,
            rt_contract = min_contract,
            rt_contract_roll_months = rt_contract_roll_months,
            start_rt_contract = min_contract[:-2] + str(start_year%100),
            multiplier =  multipliers[itr],
            contract_spec = contract_spec,
            interval = interval,
        )

        legs.append(leg)

    sb = SyntheticBuilder(legs)
    df1 = sb.get()

    # for extracting ideal roll date
    sl = SyntheticLeg(
        contract = min_contract,
        contract_roll_months = rt_contract_roll_months,
        rt_contract = min_contract,
        rt_contract_roll_months = rt_contract_roll_months,
        multiplier = 1,
        start_rt_contract = min_contract[:-2] + str(start_year%100),
        contract_spec = contract_spec,
        ohlcv = "c",
        interval = interval,
        extra_columns = ["ideal_roll_date"],
    )
    df2 = sl.get()
    df1["ideal_roll_date"] = df2["ideal_roll_date"]

    # cropping first df
    approx_start_date = df2["ideal_roll_date"].iloc[0]
    return df1[df1["ideal_roll_date"] != approx_start_date]


def sbw_synthetic_from_toml_stream_common_spec(
        file : str | io.BytesIO,
    ) -> pd.DataFrame:

    config = None
    if isinstance(file, str):
        with open(file, "rb") as f:
            config = tomllib.load(f)
    elif isinstance(file, io.BytesIO):
        config = tomllib.load(file)
    else:
        raise ValueError(f"file should be file_path or io.BytesIO")

    contract_spec = ContractSpec(
        data_type = DATATYPEDICT[config.get("data_type")],
        valuation_type = VALUATIONTYPEDICT[config.get("valuation_type")],
        roll_method = ROLLMETHODDICT[config.get("roll_method")]
    )
    interval = config.get("interval")

    legs = []
    for leg in config.get("legs", []):
        temp_contract_spec = contract_spec
        temp_contract_spec.roll_params = RollParams(
            offset = leg.get("offset"),
            max_lookahead = leg.get("max_lookahead"),
        )

        leg["contract_spec"] = temp_contract_spec
        leg["interval"] = interval
        legs.append(leg)

    sb = SyntheticBuilder(legs)
    df1 = sb.get()

    return df1

def sbw_create_toml_skeleton_common_spec(
    file: str | io.StringIO,
    leg_count: int = 1,
) -> None:

    if isinstance(file, str):
        f = open(file, "w")
    elif isinstance(file, io.StringIO):
        f = file

    # Write header section
    f.write('data_type = "forward"\n')
    f.write('valuation_type = "de"\n')
    f.write('roll_method = "offset"\n')
    f.write('interval = "1d"\n\n')

    # Write each leg as a TOML table array
    for i in range(1, leg_count + 1):
        f.write('[[legs]]\n')
        f.write(f'leg = {i}\n')
        f.write('contract = ""\n')
        f.write('contract_roll_months = ""\n')
        f.write('rt_contract = ""\n')
        f.write('rt_contract_roll_months = ""\n')
        f.write('start_rt_contract = ""\n')
        f.write('multiplier = \n')
        f.write('offset = 10\n')
        f.write('max_lookahead = 2\n\n')

    if isinstance(file, str):
        f.close()