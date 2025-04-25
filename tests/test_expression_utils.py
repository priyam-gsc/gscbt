from pathlib import Path
from datetime import datetime

import pytest
import pandas as pd

from gscbt.ticker import Ticker
from gscbt.expression_utils import (
    extract_sym_month_year_from_contract,
    extract_contracts_multipliers_operators,
    extract_min_year_from_contracts,
    extract_year_offset,
    extract_min_month_from_contracts_for_given_year,
    extract_min_contracts_from_contracts,
    convert_contracts_to_offset_contracts,
    convert_offset_contracts_to_given_year,
    move_contracts_to_prev_year,
    move_contracts_to_next_valid_month,
    move_contract_to_given_next_valid_month,
    move_contract_to_given_prev_valid_month,

)

TEST_DATA_PATH = Path(__file__).parent / "test_data" / "data_pipeline"

## extract_sym_month_year_from_contract

@pytest.mark.parametrize("contract, res", [
    ("CLG25", ("CL", "G", "25")),
    ("CLF00", ("CL", "F", "00")),
    ("GZ00", ("G", "Z", "00")),
    ("GZ99", ("G", "Z", "99")),
])
def test_extract_sym_month_year_from_contract(contract, res):
    assert extract_sym_month_year_from_contract(contract) == res


## extract_contracts_multipliers_operators

@pytest.mark.parametrize("expression, res", [
    ("CLG29+2*CLZ30", (["CLG29", "CLZ30"], [1, 2], ["+", "+"])),
    ("-CLG29+2*CLZ30", (["CLG29", "CLZ30"], [1, 2], ["-", "+"])),
    ("-2*CLG29-GZ30", (["CLG29", "GZ30"], [2, 1], ["-", "-"])),
    ("-2*CLG29-1*GZ30", (["CLG29", "GZ30"], [2, 1], ["-", "-"])),
])
def test_extract_contracts_multipliers_operators(expression, res):
    
    assert extract_contracts_multipliers_operators(expression) == res

@pytest.mark.parametrize("expression, e_type", [
    ("CLG29+2*GF230", ValueError),
    ("CLG29+2*CLZ223", ValueError),
    ("CLG29+2*CLA30", ValueError),
    ("XXG29+2*CLA30", ValueError),
    ("CLG29+2*CLZ30+", ValueError),
    ("*CLG29+2*CLZ30", ValueError),
    ("CLG29+2*CLZ30-", ValueError),
    ("CLG23+2*CLZ30", ValueError),
    ("CLG23+2*CLZ99", ValueError),
])
def test_exception_extract_contracts_multipliers_operators(expression, e_type):
    
    with pytest.raises(e_type):
        extract_contracts_multipliers_operators(expression)


## extract_min_year_from_contracts

@pytest.mark.parametrize("contracts, res", [
    (["CLG25", "CLG99", "CLJ00", "CLZ09"], "99"),
    (["CLG02", "CLG09", "CLJ00", "CLZ09"], "00"),
    (["CLG12", "CLG19", "CLJ20", "CLZ10"], "10"),
    (["CLG29", "CLG29", "CLJ28", "CLZ30"], "28"),
    (["CLG25", "CLG29", "CLJ24", "CLZ23"], "23"),
])
def test_extract_min_year_from_contracts(contracts,res):
    
    assert extract_min_year_from_contracts(contracts) == res


## extract_year_offset

@pytest.mark.parametrize("contracts", [
    (["CLG25", "CLG99", "CLJ00", "CLZ09"]),
    (["CLG02", "CLG09", "CLJ00", "CLZ09"]),
    (["CLG12", "CLG19", "CLJ20", "CLZ10"]),
    (["CLG29", "CLG29", "CLJ28", "CLZ30"]),
    (["CLG25", "CLG29", "CLJ24", "CLZ23"]),
])
def test_year_offset(contracts):
    
    offset = extract_year_offset(contracts)
    min_year = int(extract_min_year_from_contracts(contracts))
    print(min_year - offset)
    assert  (min_year - offset)%100 == int(str(datetime.today().year)[-2:])


## extract_min_month_from_contracts_for_given_year

@pytest.mark.parametrize("contracts, year, res", [
    (["CLG25", "CLG99", "CLJ00", "CLZ99"], "99", ("G", 2)),
    (["CLG02", "CLG02", "CLJ02", "CLZ02"], "00", ("", 13)),
    (["CLG12", "CLG19", "CLJ20", "CLZ10"], "10", ("Z", 12)),
])
def test_extract_min_month_from_contracts_for_given_year(
    contracts,
    year,
    res,
):
    
    assert extract_min_month_from_contracts_for_given_year(contracts, year) == res


## extract_min_contracts_from_contracts

## convert_contracts_to_offset_contracts

## convert_offset_contracts_to_given_year

## move_contracts_to_prev_year

@pytest.mark.parametrize("contracts, res", [
    (["CLG25", "CLG99", "CLJ00", "CLZ09"], ["CLG24", "CLG98", "CLJ99", "CLZ08"]),
    (["CLG02", "CLG09", "CLJ20", "CLZ01"], ["CLG01", "CLG08", "CLJ19", "CLZ00"]),
    (["CLG12", "CLG19", "CLJ20", "CLZ10"], ["CLG11", "CLG18", "CLJ19", "CLZ09"]),
    (["CLG29", "CLG29", "CLJ28", "CLZ30"], ["CLG28", "CLG28", "CLJ27", "CLZ29"]),
    (["CLG25", "CLG29", "CLJ24", "CLZ23"], ["CLG24", "CLG28", "CLJ23", "CLZ22"]),
])
def test_move_contracts_to_prev_year(contracts, res):
    
    assert move_contracts_to_prev_year(contracts) == res


## move_contracts_to_next_valid_month

@pytest.mark.parametrize("contracts, res", [
    (["CLF23", "ZCH99"], ["CLG23", "ZCK99"]),
    (["CLF25", "CLG25"], ["CLG25", "CLH25"]),
    (["CLF25", "CLZ99"], ["CLG25", "CLF00"]),
])
def test_move_contracts_to_next_valid_month(contracts, res):
    
    assert move_contracts_to_next_valid_month(contracts) == res

@pytest.mark.parametrize("contracts, e_type", [
    (["CLA23", "ZCH99"], ValueError),
    (["CLF025", "CLG25"], ValueError),
])
def test__exception_move_contracts_to_next_valid_month(contracts, e_type):
    
    with pytest.raises(e_type):
        move_contracts_to_next_valid_month(contracts)


## move_contract_to_given_next_valid_month

@pytest.mark.parametrize("contract, valid_months, res", [
    ("CLF25", "FZ", "CLZ25"),
    ("CLQ25", "KMQUVXZ", "CLU25"),
    ("CLZ25", "KMQUVXZ", "CLK26"),
    ("CLZ99", "KMQUVXZ", "CLK00"),
])
def test_move_contract_to_given_next_valid_month(
    contract,
    valid_months,
    res
):
    assert move_contract_to_given_next_valid_month(
        contract,
        valid_months,
    ) == res


## move_contract_to_given_prev_valid_month

@pytest.mark.parametrize("contract, valid_months, res", [
    ("CLZ25", "FZ", "CLF25"),
    ("CLQ25", "KMQUVXZ", "CLM25"),
    ("CLK25", "KMQUVXZ", "CLZ24"),
    ("CLK00", "KMQUVXZ", "CLZ99"),
])
def test_move_contract_to_given_prev_valid_month(
    contract,
    valid_months,
    res
):
    assert move_contract_to_given_prev_valid_month(
        contract,
        valid_months,
    ) == res


## get_outright

# @pytest.mark.parametrize("ticker, contract, ohlcv, interval, res, res_file", [
#     (Ticker.TICKERS.cme.cl.f, "CLF23", "ohlcv", "1d", True, "CLF23_1d.csv"),
#     (Ticker.TICKERS.cme.cl.f, "CLF75", "ohlcv", "1d", False, ""),
# ])
# def test_get_outright(ticker, contract, ohlcv, interval, res, res_file):
    
#     df1, res1 = get_outright(ticker, contract, ohlcv, interval)

#     df2 = pd.DataFrame()
#     if res:
#         df2 = pd.read_csv(TEST_DATA_PATH / res_file)
#         df2.rename(
#             columns={
#                 "Timestamp" : "timestamp",
#                 "Open" : "open",
#                 "High" : "high",
#                 "Low" : "low",
#                 "Close" : "close",
#                 "Volume" : "volume",
#             },
#             inplace= True,
#         )
#         df2.drop(
#             columns=["Sym", "OpenInterest"],    
#             axis=1,
#             inplace= True,
#         )
#         df2["timestamp"] = pd.to_datetime(df2["timestamp"], utc=True)
#         df2.set_index(["timestamp"], inplace=True)
    
#         # for itr in range(len(df1.index)):
#         #     if type(df1.index[itr]) != type(df2.index[itr]):
#         #         print(df1.index[itr])
#         #         print(df2.index[itr])

#         # print(df1.index.dtype)
#         # print(df2.index.dtype)

#     assert res1 == res and df1.equals(df2) == True