import pytest

from gscbt.expression_utils import (
    extract_sym_month_year_from_contract,
    get_full_year,
    extract_contracts_multipliers_operators,
    extract_contracts_multipliers,
    extract_full_min_year_from_contracts,
    move_contracts_to_prev_year,
    move_contracts_to_next_valid_month,
    move_contracts_to_prev_valid_month,
    move_contract_to_given_next_valid_month,
    move_contract_to_given_prev_valid_month,
    move_contracts_to_given_year_from_min,
)


## extract_sym_month_year_from_contract
@pytest.mark.parametrize("contract, res", [
    ("CLG25", ("CL", "G", "25")),
    ("CLF00", ("CL", "F", "00")),
    ("GZ00", ("G", "Z", "00")),
    ("GZ99", ("G", "Z", "99")),
])
def test_extract_sym_month_year_from_contract(contract, res):
    assert extract_sym_month_year_from_contract(contract) == res

## get_full_year
@pytest.mark.parametrize("contract_year, res", [
    (99, 1999), (49, 2049), (50, 1950),
])
def test_get_full_year(contract_year, res):
    assert get_full_year(contract_year) == int(res)


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
])
def test_exception_extract_contracts_multipliers_operators(expression, e_type):
    with pytest.raises(e_type):
        extract_contracts_multipliers_operators(expression)


## extract_contracts_multipliers
@pytest.mark.parametrize("expression, res", [
    ("CLG29+2*CLZ30", (["CLG29", "CLZ30"], [1, 2])),
    ("-CLG29+2*CLZ30", (["CLG29", "CLZ30"], [-1, 2])),
    ("-2*CLG29-GZ30", (["CLG29", "GZ30"], [-2, -1])),
    ("-2*CLG29-1*GZ30", (["CLG29", "GZ30"], [-2, -1])),
])
def test_extract_contracts_multipliers(expression, res):
    assert extract_contracts_multipliers(expression) == res


## extract_full_min_year_from_contracts
@pytest.mark.parametrize("contracts, res", [
    (["CLG25", "CLG99", "CLJ00", "CLZ09"], 1999),
    (["CLG02", "CLG09", "CLJ00", "CLZ09"], 2000),
    (["CLG12", "CLG19", "CLJ20", "CLZ10"], 2010),
    (["CLG29", "CLG29", "CLJ28", "CLZ30"], 2028),
    (["CLG25", "CLG29", "CLJ24", "CLZ23"], 2023),
])
def test_extract_min_year_from_contracts(contracts,res):
    assert extract_full_min_year_from_contracts(contracts) == res


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


## move_contracts_to_prev_valid_month
@pytest.mark.parametrize("contracts, res", [
    (["CLF23", "ZCH99"], ["CLZ22", "ZCZ98"]),
    (["CLF25", "CLG25"], ["CLZ24", "CLF25"]),
    (["CLF25", "CLZ99"], ["CLZ24", "CLX99"]),
])
def test_move_contracts_to_prev_valid_month(contracts, res):
    assert move_contracts_to_prev_valid_month(contracts) == res

@pytest.mark.parametrize("contracts, e_type", [
    (["CLA23", "ZCH99"], ValueError),
    (["CLF025", "CLG25"], ValueError),
])
def test__exception_move_contracts_to_prev_valid_month(contracts, e_type):
    with pytest.raises(e_type):
        move_contracts_to_prev_valid_month(contracts)


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


## move_contracts_to_given_year_from_min
@pytest.mark.parametrize("contracts, year, res", [
    (["CLG25", "CLG24", "CLJ25", "CLZ26"], 2023, ["CLG24", "CLG23", "CLJ24", "CLZ25"]),
    (["CLG02", "CLG03", "CLJ04", "CLZ01"], 2027, ["CLG28", "CLG29", "CLJ30", "CLZ27"]),
    (["CLG12", "CLG19", "CLJ20", "CLZ10"], 1999, ["CLG01", "CLG08", "CLJ09", "CLZ99"]),
])
def test_move_contracts_to_given_year_from_min(contracts, year, res):
    assert move_contracts_to_given_year_from_min(contracts, year) == res