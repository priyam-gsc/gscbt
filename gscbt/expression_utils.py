from datetime import datetime

from gscbt.ticker import Ticker
from gscbt.utils import MonthMap
from gscbt.data.utils import get_full_year


def extract_sym_month_year_from_contract(         
    contract: str
) -> tuple[str, str, str]:
    return contract[:-3], contract[-3], contract[-2:]


def extract_contracts_multipliers_operators( exp: str):
    try:
        exp = exp.replace(" ", "")
        contracts = []
        multipliers = []
        operators = []
        s = "" 

        itr  = 0
        while itr < len(exp):
            if itr == 0:
                if exp[itr] == "-":
                    operators.append(exp[itr])
                else:
                    operators.append("+")

            if exp[itr] not in "+-*0123456789":
                if(itr == 0 or exp[itr-1] in "+-"):
                    multipliers.append(1)
                else:
                    mul = ""
                    temp_itr = itr-2
                    while(temp_itr >= 0):
                        if(not("0" <= exp[temp_itr] and exp[temp_itr] <= "9")):
                            break
                        else:
                            mul += exp[temp_itr]
                        temp_itr -= 1

                    multipliers.append(int(mul[::-1]))  

                s = ""
                while itr < len(exp):
                    if exp[itr] == "+" or exp[itr] == "-":
                        operators.append(exp[itr])
                        contracts.append(s)
                        break
                    else:
                        s += exp[itr]
                    
                    itr += 1
                    if itr == len(exp):
                        contracts.append(s)
            else:
                itr += 1

        if not (len(contracts) == len(multipliers) and len(multipliers) == len(operators)):
            raise 
        
        for contract in contracts:
            _ = int(contract[-2:])
            _ = MonthMap.month(contract[-3])

            sym = contract[:-3]
            valid_months = Ticker.SYMBOLS[sym].contract_months
            if valid_months.replace("-", "").find(contract[-3]) == -1:
                raise ValueError(f"[-] DataPipeline.extract_contracts_multipliers_operatiors \
                                    Invalid expression contract month in {contract} fail to parse")

        year_offset = extract_year_offset(contracts)
        if(year_offset < 0):
            raise ValueError(f"[-] DataPipeline.extract_contracts_multipliers_operatiors \
                                Invalid expression contract year is less than current year in {contracts} fail to parse")   

        return contracts, multipliers, operators
    
    except:
        raise ValueError(f"[-] DataPipeline.extract_contracts_multipliers_operatiors Invalid expression {exp} fail to parse")


def extract_contracts_multipliers(exp: str)->tuple[list[str], list[int]]:
    contracts, multipliers, operators = extract_contracts_multipliers_operators(exp)
    
    for itr in range(len(contracts)):
        if operators[itr] == '-':
            multipliers[itr] *= -1

    return contracts, multipliers


def extract_min_year_from_contracts(
    contracts: list[str]
) -> str:

    min_year = 9999
    for contract in contracts:
        full_year = get_full_year(contract[-2:])
        if min_year > full_year:
            min_year = full_year

    return f"{min_year%100}"

def extract_year_offset( contracts: list[str]) -> int:
    min_year = int(extract_min_year_from_contracts(contracts))

    l1 = (min_year - int(str(datetime.today().year)[-2:])) % 100
    l2 = (int(str(datetime.today().year)[-2:]) - min_year) % 100

    if l1 < l2:
        return l1
    else:
        return -l2 

def extract_min_month_from_contracts_for_given_year(
    contracts: list[str],
    year: str,
) -> tuple[str, int]:        
    min_month = ""
    min_month_idx = 13
    for contract in contracts:
        if contract[-2:] == year:
            month_idx = MonthMap.month(contract[-3:-2])
            if min_month_idx > month_idx:
                min_month_idx = month_idx
                min_month = contract[-3:-2]
    return min_month, min_month_idx

def extract_min_contracts_from_contracts(
    offset_contracts : list[str],
) -> list[str]:
    min_year = f"{extract_min_year_from_contracts(offset_contracts):02}"
    min_month_code, _ = extract_min_month_from_contracts_for_given_year(offset_contracts, min_year)

    min_contracts = []
    suffix = min_month_code + min_year
    for contract in offset_contracts:
        if suffix == contract[-3:]:
            min_contracts.append(contract)

    return min_contracts      
    
# below 2 functions don't have negative offset support 
def convert_contracts_to_offset_contracts(
    contracts : list[str], 
) -> list[str]:
    offset_contracts = []
    min_year = extract_min_year_from_contracts(contracts)
    year_offset = extract_year_offset(contracts)

    for contract in contracts:
        val = (int(contract[-2:]) - int(min_year) + year_offset)
        offset_contracts.append(f"{contract[:-2]}{val:02}")

    return offset_contracts

def convert_offset_contracts_to_given_year(
    offset_contracts : list[str],
    year : str,
) -> list[str]:
    contracts = []
    for contract in offset_contracts:
        contracts.append(f"{contract[:-2]}{(int(contract[-2:]) + int(year))%100:02}")
    
    return contracts

def move_contracts_to_prev_year(
    contracts : list[str],
) -> list[str]:
    moved_contracts = []
    for contract in contracts:
        moved_contracts.append(f"{contract[:-2]}{(int(contract[-2:])-1)%100:02}")

    return moved_contracts

def move_contracts_to_next_valid_month(
    contracts : list[str],
) -> list[str]:
    try:
        moved_contracts = []
        for contract in contracts:
            sym = contract[:-3]
            valid_months = Ticker.SYMBOLS[sym].contract_months
            valid_months = valid_months.replace("-", "")

            idx = valid_months.find(contract[-3])

            if idx == -1:
                raise 

            if idx == len(valid_months)-1:
                contract = contract[:-3] + valid_months[0]  + f"{(int(contract[-2:])+1)%100:02}"
            else:
                contract = contract[:-3] + valid_months[idx+1] + contract[-2:]

            moved_contracts.append(contract)

        return moved_contracts
    except:
        raise ValueError(f"[-] DataPipeline.move_contracts_to_next_valid_month Invalid contract value {contract}")

def move_contracts_to_prev_valid_month(
    contracts : list[str],
) -> list[str]:
    try:
        moved_contracts = []
        for contract in contracts:
            sym = contract[:-3]
            valid_months = Ticker.SYMBOLS[sym].contract_months
            valid_months = valid_months.replace("-", "")

            idx = valid_months.find(contract[-3])

            if idx == -1:
                raise 

            if idx == 0:
                contract = contract[:-3] + valid_months[-1]  + f"{(int(contract[-2:])-1)%100:02}"
            else:
                contract = contract[:-3] + valid_months[idx-1] + contract[-2:]

            moved_contracts.append(contract)

        return moved_contracts
    except:
        raise ValueError(f"[-] DataPipeline.move_contracts_to_prev_valid_month Invalid contract value {contract}")

def move_contract_to_given_next_valid_month(
    contract : str,
    valid_months : str,
) -> str:
    idx = valid_months.find(contract[-3])

    if idx == -1:
        raise

    if idx == len(valid_months) - 1:
        return f"{contract[:-3]}{valid_months[0]}{(int(contract[-2:])+1)%100:02}"

    return f"{contract[:-3]}{valid_months[idx+1]}{contract[-2:]}"

def move_contract_to_given_prev_valid_month(
    contract : str,
    valid_months : str,
) -> str:
    idx = valid_months.find(contract[-3])

    if idx == -1:
        raise

    if idx == 0:
        idx = len(valid_months) -1
        return f"{contract[:-3]}{valid_months[idx]}{(int(contract[-2:])-1)%100:02}"
    else:
        idx -= 1
        return f"{contract[:-3]}{valid_months[idx]}{contract[-2:]}"