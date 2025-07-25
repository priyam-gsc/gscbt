from datetime import datetime

from gscbt.ticker import Ticker
from gscbt.utils import MonthMap


def extract_sym_month_year_from_contract(         
    contract: str
) -> tuple[str, str, str]:
    return contract[:-3], contract[-3], contract[-2:]

def get_full_year(
    two_digit_year : int,
    pivot : int = 50,
) -> int:
    if two_digit_year < pivot:
        return 2000 + two_digit_year
    else: 
        return 1900 + two_digit_year

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

        return contracts, multipliers, operators
    
    except:
        raise ValueError(f"[-] DataPipeline.extract_contracts_multipliers_operatiors Invalid expression {exp} fail to parse")

def extract_contracts_multipliers(exp: str)->tuple[list[str], list[int]]:
    contracts, multipliers, operators = extract_contracts_multipliers_operators(exp)
    
    for itr in range(len(contracts)):
        if operators[itr] == '-':
            multipliers[itr] *= -1

    return contracts, multipliers

def extract_full_min_year_from_contracts(
    contracts: list[str]
) -> int:

    min_year = 9999
    for contract in contracts:
        full_year = get_full_year(int(contract[-2:]))
        if min_year > full_year:
            min_year = full_year

    return min_year

def move_contracts_to_prev_year(
    contracts : list[str],
) -> list[str]:
    moved_contracts = []
    for contract in contracts:
        new_year = get_full_year(int(contract[-2:])) - 1
        moved_contracts.append(f"{contract[:-2]}{new_year % 100:02}")

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
                new_year = get_full_year(int(contract[-2:])) + 1
                contract = contract[:-3] + valid_months[0]  + f"{new_year % 100:02}"
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
                new_year = get_full_year(int(contract[-2:])) - 1
                contract = contract[:-3] + valid_months[-1]  + f"{new_year%100:02}"
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
        new_year = get_full_year(int(contract[-2:])) + 1
        return f"{contract[:-3]}{valid_months[0]}{new_year%100:02}"

    return f"{contract[:-3]}{valid_months[idx+1]}{contract[-2:]}"

def move_contract_to_given_prev_valid_month(
    contract : str,
    valid_months : str,
) -> str:
    idx = valid_months.find(contract[-3])

    if idx == -1:
        raise

    if idx == 0:
        new_year = get_full_year(int(contract[-2:])) - 1
        return f"{contract[:-3]}{valid_months[-1]}{new_year%100:02}"
    else:
        return f"{contract[:-3]}{valid_months[idx-1]}{contract[-2:]}"
       
def move_contracts_to_given_year_from_min(contracts: list[str], year: int) -> list[str]:
    min_year = None
    for contract in contracts:
        if min_year == None or min_year > get_full_year(int(contract[-2:])):
            min_year = get_full_year(int(contract[-2:]))

    diff = year - min_year
    updated_contracts = []
    for contract in contracts:
        updated_year = get_full_year(int(contract[-2:])) + diff
        
        updated_contracts.append(f"{contract[:-2]}{updated_year%100:02}")

    return updated_contracts
