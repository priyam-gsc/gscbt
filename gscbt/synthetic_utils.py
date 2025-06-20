from .ticker import Ticker
from .expression_utils import (
    extract_contracts_multipliers_operators
)


def get_cost(expression : str) -> float:
    cost : float = 0.0

    contracts, multipliers, _ = extract_contracts_multipliers_operators(expression)

    for itr in range(len(contracts)):
        sym = contracts[itr][:-3]
        sym_ticker = Ticker.SYMBOLS[sym]
        cost += multipliers[itr] * (sym_ticker.commission_cost)

    return cost
        
    
def get_slippage(expression: str) -> float:
    slippage : float = 0.0

    contracts, multipliers, _ = extract_contracts_multipliers_operators(expression)

    for itr in range(len(contracts)):
        sym = contracts[itr][:-3]
        sym_ticker = Ticker.SYMBOLS[sym]
        slippage += multipliers[itr] * (
            sym_ticker.cost_in_ticks * 
            sym_ticker.min_price_fluctuation *
            sym_ticker.currency_multiplier
        )

    return slippage
        