from .contract_spec import (
    DataType,
    ValuationType,
    RollMethod,
    RollParams,
    ContractSpec,
)


DATATYPEDICT = {
    "cont" : DataType.CONTINUOUS,
    "cont_adj" : DataType.CONTINUOUSADJUSTABLE,
    "back" : DataType.BACKADJUSTED,
    "forward" : DataType.FORWARDADJUSTED,
}

VALUATIONTYPEDICT = {
    "de" : ValuationType.DOLLAR_EQUIVALENT,
    "nde" : ValuationType.NON_DOLLAR_EQUIVALENT,
}

ROLLMETHODDICT = {
    "offset" : RollMethod.OFFSET,
}