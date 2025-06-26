from enum import Enum, auto
from dataclasses import dataclass

class DataType(Enum):
    CONTINUOUS = auto()
    CONTINUOUSADJUSTABLE = auto()
    BACKADJUSTED = auto()
    FORWARDADJUSTED = auto()

class ValuationType(Enum):
    DOLLAR_EQUIVALENT = auto()
    NON_DOLLAR_EQUIVALENT = auto()

class RollMethod(Enum):
    OFFSET = auto()         

@dataclass
class RollParams:
    offset: int | None = None
    max_lookahead : int | None = None 

class ContractSpec:
    def __init__(
        self,
        data_type: DataType,
        valuation_type: ValuationType,
        roll_method: RollMethod | None = None,
        roll_params: RollParams | None = None,
    ):
        self.data_type = data_type
        self.valuation_type = valuation_type
        self.roll_method = roll_method
        self.roll_params = roll_params

        self.validate()

    def validate(self):
        if self.data_type == DataType.BACKADJUSTED:
            if not self.roll_method:
                raise ValueError("Backadjusted data requires a roll method.")
            if self.roll_method == RollMethod.OFFSET:
                if (
                    not self.roll_params or 
                    self.roll_params.offset is None or
                    self.roll_params.max_lookahead is None
                ):
                    raise ValueError("Offset roll requires 'offset' \
                        and 'max_lookahead'.")

if __name__ == "__main__":
    pass