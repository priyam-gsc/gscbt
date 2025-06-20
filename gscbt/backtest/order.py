from enum import Enum, auto

import pandas as pd

class OrderSide(Enum):
    buy = auto()
    sell = auto()
    
class Order:
    def __init__(
        self,
        timestamp : pd.Timestamp,
        side : OrderSide,
        lot : int,
    ):
        self.timestamp = timestamp
        self.side = side
        self.lot = lot

    def __str__(self):
        return f"{self.timestamp=} || {self.side=} || {self.lot=}"

class MarketOrder(Order):
    def __init__(self, timestamp, side, lot):
        super().__init__(timestamp, side, lot)

class LimitOrder(Order):
    def __init__(self, timestamp, side, lot, price):
        super().__init__(timestamp, side, lot)
        self.price = price


class PositionAwareOrder:
    def __init__(
        self,
        timestamp : pd.Timestamp,
        position : int,
    ):
        self.timestamp = timestamp
        self.position = position

class PositionAwareMarketOrder(PositionAwareOrder):
    def __init__(self, timestamp, position):
        super().__init__(timestamp, position)
