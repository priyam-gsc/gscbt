from enum import Enum, auto

import pandas as pd
import numpy as np

from .order import (
    OrderSide,
    Order,
    MarketOrder,
    LimitOrder,
    PositionAwareMarketOrder,
)

from .utils import (
    avg_price_calculation,
)


class Spread:
    # pre-req
    #   timeseries - UTC timezone
    #              - Close price
    class LimitOrderExecMode(Enum):
        given_price = auto()
        worst_case = auto()

    def __init__(
        self,
        timeseries : pd.DataFrame,
        common_settlement_time : str,
        trade_cost : float,
        slippage : float,
        limit_order_exec_mode : LimitOrderExecMode = LimitOrderExecMode.worst_case,
    ):
        self.settle_on : pd.Timestamp = pd.Timestamp("1970-01-01 " + common_settlement_time)
        self.trade_cost : float = trade_cost
        self.slippage : float = slippage
        self.limit_order_exec_mode = limit_order_exec_mode
        
        self.pointer : int = 1 # you can't place order on first data point 
        self.pendding_limit_orders : list[LimitOrder] = []
        self.pendding_market_orders : list[MarketOrder] = []
        self.next_settle : pd.Timestamp = None

        # pandas dataframe to numpy 
        self.colToIdxItr : int = 0
        self.colToIdx : dict = {}

        # data inform of numpy
        self.Ntimestamp : np.ndarray = None
        self.Ndata : np.ndarray = None

        for col in timeseries.columns:
            self.colToIdx[col] = self.colToIdxItr
            self.colToIdxItr += 1

        self.Ntimestamp = timeseries.index.to_numpy()
        self.Ndata = timeseries.to_numpy()

        # adding external col to maintain info
        col_list_to_add : list = [
            "exec",
            "pos",
            "pos_price",
            "m2m",
            "m2m_cont",
            "cost",
            "slippage",
            "m2m_cNs_cont" # cNs = cost and slippage added
        ]

        tmp_narr = np.zeros((self.Ndata.shape[0], len(col_list_to_add)), dtype=self.Ndata.dtype)
        self.Ndata = np.hstack((self.Ndata, tmp_narr))

        for col in col_list_to_add:
            self.colToIdx[f"{col}"] = self.colToIdxItr
            self.colToIdxItr += 1


    def place_order(self, order: Order):
        if isinstance(order, MarketOrder):
            self.pendding_market_orders.append(order)
        elif isinstance(order, LimitOrder):
            self.pendding_limit_orders.append(order)
        else:
            raise Exception(f"[-] Invalid order")

    def place_order_position_aware_wrapper(self, order: Order):
        if not isinstance(order, PositionAwareMarketOrder):
            raise Exception(f"only order type allowed is [PositionAwareMarketOrder]")
        
        timestamp = order.timestamp - pd.Timedelta(seconds=1)
        order_lot = order.position
        pos = self.get_pos(timestamp)
        
        final_order_lot = order_lot - pos
        order_side = OrderSide.buy
            
        if final_order_lot == 0:
            return 
        elif final_order_lot < 0:
            order_side = OrderSide.sell
    
        self.place_order(MarketOrder(
            timestamp=order.timestamp,
            side=order_side,
            lot=abs(final_order_lot)
        ))


    def calculate(
        self,
        timestamp : pd.Timestamp,
    ):
        while self.pointer < len(self.Ntimestamp) and self.Ntimestamp[self.pointer] <= timestamp:
            # step 1 : pre value copy
            self.Ndata[self.pointer, self.colToIdx["pos"]] = self.Ndata[self.pointer-1, self.colToIdx["pos"]]
            self.Ndata[self.pointer, self.colToIdx["pos_price"]] = self.Ndata[self.pointer-1, self.colToIdx["pos_price"]]

            # step 2 : exec limit order 
            tmp_pendding_limit_orders = []    
            for pendding_order in self.pendding_limit_orders:
                c1 = self.Ndata[self.pointer - 1, self.colToIdx["close"]]
                c2 = self.Ndata[self.pointer, self.colToIdx["close"]]

                isPriceSwap = False
                if c1 > c2:
                    tmp = c1
                    c1 = c2
                    c2 = tmp
                    isPriceSwap = True

                if(
                    pendding_order.timestamp > self.Ntimestamp[self.pointer]  # pointer is in past timestamp
                    or (pendding_order.side == OrderSide.buy and pendding_order.price < c1) # buy and wrong range
                    or (pendding_order.side == OrderSide.sell and pendding_order.price > c2) # sell and wrong range
                ):
                    tmp_pendding_limit_orders.append(pendding_order)
                    continue
            
                exec_price_flag = None
                if pendding_order.side == OrderSide.buy:
                    exec_price_flag = "c2" if not isPriceSwap else "c1"
                elif pendding_order.side == OrderSide.sell:
                    exec_price_flag = "c1" if not isPriceSwap else "c2"

                order_lot : int = pendding_order.lot
                if pendding_order.side == OrderSide.sell:
                    order_lot = -order_lot
                
                curr_price = None
                if self.limit_order_exec_mode == self.LimitOrderExecMode.given_price:
                    curr_price = pendding_order.price
                elif self.limit_order_exec_mode == self.LimitOrderExecMode.worst_case:
                    if exec_price_flag == "c1":                    
                        curr_price = self.Ndata[self.pointer-1, self.colToIdx["close"]]
                    elif exec_price_flag == "c2":
                        curr_price = self.Ndata[self.pointer, self.colToIdx["close"]]
                else:
                    pass

                prev_pos : int = self.Ndata[self.pointer, self.colToIdx["pos"]]
                prev_price : float = self.Ndata[self.pointer, self.colToIdx["pos_price"]]

                avg_price, isSomePosSquareOff = avg_price_calculation(
                    prev_price = prev_price,
                    prev_pos = prev_pos,
                    curr_price = curr_price,
                    curr_pos = order_lot,
                )

                if isSomePosSquareOff:
                    sqr_pos_sign = 1 if order_lot >= 0 else -1 
                    sqr_pos = sqr_pos_sign * min(abs(prev_pos), abs(order_lot))

                    sqr_pnl = curr_price * sqr_pos + prev_price * (-sqr_pos)
                    self.Ndata[self.pointer, self.colToIdx["m2m"]] -= sqr_pnl
                    self.Ndata[self.pointer, self.colToIdx["m2m_cont"]] -= sqr_pnl
                    self.Ndata[self.pointer, self.colToIdx["m2m_cNs_cont"]] -= sqr_pnl

                self.Ndata[self.pointer, self.colToIdx["pos_price"]] = avg_price
                self.Ndata[self.pointer, self.colToIdx["pos"]] += order_lot
                self.Ndata[self.pointer, self.colToIdx["exec"]] += order_lot
                self.Ndata[self.pointer, self.colToIdx["cost"]] += self.trade_cost * abs(order_lot)
                self.Ndata[self.pointer, self.colToIdx["slippage"]] += self.slippage * abs(order_lot)
                self.Ndata[self.pointer, self.colToIdx["m2m_cNs_cont"]] -= (
                    self.trade_cost * abs(order_lot) +
                    self.slippage * abs(order_lot)
                )

            self.pendding_limit_orders = tmp_pendding_limit_orders


            # step 3 : exec market order 
            tmp_pendding_market_orders = []
            for pendding_order in self.pendding_market_orders:
                
                if pendding_order.timestamp > self.Ntimestamp[self.pointer]:
                    tmp_pendding_market_orders.append(pendding_order)
                    continue

                order_lot = pendding_order.lot
                if pendding_order.side == OrderSide.sell:
                    order_lot = -order_lot
                prev_pos : float = self.Ndata[self.pointer, self.colToIdx["pos"]]
                prev_price : float = self.Ndata[self.pointer, self.colToIdx["pos_price"]]
                curr_price : float = self.Ndata[self.pointer, self.colToIdx["close"]]

                avg_price, isSomePosSquareOff = avg_price_calculation(
                    prev_price = prev_price,
                    prev_pos = prev_pos,
                    curr_price = curr_price,
                    curr_pos = order_lot,
                )

                if isSomePosSquareOff:
                    sqr_pos_sign = 1 if order_lot >= 0 else -1 
                    sqr_pos = sqr_pos_sign * min(abs(prev_pos), abs(order_lot))

                    sqr_pnl = curr_price * sqr_pos + prev_price * (-sqr_pos)
                    self.Ndata[self.pointer, self.colToIdx["m2m"]] -= sqr_pnl
                    self.Ndata[self.pointer, self.colToIdx["m2m_cont"]] -= sqr_pnl
                    self.Ndata[self.pointer, self.colToIdx["m2m_cNs_cont"]] -= sqr_pnl

                self.Ndata[self.pointer, self.colToIdx["pos_price"]] = avg_price
                self.Ndata[self.pointer, self.colToIdx["pos"]] += order_lot
                self.Ndata[self.pointer, self.colToIdx["exec"]] += order_lot
                self.Ndata[self.pointer, self.colToIdx["cost"]] += self.trade_cost * abs(order_lot)
                self.Ndata[self.pointer, self.colToIdx["slippage"]] += self.slippage * abs(order_lot)
                self.Ndata[self.pointer, self.colToIdx["m2m_cNs_cont"]] -= (
                    self.trade_cost * abs(order_lot) +
                    self.slippage * abs(order_lot)
                )


            self.pendding_market_orders = tmp_pendding_market_orders

            # step 4 : settle
            if self.next_settle == None:
                self.next_settle = pd.Timestamp.combine(self.Ntimestamp[self.pointer].date(), self.settle_on.time())
                self.next_settle = self.next_settle.tz_localize(self.Ntimestamp[self.pointer].tz)

            # if there is some active position than only we required to 
            # settle the price 
            if self.Ndata[self.pointer, self.colToIdx["pos"]] != 0.0:
                if self.Ntimestamp[self.pointer] >= self.next_settle:
                    tmp_ts = self.Ntimestamp[self.pointer] + pd.Timedelta(days=1)
                    self.next_settle = pd.Timestamp.combine(tmp_ts.date(), self.settle_on.time())
                    self.next_settle = self.next_settle.tz_localize(self.Ntimestamp[self.pointer].tz)

                    settle_price = self.Ndata[self.pointer, self.colToIdx["close"]]
                    pos = self.Ndata[self.pointer, self.colToIdx["pos"]]
                    pos_price = self.Ndata[self.pointer, self.colToIdx["pos_price"]]

                    tmp_pnl =  settle_price * pos + pos_price * (-pos)

                    self.Ndata[self.pointer, self.colToIdx["pos_price"]] = settle_price
                    self.Ndata[self.pointer, self.colToIdx["m2m"]] += tmp_pnl
                    self.Ndata[self.pointer, self.colToIdx["m2m_cont"]] += tmp_pnl
                    self.Ndata[self.pointer, self.colToIdx["m2m_cNs_cont"]] += tmp_pnl

                elif self.pointer > 1:
                    self.Ndata[self.pointer, self.colToIdx["m2m_cont"]] += self.Ndata[self.pointer-1, self.colToIdx["m2m_cont"]]
                    self.Ndata[self.pointer, self.colToIdx["m2m_cNs_cont"]] += self.Ndata[self.pointer-1, self.colToIdx["m2m_cNs_cont"]]

            # step 5 : increment itr
            self.pointer += 1


    def get_m2m(
        self,
        timestamp : pd.Timestamp,
    ): 
        self.calculate(timestamp)
        return self.Ndata[self.pointer-1, self.colToIdx["m2m_cont"]]
        # ASSUMPTION
        # to get row with given timestamp 
        # data source and order placing data should be same

    def get_m2m_cNs(
        self,
        timestamp : pd.Timestamp,
    ):
        self.calculate(timestamp)
        return self.Ndata[self.pointer-1, self.colToIdx["m2m_cNs_cont"]]

    def get_pos(
        self,
        timestamp : pd.Timestamp,
    ):
        self.calculate(timestamp)
        return self.Ndata[self.pointer-1, self.colToIdx["pos"]]

    def complete(self):
        self.calculate(self.Ntimestamp[-1])

    def get_pd_data(self):
        df_timestamp = pd.DataFrame(self.Ntimestamp, columns=["timestamp"])
        df_data = pd.DataFrame(self.Ndata, columns=list(self.colToIdx.keys()))

        df = pd.concat([df_timestamp, df_data], axis=1)

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index(["timestamp"], inplace=True)

        return df
