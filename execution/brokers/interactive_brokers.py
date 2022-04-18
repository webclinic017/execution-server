from ib_insync import *
from datetime import datetime
import numpy as np

from common.data.constants import SYMBOL_TO_RIC
from common.execution.optimal_limit_order.pricer import get_optimal_quote

PORT_PAPER = 7497
PORT_LIVE = 7496


class InteractiveBrokers():

    def __init__(self):
        self.ib = IB()
        self.ib.connect('127.0.0.1', PORT_LIVE, clientId=1)

    def submit_market_order(self, ticker=None, size=0):
        contract = self._get_contact(ticker)
        action = self._get_action(size)
        abs_quantity = np.abs(size)
        market_order = MarketOrder(action=action,
                                   totalQuantity=abs_quantity)
        market_trade = self.ib.placeOrder(contract, market_order)
        self.ib.sleep(1)
        assert market_trade.orderStatus.status == 'Submitted'
        while not market_trade.isDone():
            self.ib.waitOnUpdate()
        print(self.ib.positions())

    def submit_limit_order(self, ticker=None, size=0, time_in_seconds=300):
        contract = self._get_contact(ticker)
        action = self._get_action(size)
        abs_quantity = np.abs(size)
        limit_order = None
        limit_trade = None
        for t in np.linspace(300, 0, 6):
            bid, ask = self._get_bid_ask(contract)
            if limit_trade is not None:
                if abs_quantity == limit_trade.orderStatus.filled:
                    break
                abs_quantity -= limit_trade.orderStatus.filled
            ric = SYMBOL_TO_RIC[ticker]
            delta_quote = get_optimal_quote(
                ric=ric, quantity=abs_quantity, time_in_seconds=int(t))
            limit_price = bid - delta_quote if action == 'BUY' else ask + delta_quote
            if limit_order is None:
                limit_order = LimitOrder(action, abs_quantity, limit_price)
                limit_trade = self.ib.placeOrder(contract, limit_order)
            else:
                limit_order.lmtPrice = limit_price
            limit_trade = self.ib.placeOrder(contract, limit_order)
            self.ib.sleep(1)
            assert limit_trade.orderStatus.status == 'Submitted'
            assert limit_trade in ib.openTrades()
        while not limit_trade.isDone():
            self.ib.waitOnUpdate()
        print(self.ib.positions())

    def _get_contact(self, ticker):
        contract = Stock(ticker, 'SMART', 'USD')
        self.ib.qualifyContracts(contract)
        return contract

    def _get_action(self, size):
        return 'BUY' if size > 0 else 'SELL'

    def _get_bid_ask(self, contract):
        start = ''
        end = datetime.now()
        number_of_ticks = 10
        ticks = self.ib.reqHistoricalTicks(
            contract, start, end, number_of_ticks, 'BID_ASK', useRth=False)
        return ticks[-1].priceBid, ticks[-1].priceAsk

    def __del__(self):
        self.ib.disconnect()


if __name__ == '__main__':
    ib = InteractiveBrokers()
    ib.submit_market_order('SPY', 1)
    del ib
