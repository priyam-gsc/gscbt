import pytest

from gscbt.ticker import Ticker

### Ticker

@pytest.mark.parametrize("ticker, symbol, iqfeed_sym, currency_multiplier", [
    (Ticker.TICKERS.cme.cl.f, Ticker.SYMBOLS["CL"], "QCL", 1000),
])
def test_Ticker(ticker, symbol, iqfeed_sym, currency_multiplier):
    assert ticker.symbol == symbol.symbol
    assert ticker.iqfeed_symbol == symbol.iqfeed_symbol
    assert ticker.currency_multiplier == symbol.currency_multiplier

    assert ticker.iqfeed_symbol == iqfeed_sym
    assert ticker.currency_multiplier == currency_multiplier