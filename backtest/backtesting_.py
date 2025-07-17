""""""
import logging, logging.config

import numpy as np
import pandas as pd

from backtesting import Backtest, Strategy
from backtesting.lib import crossover

from backtesting.test import SMA, GOOG


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logger = logging.getLogger(__name__)

ctx = {
    "signal_df": pd.read_pickle("../data/signal_df.pkl"),
    "stonk_df": pd.read_pickle("../data/stonk_df.pkl"),
}

GOOG["Signal"] = np.random.randint(-1, 2, len(GOOG))


class SignalStrategy(Strategy):
    """"""
    def init(self):
        pass

    def next(self):
        current_signal = self.data.Signal[-1]
        if current_signal == 1:
            if not self.position:
                self.buy()
        elif current_signal == -1:
            if self.position:
                self.position.close()


def main(ctx: dict) -> None:
    if DEBUG: logger.debug(f"main(ctx={type(ctx)})")
    # if DEBUG: logger.debug(f"GOOG:\n{GOOG} {type(GOOG)}")

    bt = Backtest(GOOG, SignalStrategy, cash=10_000, commission=.002, exclusive_orders=True)
    stats = bt.run()
    if DEBUG: logger.debug(f"stats:\n{stats}")
    bt.plot()


if __name__ == "__main__":
    if DEBUG: logger.debug(f"******* START - backtest/backtesting_.py.main() *******")

    main(ctx=ctx)
