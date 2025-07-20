""""""
import datetime
import logging, logging.config

import numpy as np
import pandas as pd

from dotenv import load_dotenv

import utils


DEBUG = True


load_dotenv()

logging.config.fileConfig(fname="../logger.ini")
logging.getLogger("peewee").setLevel(logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class BaseProcessor:
    """"""

    def __init__(self, ctx: dict):
        self.data_line = ctx["interface"]["data_line"]
        self.data_provider = ctx["data_service"]["data_provider"]
        self.target = ctx["interface"]["target"]
        self.frequency = ctx["data_service"]["data_frequency"]
        self.index = None
        self.lookback = int(ctx["data_service"]["data_lookback"])
        self.ohlc = ctx["data_service"]["ohlc"]
        self.scaler = self._set_sklearn_scaler(ctx["data_service"]["sklearn_scaler"])
        self.start_date, self.end_date = self._start_end_date

    @property
    def _start_end_date(self):
        """Set the start and end dates"""
        lookback = int(self.lookback)
        start = datetime.date.today() - datetime.timedelta(days=lookback)
        end = datetime.date.today()
        return start, end

    def _set_sklearn_scaler(self, scaler):
        """Uses config file [data_service][sklearn_scaler] value"""
        if scaler == "MinMaxScaler":
            from sklearn.preprocessing import MinMaxScaler
            return MinMaxScaler()
        elif scaler == "RobustScaler":
            from sklearn.preprocessing import RobustScaler
            return RobustScaler(quantile_range=(0.0, 100.0))

    def download_and_parse_data(self, ticker: str) -> tuple:
        """Returns a tuple, (ticker, dataframe)"""
        if DEBUG:
            logger.debug(f"download_and_parse_data(self={self}, ticker={ticker})")

        data_gen = eval(f"self._{self.data_provider}_data_generator(ticker=ticker)")

        return eval(f"self._process_{self.data_provider}_data(data_gen=data_gen)")


class YahooFinanceDataProcessor(BaseProcessor):
    """Fetch ohlc price data using yfinance"""
    import yfinance as yf

    def __init__(self, ctx: dict):
        super().__init__(ctx=ctx)
        self.interval = self._parse_frequency

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"data_provider={self.data_provider}, "
            f"interval={self.interval}, "
            f"scaler={self.scaler}, "
            f"start_date={self.start_date}, "
            f"end_date={self.end_date}, "
            f"target={self.target})"
        )

    @property
    def _parse_frequency(self):
        """Convert daily/weekly frequency to provider format"""
        frequency_dict = {"daily": "1d", "weekly": "1w"}
        return frequency_dict[self.frequency]

    def _yfinance_data_generator(self, ticker: str) -> object:
        """Yields a generator object tuple (ticker, dataframe)"""
        if DEBUG: logger.debug(f"_yfinance_data_generator(ticker={ticker})")

        try:
            yf_data = self.yf.Ticker(ticker=ticker)
            yf_df = yf_data.history(start=self.start_date, end=self.end_date, interval=self.interval)
        except Exception as e:
            logger.debug(f"*** ERROR *** {e}")
        else:
            yield ticker, yf_df

    def _process_yfinance_data(self, data_gen: object) -> pd.DataFrame:
        """Returns a tuple (ticker, dataframe)"""
        if DEBUG: logger.debug(f"_process_yfinance_data(self={self}, data_gen={type(data_gen)})")

        ticker, yf_df = next(data_gen)
        yf_df = yf_df.drop(columns=yf_df.columns.values[-3:], axis=1)

        # create empty dataframe with index as a timestamp
        df = pd.DataFrame(index=yf_df.index.values.astype(int) // 10**9)
        df.index.name = "date"

        # open_ = np.array(round(yf_df["Open"] * 100).astype(int))
        open_ = list(round(yf_df["Open"] * 100).astype(int))
        if DEBUG: logger.debug(f"open_:\n{open_} {type(open_)}")

        # high = np.array(round(yf_df["High"] * 100).astype(int))
        high = list(round(yf_df["High"] * 100).astype(int))
        if DEBUG: logger.debug(f"high:\n{high} {type(high)}")

        # low = np.array(round(yf_df["Low"] * 100).astype(int))
        low = list(round(yf_df["Low"] * 100).astype(int))
        if DEBUG: logger.debug(f"low:\n{low} {type(low)}")

        # close = np.array(round(yf_df["Close"] * 100).astype(int))
        close = list(round(yf_df["Close"] * 100).astype(int))
        if DEBUG: logger.debug(f"close:\n{close} {type(close)}")

        # number of shares traded
        volume = list(yf_df["Volume"])
        if DEBUG: logger.debug(f"volume:\n{volume} {type(volume)}")

        # # difference between the close and open price
        # clop = list(round((yf_df["Close"] - yf_df["Open"]) * 100).astype(int))
        # if DEBUG: logger.debug(f"clop:\n{clop} {type(clop)}")

        # # close location value, relative to the high-low range
        # try:
        #     clv = list(
        #         round((2 * yf_df["Close"] - yf_df["Low"] - yf_df["High"]) / (yf_df["High"] - yf_df["Low"]) * 100).astype(int)
        #     )
        #     if DEBUG: logger.debug(f"clv:\n{clv} {type(clv)}")
        # except ZeroDivisionError as e:
        #     logger.debug(f"*** ERROR *** {e}")

        # close weighted average price exclude open price
        cwap = np.array(
            list((2 * yf_df["Close"] + yf_df["High"] + yf_df["Low"]) / 4)
        ).reshape(-1, 1)
        cwap = np.rint((self.scaler.fit_transform(cwap).flatten() + 10) * 100).astype(int)
        if DEBUG: logger.debug(f"scaled_cwap:\n{cwap} {type(cwap)}")

        # # difference between the high and low price
        # hilo = list(round((yf_df["High"] - yf_df["Low"]) * 100).astype(int))
        # if DEBUG: logger.debug(f"hilo:\n{hilo} {type(hilo)}")

        # # price times number of shares traded
        # vol = np.rint(
        #     (self.scaler.fit_transform(np.array(list(yf_df["Volume"])).reshape(-1, 1)).flatten() + 10) * 100
        # ).astype(int)
        # if DEBUG: logger.debug(f"scaled volume:\n{vol} {type(vol)}")
        # mass = np.array(cwap * vol)
        # if DEBUG: logger.debug(f"scaled mass:\n{mass} {type(mass)}")

        # insert values for each ohlc and data_line into df
        if ticker in ctx["interface"]["target"]:
            for i, item in enumerate(self.ohlc + self.data_line):
                df.insert(loc=i, column=f"{item.lower()}", value=eval(item.lower()), allow_duplicates=True)
        else:
            for i, item in enumerate(self.data_line):
                df.insert(loc=i, column=f"{item.lower()}", value=eval(item.lower()), allow_duplicates=True)

        return ticker, df


def main(ctx: dict) -> None:
    if DEBUG: logger.debug(f"main(ctx={ctx})")

    ctx['interface']['download_list'] = set(ctx['interface']['arguments'] + ctx['interface']['target'])

    # create database
    utils.create_sqlite_stonk_database(ctx=ctx)

    # select data processor
    processor = YahooFinanceDataProcessor(ctx=ctx)

    # get and save data for each stonk
    for index, ticker in enumerate(ctx['interface']['download_list']):
        ctx['interface']['index'] = index  # for alphavantage, may throttle at five downloads
        data_tuple = processor.download_and_parse_data(ticker=ticker)
        if DEBUG: logger.debug(f"data_tuple {data_tuple[0]}:\n{data_tuple[1]}")

        if ticker in ctx["interface"]["target"]:
            utils.write_target_data_to_sqlite_db(ctx=ctx, data_tuple=data_tuple)
        else:
            utils.write_indicator_data_to_sqlite_db(ctx=ctx, data_tuple=data_tuple)


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - yfinance.py.main() *******")

    ctx = {
        'default': {
            'debug': True,
            'work_dir': '/home/la/dev/beta/',
        },
        'interface': {
            'command': 'data',
            'download_list': None,
            # 'target': ['SPXL',],
            'target': ['SPXL', 'SPXS', 'YINN', 'YANG'],
            # 'arguments': ['HYG',],
            'arguments': ['ECNS', 'FXI', 'HYG', 'XLF', 'XLY'],
            'database': 'default.db',
            'data_line': ['CWAP',]
            # 'data_line': ['CLOP', 'CLV', 'CWAP', 'HILO']
        },
        'data_service': {
            'data_frequency': 'daily',
            'data_line': 'CWAP',
            # 'data_line': 'CLOP CLV CWAP HILO',
            'data_list': 'ECNS FXI HYG XLF XLY',
            # 'data_lookback': '21',
            'data_lookback': '2555',
            'data_provider': 'yfinance',
            'ohlc': ['open_', 'high', 'low', 'close', 'volume'],
            # 'sklearn_scaler': 'MinMaxScaler',
            'sklearn_scaler': 'RobustScaler',
            'target': 'SPXL SPXS YINN YANG',
            'url_alphavantage': 'https://www.alphavantage.co/query',
            'url_tiingo': '',
            'url_yfinance': ''
        }
    }

    main(ctx=ctx)
