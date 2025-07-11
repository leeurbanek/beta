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
        self.frequency = ctx["data_service"]["data_frequency"]
        self.index = None
        self.lookback = int(ctx["data_service"]["data_lookback"])
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

    def download_and_parse_price_data(self, ticker: str) -> tuple:
        """Returns a tuple, (ticker, dataframe)"""
        if DEBUG:
            logger.debug(f"download_and_parse_price_data(self={self}, ticker={ticker})")

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
            f"data_line={self.data_line}, "
            f"data_provider={self.data_provider}, "
            f"interval={self.interval}, "
            f"scaler={self.scaler}, "
            f"start_date={self.start_date}, "
            f"end_date={self.end_date})"
        )

    @property
    def _parse_frequency(self):
        """Convert daily/weekly frequency to provider format"""
        frequency_dict = {"daily": "1d", "weekly": "1w"}
        return frequency_dict[self.frequency]

    def _yfinance_data_generator(self, ticker: str) -> object:
        """Yields a generator object tuple (ticker, dataframe)"""
        if DEBUG:
            logger.debug(f"_yfinance_data_generator(ticker={ticker})")

        try:
            yf_data = self.yf.Ticker(ticker=ticker)
            yf_df = yf_data.history(start=self.start_date, end=self.end_date, interval=self.interval)
        except Exception as e:
            logger.debug(f"*** ERROR *** {e}")
        else:
            yield ticker, yf_df

    def _process_yfinance_data(self, data_gen: object) -> pd.DataFrame:
        """Returns a tuple (ticker, dataframe)"""
        if DEBUG:
            logger.debug(f"_process_yfinance_data(data_gen={type(data_gen)})")

        ticker, yf_df = next(data_gen)
        yf_df = yf_df.drop(columns=yf_df.columns.values[-3:], axis=1)

        # create empty dataframe with index as a timestamp
        df = pd.DataFrame(index=yf_df.index.values.astype(int) // 10**9)
        df.index.name = "date"

        # insert ticker symbol column
        df.insert(loc=0, column="ticker", value=ticker, allow_duplicates=True)

        # difference between the close and open price
        clop = list(round((yf_df["Close"] - yf_df["Open"]) * 100).astype(int))
        if DEBUG: logger.debug(f"clop: {clop} {type(clop)}")

        # close location value, relative to the high-low range
        try:
            clv = list(
                round((2 * yf_df["Close"] - yf_df["Low"] - yf_df["High"]) / (yf_df["High"] - yf_df["Low"]) * 100)
            )
            if DEBUG: logger.debug(f"clv: {clv} {type(clv)}")
        except ZeroDivisionError as e:
            logger.debug(f"*** ERROR *** {e}")

        # close weighted average price exclude open price
        cwap = np.array(
            list((2 * yf_df["Close"] + yf_df["High"] + yf_df["Low"]) / 4)
        ).reshape(-1, 1)
        cwap = np.rint((self.scaler.fit_transform(cwap).flatten() + 10) * 100).astype(int)
        if DEBUG: logger.debug(f"scaled_cwap: {cwap} {type(cwap)}")

        # difference between the high and low price
        hilo = list(round((yf_df["High"] - yf_df["Low"]) * 100).astype(int))
        if DEBUG: logger.debug(f"hilo: {hilo} {type(hilo)}")

        # number of shares traded
        volume = np.array(list(yf_df["Volume"])).reshape(-1, 1)
        volume = np.rint((self.scaler.fit_transform(volume).flatten() + 10) * 100).astype(int)
        if DEBUG: logger.debug(f"scaled_volume: {volume} {type(volume)}")

        # price times number of shares traded
        mass = np.array(cwap * volume).reshape(-1, 1)
        mass = np.rint((self.scaler.fit_transform(mass).flatten() + 10) * 100).astype(int)
        if DEBUG: logger.debug(f"scaled_mass: {mass} {type(mass)}")

        # insert values for each data line into df
        for i, item in enumerate(self.data_line):
            df.insert(loc=i+1, column=f"{item.lower()}", value=eval(item.lower()), allow_duplicates=True)

        return ticker, df


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")

    # create database
    utils.create_sqlite_indicator_database(ctx=ctx)

    # select data processor
    processor = YahooFinanceDataProcessor(ctx=ctx)

    # get and save data for each ticker
    for index, ticker in enumerate(ctx['interface']['arguments']):
        ctx['interface']['index'] = index  # for alphavantage, may throttle at five downloads
        data_tuple = processor.download_and_parse_price_data(ticker=ticker)
        if DEBUG: logger.debug(f"data_tuple {data_tuple[0]}\n{data_tuple[1]}")

        utils.write_indicator_data_to_sqlite_db(ctx=ctx, data_tuple=data_tuple)


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - yfinance.py.main() *******")

    ctx = {
        'default': {
            'debug': True,
            'work_dir': '/home/la/dev/beta/ohlc',
            'cfg_chart': '/home/la/dev/stomartat/src/pkg/chart_srv/cfg_chart.ini',
            'cfg_data': '/home/la/dev/stomartat/src/pkg/data_srv/cfg_data.ini',
            'cfg_main': '/home/la/dev/stomartat/src/config.ini'
        },
        'interface': {
            'command': 'data',
            'target_data': ['SPXL', 'YINN'],
            'arguments': ['ECNS', 'FXI', 'HYG', 'XLF', 'XLY'],
            'database': 'default.db',
            'data_line': ['CWAP','MASS']
            # 'data_line': ['CLOP', 'CLV', 'CWAP', 'HILO', 'MASS', 'VOLUME']
        },
        'data_service': {
            'data_frequency': 'daily',
            'data_line': 'CWAP MASS',
            # 'data_line': 'CLOP CLV CWAP HILO MASS VOLUME',
            'data_list': 'ECNS FXI HYG XLF XLY',
            'data_lookback': '42',
            # 'data_lookback': '1825',
            'data_provider': 'yfinance',
            'sklearn_scaler': 'MinMaxScaler',
            # 'sklearn_scaler': 'RobustScaler',
            'target_data': 'SPXL YINN',
            'url_alphavantage': 'https://www.alphavantage.co/query',
            'url_tiingo': '',
            'url_yfinance': ''
        }
    }

    main(ctx=ctx)
