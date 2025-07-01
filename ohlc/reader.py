import datetime
import logging
import os
import requests
from collections import namedtuple

from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)


class TiingoReader():
    """Retrieve stock market OHLC data from https://www.tiingo.com/.
    ------------------------------------------------------------
    Instance uses public method `get_data_tuple(symbol)` to return a list.\n
    Parameters
    ----------
    `ctx` : dictionary
        python-click command line interface context dictionary\n
    Returns
    -------
    python list of named tuples\n
    """
    def __init__(self, ctx) -> None:
        self.back_days = ctx.obj['data_service']['back_days']
        self.debug = ctx.obj['default']['debug'] == 'True'
        self.freq = ctx.obj['data_service']['frequency']
        self.key = os.getenv('TIINGO_KEY')
        self.start = self._default_start_date
        self.url = self._url

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"back_days={self.back_days}, "
            f"debug={self.debug}, "
            f"freq={self.freq}, "
            f"key={self.key}, "
            f"start={self.start}, "
            f"url={self._url})"
            )

    @property
    def _default_start_date(self):
        """"""
        lookback = int(self.back_days)
        return datetime.date.today() - datetime.timedelta(days=lookback)

    @property
    def _url(self):
        """API url"""
        return "https://api.tiingo.com/tiingo"

    def get_data_tuple(self, symbol: str) -> tuple:
        """TiingoReader.get_data_tuple()
        -----------------------------
        Public method of TiingoReader class. Takes a ticker symbol string.\n
        Returns
        -------
        python namedtuple('symbol', 'date', 'open', 'high', 'low', 'close', 'volume')\n
        """
        if self.debug: logger.debug(f"get_data_tuple(self={self}, symbol={symbol})")
        return self._parse_price_data(symbol)

    def _parse_price_data(self, symbol: list) -> list:
        """"""
        OHLC = namedtuple('OHLC', ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])
        ohlc_list = []

        for item in self._read_one_price_data(symbol):
            data = OHLC(
                symbol.upper(),
                # datetime.date.fromisoformat(item.get('date')[:10]),
                datetime.date.fromisoformat(item.get('date')[:10]).toordinal(),
                round(item.get('adjOpen')*100),
                round(item.get('adjHigh')*100),
                round(item.get('adjLow')*100),
                round(item.get('adjClose')*100),
                item.get('adjVolume')
            )
            ohlc_list.append(data)
        if self.debug: logger.debug(f"_parse_price_data(self, symbol={symbol}) -> namedtuple list:\n{ohlc_list}")
        return ohlc_list

    def _read_one_price_data(self, symbol):
        """"""
        headers = {
            'Content-Type': 'application/json'
        }
        request_response = requests.get(
            f"{self.url}/{self.freq}/"
            f"{symbol}/prices?"
            f"startDate={self.start}"
            f"&token={self.key}",
            headers=headers
            )
        if self.debug: logger.debug(f"_read_one_price_data(self, symbol={symbol}) -> request response:\n{request_response.json()}")
        return request_response.json()
