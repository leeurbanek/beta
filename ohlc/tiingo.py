""""""
import datetime
import logging, logging.config

import requests

from data import config_dict, ohlc


DEBUG = True
ctx = config_dict
username = "kernelbeau"
password = "kerne1be@]tiingo"
token = "75a0aa12dd1e297d613cfe92ecfe96dab95a4589"

logging.config.fileConfig(fname="../logger.ini")
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class TiingoReader:
    """"""
    def __init__(self, ctx):
        self.ctx = ctx
        self.lookback = ctx['data_service']['data_lookback']
        # self.data_list = ctx['data_service']['data_list']
        self.data_list = ['IWM']
        self.frequency = ctx['data_service']['data_frequency']
        # self.api_token = os.getenv('API_TOKEN')
        self.api_token = token
        self.start = self._default_start_date
        self.url = self._url

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"api_token={self.api_token}, "
            f"lookback={self.lookback}, "
            f"data_list={self.data_list}, "
            f"frequency={self.frequency}, "
            # f"key={self.key}, "
            f"start={self.start}, "
            f"url={self._url})"
            )

    @property
    def _default_start_date(self):
        """"""
        lookback = int(self.lookback)
        return datetime.date.today() - datetime.timedelta(days=lookback)

    @property
    def _url(self):
        """API url"""
        data_provider = self.ctx['data_service']['data_provider']
        return self.ctx['data_service'][f"url_{data_provider}"]

    def parse_price_data(self):
        """"""
        if DEBUG: logger.debug(f"parse_price_data()")
        data = self._data_generator()
        if DEBUG: logger.debug(data)
        data_list = next(data)
        print(f"symbol: {data_list[0]}")
        for item in data_list[1]:
            print(item.get('date')[0:10], item.get('adjClose'))


    def _data_generator(self):
        """"""
        for symbol in self.data_list:
            yield symbol, ohlc


def client_get_ohlc_price_data(ctx):
    """"""
    if DEBUG: logger.debug(f"client_get_ohlc_price_data()")

    # select data provider
    if ctx['data_service']['provider'] == "tiingo":
        data_generator = use_tiingo_reader(ctx=ctx)
        if DEBUG: logger.debug(f"TiingoReader._data_generator -> {data_generator}")


def use_tiingo_reader(ctx):
    """"""
    if DEBUG: logger.debug(f"use_tiingo_reader()")

    reader = TiingoReader(ctx)
    if DEBUG: logger.debug(f"{reader}")

    reader.parse_price_data()

    # return (  # generator object of ohlc price data
    #     reader.get_data_tuple(symbol)
    #     for symbol in ctx['data_service']['symbol']
    # )


def main(ctx):
    if DEBUG: logger.debug(f"main(ctx={ctx})")

    client_get_ohlc_price_data(ctx=ctx)

    # headers = {"Content-Type": "application/json"}
    # url = "https://api.tiingo.com/tiingo"
    # frequency = "daily"
    # symbol = "IWM"
    # start = "2025-04-03"
    # token = "75a0aa12dd1e297d613cfe92ecfe96dab95a4589"
    # request_response = requests.get(
    #     f"{url}/{frequency}/{symbol}/prices?startDate={start}&token={token}",
    #     headers=headers
    # )
    # return request_response.json()


if __name__ == "__main__":
    # print(f"\nohlc = {ohlc}\n")
    main(ctx=ctx)
    # ohlc = main(ctx=ctx)
    # print(f"\nohlc: {ohlc}\ntype: {type(ohlc)}")
