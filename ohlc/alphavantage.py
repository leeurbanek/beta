""""""
import logging, logging.config
import sqlite3
import time

from datetime import datetime as dt
from pathlib import Path

import pandas as pd
import requests


DEBUG = True

logging.config.fileConfig(fname="src/logger.ini")
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# data_line = ['clop',]
data_line = ['clop', 'clv', 'cwap', 'hilo', 'volume']
# ticker_list = ['IWM', 'EEM']
# ticker_list = ['EURL', 'GDXU', 'TNA', 'YINN']
ticker_list = ['AFK', 'ASEA', 'BOTZ', 'DBC', 'ECNS', 'EEM', 'EURL', 'EZU', 'FXI', 'GDXU', 'HYG', 'ICLN', 'ILF', 'IWM', 'IYR', 'IYZ']
# ticker_list =  'JETS', 'LIT', 'LQD', 'SGOL', 'SIVR', 'SPY', 'TAN', 'TIP', 'TNA', 'VEGI', 'XAR', 'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'YINN']


def fetch_and_parse_price_data(data_line: list, provider: str, ticker_list: list):
    """"""
    if DEBUG: logger.debug(f"fetch_and_parse_price_data(data_line={data_line}, provider={provider}, ticker_list={ticker_list})")
    if not DEBUG: print(" Begin download process.")

    for index, ticker in enumerate(ticker_list):
        data_gen = eval(f"_{provider}_data_generator(index=index, ticker=ticker)")
        tuple_list = eval(f"_process_{provider}_data(data_gen=data_gen, data_line=data_line)")
        del tuple_list

    if not DEBUG: print(" Finished.")


def _alphavantage_data_generator(index: int, ticker: str)->object:
    """"""
    if DEBUG: logger.debug(f"_fetch_alphavantage_data(index={index}, ticker={ticker})")

    API_KEY = ""
    API_KEY_1 = ""
    API_URL = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        # function = "TIME_SERIES_WEEKLY"
        "symbol": ticker,
        "outputsize": "compact",
        # output_size = "full"
        "datatype": "json",
        "apikey": API_KEY,
        }

    try:
        # params['apikey'] = API_KEY if index < 24 else API_KEY_1  # site throttles at 25 downloads
        r = requests.get(API_URL, params=params)
    except Exception as e:
        logger.debug(f"*** ERROR *** {e}")
    else:
        if not DEBUG: print(f" downloading '{ticker}'")
        yield r.json()
    # finally:  # pause after five downloads
    #     if not((index + 1) % 5):
    #         for i in range(63, 0, -1):
    #             print(f" pausing for {i} ", end='\r', flush=True)
    #             time.sleep(1)


def _process_alphavantage_data(data_gen: object, data_line: list)->list[tuple]:
    """"""
    if DEBUG: logger.debug(f"_process_alphavantage_data(data_gen={type(data_gen)}, data_line={type(data_line)})")

    alphavantage_dict = next(data_gen)
    ticker = alphavantage_dict['Meta Data']['2. Symbol']
    time_series_dict =  alphavantage_dict['Time Series (Daily)']

    if DEBUG: logger.debug(f"ticker: {ticker}, type(time_series_dict): {type(time_series_dict)}\n{time_series_dict}")

    # create empty dataframe with index as a timestamp
    df = pd.DataFrame(
        index=[round(time.mktime(dt.strptime(d[:10], '%Y-%m-%d').timetuple())) for d in time_series_dict]
    )
    df.index.name = 'date'

    # add the data lines to dataframe
    def add_clop_series(loc: int)->None:
        """difference between the close and open price"""
        if DEBUG: logger.debug(f"add_clop_series(loc={loc})")
        series_list = list()
        for i in time_series_dict:
            open_ = float(time_series_dict[i]['1. open'])
            close = float(time_series_dict[i]['4. close'])
            series_list.append(round((close - open_) * 100))
        df.insert(loc=loc, column='clop', value=series_list, allow_duplicates=True)

    def add_clv_series(loc: int)->None:
        """close location value, relative to the high-low range"""
        if DEBUG: logger.debug(f"add_clv_series(loc={loc})")
        series_list = list()
        for i in time_series_dict:
            high = float(time_series_dict[i]['2. high'])
            low = float(time_series_dict[i]['3. low'])
            close = float(time_series_dict[i]['4. close'])
            try:
                series_list.append(round(((2 * close - low - high) / (high - low)) * 100))
            except ZeroDivisionError as e:
                logger.debug(f"*** ERROR *** {e}")
        df.insert(loc=loc, column='clv', value=series_list, allow_duplicates=True)

    def add_cwap_series(loc: int)->None:
        """close weighted average price excluding open price"""
        if DEBUG: logger.debug(f"add_cwap_series(loc={loc})")
        series_list = list()
        for i in time_series_dict:
            high = float(time_series_dict[i]['2. high'])
            low = float(time_series_dict[i]['3. low'])
            close = float(time_series_dict[i]['4. close'])
            series_list.append(round(((high + low + close * 2) / 4) * 100))
        df.insert(loc=loc, column='cwap', value=series_list, allow_duplicates=True)

    def add_hilo_series(loc: int)->None:
        """difference between the high and low price"""
        if DEBUG: logger.debug(f"add_hilo_series(loc={loc})")
        series_list = list()
        for i in time_series_dict:
            high = float(time_series_dict[i]['2. high'])
            low = float(time_series_dict[i]['3. low'])
            series_list.append(round((high - low) * 100))
        df.insert(loc=loc, column='hilo', value=series_list, allow_duplicates=True)

    def add_volume_series(loc: int)->None:
        """number of shares traded"""
        if DEBUG: logger.debug(f"add_volume_series(loc={loc})")
        volume = [
            int(time_series_dict[i]['5. volume']) for i in time_series_dict
        ]
        df.insert(loc=loc, column='volume', value=volume, allow_duplicates=True)

    # insert values for each data line into df
    for i, item in enumerate(data_line):
        eval(f"add_{item}_series({i})")

    return ticker, df

    # from itertools import repeat
    # clop, clv, cwap, hilo, mass, volume = map(lambda x: list(x), repeat(list(), 6))

    # for i in time_series_dict:
    #     # get the 'ohlc' data
    #     open_ = float(time_series_dict[i]["1. open"])
    #     high = float(time_series_dict[i]["2. high"])
    #     low = float(time_series_dict[i]["3. low"])
    #     close = float(time_series_dict[i]["4. close"])
    #     volume_ = int(time_series_dict[i]["5. volume"])

    #     # append each data line
    #     clop.append(round((close - open_) * 100))
    #     try:
    #         clv.append(round(((2 * close - low - high) / (high - low)) * 100))
    #     except ZeroDivisionError as e:
    #         logger.debug(f"*** ERROR *** {e}")
    #     cwap.append(round(((high + low + close * 2) / 4) * 100))
    #     hilo.append(round((high - low) * 100))
    #     mass.append()
    #     volume.append(volume_)

# cwap = np.array(
#     list((2 * yf_df["Close"] + yf_df["High"] + yf_df["Low"]) / 4)
# ).reshape(-1, 1)
# cwap = np.rint((self.scaler.fit_transform(cwap).flatten() + 10) * 100).astype(int)
# volume = np.array(list(yf_df["Volume"])).reshape(-1, 1)
# volume = np.rint((self.scaler.fit_transform(volume).flatten() + 10) * 100).astype(int)
# mass = np.array(cwap * volume).reshape(-1, 1)
# mass = np.rint((self.scaler.fit_transform(mass).flatten() + 10) * 100).astype(int)


def main():
    fetch_and_parse_price_data(data_line=data_line, provider='alphavantage', ticker_list=ticker_list)


if __name__ == '__main__':
    if DEBUG: logger.debug(f"******* START - beta_alphavantage() *******")
    main()
