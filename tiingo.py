""""""
import logging, logging.config
import sqlite3
import time

from datetime import datetime as dt
from pathlib import Path

import pandas as pd

from tiingo import TiingoClient


DEBUG = True
DEBUG = False

logging.config.fileConfig(fname="src/logger.ini")
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()

# self.api_key = os.getenv(f"API_TOKEN_{self.data_provider.upper()}")
# self.api_key_1 = os.getenv(f"API_TOKEN_{self.data_provider.upper()}_1")

data_line = ['clop',]
data_line = ['clop', 'clv', 'cwap', 'hilo', 'volume']
ticker_list = ['IWM', 'EEM']
# ticker_list = ['EURL', 'GDXU', 'TNA', 'YINN']
ticker_list = ['AFK', 'ASEA', 'BOTZ', 'DBC', 'ECNS', 'EEM', 'EURL', 'EZU', 'FXI', 'GDXU', 'HYG', 'ICLN', 'ILF', 'IWM', 'IYR', 'IYZ', 'JETS', 'LIT', 'LQD', 'SGOL', 'SIVR', 'SPY', 'TAN', 'TIP', 'TNA', 'VEGI', 'XAR', 'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'YINN']
# ticker_list = ['XAR', 'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'YINN']


def fetch_and_parse_price_data(data_line: list, provider: str, ticker_list: list):
    """"""
    if DEBUG: logger.debug(f"fetch_and_parse_price_data(data_line={data_line}, provider={provider}, ticker_list={ticker_list})")
    if not DEBUG: print(" Begin download process.")

    _create_sqlite_database(provider=provider, data_line=data_line, ticker_list=ticker_list)

    for index, ticker in enumerate(ticker_list):
        data_gen = eval(f"_{provider}_data_generator(index=index, ticker=ticker)")
        tuple_list = eval(f"_process_{provider}_data(data_gen=data_gen, data_line=data_line)")
        _write_data_tuple_to_sqlite_db(provider=provider, tuple_list=tuple_list, data_line=data_line, index=index)
        del tuple_list

    if not DEBUG: print(" Finished.")


def _create_sqlite_database(provider: str, data_line: list, ticker_list: list):
    """"""
    if DEBUG: logger.debug(f"_create_sqlite_database(provider={provider}, data_line={data_line}, ticker_list={ticker_list})")

    # if old database exists remove it
    Path(f"temp/data/{provider}.db").unlink(missing_ok=True)
    db = f"temp/data/{provider}.db"

    try:
        with sqlite3.connect(database=db) as conn:
            cursor = conn.cursor()
            # create table for each data line
            for item in data_line:
                cursor.execute(f'''
                    CREATE TABLE {item.lower()} (
                        Date    INTEGER    NOT NULL,
                        PRIMARY KEY (Date)
                    )
                    WITHOUT ROWID
                ''')
                # add column for each ticker symbol
                for ticker in ticker_list:
                    cursor.execute(f'''
                        ALTER TABLE {item} ADD COLUMN {ticker} INTEGER
                    ''')
    except sqlite3.OperationalError as e:
        logger.debug(f"*** ERROR *** {e}")
    else:
        conn.commit()
        if not DEBUG: print(f" created database '{db}'")


def _tiingo_data_generator(index: int, ticker: str)->object:
    """"""
    if DEBUG: logger.debug(f"_tiingo_data_generator(index={index}, ticker={ticker})")

    config = {
        'api_key': "",
        'session': True,  # reuse the same HTTP Session across API calls
    }
    client = TiingoClient(config)

    try:
        historical_prices = client.get_ticker_price(
            ticker=ticker,
            fmt='json',
            # startDate='2023-05-21',
            startDate='2025-04-21',
            endDate='2025-05-31',
            frequency='daily'
        )
    except Exception as e:
        logger.debug(f"*** ERROR *** {e}")
    else:
        if not DEBUG: print(f" fetching {ticker}...")
        yield ticker, historical_prices


def _process_tiingo_data(data_gen: object, data_line: list)->list[tuple]:
    """"""
    if DEBUG: logger.debug(f"_process_tiingo_data(data_gen={type(data_gen)}, data_line={type(data_line)}\nohlc_data:\n{data_gen})")

    ticker, dict_list = next(data_gen)  # unpack items in data_gen

    # create empty dataframe with index as a timestamp
    df = pd.DataFrame(
        index=[round(time.mktime(dt.strptime(d['date'][:10], '%Y-%m-%d').timetuple())) for d in dict_list]
    )
    df.index.name = 'date'

    # add each data line to dataframe
    def add_clop_series(loc: int)->None:
        """close location value, relative to the high-low range"""
        if DEBUG: logger.debug(f"add_clop_series(loc={loc})")
        clop = [round((d['adjClose'] - d['adjOpen']) * 100) for d in dict_list]
        df.insert(loc=loc, column='clop', value=clop, allow_duplicates=True)

    def add_clv_series(loc: int)->None:
        """close location value, relative to the high-low range"""
        if DEBUG: logger.debug(f"add_clv_series(loc={loc})")
        try:
            clv = [round(((2 * d['adjClose'] - d['adjLow'] - d['adjHigh']) / (d['adjHigh'] - d['adjLow'])) * 100) for d in dict_list]
            df.insert(loc=loc, column='clv', value=clv, allow_duplicates=True)
        except ZeroDivisionError as e:
            logger.debug(f"*** ERROR *** {e}")

    def add_cwap_series(loc: int)->None:
        """close weighted average price excluding open price"""
        if DEBUG: logger.debug(f"add_cwap_series(loc={loc})")
        cwap = [round(((d['adjHigh'] + d['adjLow'] + 2 * d['adjClose']) / 4) * 100) for d in dict_list]
        df.insert(loc=loc, column='cwap', value=cwap, allow_duplicates=True)

    def add_hilo_series(loc: int)->None:
        """difference between the high and low price"""
        if DEBUG: logger.debug(f"add_hilo_series(loc={loc})")
        hilo = [round((d['adjHigh'] - d['adjLow']) * 100) for d in dict_list]
        df.insert(loc=loc, column='hilo', value=hilo, allow_duplicates=True)

    def add_volume_series(loc: int)->None:
        """number of shares traded"""
        if DEBUG: logger.debug(f"add_volume_series(loc={loc})")
        volume = [d['adjVolume'] for d in dict_list]
        df.insert(loc=loc, column='volume', value=volume, allow_duplicates=True)

    # insert values from each data line into df
    for i, item in enumerate(data_line):
        eval(f"add_{item}_series({i})")

    # convert dataframe to list of tuples, tuple name is ticker symbol
    if not DEBUG: print(f" writing {ticker} data to db.\n")
    return list(df.itertuples(index=True, name=ticker))


def _write_data_tuple_to_sqlite_db(provider: str, tuple_list: list, data_line: list, index: int):
    """"""
    if DEBUG: logger.debug(f"_write_data_tuple_to_sqlite_db(tuple_list={type(tuple_list)}, data_line={type(data_line)}, index={index})")

    db = f"temp/data/{provider}.db"
    with sqlite3.connect(database=db) as conn:
        cursor = conn.cursor()
        for row in tuple_list:
            symbol = type(row).__name__
            date = row.Index
            for dl in data_line:
                table = dl.lower()
                value = getattr(row, table)
                try:
                    if index == 0:
                        query = f"INSERT INTO {table} (Date, {symbol}) VALUES (?, ?)"
                        # if DEBUG: logger.debug(f"query: {query}")
                        cursor.execute(query, (date, value))
                    else:
                        # query = f"UPDATE {table} SET {symbol} = ? WHERE Date = {date}", (value,)
                        # if DEBUG: logger.debug(f"query: {query}")
                        cursor.execute(f"UPDATE {table} SET {symbol} = ? WHERE Date = {date}", (value,))
                except Exception as e:
                    logger.debug(f"*** ERROR *** {e}")
                else:
                    conn.commit()


def main():
    fetch_and_parse_price_data(data_line=data_line, provider='tiingo', ticker_list=ticker_list)


if __name__ == '__main__':
    if DEBUG: logger.debug(f"******* START - beta_alphavantage() *******")
    main()
