""""""

import importlib
import logging, logging.config
import sqlite3

from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf


DEBUG = True
# DEBUG = False
SCALER = "MinMaxScaler"
# SCALER = "RobustScaler"

logging.config.fileConfig(fname="src/logger.ini")
logging.getLogger("yfinance").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

ctx = {
    # "data_line": ["mass"],
    "data_line": ["clop", "clv", "cwap", "hilo", "mass", "volume"],
    "provider": "beta_test",
    "scaler": SCALER,
    "ticker_list": ["EEM", "IWM"],
    # "ticker_list": ['EURL', 'GDXU', 'TNA', 'YINN'],
    # "ticker_list": ['AFK', 'ASEA', 'BOTZ', 'DBC', 'ECNS', 'EEM', 'EURL', 'EZU', 'FXI', 'GDXU', 'HYG', 'ICLN', 'ILF', 'IWM', 'IYR', 'IYZ', 'JETS', 'LIT', 'LQD', 'SGOL', 'SIVR', 'SPY', 'TAN', 'TIP', 'TNA', 'VEGI', 'XAR', 'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'YINN'],
}


def fetch_and_parce_price_data(ctx: dict)->None:
    """"""
    if DEBUG:
        logger.debug(
            f"fetch_and_parce_price_data(ctx={ctx})"
        )
    if not DEBUG:
        print(" Begin download process:")

    _create_sqlite_database(provider=ctx["provider"], data_line=ctx["data_line"], ticker_list=ctx["ticker_list"])

    for index, ticker in enumerate(ctx["ticker_list"]):
        data_gen = _yfinance_data_generator(index=index, ticker=ticker)
        tuple_list = _process_yfinance_data(ctx=ctx, data_gen=data_gen)
        if DEBUG: logger.debug(f"tuple_list: {tuple_list}")

        _write_data_tuple_to_sqlite_db(provider=ctx["provider"], tuple_list=tuple_list, data_line=ctx["data_line"], index=index)
        del tuple_list

    if not DEBUG:
        print(" finished.")


def _create_sqlite_database(provider: str, data_line: list, ticker_list: list):
    """"""
    if DEBUG:
        logger.debug(f"_create_sqlite_database(provider={provider}, data_line={data_line}, ticker_list={ticker_list})")

    # if old database exists remove it
    Path(f"temp/data/{provider}.db").unlink(missing_ok=True)
    db = f"temp/data/{provider}.db"

    try:
        with sqlite3.connect(database=db) as conn:
            cursor = conn.cursor()
            # create table for each data line
            for item in data_line:
                cursor.execute(f"""
                    CREATE TABLE {item.lower()} (
                        Date    INTEGER    NOT NULL,
                        PRIMARY KEY (Date)
                    )
                    WITHOUT ROWID
                """)
                # add column for each ticker symbol
                for ticker in ticker_list:
                    cursor.execute(f"""
                        ALTER TABLE {item} ADD COLUMN {ticker} INTEGER
                    """)
    except sqlite3.OperationalError as e:
        logger.debug(f"*** ERROR *** {e}")
    else:
        conn.commit()
        if not DEBUG:
            print(f" create database '{db}',")


def _yfinance_data_generator(index: int, ticker: str) -> object:
    """"""
    if DEBUG:
        logger.debug(f"_yfinance_data_generator(index={index}, ticker={ticker})")

    if not DEBUG:
        print(f"  - fetching {ticker}...\t", end="")
    try:
        yf_ticker = yf.Ticker(ticker=ticker)
        df = yf_ticker.history(
            # start="2023-05-21",
            start='2025-05-21',
            end="2025-05-31",
            interval="1d",
        )
    except Exception as e:
        logger.debug(f"*** ERROR *** {e}")
    else:
        yield ticker, df


def _process_yfinance_data(ctx: dict, data_gen: object) -> list[tuple]:
    """"""
    if DEBUG:
        logger.debug(f"_process_yfinance_data(ctx={ctx}, data_gen={type(data_gen)})")

    if ctx["scaler"] == "MinMaxScaler":
        from sklearn.preprocessing import MinMaxScaler
        scaler = MinMaxScaler()
    elif ctx["scaler"] == "RobustScaler":
        from sklearn.preprocessing import RobustScaler
        scaler = RobustScaler(quantile_range=(0.0, 100.0))

    # ticker, yf_df = next(data_gen)
    ticker, yf_df = next(data_gen)
    yf_df = yf_df.drop(columns=yf_df.columns.values[-3:], axis=1)

    if not DEBUG:
        print("processing data\t", end="")

    # create empty dataframe with index as a timestamp
    df = pd.DataFrame(index=yf_df.index.values.astype(int) // 10**9)
    df.index.name = "date"

    # difference between the close and open price
    clop = list(round((yf_df["Close"] - yf_df["Open"]) * 100).astype(int))
    if DEBUG: logger.debug(f"clop: {clop} {type(clop)}")

    # close location value, relative to the high-low range
    try:
        clv = list(
            round((2 * yf_df["Close"] - yf_df["Low"] - yf_df["High"]) / (yf_df["High"] - yf_df["Low"]) * 100).astype(int)
        )
        if DEBUG: logger.debug(f"clv: {clv} {type(clv)}")
    except ZeroDivisionError as e:
        logger.debug(f"*** ERROR *** {e}")

    # close weighted average price exclude open price
    cwap = np.array(
        list((2 * yf_df["Close"] + yf_df["High"] + yf_df["Low"]) / 4)
    ).reshape(-1, 1)
    cwap = np.rint((scaler.fit_transform(cwap).flatten() + 10) * 100).astype(int)
    if DEBUG: logger.debug(f"scaled_cwap: {cwap} {type(cwap)}")

    # difference between the high and low price
    hilo = list(round((yf_df["High"] - yf_df["Low"]) * 100).astype(int))
    if DEBUG: logger.debug(f"hilo: {hilo} {type(hilo)}")

    # number of shares traded
    volume = np.array(list(yf_df["Volume"])).reshape(-1, 1)
    volume = np.rint((scaler.fit_transform(volume).flatten() + 10) * 100).astype(int)
    if DEBUG: logger.debug(f"scaled_volume: {volume} {type(volume)}")

    # price times number of shares traded
    mass = np.array(cwap * volume).reshape(-1, 1)
    mass = np.rint((scaler.fit_transform(mass).flatten() + 10) * 100).astype(int)
    if DEBUG: logger.debug(f"scaled_mass: {mass} {type(mass)}")

    # insert values for each data line into df
    for i, item in enumerate(ctx["data_line"]):
        df.insert(loc=i, column=f"{item}", value=eval(item), allow_duplicates=True)

    # convert dataframe to list of tuples
    return list(df.itertuples(index=True, name=ticker))


def _write_data_tuple_to_sqlite_db(provider: str, tuple_list: list, data_line: list, index: int):
    """"""
    if DEBUG:
        logger.debug(f"_write_data_tuple_to_sqlite_db(tuple_list={tuple_list}, data_line={type(data_line)}")

    db = f"temp/data/{provider}.db"
    with sqlite3.connect(database=db) as conn:
        cursor = conn.cursor()
        if not DEBUG:
            print(" writing to db\t", end="")
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
        if not DEBUG:
            print(f"done,")


def main(ctx: dict):
    fetch_and_parce_price_data(ctx=ctx)


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - beta_yfinance() *******")
    main(ctx=ctx)
