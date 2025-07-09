"""pandas utilities"""

import logging, logging.config
import sqlite3

import pandas as pd


DEBUG = True

logging.config.fileConfig(fname="../logger.ini")
logger = logging.getLogger(__name__)

ctx = {
    "database": "/home/la/dev/beta/ohlc/data/xdefault.db",
}


def create_df_from_one_column_in_each_table(ctx: dict, indicator: str) -> pd.DataFrame:
    """Select data from the named column out of each table in the sqlite database"""
    if DEBUG:
        logger.debug(f"create_df_from_sqlite_table_data(ctx={ctx}, indicator={indicator})")

    db_con = sqlite3.connect(database=ctx["database"])

    # get a numpy ndarray of table names
    db_table_array = pd.read_sql(
        f"SELECT name FROM sqlite_schema WHERE type='table' AND name NOT like 'sqlite%'", db_con,
    ).name.values

    index_array = pd.read_sql(  # get a numpy ndarray of Date index
        f"SELECT date FROM {db_table_array[0]}", db_con
    ).date.values

    df = pd.DataFrame(index=index_array)

    for table in db_table_array:
        df[table] = pd.read_sql(
            f"SELECT date, {indicator} FROM {table}", db_con, index_col="date"
        )
    df.index = pd.to_datetime(df.index, unit="s").date
    df.index.names = ['date']

    return df


def create_df_from_sqlite_table_data(ctx: dict) -> pd.DataFrame:
    """"""
    if DEBUG:
        logger.debug(f"create_df_from_sqlite(database={ctx['database']}, table={ctx['table']})")

    db_con = sqlite3.connect(database=ctx["database"])
    df = pd.read_sql(f"SELECT date, ticker, {ctx['indicator']} FROM {ctx['table']}", db_con, index_col="date")
    df.index = pd.to_datetime(df.index, unit="s").date
    df.index.names = ['date']

    return df


if __name__ == "__main__":
    import unittest

    if DEBUG:
        logger.debug(f"******* START - utils_pd.py.main() *******")

    class TestDataframeUtilityFunctions(unittest.TestCase):
        """"""
        def setUp(self):
            if DEBUG: logger.debug(f"setUp(self={self})")

    unittest.main()
