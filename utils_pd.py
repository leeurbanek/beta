"""pandas utilities"""

import logging, logging.config
import sqlite3

import pandas as pd


DEBUG = True

logging.config.fileConfig(fname="../logger.ini")
logger = logging.getLogger(__name__)

ctx = {
    "database": "/home/la/dev/stomartat/temp/data/xdefault.db",
    "data_line": "cwap",
}


def create_df_from_sqlite_table_data(ctx: dict) -> pd.DataFrame:
    """"""
    if DEBUG:
        logger.debug(f"create_df_from_sqlite_table_data(ctx={type(ctx)})")

    db_con = sqlite3.connect(database=ctx["database"])
    db_table_array = pd.read_sql(  # returns numpy ndarray
        f"SELECT name FROM sqlite_schema WHERE type='table' AND name NOT like 'sqlite%'", db_con,
    ).name.values

    index_array = pd.read_sql(  # returns numpy ndarray
        f"SELECT date FROM {db_table_array[0]}", db_con
    ).date.values

    df = pd.DataFrame(index=index_array)

    for table in db_table_array:
        df[table] = pd.read_sql(
            f"SELECT date, {ctx['data_line']} FROM {table}", db_con, index_col="date"
        )
    df.index = pd.to_datetime(df.index, unit="s").date

    return df


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")
    if DEBUG:
        logger.debug(f"dataframe for {ctx['data_line']}:\n{create_df_from_sqlite_table_data(ctx=ctx)}")


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - utils_pd.py.main() *******")
    main(ctx=ctx)
