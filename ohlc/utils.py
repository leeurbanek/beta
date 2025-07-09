""""""

import logging, logging.config
import sqlite3

from pathlib import Path


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logger = logging.getLogger(__name__)


def create_sqlite_indicator_database(ctx: dict) -> None:
    """Create sqlite3 database. Table for each ticker symbol, column for each data line."""
    if DEBUG:
        logger.debug(f"create_sqlite_indicator_database(ctx={type(ctx)})")

    # create data folder in users work_dir
    Path(f"{ctx['default']['work_dir']}/data").mkdir(parents=True, exist_ok=True)
    # if old database exists remove it
    Path(f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}").unlink(missing_ok=True)

    DB = f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}"
    try:
        with sqlite3.connect(DB) as con:
            # create table that holds the data lines
            con.execute(
                f"""
                CREATE TABLE data_line (
                    date    INTEGER    NOT NULL,
                    ticker  TEXT       NOT NULL,
                    PRIMARY KEY (date, ticker)
                )
            """
            )
            # add column for each indicator (data_line)
            for col in ctx["interface"]["data_line"]:
                con.execute(
                    f"""
                    ALTER TABLE data_line ADD COLUMN {col.lower()} INTEGER
                """
                )
    except con.DatabaseError as e:
        logger.debug(f"*** ERROR *** {e}")


def write_indicator_data_to_sqlite_db(ctx: dict, data_tuple: tuple)->None:
    """"""
    if DEBUG:
        logger.debug(f"write_indicator_data_to_sqlite_db(ctx={type(ctx)}, data_tuple={type(data_tuple)})")

    DB = f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}"
    try:
        with sqlite3.connect(DB) as con:
            for row in data_tuple[1].itertuples(index=True, name=None):
                con.execute(f"INSERT INTO data_line VALUES (?,?,?,?,?,?,?,?)", row)
    except con.DatabaseError as e:
        logger.debug(f"*** Error *** {e}")
