""""""

import logging, logging.config
import sqlite3

from pathlib import Path


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logger = logging.getLogger(__name__)


def create_sqlite_data_line_database(ctx: dict) -> None:
    """Create sqlite3 database. Table for each ticker symbol, column for each data line."""
    if DEBUG: logger.debug(f"create_sqlite_data_line_database(ctx={ctx})")

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


def create_sqlite_indicator_database(ctx: dict) -> None:
    """Create sqlite3 database. Table for each ticker symbol, column for each data line."""
    if DEBUG: logger.debug(f"create_sqlite_indicator_database(ctx={ctx})")

    # create data folder in users work_dir
    Path(f"{ctx['default']['work_dir']}/data").mkdir(parents=True, exist_ok=True)
    # if old database exists remove it
    Path(f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}").unlink(missing_ok=True)

    DB = f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}"
    try:
        with sqlite3.connect(DB) as con:
            # create table for each ticker symbol
            for table in ctx["interface"]["arguments"]:
                print(f" creating indicator table: {table}")
                con.execute(
                    f"""
                    CREATE TABLE {table.upper()} (
                        date    INTEGER    NOT NULL,
                        PRIMARY KEY (date)
                    )
                """
                )
                # add column for each indicator (data_line)
                for col in ctx["interface"]["data_line"]:
                    con.execute(
                        f"""
                        ALTER TABLE {table} ADD COLUMN {col.lower()} INTEGER
                    """
                    )

    except con.DatabaseError as e:
        logger.debug(f"*** ERROR *** {e}")


def create_sqlite_stonk_database(ctx: dict) -> None:
    """Create sqlite3 database. Table for each ticker symbol, column for each data line."""
    if DEBUG: logger.debug(f"create_sqlite_indicator_database(ctx={ctx})")

    # create data folder in users work_dir
    Path(f"{ctx['default']['work_dir']}/data").mkdir(parents=True, exist_ok=True)
    # if old database exists remove it
    Path(f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}").unlink(missing_ok=True)

    DB = f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}"

    download_set = ctx['interface']['download_list']
    target_list = ctx['interface']['target']
    print(f"download_set: {download_set} {type(download_set)}")
    print(f"target_list: {target_list} {type(target_list)}")

    with sqlite3.connect(DB) as con:
        # create table for each ticker symbol
        # for table in ctx["interface"]["arguments"]:

        for table in iter(download_set):
            print(f"table: {table} {type(table)}", end=' ')
            if table in target_list:
                print(f"target: {table}")
                # create table for target symbol (ohlc prices)
                con.execute(f'''
                    CREATE TABLE {table} (
                        date      INTEGER    NOT NULL,
                        open      INTEGER,
                        high      INTEGER,
                        low       INTEGER,
                        close     INTEGER,
                        PRIMARY KEY (date)
                    )'''
                )
                # add column for each indicator (data_line)
                for col in ctx["interface"]["data_line"]:
                    con.execute(
                        f"""
                        ALTER TABLE {table} ADD COLUMN {col.lower()} INTEGER
                    """
                    )
            else:
                print(f"indicator: {table}")
                con.execute(
                    f"""
                    CREATE TABLE {table.upper()} (
                        date    INTEGER    NOT NULL,
                        PRIMARY KEY (date)
                    )"""
                )
                # add column for each indicator (data_line)
                for col in ctx["interface"]["data_line"]:
                    con.execute(
                        f"""
                        ALTER TABLE {table} ADD COLUMN {col.lower()} INTEGER
                    """
                    )

        # # create table for target symbol (ohlc prices)
        # for table in ctx['interface']['target']:
        #     print(f" creating target table: {table}")
        #     con.execute(f'''
        #         CREATE TABLE {table+'t'} (
        #             Date      INTEGER    NOT NULL,
        #             Open      INTEGER    NOT NULL,
        #             High      INTEGER    NOT NULL,
        #             Low       INTEGER    NOT NULL,
        #             Close     INTEGER    NOT NULL,
        #             Volume    INTEGER    NOT NULL,
        #             PRIMARY KEY (Date)
        #         )'''
        #     )


def write_data_line_data_to_sqlite_db(ctx: dict, data_tuple: tuple)->None:
    """"""
    if DEBUG: logger.debug(f"write_data_line_data_to_sqlite_db(ctx={ctx}, data_tuple={data_tuple})")
    print(f"write_data_line_data_to_sqlite_db(ctx={ctx}, data_tuple={data_tuple})")

    DB = f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}"
    print(f" database: {DB}")
    try:
        with sqlite3.connect(DB) as con:
            for row in data_tuple[1].itertuples(index=True, name=None):
                con.execute(f"INSERT INTO data_line VALUES (?,?,?,?,?,?,?,?)", row)
    except con.DatabaseError as e:
        logger.debug(f"*** Error *** {e}")


def write_indicator_data_to_sqlite_db(ctx: dict, data_tuple: tuple)->None:
    """"""
    if DEBUG: logger.debug(f"write_indicator_data_to_sqlite_db(ctx={ctx}, data_tuple={data_tuple})")
    print(f"write_indicator_data_to_sqlite_db(ctx={ctx}, data_tuple={data_tuple})")

    DB = f"{ctx['default']['work_dir']}/data/{ctx['interface']['database']}"
    print(f" database: {DB}")
    try:
        with sqlite3.connect(DB) as con:
            for row in data_tuple[1].itertuples(index=True, name=None):
                con.execute(f"INSERT INTO {data_tuple[0]} VALUES (?,?,?,?,?,?,?)", row)
    except con.DatabaseError as e:
        logger.debug(f"*** Error *** {e}")
