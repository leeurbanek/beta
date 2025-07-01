import logging
import sqlite3
import os

from src import config_dict
from src.data_service.processor import (
    close_location_value,
    close_weighted_price,
    price_volume_mass,
    volume_data
)

debug = config_dict['default']['debug'] == 'True'
logger = logging.getLogger(__name__)


class SqliteWriter:
    """"""
    def __init__(self, ctx):
        self.db_path = f"{ctx.obj['default']['temp_dir']}/{ctx.obj['data_service']['database']}"
        self.debug = f"{ctx.obj['default']['debug']}" == 'True'
        if self.debug: logger.debug(self)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"db_path={self.db_path}, "
            f"debug={self.debug})"
        )

    def save_data(self, idx, gen):
        if self.debug: logger.debug(f"{self}.save_data(data={gen})")

        with SqliteConnectManager(db_path=self.db_path, mode='rwc') as db:
            for row in close_location_value(gen):
                table = {type(row).__name__.lower()}.pop()
                symbol = {row.symbol}.pop()
                date = {row.date}.pop()
                clv = {row.clv}.pop()
                if not bool(idx):
                    db.cursor.execute(f'''
                        INSERT INTO {table} (Date, {symbol}) 
                        VALUES (?, ?)''', (date, clv)
                    ) 
                else:
                    db.cursor.execute(f'''
                        UPDATE {table} SET {symbol} = ? 
                        WHERE Date = {date}''', (clv,)
                    )
            for row in close_weighted_price(gen):
                table = {type(row).__name__.lower()}.pop()
                symbol = {row.symbol}.pop()
                date = {row.date}.pop()
                price = {row.price}.pop()
                if not bool(idx):
                    db.cursor.execute(f'''
                        INSERT INTO {table} (Date, {symbol}) 
                        VALUES (?, ?)''', (date, price)
                    ) 
                else:
                    db.cursor.execute(f'''
                        UPDATE {table} SET {symbol} = ? 
                        WHERE Date = {date}''', (price,)
                    )
            for row in price_volume_mass(gen):
                table = {type(row).__name__.lower()}.pop()
                symbol = {row.symbol}.pop()
                date = {row.date}.pop()
                mass = {row.mass}.pop()
                if not bool(idx):
                    db.cursor.execute(f'''
                        INSERT INTO {table} (Date, {symbol}) 
                        VALUES (?, ?)''', (date, mass)
                    ) 
                else:
                    db.cursor.execute(f'''
                        UPDATE {table} SET {symbol} = ? 
                        WHERE Date = {date}''', (mass,)
                    )
            for row in volume_data(gen):
                table = {type(row).__name__.lower()}.pop()
                symbol = {row.symbol}.pop()
                date = {row.date}.pop()
                volume = {row.volume}.pop()
                if not bool(idx):
                    db.cursor.execute(f'''
                        INSERT INTO {table} (Date, {symbol}) 
                        VALUES (?, ?)''', (date, volume)
                    ) 
                else:
                    db.cursor.execute(f'''
                        UPDATE {table} SET {symbol} = ? 
                        WHERE Date = {date}''', (volume,)
                    )


class SqliteConnectManager:
    """Context manager for Sqlite3 databases.
    -----------------------------------------
    Commits changes on exit.\n
    Parameters
    ----------
    `db_path` : string
        Path to an Sqlite3 database (db_path='test.db' for in memory db).\n
    `mode` : string
        determines if the new database is opened read-only 'ro', read-write 'rw',\n
        read-write-create 'rwc' (mode='memory' for in-memory database.)\n
    Returns
    -------
    An Sqlite3 connection object.\n
    """
    def __init__(self, db_path=None, mode=None):
        self.db_path = db_path
        self.debug = debug
        self.mode = mode

    def __enter__(self):
        if self.debug: logger.debug('DatabaseConnectionManager.__enter__()')
        try:
            self.connection = sqlite3.connect(
                f'file:{os.path.abspath(self.db_path)}?mode={self.mode}',
                # detect_types=sqlite3.PARSE_DECLTYPES, uri=True
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, uri=True
            )
            self.cursor = self.connection.cursor()
            if self.debug: logger.debug(f"connected '{os.path.basename(self.db_path)}', mode: {self.mode}")
            return self
        except sqlite3.Error as e:
            print(f'{e}: {self.db_path}')

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.debug: logger.debug('DatabaseConnectionManager.__exit__()')
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()


def sqlite_create_database(ctx):
    """"""
    if debug: logger.debug(f"create_database(ctx={ctx})")

    db_path = f"{ctx.obj['default']['temp_dir']}/{ctx.obj['data_service']['database']}"

    import re
    with SqliteConnectManager(db_path=db_path, mode='rwc') as db:
        table_list = [t.lower() for t in re.findall(r'[^,;\s]+', ctx.obj['data_service']['table'])]
        for table in table_list:
            db.cursor.execute(f'''
                CREATE TABLE {table} (
                    Date    INTEGER    NOT NULL, 
                    PRIMARY KEY (Date)
                )
            ''')
            # add symbol column to table
            col_list = ctx.obj['data_service']['symbol']
            for col in col_list:
                db.cursor.execute(f'''
                    ALTER TABLE {table} ADD COLUMN {col} INTEGER
                ''')
    if not debug: print(f"created database: '{db_path}'")
