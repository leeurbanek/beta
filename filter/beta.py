""""""
import logging, logging.config
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd

from scipy.signal import savgol_filter


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

ctx = {
    "database": "/home/la/dev/stomartat/temp/data/xdefault.db",
    "dataframe": None,
    "table": "data_line",
    "indicator": "cwap",
    "window_length_list": [21, 63],
    "poly_order_list": [2, 4]
}


def apply_savgol_filter(ctx: dict)->None:
    """"""
    if DEBUG:
        logger.debug(f"apply_savgol_filter(ctx={type(ctx)})")

    for col in ctx["dataframe"].columns:
        fig, axs = plt.subplots(ncols=2, nrows=2, figsize=(12.5, 7))
        plt.xlabel("Date")
        plt.ylabel("Value")

        series_raw = pd.Series(data=ctx["dataframe"][col], name=ctx["dataframe"][col].name)
        if DEBUG: logger.debug(f"series_raw {series_raw}")

        for i, window_length in enumerate(ctx["window_length_list"]):
            for j, poly_order in enumerate(ctx["poly_order_list"]):
                series_smoothed = savgol_filter(
                    series_raw, window_length=window_length, polyorder=poly_order
                )
                if DEBUG: logger.debug(f"series_smoothed {i, j}:\n{series_smoothed}")

                axs[i, j].plot(series_raw.index, series_raw, label=f"raw {ctx['indicator']} data")
                axs[i, j].plot(series_raw.index, series_smoothed, label="filtered data")
                axs[i, j].legend()
                axs[i, j].set_title(
                    f"{series_raw.name} window_length: {window_length},  poly_order: {poly_order}"
                )
        plt.tight_layout()
        # plt.show()
        plt.savefig(f"../img/filter/{series_raw.name}_{ctx['indicator']}")
        plt.close()


def create_df_from_one_column_in_each_table(ctx: dict, indicator: str) -> pd.DataFrame:
    """Select data from the named column out of each table in the sqlite database"""
    if DEBUG:
        logger.debug(f"create_df_from_sqlite_table_data(ctx={type(ctx)}, indicator={indicator})")

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
    # df.index = pd.to_datetime(df.index, unit="s")
    df.index = pd.to_datetime(df.index, unit="s").date
    df.index.names = ['date']

    return df


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")

    df = create_df_from_one_column_in_each_table(ctx=ctx, indicator="cwap")
    # df_mask = ~(df.columns.isin(["SPXL", "YINN"]))
    # shift_cols = df.columns[df_mask]
    # df[shift_cols] = df[shift_cols].shift(periods=ctx["shift_period"], fill_value=0, freq=None)
    ctx["dataframe"] = df

    apply_savgol_filter(ctx=ctx)


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - filter/beta.py.main() *******")

    main(ctx=ctx)
