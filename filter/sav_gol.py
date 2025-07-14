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
    "poly_order_list": [2,],
    "slope_param": [21, 63],
}


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


def create_filtered_dataframe(ctx: dict)->None:
    """"""
    if DEBUG:
        logger.debug(f"create_filtered_dataframe(ctx={type(ctx)})")

    # create empty dataframe with index as a timestamp
    df = pd.DataFrame(index=ctx["dataframe"].index.values)
    df.index.name = "date"

    # insert values for each ticker into df
    for col in ctx["dataframe"].columns:
        if col in ['SPXL', 'YINN']:
            continue
        df[col] = savgol_filter(
            x=ctx["dataframe"][col].values,
            window_length=ctx["window_length_list"][0],
            polyorder=ctx["poly_order_list"][0],
            deriv=1
        )
    return df


def plot_alternate_filter_params(ctx: dict)->None:
    """"""
    if DEBUG:
        logger.debug(f"plot_alternate_filter_params(ctx={type(ctx)})")

    for col in ctx["dataframe"].columns:
        fig, axs = plt.subplots(ncols=2, nrows=2, figsize=(12.5, 7))
        plt.xlabel("Date")
        plt.ylabel("Value")

        series_raw = pd.Series(data=ctx["dataframe"][col], name=ctx["dataframe"][col].name)
        if DEBUG: logger.debug(f"{col} series_raw {type(series_raw)}")

        for i, poly_order in enumerate(ctx["poly_order_list"]):
            for j, window_length in enumerate(ctx["window_length_list"]):
                series_smoothed = savgol_filter(
                    x=series_raw, window_length=window_length, polyorder=poly_order, deriv=0
                )
                axs[i, j].plot(series_raw.index, series_raw, label=f"raw {ctx['indicator']} data")
                axs[i, j].plot(series_raw.index, series_smoothed, label="filtered data")
                axs[i, j].legend()
                axs[i, j].set_title(
                    f"{series_raw.name} window_length: {window_length},  poly_order: {poly_order}"
                )
            for j, window_length in enumerate(ctx["window_length_list"]):
                series_slope = savgol_filter(
                    x=series_raw, window_length=window_length, polyorder=poly_order, deriv=1
                )
                axs[i+1, j].plot(series_raw.index, series_slope, label=f"derivitave 1")
                axs[i+1, j].legend()
                axs[i+1, j].set_title(
                    f"{series_raw.name} window_length: {window_length},  poly_order: {poly_order}"
                )
        plt.tight_layout()
        plt.savefig(f"../img/filter/{series_raw.name}_{ctx['indicator']}")
        plt.close()


def zero_crossing_matrix(ctx: dict):
    if DEBUG: logger.debug(f"zero_crossing_matrix(ctx={ctx}")

    for i, col in enumerate(ctx["dataframe"].columns):
        if DEBUG: logger.debug(f"i: {i},  col: {col},  values:\n{ctx['dataframe'].values}")

# for i in xrange(len(Derivative)-1):
#     if Derivative[i] > 0 and Derivative[i+1] < 0:
#         print "crossing between indexes {} and {}".format(i, i+1)


def main(ctx: dict) -> None:
    if DEBUG: logger.debug(f"main(ctx={ctx})")

    df = create_df_from_one_column_in_each_table(ctx=ctx, indicator="cwap")
    ctx["dataframe"] = df
    # plot_alternate_filter_params(ctx=ctx)
    ctx["dataframe"] = create_filtered_dataframe(ctx=ctx)
    zero_crossing_matrix(ctx=ctx)


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - filter/beta.py.main() *******")

    main(ctx=ctx)
