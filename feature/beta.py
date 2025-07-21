""""""
import logging, logging.config
import sqlite3

import pandas as pd
import tsfel
import tsfel.feature_extraction
import tsfel.feature_extraction.features


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

ctx = {
    "database": None,
    "dataframe": None,
    "table": "data_line",
    "stonk_indicator": "cwap",
    # "stonk_indicator": "mass",
    "target_indicator": "cwap",
    # "scaler": "MinMax",
    "scaler": "Robust",
    "shift_period": 3
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
    # df.index = pd.to_datetime(df.index, unit="s")
    df.index = pd.to_datetime(df.index, unit="s").date
    df.index.names = ['date']

    return df


def extract_pre_defined_config(ctx: dict):
    """"""
    if DEBUG:
        logger.debug(f"extract_pre_defined_config(ctx={type(ctx)})")

    cfg = tsfel.get_features_by_domain()

    # timeseries = tsfel.datasets.load_biopluxecg()
    # timeseries = pd.Series(data=ctx["dataframe"]["SPXL"], name=ctx["dataframe"]["SPXL"].name)
    timeseries = pd.Series(data=ctx["dataframe"]["YINN"], name=ctx["dataframe"]["YINN"].name)
    if DEBUG: logger.debug(f"series {timeseries.name}:\n{timeseries} {type(timeseries)}")

    # return tsfel.time_series_features_extractor(
    #     config=cfg, timeseries=timeseries,
    # )
    return tsfel.feature_extraction.features.fundamental_frequency(
        signal=timeseries, fs=1.0
    )


def main(ctx: dict) -> None:
    if DEBUG: logger.debug(f"main(ctx={type(ctx)})")

    if ctx["scaler"].lower() == "minmax":
        DB = "/home/la/dev/stomartat/temp/data/xminmax.db"
    elif ctx["scaler"].lower() == "robust":
        DB = "/home/la/dev/stomartat/temp/data/xrobust.db"
    if DEBUG: logger.debug(f"database: {DB}")
    ctx["database"] = DB

    df = create_df_from_one_column_in_each_table(ctx=ctx, indicator="cwap")
    # df_mask = ~(df.columns.isin(["SPXL", "YINN"]))
    # shift_cols = df.columns[df_mask]
    # df[shift_cols] = df[shift_cols].shift(periods=ctx["shift_period"], fill_value=0, freq=None)

    ctx["dataframe"] = df
    X = extract_pre_defined_config(ctx=ctx)

    if DEBUG: logger.debug(f"feature: {X} {type(X)}")
    # for i, f in enumerate(X):
    #     if DEBUG: logger.debug(f"feature {i+1}, {type(f)}\n{f}")


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - beta.py.main() *******")

    main(ctx=ctx)
