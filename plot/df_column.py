""""""

import logging, logging.config
# import sqlite3

import matplotlib as mpl
import matplotlib.pyplot as plt

# # mpl.style.use('seaborn-v0_8-bright')
mpl.style.use("seaborn-v0_8-pastel")
import pandas as pd
import seaborn as sns

from utils_pd import create_df_from_one_column_in_each_table


DB = "/home/la/dev/stomartat/temp/data/xminmax.db"
# DB = "/home/la/dev/stomartat/temp/data/xrobust.db"
DEBUG = True

logging.config.fileConfig(fname="logger.ini")
logger = logging.getLogger(__name__)

ctx = {
    "database": DB,
}

df = create_df_from_one_column_in_each_table(ctx=ctx, column_name="cwap")

# fig, ax = plt.subplots(len(df.columns), 1, figsize=(10,40), sharex=True)
# for i, col in enumerate(df.columns):
#     subset = df[col]
#     plt.figure(i, figsize=(10, 6))
#     sns.lineplot(x=subset.index, y=subset.values, data=df)
#     plt.title(col)
#     plt.xlabel('Date')

#     plt.show()


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - df_plot.py.main() *******")
    main(ctx=ctx)
