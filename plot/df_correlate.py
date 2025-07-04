""""""

import logging, logging.config
# import sqlite3

# import matplotlib as mpl
import matplotlib.pyplot as plt

# # mpl.style.use('seaborn-v0_8-bright')
# mpl.style.use("seaborn-v0_8-pastel")
# import pandas as pd
import seaborn as sns

from utils_pd import create_df_from_one_column_in_each_table


DB = "/home/la/dev/stomartat/temp/data/xminmax.db"
# DB = "/home/la/dev/stomartat/temp/data/xrobust.db"
DEBUG = True

logging.config.fileConfig(fname="../logger.ini")
logger = logging.getLogger(__name__)

ctx = {
    "database": DB,
}

df = create_df_from_one_column_in_each_table(ctx=ctx, column_name="cwap")
if DEBUG: logger.debug(f"cwap\n{df}")

grid = sns.relplot(
    data=df,
    col=df.columns,
    kind="line",
)
if DEBUG: logger.debug(f"grid: {grid}\naxes_dict: {grid.axes_dict}")

# for i, col in enumerate(df.columns):
#     if DEBUG: logger.debug(f"i: {i+1} {type(i+1)}, col: {col} {type(col)}")

# grid = sns.relplot(
#     data=df,
#     x=df.index,
#     # y=df.values,
#     # row=df.index,
#     # col=df.values,
#     col_wrap=4, height=2,
#     kind="line",
# )
# if DEBUG: logger.debug(f"grid: {grid} {type(grid)}")

# grid.figure.tight_layout(w_pad=1)
# plt.show()


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - df_corr.py.main() *******")
    main(ctx=ctx)
