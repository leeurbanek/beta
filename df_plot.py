""""""

import logging, logging.config
import sqlite3

import matplotlib as mpl
import matplotlib.pyplot as plt

# mpl.style.use('seaborn-v0_8-bright')
mpl.style.use("seaborn-v0_8-pastel")
import pandas as pd
import seaborn as sns


DEBUG = True

logging.config.fileConfig(fname="../src/logger.ini")
logger = logging.getLogger(__name__)

ctx = {
    "database": "/home/la/dev/stomartat/temp/data/xdefault.db",
}


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - df_plot.py.main() *******")
    main(ctx=ctx)
