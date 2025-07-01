import logging

from src.data_service.utils import SqliteWriter


logger = logging.getLogger(__name__)


def get_data(ctx):
    """"""
    debug = ctx.obj['default']['debug'] == 'True'
    if debug: logger.debug(f"get_data(ctx={ctx})")

    # select data provider
    if ctx.obj['data_service']['provider'] == 'alpha':
        data_generator = _use_alpha_reader(ctx)
    elif ctx.obj['data_service']['provider'] == 'tiingo':
        data_generator = _use_tiingo_reader(ctx)

    if debug: logger.debug(f"-> {data_generator}")

    writer = SqliteWriter(ctx)
    if not debug: print(f"writing data to database... ", end=" ")
    for idx, gen in enumerate(data_generator):
        writer.save_data(idx, gen)
    if not debug: print(f"done")


def _use_alpha_reader(ctx):
    """"""
    if ctx.obj['default']['debug'] == 'True': 
        logger.debug(f"_use_alpha_reader(ctx={ctx.obj}) -> generator object")
    
    from src.data_service.reader import AlphavantageReader
    reader = AlphavantageReader(ctx)
    return (  # generator object of ohlc price data
        reader.get_data_tuple(symbol) 
        for symbol in ctx.obj['data_service']['symbol']
    )


def _use_tiingo_reader(ctx):
    """"""
    if ctx.obj['default']['debug'] == 'True': 
        logger.debug(f"_use_tiingo_reader(ctx={ctx.obj}) -> generator object")

    from src.data_service.reader import TiingoReader
    reader = TiingoReader(ctx)
    return (  # generator object of ohlc price data
        reader.get_data_tuple(symbol) 
        for symbol in ctx.obj['data_service']['symbol']
    )
