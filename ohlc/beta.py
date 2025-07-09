import logging, logging.config

import utils

from yf_data import YahooFinanceDataProcessor


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")

    # create database
    utils.create_sqlite_indicator_database(ctx=ctx)

    # select data processor
    processor = YahooFinanceDataProcessor(ctx=ctx)

    # get and save data for each ticker
    for index, ticker in enumerate(ctx['interface']['arguments']):
        ctx['interface']['index'] = index  # alphavantage may throttle at five downloads
        data_tuple = processor.download_and_parse_price_data(ticker=ticker)
        if DEBUG: logger.debug(f"data_tuple {data_tuple[0]}\n{data_tuple[1]}")

        utils.write_indicator_data_to_sqlite_db(ctx=ctx, data_tuple=data_tuple)


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - yfinance.py.main() *******")

    ctx = {
        'default': {
            'debug': True,
            'work_dir': '/home/la/dev/beta/ohlc',
            'cfg_chart': '/home/la/dev/stomartat/src/pkg/chart_srv/cfg_chart.ini',
            'cfg_data': '/home/la/dev/stomartat/src/pkg/data_srv/cfg_data.ini',
            'cfg_main': '/home/la/dev/stomartat/src/config.ini'
        },
        'interface': {
            'command': 'data',
            'target_data': ['LQD'],
            # 'arguments': ['EEM', 'IWM'],
            'arguments': ['AFK', 'ASEA', 'BOTZ', 'DBC', 'ECNS', 'EEM', 'EURL', 'EZU', 'FXI', 'GDXU', 'HYG', 'ICLN', 'ILF', 'IWM', 'IYR', 'IYZ', 'JETS', 'LIT', 'LQD', 'SGOL', 'SIVR', 'SPY', 'TAN', 'TIP', 'TNA', 'VEGI', 'XAR', 'XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'YINN'],
            'database': 'default.db',
            'data_line': ['CLOP', 'CLV', 'CWAP', 'HILO', 'MASS', 'VOLUME']
        },
        'data_service': {
            'data_frequency': 'daily',
            'data_line': 'CLOP CLV CWAP HILO MASS VOLUME',
            'data_list': 'EEM IWM',
            'data_lookback': '42',
            # 'data_lookback': '768',
            'data_provider': 'yfinance',
            'sklearn_scaler': 'MinMaxScaler',
            # 'sklearn_scaler': 'RobustScaler',
            'target_data': 'LQD',
            'url_alphavantage': 'https://www.alphavantage.co/query',
            'url_tiingo': '',
            'url_yfinance': ''
        }
    }

    main(ctx=ctx)
