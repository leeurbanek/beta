"""data/data.py"""

ctx = {'default': {'debug': True, 'work_dir': '/home/la/dev/stomartat/temp/', 'cfg_chart': '/home/la/dev/stomartat/src/pkg/chart_srv/cfg_chart.ini', 'cfg_data': '/home/la/dev/stomartat/src/pkg/data_srv/cfg_data.ini', 'cfg_main': '/home/la/dev/stomartat/src/config.ini'}, 'interface': {}, 'chart_service': {'adblock': '', 'chart_list': 'EEM FXI IWM DBC HYG SPY IAU LQD MOO TIP IYR IYZ XLB XLE XLF XLI XLK XLP XLU XLV XLY BOTZ GBTC LIT XAR', 'heatmap_list': '1W 1M 3M 6M', 'url_stockchart': 'https://stockcharts.com/sc3/ui/?s=AAPL', 'url_heatmap': 'https://stockanalysis.com/markets/heatmap/', 'webdriver': 'geckodriver'}, 'data_service': {'data_list': 'EEM IWM', 'data_lookback': '21', 'data_frequency': 'daily', 'data_provider': 'tiingo', 'url_alphavantage': '', 'url_tiingo': 'https://api.tiingo.com/tiingo', 'url_yahoo': ''}}

config_dict = {
    'default': {
        'debug': True,
        'work_dir': '/home/la/dev/stomartat/temp/',
        'cfg_chart': '/home/la/dev/stomartat/src/pkg/chart_srv/cfg_chart.ini',
        'cfg_data': '/home/la/dev/stomartat/src/pkg/data_srv/cfg_data.ini',
        'cfg_main': '/home/la/dev/stomartat/src/config.ini'
    },
    'interface': {},
    'chart_service': {
        'adblock': '',
        'chart_list': 'EEM IWM LQD IAU SLV',
        'heatmap_list': '1M 3M 1W',
        'url_stockchart': 'https://stockcharts.com/sc3/ui/?s=AAPL',
        'url_heatmap': 'https://stockanalysis.com/markets/heatmap/',
        'webdriver': 'geckodriver'
    },
    'data_service': {
        'back_days': '21',
        'data_list': 'EEM IWM LQD',
        'frequency': 'daily',
        'provider': 'tiingo',
        'url_tiingo': 'https://api.tiingo.com/tiingo'
    }
}

ohlc = [
    {'date': '2025-04-03T00:00:00.000Z', 'close': 189.65, 'high': 195.0599, 'low': 189.2, 'open': 193.11, 'volume': 60551537, 'adjClose': 189.65, 'adjHigh': 195.0599, 'adjLow': 189.2, 'adjOpen': 193.11, 'adjVolume': 60551537, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-04T00:00:00.000Z', 'close': 181.19, 'high': 184.05, 'low': 176.67, 'open': 182.61, 'volume': 92612780, 'adjClose': 181.19, 'adjHigh': 184.05, 'adjLow': 176.67, 'adjOpen': 182.61, 'adjVolume': 92612780, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-07T00:00:00.000Z', 'close': 179.55, 'high': 190.25, 'low': 171.73, 'open': 174.32, 'volume': 96706222, 'adjClose': 179.55, 'adjHigh': 190.25, 'adjLow': 171.73, 'adjOpen': 174.32, 'adjVolume': 96706222, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-08T00:00:00.000Z', 'close': 174.82, 'high': 185.92, 'low': 172.34, 'open': 185.69, 'volume': 61090556, 'adjClose': 174.82, 'adjHigh': 185.92, 'adjLow': 172.34, 'adjOpen': 185.69, 'adjVolume': 61090556, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-09T00:00:00.000Z', 'close': 189.68, 'high': 192.11, 'low': 171.74, 'open': 172.72, 'volume': 123015115, 'adjClose': 189.68, 'adjHigh': 192.11, 'adjLow': 171.74, 'adjOpen': 172.72, 'adjVolume': 123015115, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-10T00:00:00.000Z', 'close': 181.71, 'high': 185.48, 'low': 176.98, 'open': 184.52, 'volume': 67503008, 'adjClose': 181.71, 'adjHigh': 185.48, 'adjLow': 176.98, 'adjOpen': 184.52, 'adjVolume': 67503008, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-11T00:00:00.000Z', 'close': 184.36, 'high': 184.85, 'low': 178.58, 'open': 181.14, 'volume': 44954386, 'adjClose': 184.36, 'adjHigh': 184.85, 'adjLow': 178.58, 'adjOpen': 181.14, 'adjVolume': 44954386, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-14T00:00:00.000Z', 'close': 186.53, 'high': 187.97, 'low': 183.05, 'open': 187.81, 'volume': 35214609, 'adjClose': 186.53, 'adjHigh': 187.97, 'adjLow': 183.05, 'adjOpen': 187.81, 'adjVolume': 35214609, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-15T00:00:00.000Z', 'close': 186.76, 'high': 189.05, 'low': 185.84, 'open': 186.34, 'volume': 30715458, 'adjClose': 186.76, 'adjHigh': 189.05, 'adjLow': 185.84, 'adjOpen': 186.34, 'adjVolume': 30715458, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-16T00:00:00.000Z', 'close': 184.97, 'high': 186.73, 'low': 182.56, 'open': 185.7, 'volume': 35063316, 'adjClose': 184.97, 'adjHigh': 186.73, 'adjLow': 182.56, 'adjOpen': 185.7, 'adjVolume': 35063316, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-17T00:00:00.000Z', 'close': 186.48, 'high': 187.43, 'low': 184.46, 'open': 184.99, 'volume': 42359249, 'adjClose': 186.48, 'adjHigh': 187.43, 'adjLow': 184.46, 'adjOpen': 184.99, 'adjVolume': 42359249, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-21T00:00:00.000Z', 'close': 182.74, 'high': 185.29, 'low': 180.765, 'open': 185.0, 'volume': 26018999, 'adjClose': 182.74, 'adjHigh': 185.29, 'adjLow': 180.765, 'adjOpen': 185.0, 'adjVolume': 26018999, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-22T00:00:00.000Z', 'close': 187.47, 'high': 188.1, 'low': 184.55, 'open': 185.15, 'volume': 34477053, 'adjClose': 187.47, 'adjHigh': 188.1, 'adjLow': 184.55, 'adjOpen': 185.15, 'adjVolume': 34477053, 'divCash': 0.0, 'splitFactor': 1.0},
    {'date': '2025-04-23T00:00:00.000Z', 'close': 190.25, 'high': 195.51, 'low': 189.84, 'open': 192.92, 'volume': 44866133, 'adjClose': 190.25, 'adjHigh': 195.51, 'adjLow': 189.84, 'adjOpen': 192.92, 'adjVolume': 44866133, 'divCash': 0.0, 'splitFactor': 1.0}
]
