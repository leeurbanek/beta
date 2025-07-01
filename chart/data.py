"""test/data.py"""


ctx = {
    'default': {
        'debug': True,
        'work_dir': '/home/la/dev/stomartat/temp/',
        'cfg_chart': '/home/la/dev/stomartat/src/pkg/chart_srv/cfg_chart.ini',
        'cfg_main': '/home/la/dev/stomartat/src/config.ini'
    },
    'interface': {
        'opt_trans': ['Daily', 'Weekly'],
        'arguments': ['EEM', 'IAU', 'IWM', 'LQD', 'SLV']
    },
    'chart_service': {
        'adblock': '',
        'base_url': 'https://stockcharts.com/sc3/ui/?s=AAPL',
        'chart_light': 'https://stockcharts.com/c-sc/sc?s=AAPL&p=D&yr=1&mn=0&dy=0&i=t0421500702c&r=1741298195592',
        'chart_dark': 'https://stockcharts.com/c-sc/sc?s=AAPL&p=D&yr=1&mn=0&dy=0&i=t6555263975c&r=1741298265509',
        'chart_skin': 'dark',
        'chart_list': 'EEM IWM LQD IAU SLV',
        'driver': 'geckodriver',
        'scraper': 'requests'
    }
}
