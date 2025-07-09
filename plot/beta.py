""""""

import logging, logging.config


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logger = logging.getLogger(__name__)
