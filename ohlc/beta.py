""""""
import logging, logging.config


DEBUG = True

logging.config.fileConfig(fname='../logger.ini')
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main(ctx: dict) -> None:
    if DEBUG:
        logger.debug(f"main(ctx={ctx})")


if __name__ == "__main__":
    if DEBUG:
        logger.debug(f"******* START - beta.py.main() *******")

    ctx = {}

    main(ctx=ctx)
