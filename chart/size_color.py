""""""
import logging, logging.config
from time import sleep

# from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    TimeoutException,
)
from data import ctx


logging.config.fileConfig(fname="../logger.ini")
logging.getLogger("urllib").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def webscraper():
    """"""
    logger.debug(f"ctx={ctx}\n")
    base_url = ctx['chart_service']['base_url']

    # opt = ChromeOptions()
    opt = FirefoxOptions()
    opt.add_argument("--headless=new")
    # opt.add_argument("--user-agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'")
    opt.add_argument("--user-agent='Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0'")
    # opt.page_load_strategy = "eager"
    opt.page_load_strategy = "none"
    # driver = Chrome(options=opt)
    driver = Firefox(options=opt)
    driver.get(base_url)
    agent = driver.execute_script("return navigator.userAgent")
    logger.debug(f"\ndriver: {driver},\nagent: {agent}\n")

    try:
        _set_chart_size(driver=driver)
        _set_color_dark(driver=driver)
        _set_indicator(driver=driver)
        _click_button(driver=driver)
        return _get_chart_url(driver=driver)
    except (ElementClickInterceptedException,
            ElementNotInteractableException,
            TimeoutException,) as e:
        logger.debug(f"*** ERROR *** {e}")
    finally:
        driver.close()


def _click_button(driver: object):
    """"""
    button = WebDriverWait(driver=driver, timeout=10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[5]/div[2]/div[1]/div[1]/div[3]/button[1]")
        # EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div[6]/div[2]/div[1]/div[1]/div[3]/button[1]")
    ))
    loc = button.location_once_scrolled_into_view
    logger.debug(f"button: {button}, loc: {loc}")
    button.click()


def _get_chart_url(driver: object)->str:
    """"""
    sleep(1)
    img_element = WebDriverWait(driver=driver, timeout=10).until(
        # EC.presence_of_element_located((By.CSS_SELECTOR, '#chart-image'))
        EC.presence_of_element_located((By.XPATH, '//*[@id="chart-image"]'))
    )
    loc = img_element.location_once_scrolled_into_view
    logger.debug(f"img_element: {img_element}, loc: {loc}")

    return img_element.get_attribute("src")


def _set_color_dark(driver: object):
    """"""
    color_element = WebDriverWait(driver=driver, timeout=10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="chart-settings-color-scheme-menu"]')
    ))
    loc = color_element.location_once_scrolled_into_view
    logger.debug(f"color_element: {color_element}, loc: {loc}")

    color = Select(color_element)
    color.select_by_value("night")
    logger.debug(f"color: {color}")


def _set_indicator(driver: object):
    """"""
    indicator_element = WebDriverWait(driver=driver, timeout=10).until(
        # EC.element_to_be_clickable((By.CSS_SELECTOR, "#indicator-menu-1")
        EC.element_to_be_clickable((By.XPATH, '//*[@id="indicator-menu-1"]')
    ))
    loc = indicator_element.location_once_scrolled_into_view
    logger.debug(f"indicator_element: {indicator_element}, loc: {loc}")

    indicator = Select(indicator_element)
    indicator.select_by_value("RSI")
    logger.debug(f"indicator: {indicator}")


def _set_chart_size(driver: object):
    """"""
    size_element = WebDriverWait(driver=driver, timeout=10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="chart-settings-chart-size-menu"]')
    ))
    loc = size_element.location_once_scrolled_into_view
    logger.debug(f"size_element: {size_element}, loc: {loc}")

    size = Select(size_element)
    size.select_by_value("Landscape")
    logger.debug(f"size: {size}")


if __name__ == "__main__":
    url = webscraper()
    logger.debug(f"url: {url}")
