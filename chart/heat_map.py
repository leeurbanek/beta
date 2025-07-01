""""""
import logging, logging.config
import os
from io import BytesIO
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from PIL import Image

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

base_url = "https://stockanalysis.com/markets/heatmap/"
heat_map = ('1W', '1M', '3M', '6M')


def webscraper():
    """"""
    logger.debug(f"ctx={ctx}\n")

    # opt = ChromeOptions()
    opt = FirefoxOptions()
    opt.add_argument("--headless=new")
    # opt.add_argument("--user-agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'")
    opt.add_argument("--user-agent='Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0'")
    # opt.page_load_strategy = "eager"
    opt.page_load_strategy = "none"
    # driver = Chrome(options=opt)
    # driver = Firefox(options=opt)

    for period in heat_map:
        try:
            driver = Firefox(options=opt)
            mod_url = _modify_query_time_period(period=period)
            driver.get(mod_url)
            image_src = _get_png_img_bytes(driver=driver)
            _save_png_image(image_src=image_src, period=period)
        except (ElementClickInterceptedException,
                ElementNotInteractableException,
                TimeoutException,) as e:
            logger.debug(f"*** ERROR *** {e}")
        finally:
            driver.quit()


def _modify_query_time_period(period: str):
    """Use urllib.parse to modify the default query parameters
    with new period, symbol.
    """
    logger.debug(f"_modify_query_time_period(period={period})")

    parsed_url = urlparse(url=base_url)
    query_dict = parse_qs(parsed_url.query)
    query_dict['time'] = period
    encoded_params = urlencode(query_dict, doseq=True)
    url = urlunparse(parsed_url._replace(query=encoded_params))
    logger.debug(f"mod_url: {url}")

    return url


def _get_png_img_bytes(driver: object):
    """Get the chart image source and convert the bytes to
    a .png image then save to the chart work directory.
    """
    logger.debug(f"_get_png_img_bytes(driver{driver})")

    canvas_element = WebDriverWait(driver=driver, timeout=10).until(
        EC.presence_of_element_located((
            # By.XPATH, "/html/body/div/div[1]/div[2]/main/div[2]/div[1]/div/div/svg[1]")
            By.CSS_SELECTOR, "svg.main-svg:nth-child(1)")
            # By.CSS_SELECTOR, "g.slice:nth-child(1) > path:nth-child(1)")
            # By.XPATH, "/html/body/div/div[1]/div[2]/main/div[2]/div[1]/div/div/svg[1]/g[12]/g/g[1]/path")
        ))


    loc = canvas_element.location_once_scrolled_into_view
    logger.debug(f"canvas_element: {canvas_element}, loc: {loc}")

    # return Image.open(BytesIO(canvas_element.data)).convert('RGB')
    return canvas_element.screenshot_as_png


def _save_png_image(image_src: bytes, period: str):
    """"""
    logger.debug(f"_save_png_image(period={period})")

    # image_src = self.http.request('GET', mod_url, headers={'User-agent': 'Mozilla/5.0'})
    png_image = Image.open(BytesIO(image_src)).convert('RGB')
    # png_image.save(os.path.join('/chart', f"heatmap_{period}.png"), 'PNG', quality=80)
    png_image.save(f'heatmap_{period}.png', 'PNG', quality=80)


if __name__ == "__main__":
    webscraper()
