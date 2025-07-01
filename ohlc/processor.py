import logging
from collections import namedtuple

from src import debug


logger = logging.getLogger(__name__)


def close_location_value(tuple_list):
    """"""
    if debug: logger.debug(f"close_location_value(tuple_list={tuple_list})")

    CLV = namedtuple('CLV', ['symbol', 'date', 'clv'])
    clv_list =[]

    for item in tuple_list:
        close_location_value = CLV(
            item.symbol,
            item.date,
            round(((2 * item.close - item.low - item.high) / (item.high - item.low)) * 100)
        )
        clv_list.append(close_location_value)
    
    if debug: logger.debug(f"close_location_value() -> clv_list:\n{clv_list})")
    return clv_list


def close_weighted_price(tuple_list):
    """"""
    if debug: logger.debug(f"close_weighted_price(tuple_list={tuple_list})")

    Price = namedtuple('Price', ['symbol', 'date', 'price'])
    price_list =[]

    for item in tuple_list:
        close_weighted_price = Price(
            item.symbol,
            item.date,
            round((item.high + item.low + item.close * 2) / 4)
        )
        price_list.append(close_weighted_price)
    
    if debug: logger.debug(f"close_weighted_price() -> price_list:\n{price_list})")
    return price_list


def price_volume_mass(tuple_list):
    """"""
    if debug: logger.debug(f"price_volume_mass(tuple_list={tuple_list})")

    Mass = namedtuple('Mass', ['symbol', 'date', 'mass'])
    mass_list =[]

    for item in tuple_list:
        price_volume_mass = Mass(
            item.symbol,
            item.date,
            round((item.high + item.low + item.close * 2) / 4) * item.volume
        )
        mass_list.append(price_volume_mass)

    if debug: logger.debug(f"price_volume_mass() -> mass_list:\n{mass_list}")
    return mass_list


def volume_data(tuple_list):
    """"""
    if debug: logger.debug(f"volume_data(tuple_list={tuple_list})")

    Volume = namedtuple('Volume', ['symbol', 'date', 'volume'])
    volume_list =[]

    for item in tuple_list:
        volume = Volume(
            item.symbol,
            item.date,
            item.volume
        )
        volume_list.append(volume)
    
    if debug: logger.debug(f"volume_data() -> volume_list:\n{volume_list})")
    return volume_list
