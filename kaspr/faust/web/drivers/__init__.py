"""Web server driver registry."""
from typing import Type
from kaspr.mode.utils.imports import FactoryMapping
from kaspr.faust.web.base import Web

__all__ = ['by_name', 'by_url']

DRIVERS: FactoryMapping[Type[Web]] = FactoryMapping(
    aiohttp='kaspr.faust.web.drivers.aiohttp:Web',
)
DRIVERS.include_setuptools_namespace('faust.web.drivers')
by_name = DRIVERS.by_name
by_url = DRIVERS.by_url
