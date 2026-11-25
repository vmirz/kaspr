"""Transport registry."""
from typing import Type
from kaspr.mode.utils.imports import FactoryMapping
from kaspr.faust.types import TransportT

__all__ = ['by_name', 'by_url']

TRANSPORTS: FactoryMapping[Type[TransportT]] = FactoryMapping(
    aiokafka='kaspr.faust.transport.drivers.aiokafka:Transport',
    confluent='kaspr.faust.transport.drivers.confluent:Transport',
    kafka='kaspr.faust.transport.drivers.aiokafka:Transport',
)
TRANSPORTS.include_setuptools_namespace('faust.transports')
by_name = TRANSPORTS.by_name
by_url = TRANSPORTS.by_url
