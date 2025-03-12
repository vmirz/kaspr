from typing import Callable, TypeVar, Union, Awaitable, Optional, Dict
from kaspr.types.models.base import SpecComponent
from kaspr.types.app import KasprAppT
from kaspr.types.models.pycode import PyCode
from kaspr.types import KasprTableT

T = TypeVar("T")
Function = Callable[[T], Union[T, Awaitable[T]]]


class TableSpec(SpecComponent):
    name: str
    description: Optional[str]
    is_global: Optional[bool]
    default_selector: Optional[PyCode]
    key_serializer: Optional[str]
    value_serializer: Optional[str]
    partitions: Optional[int]
    extra_topic_configs: Optional[Dict]

    app: KasprAppT = None

    _table: KasprTableT

    def prepare_table(self) -> KasprTableT:
        key_type = None
        value_type = None
        default = None
        if self.key_serializer == "raw":
            key_type = bytes
        if self.value_serializer == "raw":
            value_type = bytes
        if self.default_selector is not None:
            default = self.default_selector.func()
        if self.is_global:
            _Table = self.app.GlobalTable
        else:
            _Table = self.app.Table
        return _Table(
            name=self.name,
            help=self.description,
            default_selector=default,
            key_type=key_type,
            value_type=value_type,
            partitions=self.partitions,
            extra_topic_configs=self.extra_topic_configs,
        )

    @property
    def table(self) -> KasprTableT:
        if self._table is None:
            self._table = self.prepare_table()
        return self._table

    @property
    def label(self) -> str:
        """Return description of component, used in logs."""
        return f"{type(self).__name__}: {self.__repr__()}"

    @property
    def shortlabel(self) -> str:
        """Return short description of table."""
        return f"{type(self).__name__}: {self.name}"
