import inspect
from datetime import timedelta
from typing import Callable, TypeVar, Union, Awaitable, Optional, Dict
from kaspr.types.models.base import SpecComponent, BaseModel
from kaspr.types.app import KasprAppT
from kaspr.types.models.pycode import PyCode
from kaspr.types import KasprTableT

T = TypeVar("T")
Function = Callable[[T], Union[T, Awaitable[T]]]


class TableWindowTumblingSpec(BaseModel):
    size: int
    expires: Optional[timedelta]


class TableWindowHoppingSpec(BaseModel):
    size: int
    step: int
    expires: Optional[timedelta]


class TableWindowSpec(BaseModel):
    tumbling: Optional[TableWindowTumblingSpec]
    hopping: Optional[TableWindowHoppingSpec]
    relative_to: Optional[str]
    relative_to_selector: Optional[PyCode]


class TableSpec(SpecComponent):
    name: str
    description: Optional[str]
    is_global: Optional[bool]
    default_selector: Optional[PyCode]
    key_serializer: Optional[str]
    value_serializer: Optional[str]
    partitions: Optional[int]
    extra_topic_configs: Optional[Dict]
    options: Optional[Dict]
    window: Optional[TableWindowSpec]

    app: KasprAppT = None

    _table: KasprTableT

    def prepare_table(self) -> KasprTableT:
        """Prepare table instance."""
        # TODO: Implement window spec
        _Table = self.app.Table
        if self.is_global:
            _Table = self.app.GlobalTable
        return _Table(
            name=self.name,
            help=self.description,
            default_selector=self._default_type(),
            key_type=self._serializer_to_type(self.key_serializer),
            value_type=self._serializer_to_type(self.value_serializer),
            partitions=self.partitions,
            extra_topic_configs=self.extra_topic_configs,
        )

    def _serializer_to_type(self) -> T:
        """Map serializer to type."""
        if self.key_serializer == "raw":
            return bytes
        return None

    def _default_type(self) -> T:
        """Return table's default value type"""
        if self.default_selector is not None:
            _t = self.default_selector.func()
            if not inspect.isclass(_t):
                raise ValueError("Default selector must return a class type.")
            return _t
        return self.table.default

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
