import abc
import typing
from typing import Optional
from kaspr.types import TTLocation
from faust.types import ServiceT
from mode.utils.objects import cached_property
from mode.utils.locks import Event

if typing.TYPE_CHECKING:
    from .app import KasprAppT as _KasprAppT
else:

    class _KasprAppT:
        ...  # noqa


class DispatcherT(ServiceT):
    """Abstract type for the dispatcher service."""

    app: _KasprAppT

    partition: int
    can_resume: Event
    flow_active: bool

    delivered_total: int

    def pause(self):
        ...

    def resume(self):
        ...

    @property
    @abc.abstractmethod
    def default_checkpoint(self) -> TTLocation:
        ...

    @property
    @abc.abstractmethod
    def highwater(self) -> TTLocation:
        ...

    @cached_property
    def pt(self):
        ...

    @cached_property
    def name(self):
        ...

    @property
    def last_location(self) -> Optional[TTLocation]:
        ... 

    async def wait_empty(self) -> None:
        ...  