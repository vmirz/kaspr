import typing
from faust.types import ServiceT
from mode.utils.locks import Event

if typing.TYPE_CHECKING:
    from .app import KasprAppT as _KasprAppT
else:

    class _KasprAppT:
        ...  # noqa


class CronTickerT(ServiceT):
    """Abstract type for the cron ticker service."""

    app: _KasprAppT

    partition: int
    can_resume: Event
    flow_active: bool

    def pause(self):
        ...

    def resume(self):
        ...
