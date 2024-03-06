import typing
from typing import Optional
from faust.types import ServiceT
from kaspr.types.tuples import TTLocation, PT
from mode.utils.locks import Event

if typing.TYPE_CHECKING:
    from .app import KasprAppT as _KasprAppT
else:

    class _KasprAppT:
        ...  # noqa


class CheckpointT(ServiceT):
    """Abstract type for the checkpoint service."""

    app: _KasprAppT
    can_resume: Event
    flow_active: bool

    dispatcher_checkpointed: Event

    def pause(self):
        ...

    def resume(self):
        ...

    def update(self, tp: PT, location: TTLocation):
        ...

    def get(self, tp: PT, default: TTLocation = None) -> Optional[TTLocation]:
        ...

    async def flush(self):
        ...

    def on_rebalance_started(self) -> None:
        ...        