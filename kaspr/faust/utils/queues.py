"""Queues."""
from kaspr.mode.utils.queues import ThrowableQueue

from kaspr.faust.types import EventT

__all__ = ['EventQueue']


class EventQueue(ThrowableQueue):
    """Events waiting for a stream.

    A queue is cleared when the app rebalances or stops. The stream never
    sees what was in it, so each event's message is told not to expect an
    ack from this stream.
    """

    def clear(self) -> None:
        for event in self._queue:  # type: ignore
            if isinstance(event, EventT):
                event.message.drop()
        super().clear()
