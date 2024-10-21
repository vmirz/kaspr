import abc
from mode import Seconds
from typing import AsyncIterable, Sequence
from faust.types import StreamT, EventT


class KasprStreamT(StreamT):
    """Abstract type for the janitor service."""

    @abc.abstractmethod
    async def take_events(
        self, max_: int, within: Seconds
    ) -> AsyncIterable[Sequence[EventT]]:
        ...
