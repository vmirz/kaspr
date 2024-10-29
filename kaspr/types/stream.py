import abc
from mode import Seconds
from typing import AsyncIterable, Sequence, TypeVar, Union, Awaitable, Callable
from faust.types import StreamT, EventT

# Used for typing StreamT[Withdrawal]
T = TypeVar("T")

Processor = Callable[[T], Union[T, Awaitable[T]]]


class KasprStreamT(StreamT):
    """Abstract type for the janitor service."""

    @abc.abstractmethod
    async def take_events(
        self, max_: int, within: Seconds
    ) -> AsyncIterable[Sequence[EventT]]: ...

    @abc.abstractmethod
    def filter(self, fun: Processor[T]) -> "KasprStreamT": ...
