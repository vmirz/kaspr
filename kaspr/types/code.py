import abc
from typing import Union, Dict, TypeVar, Callable, Awaitable

T = TypeVar("T")

Function = Callable[[T], Union[T, Awaitable[T]]]

class CodeT:
    
    @abc.abstractmethod
    def with_scope(self, scope: Dict[str, T]) -> Union["CodeT", None]: ...

    @abc.abstractmethod
    def execute(self) -> Union["CodeT", None]: ...    

    @property
    def func(self) -> Callable[[T], Union[T, Awaitable[T]]]:
        ...