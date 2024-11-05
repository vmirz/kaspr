import abc
from typing import Union, Dict, TypeVar
from kaspr.types.stream import KasprStreamT

T = TypeVar("T")

class CodeT:
    
    @abc.abstractmethod
    def with_scope(self, scope: Dict[str, T]) -> Union["CodeT", None]: ...

    @abc.abstractmethod
    def execute(self, stream: KasprStreamT) -> Union["CodeT", None]: ...    