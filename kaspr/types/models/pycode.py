"""PyCode model"""

from typing import Union, Callable, TypeVar, Awaitable, Optional, Dict
from kaspr.types.models.base import SpecComponent
from kaspr.types.code import CodeT

T = TypeVar("T")
Function = Callable[[T], Union[T, Awaitable[T]]]


class PyCode(CodeT, SpecComponent):
    """Python code definition"""

    python: str

    _scope: Dict[str, T]
    _func: Function

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._scope = {}
        self._func = None

    def with_scope(self, scope: Dict[str, T]) -> "PyCode":
        """Set the scope for executing source code"""
        self._scope = scope
        return self

    def clear_scope(self) -> "PyCode":
        """Clear the scope"""
        self._scope.clear()
        return self
    
    def execute(self) -> "PyCode":
        """Execute the source code"""
        exec(self.python, self._scope)
        # Find the first callable object in the scope - this will be the entrypoint function
        self._func = next((v for v in self._scope.values() if callable(v)), None)
        return self

    @property
    def func(self) -> Optional[Function]:
        self.execute()
        return self._func

    @property
    def scope(self) -> Dict[str, T]:
        return self._scope

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f'{type(self).__name__}'

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label