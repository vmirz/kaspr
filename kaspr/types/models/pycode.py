"""PyCode model"""

import inspect
from typing import Union, Callable, TypeVar, Awaitable
from kaspr.types.models.base import BaseModel

T = TypeVar('T')
Function = Callable[[T], Union[T, Awaitable[T]]]

class PyCode(BaseModel):
    """Dynamic python code"""

    python: str

    _func: Function = None

    @property
    def func(self) -> Function:
        """Callable function derived from python string"""
        if self._func is None:
            if self.python:
                local_scope = {}
                exec(self.python, {}, local_scope)
                self._func = next(v for v in local_scope.values() if callable(v))
                if not self._func:
                    raise ValueError("Python function not defined!")
        return self._func
    
    @property
    def is_async(self) -> bool:
        """Check if the function is async"""
        if self.func is None:
            return False
        return inspect.iscoroutinefunction(self.func)
    
    @property
    def signature(self) -> inspect.Signature:
        """Signature of the function"""
        if self.func is None:
            return None
        return inspect.signature(self.func)
    
