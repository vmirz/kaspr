import abc
from typing import Union, TypeVar
from kaspr.types.code import CodeT

T = TypeVar("T")


class AgentProcessorOperatorT(CodeT):
    skip_value = object()

    @abc.abstractmethod
    async def process(self, value: T) -> Union["T", None]: ...


class WebViewProcessorOperatorT(CodeT):
    skip_value = object()

    @abc.abstractmethod
    async def process(self, value: T) -> Union["T", None]: ...
