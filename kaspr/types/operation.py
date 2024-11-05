import abc
from typing import Union, TypeVar, Dict
from kaspr.types.stream import KasprStreamT
from kaspr.types.code import CodeT

T = TypeVar("T")

class AgentProcessorOperatorT(CodeT):
    
    @abc.abstractmethod
    def process(self, stream: KasprStreamT) -> Union["KasprStreamT", None]: ...    