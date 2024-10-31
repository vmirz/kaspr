import abc
from typing import Union
from kaspr.types.stream import KasprStreamT

class AgentProcessorOperationT:
    
    @abc.abstractmethod
    def process(self, stream: KasprStreamT) -> Union["KasprStreamT", None]: ...    