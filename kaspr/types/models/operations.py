from typing import Optional
from kaspr.types.models.base import BaseModel
from kaspr.types.stream import KasprStreamT
from kaspr.types.models.pycode import PyCode


class AgentProcessorOperationFilter(PyCode):
    
    def process(self, stream: KasprStreamT) -> KasprStreamT:
        return stream.filter(self.func)


class AgentProcessorOperation(BaseModel):
    name: str
    filter: Optional[AgentProcessorOperationFilter]

    def process(self, stream: KasprStreamT) -> KasprStreamT:
        """Process the stream with the provided operation."""
        if self.filter:
            return self.filter.process(stream)
        else:
            raise NotImplementedError("Operation not found.")
