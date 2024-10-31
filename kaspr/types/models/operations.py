from typing import Optional
from kaspr.types.models.base import BaseModel
from kaspr.types.stream import KasprStreamT
from kaspr.types.models.pycode import PyCode
from kaspr.types.operation import AgentProcessorOperationT


class AgentProcessorOperationFilter(AgentProcessorOperationT, PyCode):
    def process(self, stream: KasprStreamT) -> KasprStreamT:
        return stream.filter(self.func)


class AgentProcessorOperationMap(AgentProcessorOperationT, PyCode):
    def process(self, stream: KasprStreamT) -> KasprStreamT:
        stream.add_processor(self.func)
        return stream

class AgentProcessorOperation(BaseModel):
    name: str
    init: Optional[PyCode]
    filter: Optional[AgentProcessorOperationFilter]
    map: Optional[AgentProcessorOperationMap]

    _operation: AgentProcessorOperationT = None

    def process(self, stream: KasprStreamT) -> KasprStreamT:
        """Process the stream with the provided operation."""
        return self.operation.process(stream)

    def prepare_operation(self) -> AgentProcessorOperationT:
        return next((x for x in [self.filter, self.map] if x is not None), None)

    @property
    def operation(self) -> AgentProcessorOperationT:
        if self._operation is None:
            self._operation = self.prepare_operation()
        return self._operation
