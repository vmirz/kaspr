from typing import Dict, Optional
from kaspr.types.models.base import BaseModel
from kaspr.types.stream import KasprStreamT
from kaspr.types.models.pycode import PyCode
from kaspr.types.operation import AgentProcessorOperatorT


class AgentProcessorFilterOperator(AgentProcessorOperatorT, PyCode):
    
    def process(self, stream: KasprStreamT) -> KasprStreamT:
        return stream.filter(self.func)


class AgentProcessorMapOperator(AgentProcessorOperatorT, PyCode):
    def process(self, stream: KasprStreamT) -> KasprStreamT:
        stream.add_processor(self.func)
        return stream


class AgentProcessorOperation(BaseModel):
    name: str
    filter: Optional[AgentProcessorFilterOperator]
    map: Optional[AgentProcessorMapOperator]

    _operator: AgentProcessorOperatorT = None
    
    def get_operator(self) -> AgentProcessorOperatorT:
        """Get the specific operator type for this operation block."""
        return next((x for x in [self.filter, self.map] if x is not None), None)

    @property
    def operator(self) -> AgentProcessorOperatorT:
        if self._operator is None:
            self._operator = self.get_operator()
        return self._operator
