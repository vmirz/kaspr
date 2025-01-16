from typing import Optional, TypeVar
from kaspr.utils import maybe_async
from kaspr.exceptions import Skip
from kaspr.types.models.base import BaseModel
from kaspr.types.models.pycode import PyCode
from kaspr.types.operation import AgentProcessorOperatorT

T = TypeVar("T")

class AgentProcessorFilterOperator(AgentProcessorOperatorT, PyCode):
    
    async def process(self, value: T) -> T:
        if not await maybe_async(self.func(value)):
            return self.skip_value
        else:
            return value


class AgentProcessorMapOperator(AgentProcessorOperatorT, PyCode):
    async def process(self, value: T) -> T:
        return await maybe_async(self.func(value))


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
