from typing import Optional, TypeVar
from kaspr.utils.functional import maybe_async
from kaspr.types.models.base import SpecComponent
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


class AgentProcessorOperation(SpecComponent):
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

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f'{type(self).__name__}: {self.name}'

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label