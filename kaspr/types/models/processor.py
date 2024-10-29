from typing import Optional, List, Awaitable, Any, Callable
from kaspr.types.models.base import BaseModel
from kaspr.types.models.operations import AgentProcessorOperation
from kaspr.types.app import KasprAppT
from kaspr.types.stream import KasprStreamT

class AgentProcessorSpec(BaseModel):

    pipeline: Optional[List[str]]
    operations: List[AgentProcessorOperation]

    app: KasprAppT = None

    _processor: Callable[..., Awaitable[Any]] = None

    def prepare_processor(self) -> Callable[..., Awaitable[Any]]:
        ops = { op.name: op for op in self.operations }
        async def _main_processor(stream: KasprStreamT):
            _stream = stream
            for op_name in self.pipeline:
                _stream = ops[op_name].process(_stream)
            async for event in _stream:
                print(event)
        return _main_processor
    
    @property
    def processor(self) -> Callable[..., Awaitable[Any]]:
        if self._processor is None:
            self._processor = self.prepare_processor()
        return self._processor
