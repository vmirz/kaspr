from typing import Optional, List, Awaitable, Any, Callable
from kaspr.types.models.base import BaseModel
from kaspr.types.models.operations import AgentProcessorOperation
from kaspr.types.models.pycode import PyCode
from kaspr.types.app import KasprAppT
from kaspr.types.stream import KasprStreamT


class AgentProcessorSpec(BaseModel):
    pipeline: Optional[List[str]]
    init: Optional[PyCode]
    operations: List[AgentProcessorOperation]

    app: KasprAppT = None

    _processor: Callable[..., Awaitable[Any]] = None

    def prepare_processor(self) -> Callable[..., Awaitable[Any]]:
        operations = {op.name: op for op in self.operations}

        async def _main(stream: KasprStreamT):
            init_scope = self.init.execute().scope if self.init else {}
            _stream = stream
            for name in self.pipeline:
                operation = operations[name]
                operation.operator.with_scope(init_scope)
                _stream = operation.operator.process(_stream)
            try:
                async for event in _stream:
                    print(event)
            except Exception as e:
                self.on_error(e)
                raise

        return _main

    def on_error(self, e: Exception):
        """Handle errors in the processor."""
        self.init.clear_scope()

    @property
    def processor(self) -> Callable[..., Awaitable[Any]]:
        if self._processor is None:
            self._processor = self.prepare_processor()
        return self._processor
