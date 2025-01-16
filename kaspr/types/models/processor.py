from typing import Optional, List, Awaitable, Any, Callable
from kaspr.utils import maybe_async, ensure_generator
from kaspr.exceptions import Skip
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

        async def _aprocessor(stream: KasprStreamT):
            init_scope = self.init.execute().scope if self.init else {}
            context = {"app": stream.app}
            ops = [operations[name] for name in self.pipeline]
            try:
                async for value in stream:
                    operation = ops[0]
                    operator = operation.operator
                    scope = {
                        **init_scope,
                        "context": {**context, "event": stream.current_event},
                    }
                    operator.with_scope(scope)
                    value = await operator.process(value)
                    if value == operator.skip_value:
                        continue
                    gen = ensure_generator(value)
                    for value in gen:
                        # Start with the initial value
                        current_values = [value]
                        for operation in ops[1:]:
                            operator = operation.operator
                            next_values = []
                            for current_value in current_values:
                                scope = {
                                    **init_scope,
                                    "context": {**context, "event": stream.current_event},
                                }
                                operator.with_scope(scope)
                                value = await operator.process(current_value)
                                if value == operator.skip_value:
                                    continue
                                # Collect all results
                                next_values.extend(ensure_generator(value))
                            # Update for the next callback
                            current_values = next_values

            except Exception as e:
                self.on_error(e)
                raise

        return _aprocessor

    def on_error(self, e: Exception):
        """Handle errors in the processor."""
        self.init.clear_scope()

    @property
    def processor(self) -> Callable[..., Awaitable[Any]]:
        if self._processor is None:
            self._processor = self.prepare_processor()
        return self._processor
