from typing import Optional, List, Awaitable, Any, Callable
from kaspr.utils.functional import ensure_generator
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.agent.operations import AgentProcessorOperation
from kaspr.types.models.agent.output import AgentOutputSpec
from kaspr.types.models.pycode import PyCode
from kaspr.types.app import KasprAppT
from kaspr.types.stream import KasprStreamT

class AgentProcessorSpec(SpecComponent):
    """Processor specification."""
    
    pipeline: Optional[List[str]]
    init: Optional[PyCode]
    operations: List[AgentProcessorOperation]

    app: KasprAppT = None

    _processor: Callable[..., Awaitable[Any]] = None
    _output: AgentOutputSpec = None

    def prepare_processor(self) -> Callable[..., Awaitable[Any]]:
        operations = {op.name: op for op in self.operations}
        output = self.output    

        async def _aprocessor(stream: KasprStreamT):
            init_scope = self.init.execute().scope if self.init else {}
            context = {"app": self.app}
            ops = []
            for name in self.pipeline:
                if name not in operations:
                    raise ValueError(f"Operation '{name}' is not defined.")
                ops.append(operations[name])
            if not ops:
                return
            try:
                async for value in stream:
                    operation = ops[0]
                    operator = operation.operator
                    tables = operation.tables
                    scope = {
                        **init_scope,
                        "context": {**context, "event": stream.current_event},
                    }
                    operator.with_scope(scope)
                    value = await operator.process(value, **tables)
                    if value == operator.skip_value:
                        continue
                    gen = ensure_generator(value)
                    for value in gen:
                        # Start with the initial value
                        current_values = [value]
                        for operation in ops[1:]:
                            next_values = []
                            operator = operation.operator
                            tables = operation.tables
                            for current_value in current_values:
                                scope = {
                                    **init_scope,
                                    "context": {**context, "event": stream.current_event},
                                }
                                operator.with_scope(scope)
                                value = await operator.process(current_value, **tables)
                                if value == operator.skip_value:
                                    continue
                                # Collect all results
                                next_values.extend(ensure_generator(value))
                            # Update for the next callback
                            current_values = next_values

                        for value in current_values:
                            # for sink in self.:
                            #     if isinstance(sink, AgentT):
                            #         await sink.send(value=value)
                            #     elif isinstance(sink, ChannelT):
                            #         await cast(TopicT, sink).send(value=value)
                            #     else:
                            #         await maybe_async(cast(Callable, sink)(value))
                            # self.log.info(f"Processed value: {value}")

                            if output:
                                await output.send(value)
                            #yield value

            except Exception as e:
                self.on_error(e)
                raise

        return _aprocessor

    def on_error(self, e: Exception):
        """Handle errors in the processor."""
        if self.init:
            self.init.clear_scope()

    @property
    def processor(self) -> Callable[..., Awaitable[Any]]:
        if self._processor is None:
            self._processor = self.prepare_processor()
        return self._processor
    
    @property
    def output(self) -> AgentOutputSpec:
        return self._output
    
    @output.setter
    def output(self, outputs: AgentOutputSpec):
        self._output = outputs
    
    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f'{type(self).__name__}'

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label    