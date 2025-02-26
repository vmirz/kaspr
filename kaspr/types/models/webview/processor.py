from typing import Optional, List, Awaitable, Any, Callable
from kaspr.utils.functional import ensure_generator
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.webview.operations import WebViewProcessorOperation
from kaspr.types.models.webview.response import WebViewResponseSpec
from kaspr.types.models.pycode import PyCode
from kaspr.types.app import KasprAppT
from kaspr.types.webview import KasprWebRequest, KasprWeb
from kaspr.exceptions import KasprProcessingError


class WebViewProcessorSpec(SpecComponent):
    """Processor specification."""

    pipeline: Optional[List[str]]
    init: Optional[PyCode]
    operations: List[WebViewProcessorOperation]

    app: KasprAppT = None

    _processor: Callable[..., Awaitable[Any]] = None
    _response: WebViewResponseSpec = None

    def prepare_processor(self) -> Callable[..., Awaitable[Any]]:
        operations = {op.name: op for op in self.operations}

        async def _request_processor(web: KasprWeb, request: KasprWebRequest):
            try:
                init_scope = self.init.execute().scope if self.init else {}
                context = {"app": self.app}
                ops = [operations[name] for name in self.pipeline]
                operation = ops[0]
                operator = operation.operator
                scope = {
                    **init_scope,
                    "context": {**context},
                }
                operator.with_scope(scope)
                result = await operator.process(request)
                if result == operator.skip_value:
                    return
                gen = ensure_generator(result)
                for value in gen:
                    # Start with the initial value
                    current_values = [value]
                    for operation in ops[1:]:
                        next_values = []
                        operator = operation.operator
                        for current_value in current_values:
                            scope = {
                                **init_scope,
                                "context": {**context},
                            }
                            operator.with_scope(scope)
                            result = await operator.process(current_value)
                            if result == operator.skip_value:
                                continue
                            # Collect all results
                            next_values.extend(ensure_generator(result))
                        # Update for the next callback
                        current_values = next_values

                    return self.response.build_success(web, current_values[0])
                
            except Exception as ex:
                error = KasprProcessingError(
                    message=str(ex),
                    cause=ex,
                    operation=operation.name,
                )
                return self.response.build_error(web, error)

        return _request_processor

    def on_error(self, e: Exception):
        """Handle errors in the processor."""
        self.init.clear_scope()

    @property
    def processor(self) -> Callable[..., Awaitable[Any]]:
        if self._processor is None:
            self._processor = self.prepare_processor()
        return self._processor

    @property
    def response(self) -> WebViewResponseSpec:
        return self._response

    @response.setter
    def response(self, response: WebViewResponseSpec):
        self._response = response

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f"{type(self).__name__}"

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label
