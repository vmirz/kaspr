from typing import Optional, TypeVar, Dict
from kaspr.utils.functional import maybe_async
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.topicout import TopicOutSpec
from kaspr.types.models.pycode import PyCode
from kaspr.types.operation import ProcessorOperatorT

T = TypeVar("T")


class WebViewProcessorTopicSendOperator(ProcessorOperatorT, TopicOutSpec):
    """Operator to send a message to a topic."""
    def with_scope(self, scope: Dict[str, T]):
        # Not applicable for this operator
        ...

    def execute(self): 
        # Not applicable for this operator
        ...

    async def process(self, value: T, **kwargs) -> T:
        if self.should_skip(value, **kwargs):
            return self.skip_value
        return await self.send(value, **kwargs)

class WebViewProcessorMapOperator(ProcessorOperatorT, PyCode):
    """Operator to reformat a value."""
    async def process(self, value: T, **kwargs) -> T:
        return await maybe_async(self.func(value, **kwargs))


class WebViewProcessorOperation(SpecComponent):
    """Defines all possible operations in a WebViewProcessor."""
    name: str
    topic_send: Optional[WebViewProcessorTopicSendOperator]
    map: Optional[WebViewProcessorMapOperator]

    _operator: ProcessorOperatorT = None

    def get_operator(self) -> ProcessorOperatorT:
        """Get the specific operator type for this operation block."""
        return next((x for x in [self.topic_send, self.map] if x is not None), None)

    @property
    def operator(self) -> ProcessorOperatorT:
        if self._operator is None:
            self._operator = self.get_operator()
        return self._operator

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f"{type(self).__name__}: {self.name}"

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label
