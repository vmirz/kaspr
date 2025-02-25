from typing import Optional, TypeVar, Union, Dict, cast
from kaspr.utils.functional import maybe_async
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.topicout import TopicOutSpec
from kaspr.types.models.pycode import PyCode
from kaspr.types.operation import WebViewProcessorOperatorT

T = TypeVar("T")


class WebViewProcessorTopicSendOperator(WebViewProcessorOperatorT, TopicOutSpec):
    """Operator to send a message to a topic."""
    def with_scope(self, scope: Dict[str, T]):
        # Not applicable for this operator
        ...

    def execute(self): 
        # Not applicable for this operator
        ...

    async def process(self, value: T) -> T:
        if self.should_skip(value):
            return self.skip_value
        return await self.send(value)

class WebViewProcessorMapOperator(WebViewProcessorOperatorT, PyCode):
    """Operator to reformat a value."""
    async def process(self, value: T) -> T:
        return await maybe_async(self.func(value))


class WebViewProcessorOperation(SpecComponent):
    """Defines all possible operations in a WebViewProcessor."""
    name: str
    topic_send: Optional[WebViewProcessorTopicSendOperator]
    map: Optional[WebViewProcessorMapOperator]

    _operator: WebViewProcessorOperatorT = None

    def get_operator(self) -> WebViewProcessorOperatorT:
        """Get the specific operator type for this operation block."""
        return next((x for x in [self.topic_send, self.map] if x is not None), None)

    @property
    def operator(self) -> WebViewProcessorOperatorT:
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
