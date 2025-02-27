from typing import Optional, List, TypeVar, Callable, Union, Awaitable, OrderedDict
from kaspr.types.models.base import SpecComponent
from kaspr.types.app import KasprAppT
from kaspr.types.topic import KasprTopicT
from kaspr.types.models.pycode import PyCode

T = TypeVar("T")
Function = Callable[[T], Union[T, Awaitable[T]]]


class AgentOutputTopicKeySelector(PyCode): ...


class AgentOutputTopicValueSelector(PyCode): ...


class AgentOutputTopicPartitionSelector(PyCode): ...


class AgentOutputTopicHeadersSelector(PyCode): ...


class AgentOutputTopicPredicate(PyCode): ...


class TopicOutSpec(SpecComponent):
    """Output topic specification."""
    
    name: List[str]
    ack: Optional[bool]
    key_serializer: Optional[str]
    value_serializer: Optional[str]
    key_selector: Optional[AgentOutputTopicKeySelector]
    value_selector: Optional[AgentOutputTopicValueSelector]
    partition_selector: Optional[AgentOutputTopicPartitionSelector]
    headers_selector: Optional[AgentOutputTopicHeadersSelector]
    predicate: Optional[AgentOutputTopicPredicate]

    app: KasprAppT = None
    _topic: KasprTopicT = None
    _key_selector_func: Function = None
    _value_selector_func: Function = None
    _partition_selector_func: Function = None
    _headers_selector_func: Function = None
    _predicate_func: Function = None

    async def send(self, value: T, **kwargs) -> Union[None, OrderedDict]:
        """Send value to topic according to spec.
        
        If ack is True, returns metadata (offset, timestamp, etc).
        of the sent message.
        """
        res = await self.topic.send(
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
            key=self.get_key(value, **kwargs),
            value=self.get_value(value, **kwargs),
            partition=self.get_partition(value, **kwargs),
            headers=self.get_headers(value, **kwargs),
        )
        if self.ack:
            return (await res)._asdict()
                
    def get_key(self, value: T, **kwargs) -> T:
        """Get key from value"""
        if self.key_selector_func is not None:
            return self.key_selector_func(value, **kwargs)

    def get_value(self, value: T, **kwargs) -> T:
        """Get value from value"""
        if self.value_selector_func is not None:
            return self.value_selector_func(value, **kwargs)
        else:
            return value

    def get_partition(self, value: T, **kwargs) -> T:
        """Get partition from value"""
        if self.partition_selector_func is not None:
            return self.partition_selector_func(value, **kwargs)

    def get_headers(self, value: T, **kwargs) -> T:
        """Get headers from value"""
        if self.headers_selector_func is not None:
            return self.headers_selector_func(value, **kwargs)

    def should_skip(self, value: T, **kwargs) -> bool:
        """Check if value should be skipped"""
        if self.predicate_func is not None:
            return not self.predicate_func(value, **kwargs)
        else:
            return False

    def prepare_topic(self):
        return self.app.topic(
            self.name,
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
        )

    @property
    def topic(self) -> KasprTopicT:
        """Topic instance"""
        if self._topic is None:
            self._topic = self.prepare_topic()
        return self._topic

    @property
    def key_selector_func(self):
        """Key selector function"""
        if self._key_selector_func is None and self.key_selector:
            self._key_selector_func = self.key_selector.func
        return self._key_selector_func

    @property
    def value_selector_func(self):
        """Value selector function"""
        if self._value_selector_func is None and self.value_selector:
            self._value_selector_func = self.value_selector.func
        return self._value_selector_func

    @property
    def partition_selector_func(self):
        """Partition selector function"""
        if self._partition_selector_func is None and self.partition_selector:
            self._partition_selector_func = self.partition_selector.func
        return self._partition_selector_func

    @property
    def headers_selector_func(self):
        """Headers selector function"""
        if self._headers_selector_func is None and self.headers_selector:
            self._headers_selector_func = self.headers_selector.func
        return self._headers_selector_func

    @property
    def predicate_func(self):
        """Predicate function"""
        if self._predicate_func is None and self.predicate:
            self._predicate_func = self.predicate.func
        return self._predicate_func

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f"{type(self).__name__}: {','.join(self.topic.topics)}"

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label
