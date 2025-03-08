from typing import Optional, Iterable, TypeVar, cast
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.topicout import TopicOutSpec
from kaspr.types.app import KasprAppT
from kaspr.types.channel import KasprChannelT
from kaspr.types.topic import KasprTopicT

T = TypeVar("T")


class AgentOutputSpec(SpecComponent):
    """Output specification.
    It is used to define how to send processed values to topics, channels, and callables.
    """

    topics_spec: Optional[TopicOutSpec]
    app: KasprAppT = None

    _topics: KasprTopicT = None
    _channel: KasprChannelT = None

    async def send(self, value: T):
        """Send value to all topics, channels, and callables."""
        if self.topics_spec:
            for ts in self.topics_spec:
                ts = cast(TopicOutSpec, ts)
                if ts.should_skip(value):
                    continue
                await ts.topic.send(
                    key_serializer=ts.key_serializer,
                    value_serializer=ts.value_serializer,
                    key=ts.get_key(value),
                    value=ts.get_value(value),
                    partition=ts.get_partition(value),
                    headers=ts.get_headers(value),
                )

    def prepre_topics(self) -> Iterable[KasprTopicT]:
        if self.topics_spec:
            return [topic.topic for topic in self.topics_spec]
        return []

    @property
    def topics(self) -> Iterable[KasprTopicT]:
        if self._topics is None:
            self._topics = self.prepre_topics()
        return self._topics

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f"{type(self).__name__}: {', '.join(self.topics)}"

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label
