from typing import Optional
from kaspr.types.models.base import SpecComponent
from kaspr.types.app import KasprAppT
from kaspr.types.topic import KasprTopicT


class TopicSrcSpec(SpecComponent):
    """Source topic specification."""

    name: str
    pattern: Optional[str]
    key_serializer: Optional[str]
    value_serializer: Optional[str]

    app: KasprAppT = None
    _topic: KasprTopicT = None

    def prepare_topic(self, **kwargs) -> KasprTopicT:
        """Prepare topic based on name and pattern."""
        if self.name:
            return self.app.topic(
                self.name,
                key_serializer=self.key_serializer,
                value_serializer=self.value_serializer,
            )
        else:
            # XXX pattern does not work yet
            return self.app.topic(
                pattern=self.pattern,
                key_serializer=self.key_serializer,
                value_serializer=self.value_serializer,
            )

    @property
    def topic(self) -> KasprTopicT:
        if self._topic is None:
            self._topic = self.prepare_topic()
        return self._topic

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f"{type(self).__name__}: {','.join(self.topic.topics)}"

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label
