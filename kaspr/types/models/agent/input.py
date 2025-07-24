from typing import Optional
from datetime import timedelta
from kaspr.utils.functional import parse_time_delta
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.topicsrc import TopicSrcSpec
from kaspr.types.models.channel import ChannelSpec
from kaspr.types.app import KasprAppT
from kaspr.types.channel import KasprChannelT

class AgentInputBufferSpec(SpecComponent):
    """Buffer specification for agent input."""
    
    max_size: int
    within: str

    app: KasprAppT = None
    _timeout: timedelta = None

    def prepare_timeout(self) -> KasprChannelT:
        return parse_time_delta(self.within)

    @property
    def timeout(self) -> timedelta:
        if self._timeout is None:
            self._timeout = self.prepare_timeout()
        return self._timeout
    
class AgentInputSpec(SpecComponent):
    """Agent input specification.
    Input can be either a topic or an in-memory channel.
    """
    declare: Optional[bool]
    topic_spec: Optional[TopicSrcSpec]
    channel_spec: Optional[ChannelSpec]
    buffer_spec: Optional[AgentInputBufferSpec]

    app: KasprAppT = None
    _channel: KasprChannelT = None

    def prepare_channel(self) -> KasprChannelT:
        return (
            self.topic_spec.topic
            if self.topic_spec and (self.topic_spec.name or self.topic_spec.pattern)
            else self.channel_spec.channel
        )

    @property
    def channel(self) -> KasprChannelT:
        if self._channel is None:
            self._channel = self.prepare_channel()
        return self._channel

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return (
            f"{type(self).__name__}: {self.topic_spec.name or self.topic_spec.pattern}"
        )

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label
