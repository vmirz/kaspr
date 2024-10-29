from typing import Optional
from kaspr.types.models.base import BaseModel
from kaspr.types.models.topicsrc import TopicSrcSpec
from kaspr.types.models.channel import ChannelSpec
from kaspr.types.app import KasprAppT
from kaspr.types.channel import KasprChannelT


class AgentInputSpec(BaseModel):
    topic_spec: Optional[TopicSrcSpec]
    channel_spec: Optional[ChannelSpec]

    app: KasprAppT = None
    _channel: KasprChannelT = None

    def prepre_channel(self) -> KasprChannelT:
        return (
            self.topic_spec.topic
            if self.topic_spec and (self.topic_spec.names or self.topic_spec.pattern)
            else self.channel_spec.channel
        )

    @property
    def channel(self) -> KasprChannelT:
        if self._channel is None:
            self._channel = self.prepre_channel()
        return self._channel
