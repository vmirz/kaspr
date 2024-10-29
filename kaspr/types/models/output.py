from kaspr.types.models.base import BaseModel
from kaspr.types.models.topicsrc import TopicSrcSpec
from kaspr.types.models.channel import ChannelSpec
from typing import Optional

class AgentOutput(BaseModel):
    topic: Optional[TopicSrcSpec]
    channel: Optional[ChannelSpec]