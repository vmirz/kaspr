from kaspr.types.models.base import BaseModel
from kaspr.types.models.topicsrc import TopicSrc
from kaspr.types.models.channel import Channel
from typing import Optional

class AgentOutput(BaseModel):
    topic: Optional[TopicSrc]
    channel: Optional[Channel]