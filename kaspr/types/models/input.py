from kaspr.types.models.base import BaseModel
from kaspr.types.models.topicsrc import TopicSrc
from kaspr.types.models.channel import Channel
from typing import Optional

class AgentInput(BaseModel):
    topic: Optional[TopicSrc]
    channel: Optional[Channel]