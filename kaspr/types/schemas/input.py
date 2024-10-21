from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.topicsrc import TopicSrcSchema
from kaspr.types.schemas.channel import ChannelSchema
from marshmallow import fields
from kaspr.types.models import Channel
from kaspr.types.models import AgentInput

class AgentInputSchema(BaseSchema):
    __model__ = AgentInput

    topic = fields.Nested(
        TopicSrcSchema(), data_key="topic", allow_none=True, load_default=None
    )
    channel = fields.Nested(
        ChannelSchema(),
        data_key="channel",
        allow_none=True,
        load_default=None,
    )