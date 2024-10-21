from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.topicsrc import TopicSrcSchema
from kaspr.types.schemas.channel import ChannelSchema
from marshmallow import fields
from kaspr.types.models import AgentOutput

class AgentOutputSchema(BaseSchema):
    __model__ = AgentOutput

    topic = fields.Nested(
        TopicSrcSchema(), data_key="topic", allow_none=True, load_default=None
    )
    channel = fields.Nested(
        ChannelSchema(),
        data_key="channel",
        allow_none=True,
        load_default=None,
    )