from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.topicsrc import TopicSrcSpecSchema
from kaspr.types.schemas.channel import ChannelSpecSchema
from marshmallow import fields
from kaspr.types.models import AgentInputSpec


class AgentInputSpecSchema(BaseSchema):
    __model__ = AgentInputSpec

    declare = fields.Bool(
        data_key="declare", required=False, load_default=None, allow_none=True
    )
    topic_spec = fields.Nested(
        TopicSrcSpecSchema(), data_key="topic", allow_none=True, load_default=None
    )
    channel_spec = fields.Nested(
        ChannelSpecSchema(),
        data_key="channel",
        allow_none=True,
        load_default=None,
    )
