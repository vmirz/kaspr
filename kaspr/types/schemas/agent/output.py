from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models import AgentOutputSpec
from kaspr.types.schemas.topicout import TopicOutSpecSchema
from marshmallow import fields


class AgentOutputSpecSchema(BaseSchema):
    __model__ = AgentOutputSpec

    topics_spec = fields.List(
        fields.Nested(TopicOutSpecSchema(), allow_none=True, load_default=None),
        data_key="topics",
        allow_none=True,
        load_default=None,
    )
