from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.agent import AgentSpecSchema
from kaspr.types.models import AppSpec
from marshmallow import fields


class AppSpecSchema(BaseSchema):
    __model__ = AppSpec

    agents = fields.List(
        fields.Nested(
            AgentSpecSchema(), data_key="agents", allow_none=False, load_default=[]
        ),
        required=False,
    )
