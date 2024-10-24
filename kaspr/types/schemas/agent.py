from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.input import AgentInputSchema
from kaspr.types.schemas.output import AgentOutputSchema
from marshmallow import fields
from kaspr.types.models import AgentSpec

class AgentSpecSchema(BaseSchema):
    __model__ = AgentSpec

    name = fields.Str(data_key="name", allow_none=False, required=True)
    description = fields.Str(data_key="description", allow_none=True, load_default=None)
    inputs = fields.Nested(AgentInputSchema(), data_key="inputs", required=True)
    outputs = fields.List(
        fields.Nested(AgentOutputSchema(), data_key="outputs", allow_none=True),
        required=True,
    )
