from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.input import AgentInputSpecSchema
from kaspr.types.schemas.output import AgentOutputSpecSchema
from kaspr.types.schemas.processor import AgentProcessorSpecSchema
from marshmallow import fields
from kaspr.types.models import AgentSpec


class AgentSpecSchema(BaseSchema):
    __model__ = AgentSpec

    name = fields.Str(data_key="name", allow_none=False, required=True)
    description = fields.Str(data_key="description", allow_none=True, load_default=None)
    input = fields.Nested(AgentInputSpecSchema(), data_key="input", required=True)
    output = fields.Nested(AgentOutputSpecSchema(), data_key="output", allow_none=True, load_default=None) 
    processors = fields.Nested(
        AgentProcessorSpecSchema(), data_key="processors", required=True
    )