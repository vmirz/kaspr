from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.operations import AgentProcessorOperationSchema
from marshmallow import fields
from kaspr.types.models import AgentProcessorSpec


class AgentProcessorSpecSchema(BaseSchema):
    __model__ = AgentProcessorSpec

    pipeline = fields.List(
        fields.Str(data_key="pipeline", allow_none=False, required=True),
        allow_none=False,
        load_default=[],
    )
    operations = fields.List(
        fields.Nested(
            AgentProcessorOperationSchema(), data_key="operations", required=True
        ),
        allow_none=False,
        load_default=[],
    )
