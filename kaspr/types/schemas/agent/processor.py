from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.agent.operations import AgentProcessorOperationSchema
from kaspr.types.schemas.pycode import PyCodeSchema
from marshmallow import fields
from kaspr.types.models import AgentProcessorSpec


class AgentProcessorSpecSchema(BaseSchema):
    __model__ = AgentProcessorSpec

    pipeline = fields.List(
        fields.Str(data_key="pipeline", allow_none=False, required=True),
        allow_none=False,
        load_default=[],
    )
    init = fields.Nested(
        PyCodeSchema(),
        data_key="init",
        allow_none=True,
        load_default=None,
    )    
    operations = fields.List(
        fields.Nested(
            AgentProcessorOperationSchema(), data_key="operations", required=True
        ),
        allow_none=False,
        load_default=[],
    )