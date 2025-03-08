from typing import Optional
from kaspr.types.schemas.base import BaseSchema
from marshmallow import fields
from kaspr.types.models import AgentProcessorOperation, AgentProcessorFilterOperator
from kaspr.types.schemas.pycode import PyCodeSchema


class AgentProcessorOperationFilterSchema(PyCodeSchema):
    __model__ = AgentProcessorFilterOperator


class AgentProcessorOperationSchema(BaseSchema):
    __model__ = AgentProcessorOperation

    name = fields.String(data_key="name", required=True)
    filter = fields.Nested(
        AgentProcessorOperationFilterSchema(),
        data_key="filter",
        allow_none=True,
        load_default=None,
    )
