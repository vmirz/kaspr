from typing import Optional
from kaspr.types.schemas.base import BaseSchema
from marshmallow import fields
from kaspr.types.models import AgentProcessorOperation, AgentProcessorOperationFilter
from kaspr.types.schemas.pycode import PyCodeSchema


class AgentProcessorOperationFilterSchema(PyCodeSchema):
    __model__ = AgentProcessorOperationFilter


class AgentProcessorOperationMapSchema(PyCodeSchema):
    __model__ = AgentProcessorOperationFilter

class AgentProcessorOperationSchema(BaseSchema):
    __model__ = AgentProcessorOperation

    name = fields.String(data_key="name", required=True)
    init = fields.Nested(
        PyCodeSchema(),
        data_key="init",
        allow_none=True,
        load_default=None,
    )
    filter = fields.Nested(
        AgentProcessorOperationFilterSchema(),
        data_key="filter",
        allow_none=True,
        load_default=None,
    )
    map = fields.Nested(
        AgentProcessorOperationMapSchema(),
        data_key="map",
        allow_none=True,
        load_default=None,
    )