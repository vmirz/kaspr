from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.task.operations import TaskProcessorOperationSchema
from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.models.task import TaskProcessorSpec
from marshmallow import fields


class TaskProcessorSpecSchema(BaseSchema):
    __model__ = TaskProcessorSpec


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
            TaskProcessorOperationSchema(), data_key="operations", required=True
        ),
        allow_none=False,
        load_default=[],
    )