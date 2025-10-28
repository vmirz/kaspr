from kaspr.types.schemas.base import BaseSchema, pre_load
from kaspr.types.schemas.task.schedule import TaskScheduleSpecSchema
from kaspr.types.schemas.task.processor import TaskProcessorSpecSchema
from kaspr.types.models.task import TaskSpec
from marshmallow import fields


class TaskSpecSchema(BaseSchema):
    __model__ = TaskSpec

    name = fields.Str(data_key="name", allow_none=False, required=True)
    description = fields.Str(data_key="description", allow_none=True, load_default=None)
    on_leader = fields.Bool(data_key="on_leader", allow_none=True, load_default=None)
    schedule = fields.Nested(
        TaskScheduleSpecSchema(),
        data_key="schedule",
        allow_none=True,
        load_default=None,
    )
    processors = fields.Nested(
        TaskProcessorSpecSchema(),
        data_key="processors",
        allow_none=True,
        load_default=None,
    )

    @pre_load
    def set_processors(self, data, **kwargs):
        # Set default/empty processors if not provided
        if "processors" not in data or data["processors"] is None:
            data["processors"] = {
                "pipeline": [],
                "init": None,
                "operations": [],
            }
        return data
