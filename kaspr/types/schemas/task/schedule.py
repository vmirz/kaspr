from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models import TaskScheduleSpec
from marshmallow import fields


class TaskScheduleSpecSchema(BaseSchema):
    __model__ = TaskScheduleSpec

    interval = fields.Str(data_key="interval", allow_none=True, load_default=None)
    cron = fields.Str(data_key="cron", allow_none=True, load_default=None)
    timezone = fields.Str(data_key="timezone", allow_none=True, load_default=None)