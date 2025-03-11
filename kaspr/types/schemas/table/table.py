from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.models import TableSpec
from marshmallow import fields

class TableSpecSchema(BaseSchema):
    __model__ = TableSpec

    name = fields.Str(data_key="name", allow_none=False, required=True)
    description = fields.Str(data_key="description", allow_none=True, load_default=None)
    is_global = fields.Bool(data_key="global", allow_none=True, load_default=False)
    default_selector = fields.Nested(
        PyCodeSchema(), data_key="default_selector", allow_none=True, load_default=None
    )
    key_serializer = fields.Str(
        data_key="key_serializer", required=False, load_default=None
    )
    value_serializer = fields.Str(
        data_key="value_serializer", required=False, load_default=None
    )
    partitions = fields.Int(data_key="partitions", required=False, load_default=None)
    extra_topic_configs = fields.Mapping(
        keys=fields.Str(required=True),
        data_key="extra_topic_configs",
        allow_none=True,
        load_default={},
    )
