from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.models import (
    TableSpec,
    TableWindowSpec,
    TableWindowHoppingSpec,
    TableWindowTumblingSpec,
)
from marshmallow import fields, validate, ValidationError


def validate_relative_to(value):
    valid_values = ["stream", "now", "custom"]
    if value.lower() not in valid_values:
        raise ValidationError(
            f"Invalid value for relative_to: {value}. Must be one of {valid_values}."
        )


class TableWindowTumblingSpecSchema(BaseSchema):
    __model__ = TableWindowTumblingSpec
    size = fields.Int(data_key="size", allow_none=False, required=True)
    expires = fields.Str(
        data_key="expires", allow_none=True, load_default=None
    )  # convert 1s, 1m, 1h, 1d to timedelta type


class TableWindowHoppingSpecSchema(BaseSchema):
    __model__ = TableWindowHoppingSpec

    size = fields.Int(data_key="size", allow_none=False, required=True)
    step = fields.Int(data_key="step", allow_none=False, required=True)
    expires = fields.Str(
        data_key="expires", allow_none=True, load_default=None
    )  # convert 1s, 1m, 1h, 1d to timedelta type


class TableWindowSpecSchema(BaseSchema):
    __model__ = TableWindowSpec

    tumbling = fields.Nested(
        TableWindowTumblingSpecSchema(),
        data_key="tumbling",
        allow_none=True,
        load_default=None,
    )
    hopping = fields.Nested(
        TableWindowHoppingSpecSchema(),
        data_key="hopping",
        allow_none=True,
        load_default=None,
    )
    relative_to = fields.Str(
        data_key="relative_to",
        validate=validate_relative_to,
        allow_none=True,
        load_default=None,
    )
    relative_to_selector = fields.Nested(
        PyCodeSchema(),
        data_key="relative_to_selector",
        allow_none=True,
        load_default=None,
    )


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
        values=fields.Str(required=True),
        data_key="extra_topic_configs",
        allow_none=True,
        load_default=dict,
    )
    options = fields.Mapping(
        keys=fields.Str(required=True),
        values=fields.Str(required=True),
        data_key="options",
        allow_none=True,
        load_default=dict,
    )    
    window = fields.Nested(
        TableWindowSpecSchema(), data_key="window", allow_none=True, load_default=None
    )
