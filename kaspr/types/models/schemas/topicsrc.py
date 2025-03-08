from typing import Dict
from marshmallow import fields, validates_schema
from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models import TopicSrcSpec


class TopicSrcSpecSchema(BaseSchema):
    __model__ = TopicSrcSpec

    names = fields.List(fields.Str(data_key="names", required=False), required=False, load_default=[])
    pattern = fields.Str(data_key="pattern", required=False, load_default=None)
    key_serializer = fields.Str(data_key="key_serializer", required=False, load_default=None)
    value_serializer = fields.Str(data_key="value_serializer", required=False, load_default=None)

    @validates_schema
    def validate_schema(self, data: Dict, **kwargs):
        if data.get("pattern") and data.get("names"):
            raise ValueError("Only one of 'pattern' or 'names' can be specified")