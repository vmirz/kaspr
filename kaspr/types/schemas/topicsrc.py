from typing import Dict
from marshmallow import fields, validates_schema
from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models import TopicSrcSpec


class TopicSrcSpecSchema(BaseSchema):
    __model__ = TopicSrcSpec

    name = fields.Str(
        data_key="name", required=False, load_default=None, allow_none=True
    )
    pattern = fields.Str(data_key="pattern", required=False, load_default=None)
    key_serializer = fields.Str(
        data_key="key_serializer", required=False, load_default=None
    )
    value_serializer = fields.Str(
        data_key="value_serializer", required=False, load_default=None
    )
    partitions = fields.Int(
        data_key="partitions", required=False, load_default=None, allow_none=True
    )
    retention = fields.Int(
        data_key="retention", required=False, load_default=None, allow_none=True
    )
    compacting = fields.Bool(
        data_key="compacting", required=False, load_default=None, allow_none=True
    )
    deleting = fields.Bool(
        data_key="deleting", required=False, load_default=None, allow_none=True
    )
    replicas = fields.Int(
        data_key="replicas", required=False, load_default=None, allow_none=True
    )
    config = fields.Mapping(
        keys=fields.Str(required=True),
        values=fields.Str(required=True),
        data_key="config",
        allow_none=True,
        load_default=dict,
    )
    
    @validates_schema
    def validate_schema(self, data: Dict, **kwargs):
        if data.get("pattern") and data.get("names"):
            raise ValueError(
                "Only one of 'pattern' or 'names' can be provided, but not both."
            )
