from marshmallow import fields
from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models import ChannelSpec

class ChannelSpecSchema(BaseSchema):
    __model__ = ChannelSpec
    name = fields.Str(data_key="name", required=True)
    