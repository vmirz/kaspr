from kaspr.types.schemas.base import BaseSchema
from marshmallow import fields
from kaspr.types.models import Channel

class ChannelSchema(BaseSchema):
    __model__ = Channel
    name = fields.Str(data_key="name", required=True)
    