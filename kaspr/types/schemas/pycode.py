from kaspr.types.schemas.base import BaseSchema
from marshmallow import fields
from kaspr.types.models import PyCode

class PyCodeSchema(BaseSchema):
    __model__ = PyCode

    python = fields.String(data_key="python", required=True)