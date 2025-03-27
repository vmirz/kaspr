from kaspr.types.schemas.base import BaseSchema
from marshmallow import fields
from kaspr.types.models import PyCode

class PyCodeSchema(BaseSchema):
    __model__ = PyCode

    python = fields.String(data_key="python", required=True)
    entrypoint = fields.String(data_key="entrypoint", allow_none=True, load_default=None)

    @classmethod
    def default(cls):
        """Return the a default instance of PyCode."""
        return PyCode.default()