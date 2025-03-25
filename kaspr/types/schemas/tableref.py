"""Reference table definitions in agent and webview processing operators."""

from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models import TableRefSpec
from marshmallow import fields

class TableRefSpecSchema(BaseSchema):
    __model__ = TableRefSpec

    name = fields.String(data_key="name", required=True)
    arg_name = fields.String(data_key="arg_name", required=True)