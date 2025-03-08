from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models.webview import WebViewRequestSpec
from marshmallow import fields


class WebViewRequestSpecSchema(BaseSchema):
    __model__ = WebViewRequestSpec

    method = fields.Str(data_key="method", allow_none=True, load_default=None)
    path = fields.Str(data_key="path", required=True)