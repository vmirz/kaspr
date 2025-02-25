from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.webview.request import WebViewRequestSpecSchema
from kaspr.types.schemas.webview.response import WebViewResponseSpecSchema
from kaspr.types.schemas.webview.processor import WebViewProcessorSpecSchema
from kaspr.types.models.webview import WebViewSpec
from marshmallow import fields


class WebViewSpecSchema(BaseSchema):
    __model__ = WebViewSpec

    name = fields.Str(data_key="name", allow_none=False, required=True)
    description = fields.Str(data_key="description", allow_none=True, load_default=None)
    request = fields.Nested(WebViewRequestSpecSchema(), data_key="request", required=True)
    response = fields.Nested(WebViewResponseSpecSchema(), data_key="response", allow_none=True, load_default=None)
    processors = fields.Nested(
        WebViewProcessorSpecSchema(), data_key="processors", required=True
    )