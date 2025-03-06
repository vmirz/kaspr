from kaspr.types.schemas.base import BaseSchema, pre_load
from kaspr.types.schemas.webview.request import WebViewRequestSpecSchema
from kaspr.types.schemas.webview.response import WebViewResponseSpecSchema
from kaspr.types.schemas.webview.processor import WebViewProcessorSpecSchema
from kaspr.types.models import WebViewSpec
from marshmallow import fields


class WebViewSpecSchema(BaseSchema):
    __model__ = WebViewSpec

    name = fields.Str(data_key="name", allow_none=False, required=True)
    description = fields.Str(data_key="description", allow_none=True, load_default=None)
    request = fields.Nested(
        WebViewRequestSpecSchema(), data_key="request", required=True
    )
    response = fields.Nested(
        WebViewResponseSpecSchema(),
        data_key="response",
        allow_none=True,
        load_default=None,
    )
    processors = fields.Nested(
        WebViewProcessorSpecSchema(),
        data_key="processors",
        allow_none=True,
        load_default=None,
    )

    @pre_load
    def set_processors(self, data, **kwargs):
        # Set default/empty processors if not provided
        if "processors" not in data or data["processors"] is None:
            data["processors"] = {
                "pipeline": [],
                "init": None,
                "operations": [],
            }
        return data

    @pre_load
    def set_response(self, data, **kwargs):
        # Set default/empty response if not provided
        if "response" not in data or data["response"] is None:
            data["response"] = {}
        return data
