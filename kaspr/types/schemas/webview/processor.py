from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.webview.operations import WebViewProcessorOperationSchema
from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.models.webview import WebViewProcessorSpec
from marshmallow import fields


class WebViewProcessorSpecSchema(BaseSchema):
    __model__ = WebViewProcessorSpec


    pipeline = fields.List(
        fields.Str(data_key="pipeline", allow_none=False, required=True),
        allow_none=False,
        load_default=[],
    )
    init = fields.Nested(
        PyCodeSchema(),
        data_key="init",
        allow_none=True,
        load_default=None,
    )    
    operations = fields.List(
        fields.Nested(
            WebViewProcessorOperationSchema(), data_key="operations", required=True
        ),
        allow_none=False,
        load_default=[],
    )