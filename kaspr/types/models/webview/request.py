from kaspr.types.models.base import BaseModel

class WebViewRequestSpec(BaseModel):
    method: str
    path: str