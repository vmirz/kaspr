from kaspr.types.models.base import BaseModel
from typing import Optional, List

class TopicSrc(BaseModel):
    names: List[str]
    pattern: Optional[str]
    key_serializer: Optional[str]
    value_serializer: Optional[str]    