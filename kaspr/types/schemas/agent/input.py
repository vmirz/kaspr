import re
from kaspr.types.schemas.base import BaseSchema
from kaspr.types.schemas.topicsrc import TopicSrcSpecSchema
from kaspr.types.schemas.channel import ChannelSpecSchema
from marshmallow import fields, ValidationError
from kaspr.types.models import AgentInputSpec, AgentInputBufferSpec

def validate_within(value: str) -> bool:
    """
    Validates whether the input string is a valid time delta string.
    
    Valid formats: "<number><unit>", where:
        - number: positive integer
        - unit: one of "s" (seconds), "m" (minutes), "h" (hours), "d" (days)

    Examples of valid inputs: "10s", "5m", "1h", "2d"
    
    Returns True if valid, raises ValueError if invalid.
    """
    if not isinstance(value, str):
        raise ValueError("Value must be a string.")

    pattern = r'^\d+[smhd]$'
    if not re.match(pattern, value):
        raise ValidationError(
            f"Invalid time delta format: '{value}'. Must be a number followed by a unit (s, m, h, d)."
        )


class AgentInputBufferSpecSchema(BaseSchema):
    __model__ = AgentInputBufferSpec

    max_size = fields.Integer(
        data_key="max", required=True
    )
    within = fields.String(
        data_key="within", validate=validate_within, required=True
    )

class AgentInputSpecSchema(BaseSchema):
    __model__ = AgentInputSpec

    declare = fields.Bool(
        data_key="declare", required=False, load_default=None, allow_none=True
    )
    topic_spec = fields.Nested(
        TopicSrcSpecSchema(), data_key="topic", allow_none=True, load_default=None
    )
    channel_spec = fields.Nested(
        ChannelSpecSchema(),
        data_key="channel",
        allow_none=True,
        load_default=None,
    )
    buffer_spec = fields.Nested(
        AgentInputBufferSpecSchema(),
        data_key="take",
        allow_none=True,
        load_default=None,
    )
