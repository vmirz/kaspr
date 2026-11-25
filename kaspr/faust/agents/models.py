"""Models used by agents internally."""
from typing import Any

from kaspr.faust.models import Record
from kaspr.faust.types import ModelT

__all__ = ['ReqRepRequest', 'ReqRepResponse']


class ReqRepRequest(Record,
                    serializer='json',
                    namespace='@ReqRepRequest',  # internal namespace
                    # any stream should allow this type
                    # to wrap other values.
                    polymorphic_fields=True):
    """Value wrapped in a Request-Reply request."""

    # agent.ask(value) wraps the value in this record
    # so that the receiving agent knows where to send the reply.

    value: Any
    reply_to: str
    correlation_id: str


# Models are named after their module unless they set a namespace, and that
# name is in the messages. Those without one keep the name they had when
# Faust was a package of its own: members that still run it read them too.

class ModelReqRepRequest(ReqRepRequest,
                         namespace='faust.agents.models.ModelReqRepRequest'):
    """Request-Reply request where value is a model."""

    value: ModelT


class ReqRepResponse(Record, serializer='json', namespace='@ReqRepResponse'):
    """Request-Reply response."""

    key: Any
    value: Any
    correlation_id: str


class ModelReqRepResponse(ReqRepResponse,
                          namespace='faust.agents.models.ModelReqRepResponse'):
    value: Any
