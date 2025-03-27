from marshmallow import fields
from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models.topicout import TopicOutSpec
from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.models.topicout import (
    TopicKeySelector,
    TopicValueSelector,
    TopicPartitionSelector,
    TopicHeadersSelector,
    TopicPredicate,
)


class TopicKeySelectorSchema(PyCodeSchema):
    __model__ = TopicKeySelector


class TopicValueSelectorSchema(PyCodeSchema):
    __model__ = TopicValueSelector


class TopicPartitionSelectorSchema(PyCodeSchema):
    __model__ = TopicPartitionSelector


class TopicHeadersSelectorSchema(PyCodeSchema):
    __model__ = TopicHeadersSelector


class TopicPredicateSchema(PyCodeSchema):
    __model__ = TopicPredicate


class TopicOutSpecSchema(BaseSchema):
    __model__ = TopicOutSpec

    name = fields.Str(data_key="name", required=True)
    ack = fields.Bool(data_key="ack", allow_none=True, load_default=False)
    key_serializer = fields.Str(
        data_key="key_serializer", allow_none=True, load_default=None
    )
    value_serializer = fields.Str(
        data_key="value_serializer", allow_none=True, load_default=None
    )
    key_selector = fields.Nested(
        TopicKeySelectorSchema(),
        data_key="key_selector",
        allow_none=True,
        load_default=None,
    )
    value_selector = fields.Nested(
        TopicValueSelectorSchema(),
        data_key="value_selector",
        allow_none=True,
        load_default=None,
    )
    partition_selector = fields.Nested(
        TopicPartitionSelectorSchema(),
        data_key="partition_selector",
        allow_none=True,
        load_default=None,
    )
    headers_selector = fields.Nested(
        TopicHeadersSelectorSchema(),
        data_key="headers_selector",
        allow_none=True,
        load_default=None,
    )
    predicate = fields.Nested(
        TopicPredicateSchema(),
        data_key="predicate",
        allow_none=True,
        load_default=None,
    )
