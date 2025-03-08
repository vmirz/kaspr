from typing import Dict
from marshmallow import fields, validates_schema
from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models.topicout import TopicOutSpec
from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.models.topicout import (
    AgentOutputTopicKeySelector,
    AgentOutputTopicValueSelector,
    AgentOutputTopicPartitionSelector,
    AgentOutputTopicHeadersSelector,
    AgentOutputTopicPredicate,
)


class AgentOutputTopicKeySelectorSchema(PyCodeSchema):
    __model__ = AgentOutputTopicKeySelector


class AgentOutputTopicValueSelectorSchema(PyCodeSchema):
    __model__ = AgentOutputTopicValueSelector


class AgentOutputTopicPartitionSelectorSchema(PyCodeSchema):
    __model__ = AgentOutputTopicPartitionSelector


class AgentOutputTopicHeadersSelectorSchema(PyCodeSchema):
    __model__ = AgentOutputTopicHeadersSelector


class AgentOutputTopicPredicateSchema(PyCodeSchema):
    __model__ = AgentOutputTopicPredicate


class TopicOutSpecSchema(BaseSchema):
    __model__ = TopicOutSpec

    name = fields.Str(data_key="name", required=True)
    key_serializer = fields.Str(
        data_key="key_serializer", allow_none=True, load_default=None
    )
    value_serializer = fields.Str(
        data_key="value_serializer", allow_none=True, load_default=None
    )
    key_selector = fields.Nested(
        AgentOutputTopicKeySelectorSchema(),
        data_key="key_selector",
        allow_none=True,
        load_default=None,
    )
    value_selector = fields.Nested(
        AgentOutputTopicValueSelectorSchema(),
        data_key="value_selector",
        allow_none=True,
        load_default=None,
    )
    partition_selector = fields.Nested(
        AgentOutputTopicPartitionSelectorSchema(),
        data_key="partition_selector",
        allow_none=True,
        load_default=None,
    )
    headers_selector = fields.Nested(
        AgentOutputTopicHeadersSelectorSchema(),
        data_key="headers_selector",
        allow_none=True,
        load_default=None,
    )
    predicate = fields.Nested(
        AgentOutputTopicPredicateSchema(),
        data_key="predicate",
        allow_none=True,
        load_default=None,
    )
