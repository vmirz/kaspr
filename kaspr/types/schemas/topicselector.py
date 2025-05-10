from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.models.topicselector import (
    TopicNameSelector,
    TopicPatternSelector,
    TopicKeySelector,
    TopicValueSelector,
    TopicPartitionSelector,
    TopicHeadersSelector,
    TopicPredicate,
)


class TopicNameSelectorSchema(PyCodeSchema):
    __model__ = TopicNameSelector


class TopicPatternSelectorSchema(PyCodeSchema):
    __model__ = TopicPatternSelector


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
