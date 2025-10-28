from kaspr.types.schemas.base import BaseSchema
from marshmallow import fields
from kaspr.types.models.task import (
    TaskProcessorOperation,
    TaskProcessorTopicSendOperator,
    TaskProcessorMapOperator,
)
from kaspr.types.schemas.pycode import PyCodeSchema
from kaspr.types.schemas.topicout import TopicOutSpecSchema
from kaspr.types.schemas.tableref import TableRefSpecSchema

class TaskProcessorTopicSendOperatorSchema(TopicOutSpecSchema):
    __model__ = TaskProcessorTopicSendOperator

class TaskProcessorMapOperatorSchema(PyCodeSchema):
    __model__ = TaskProcessorMapOperator

class TaskProcessorFilterOperatorSchema(BaseSchema):
    __model__ = TaskProcessorOperation

class TaskProcessorOperationSchema(BaseSchema):
    __model__ = TaskProcessorOperation

    name = fields.String(data_key="name", allow_none=True, load_default=None)
    topic_send = fields.Nested(
        TaskProcessorTopicSendOperatorSchema(),
        data_key="topic_send",
        allow_none=True,
        load_default=None,
    )
    map = fields.Nested(
        TaskProcessorMapOperatorSchema(),
        data_key="map",
        allow_none=True,
        load_default=None,
    )
    filter = fields.Nested(
        TaskProcessorFilterOperatorSchema(),
        data_key="filter",
        allow_none=True,
        load_default=None,
    )
    table_refs = fields.List(
        fields.Nested(
            TableRefSpecSchema(), required=True
        ),
        data_key="tables",
        allow_none=False,
        load_default=list,
    )