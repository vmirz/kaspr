import yaml
from marshmallow import fields
from kaspr.types.schemas.base import BaseSchema, post_load
from kaspr.types.schemas.agent import AgentSpecSchema
from kaspr.types.models import AppSpec
from kaspr.types import KasprAppT


class AppSpecSchema(BaseSchema):
    __model__ = AppSpec

    agents_spec = fields.List(
        fields.Nested(
            AgentSpecSchema(), allow_none=False, load_default=[]
        ),
        data_key="agents",
        required=False,
    )

    @classmethod
    def from_file(cls, file, app: KasprAppT) -> AppSpec:
        """Load an AppSpec from a file."""
        return AppSpecSchema(context={"app": app}).load(yaml.safe_load(file))
