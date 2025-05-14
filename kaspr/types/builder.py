import abc
from typing import Dict, TYPE_CHECKING
from faust.types import ServiceT, TopicT
from mode.utils.objects import cached_property
from marshmallow import INCLUDE, EXCLUDE, Schema, ValidationError, fields

if TYPE_CHECKING:
    from .app import KasprAppT as _KasprAppT
else:

    class _KasprAppT: ...  # noqa

class AppBuilderT:
    """Abstract type for an application builder."""

    app: _KasprAppT

    @abc.abstractmethod
    def build(self) -> None:
        """Build agents, tasks, etc. from external definition files."""
        ...

    @abc.abstractmethod
    def maybe_create_topics(self) -> None:
        """Maybe declare agent input topics."""
        ...
        
    @cached_property
    def agents(self):
        ...