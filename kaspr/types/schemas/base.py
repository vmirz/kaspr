from typing import Any, Dict, Optional
from marshmallow import INCLUDE, EXCLUDE, Schema, post_load
from kaspr.types.models import UnknownModel

JSON = Dict[str, Any]

class BaseSchema(Schema):
    """The default schema for all models."""

    __model__: Any = UnknownModel
    """Determine the object that is created when the load method is called."""
    __first__: Optional[str] = None
    """Determine if `make_object` will try to get the first element the input key."""

    class Meta:
        unknown = EXCLUDE
        ordered = True   

    @post_load
    def make_object(self, data: JSON, **kwargs: Any) -> "__model__":
        """Build model for the given `__model__` class attribute.
        Args:
            data: The JSON diction to use to build the model.
            **kwargs: Unused but required to match signature of `Schema.make_object`
        Returns:
            An instance of the `__model__` class.
        """
        if self.__first__ is not None:
            data_list = data.get("objects", [{}])
            # guard against empty return list of a valid results return
            data = data_list[0] if len(data_list) != 0 else {}
        return self.__model__(**data)