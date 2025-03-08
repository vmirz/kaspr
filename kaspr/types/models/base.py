import logging
import abc
from kaspr.utils.logging import CompositeLogger, get_logger
from types import SimpleNamespace
from typing import Any, Dict, Mapping
from marshmallow import EXCLUDE

EXCLUDE = EXCLUDE
JSON = Dict[str, Any]
MAX_REPR_LEN = 50

def _process_dict_values(model: Any, key: str, value: Any) -> Any:
    """Process a returned from a JSON response.
    Args:
        value: A dict, list, or value returned from a JSON response.
    Returns:
        Either an UnknownModel, a List of processed values, or the original value \
            passed through.
    """
    if isinstance(value, list):
        return [_process_dict_values(model, key, v) for v in value]
    else:
        return value
    
class BaseModel(SimpleNamespace):
    """BaseModel that all models should inherit from.
    Note:
        If a passed parameter is a nested dictionary, then it is created with the
        `UnknownModel` class. If it is a list, then it is created with
    Args:
        **kwargs: All passed parameters as converted to instance attributes.
    """

    __related__: Mapping = dict()

    def __init__(self, **kwargs: Any) -> None:
        kwargs = {k: _process_dict_values(self, k, v) for k, v in kwargs.items()}

        self.__dict__.update(kwargs)

    def __repr__(self) -> str:
        """Return a default repr of any Model.
        Returns:
            The string model parameters up to a `MAX_REPR_LEN`.
        """
        repr_ = super().__repr__()
        if len(repr_) > MAX_REPR_LEN:
            return repr_[:MAX_REPR_LEN] + " ...)"
        else:
            return repr_
        
class UnknownModel(BaseModel):
    """A convenience class that inherits from `BaseModel`."""

    def keys(self):
        return self.__dict__.keys()
    
    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()
    
class SpecComponent(BaseModel):
    """Base class for all spec components.
    Args:
        **kwargs: All passed parameters as converted to instance attributes.
    """

    log: CompositeLogger = None

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.logger = get_logger(__name__)
        self.log = self.log or CompositeLogger(self.logger, formatter=self._format_log)

    def __repr__(self) -> str:
        """Return a default repr of any SpecComponent.
        Returns:
            The string model parameters up to a `MAX_REPR_LEN`.
        """
        repr_ = super().__repr__()
        if len(repr_) > MAX_REPR_LEN:
            return repr_[:MAX_REPR_LEN] + " ...)"
        else:
            return repr_

    @property
    @abc.abstractmethod
    def label(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def shortlabel(self) -> str:
        ...

    def _format_log(self, severity: int, msg: str, *args: Any, **kwargs: Any) -> str:
        return f'[^{self.shortlabel}]: {msg}'