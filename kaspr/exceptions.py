"""Kaspr exceptions."""

__all__ = [
    "KasprError",
    "KasprPredicate",
    "Skip",
]


class KasprError(Exception):
    """Base-class for all Kaspr exceptions."""


class KasprPredicate(KasprError):
    """Base-class for semi-predicates such as :exc:`Skip`."""


class Skip(KasprPredicate):
    """Raised in stream processors to skip processing of an event."""


class KasprProcessingError(KasprError):
    """Raised when an error occurs during processing."""

    operation: str = None
    cause: Exception = None

    def __init__(self, message: str, cause: Exception = None, operation: str = None):
        super().__init__(message)
        self.operation = operation
        self.cause = cause

    def __str__(self):
        return f"{self.args[0]}: {self.cause}" if self.cause else self.args[0]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.args[0]!r}, {self.cause!r})"

    def to_dict(self):
        return {
            "error": self.args[0],
            "operation": self.operation,
            "cause": str(self.cause) if self.cause else None,
        }
