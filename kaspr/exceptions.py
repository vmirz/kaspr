"""Kaspr exceptions."""

__all__ = [
    'KasprError',
    'KasprPredicate',
    'Skip',
]

class KasprError(Exception):
    """Base-class for all Kaspr exceptions."""

class KasprPredicate(KasprError):
    """Base-class for semi-predicates such as :exc:`Skip`."""

class Skip(KasprPredicate):
    """Raised in stream processors to skip processing of an event."""
