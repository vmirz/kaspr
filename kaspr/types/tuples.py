import typing
from typing import Mapping, NamedTuple, Optional


if typing.TYPE_CHECKING:
    from .message_scheduler import SchedulerPartT as _SchedulerPartT
else:

    class _SchedulerPartT:
        ...  # noqa=

class TTLocation(NamedTuple):
    """Tuple representing a location in a sorted Timetable.
    
    Note: Take caution if adding a new property
    because this tuple compared to others to determine
    if one location is before or after the other.
    
    Adding a property can alter the behavior of this
    comparison.

    .. code-block:: python
        locations = [ 
            TTLocation(0, 1707690487, 0),
            TTLocation(0, 1707690487, 2),
            TTLocation(0, 1707690487)
        ]
        #[ TTLocation(0, 1707690487, -1), TTLocation(0, 1707690487, 0), TTLocation(0, 1707690487, 2)]
    
    """

    #: Timetable topic partition
    partition: int
    
    #: Unix timestamp
    time_key: int

    #: Message sequence number for a given time key.
    #: We default to -1 instead of `None` so the
    #: tuple can be used in comparisons
    sequence: Optional[int] = -1

class TTMessage(NamedTuple):
    message: Mapping
    location: TTLocation    

class PT(NamedTuple):
    """Tuple of SchedulerPart (janitor/dispatcher) and partition number."""
    part: _SchedulerPartT
    partition: int