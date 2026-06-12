from math import floor
from time import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List
from croniter import croniter
from kaspr.types import TTLocation

# Suffix appended to a TimeKey to store live (non-canceled) metadata.
# e.g. "1707171828:live" -> {"count": 2}
TK_LIVE_SUFFIX = ":live"

@dataclass(frozen=True)
class SchedulerPart:
    janitor: str = "J"
    dispatcher: str = "D"

def current_timekey():
    return floor(time())

def create_message_key(location: TTLocation) -> str:
    return f"{location.time_key}-{location.sequence}"

def prettydate(location: TTLocation):
    return datetime.fromtimestamp(location.time_key, tz=timezone.utc).isoformat().replace("+00:00", "Z")

def locdiff(loc1: TTLocation, loc2: TTLocation):
    return loc1.time_key - loc2.time_key


def validate_cron_expr(expr: str) -> bool:
    """Validate a cron expression string."""
    return croniter.is_valid(expr)


def compute_next_fire(expr: str, after: int) -> int:
    """Compute the next fire time (unix epoch) after the given timestamp.

    Args:
        expr: A valid cron expression string.
        after: Unix epoch to start from (exclusive).

    Returns:
        Next fire time as unix epoch (floored to integer seconds).
    """
    dt = datetime.fromtimestamp(after, tz=timezone.utc)
    cron = croniter(expr, dt)
    next_dt = cron.get_next(datetime)
    return floor(next_dt.timestamp())


def compute_fires_in_window(expr: str, after: int, until: int) -> List[int]:
    """Compute all fire times in a time window (after, until].

    Args:
        expr: A valid cron expression string.
        after: Start of window (exclusive), unix epoch.
        until: End of window (inclusive), unix epoch.

    Returns:
        List of fire times as unix epochs, sorted ascending.
    """
    fires = []
    dt = datetime.fromtimestamp(after, tz=timezone.utc)
    cron = croniter(expr, dt)
    while True:
        next_dt = cron.get_next(datetime)
        fire = floor(next_dt.timestamp())
        if fire > until:
            break
        fires.append(fire)
    return fires


def cron_min_interval(expr: str) -> float:
    """Estimate the minimum interval (seconds) between consecutive fires.

    Computes the first two fire times from epoch and returns the gap.
    Returns float('inf') if fewer than 2 fires can be computed.
    """
    dt = datetime.fromtimestamp(0, tz=timezone.utc)
    cron = croniter(expr, dt)
    first = cron.get_next(datetime)
    second = cron.get_next(datetime)
    return (second - first).total_seconds()


def due_index_key(fire_epoch: int, cron_id: str) -> str:
    """Build a due-index key from a fire epoch and cron ID.

    Key format: "{minute_bucket:010d}:{cron_id}"
    Minute bucket = fire_epoch // 60.
    """
    minute_bucket = fire_epoch // 60
    return f"{minute_bucket:010d}:{cron_id}"


def due_index_prefix(minute_bucket: int) -> str:
    """Build the prefix for scanning a minute bucket in the due-index."""
    return f"{minute_bucket:010d}:"