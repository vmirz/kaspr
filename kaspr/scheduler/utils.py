from math import floor
from time import time
from datetime import datetime, timezone
from dataclasses import dataclass
from kaspr.types import TTLocation

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