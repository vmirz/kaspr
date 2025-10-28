from typing import Optional
from kaspr.types.models.base import BaseModel

class TaskScheduleSpec(BaseModel):
    interval: Optional[str]
    cron: Optional[str]
    timezone: Optional[str]