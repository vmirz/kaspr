from kaspr.utils.functional import parse_time_delta
from typing import Callable, Union, Awaitable, Optional
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.task.schedule import TaskScheduleSpec
from kaspr.types.models.task.processor import TaskProcessorSpec
from kaspr.types.app import KasprAppT
from zoneinfo import ZoneInfo

TaskArg = Union[Callable[["KasprAppT"], Awaitable], Callable[[], Awaitable]]


class TaskSpec(SpecComponent):
    name: str
    description: str
    on_leader: Optional[bool]
    schedule: Optional[TaskScheduleSpec]
    processors: TaskProcessorSpec

    app: KasprAppT = None

    _task: TaskArg = None

    def prepare_task(self) -> TaskArg:
        self.log.info("Starting...")
        self.processors.app = self.app
        processors = self.processors
        if not self.schedule:
            return self.app.task(processors.processor, on_leader=self.on_leader)
        else:
            if self.schedule.interval and self.schedule.cron:
                raise ValueError(
                    "TaskScheduleSpec cannot have both interval and cron defined."
                )
            if self.schedule.interval:
                return self.app.timer(
                    interval=parse_time_delta(self.schedule.interval),
                    on_leader=self.on_leader,
                    name=self.name
                )(processors.processor)
            elif self.schedule.cron:
                return self.app.crontab(
                    self.schedule.cron,
                    timezone=ZoneInfo(self.schedule.timezone)
                    if self.schedule.timezone
                    else None,
                    on_leader=self.on_leader,
                )(processors.processor)

    @property
    def task(self) -> TaskArg:
        if self._task is None:
            self._task = self.prepare_task()
        return self._task

    @property
    def label(self) -> str:
        """Return description of component, used in logs."""
        return f"{type(self).__name__}: {self.__repr__()}"

    @property
    def shortlabel(self) -> str:
        """Return short description of processor."""
        return f"{type(self).__name__}: {self.name}"
