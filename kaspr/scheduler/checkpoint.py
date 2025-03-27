from mode import Service
from typing import Any, Mapping, Optional
from kaspr.types import KasprAppT, TTLocation, CheckpointT, PT
from faust.serializers.codecs import get_codec
from faust.types import FutureMessage, RecordMetadata
from mode.utils.locks import Event
from kaspr.sensors.kaspr import KasprMonitor
from kaspr.scheduler.utils import SchedulerPart


class Checkpoint(CheckpointT, Service):
    """Checkpoint services saves Timetable locations processed by dispatchors and janitors"""

    #: Records all statistics about dispatching
    monitor: KasprMonitor

    pending_checkpoints: Mapping[str, TTLocation] = None

    #: Ensures checkpointing is paused during rebalance
    can_resume: Event
    flow_active: bool

    #: Ensures janitor starts after dispatcher has made the first checkpoint
    dispatcher_checkpointed: Event

    _json_codec = get_codec("json")

    def __init__(self, app: KasprAppT, monitor: KasprMonitor, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.monitor = monitor
        self.pending_checkpoints = {}
        self.can_resume = Event()
        self.flow_active = False
        self.dispatcher_checkpointed = Event()

    async def on_start(self) -> None:
        pass

    async def on_started(self) -> None:
        pass

    async def on_stop(self) -> None:
        pass

    def on_rebalance_started(self) -> None:
        """Call when app is rebalancing."""
        self.dispatcher_checkpointed.clear()

    def pause(self):
        """Pause saving."""
        self.can_resume.clear()
        self.flow_active = False

    def resume(self):
        """Resume saving."""
        self.can_resume.set()
        self.flow_active = True

    async def _maybe_wait(self):
        """Check if checkpointing can continue. If not, wait until it can."""
        if not self.flow_active:
            self.log.dev("Waiting...")
            await self.wait(self.can_resume)

    def update(self, pt: PT, location: TTLocation):
        """Add checkpoint to pending save for scheduler type partition."""
        assert location
        self.pending_checkpoints[pt] = location
        if (
            not self.dispatcher_checkpointed.is_set()
            and pt.part == SchedulerPart.dispatcher
        ):
            self.dispatcher_checkpointed.set()

    async def flush(self):
        """Persist any pending checkpoints"""
        if self.pending_checkpoints:
            self.log.info("Flushing pending checkpoints...")
            self.persist_checkpoints()

    def _on_changelog_sent(self, pt: PT, location: TTLocation) -> None:
        """Callback after checkpoint changelog is sent and acked."""

        def _did_send(fut: FutureMessage):
            res: RecordMetadata = fut.result()
            if res.offset is not None:
                self.monitor.on_checkpoint_updated(pt, location)
                self.log.dev(f"Checkpoint sent: {pt}: {location}")

        return _did_send

    def persist_checkpoints(self):
        """Save pending checkpoints to store."""
        _pop = set()
        for pt, loc in self.pending_checkpoints.items():
            self.app.scheduler.timetable.update_for_partition(
                {pt: self._json_codec.dumps(loc._asdict())},
                partition=loc.partition,
                callback=self._on_changelog_sent(pt, loc),
            )
            _pop.add(pt)

        for k in _pop:
            self.pending_checkpoints.pop(k, None)

    def get(self, tp: PT, default: TTLocation = None) -> Optional[TTLocation]:
        """Returns the last checkpoint for a key"""

        _, partition = tp
        if tp in self.pending_checkpoints:
            return self.pending_checkpoints[tp]
        else:
            last_saved_checkpoint = self.app.scheduler.timetable.get_for_partition(
                tp, partition
            )
            if last_saved_checkpoint is not None:
                return TTLocation(**last_saved_checkpoint)
            else:
                return default

    @Service.task
    async def _save_pending(self):
        """Periodically save pending checkpoints."""

        interval = self.app.conf.scheduler_checkpoint_save_interval_seconds
        await self.app.scheduler.wait_until_topics_created()
        await self._maybe_wait()
        while not self.should_stop:
            await self._maybe_wait()
            self.persist_checkpoints()
            await self.sleep(interval)
        # One last flush
        self.persist_checkpoints()
