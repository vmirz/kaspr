from mode import Service
from mode.utils.locks import Event
from typing import Any
from kaspr.types import KasprAppT, CronTickerT, TTLocation
from kaspr.sensors.kaspr import KasprMonitor
from .utils import (
    current_timekey,
    create_message_key,
    compute_fires_in_window,
    TK_LIVE_SUFFIX,
)


class CronTicker(CronTickerT, Service):
    """Pre-materializes upcoming cron fires into the timetable.

    One CronTicker instance runs per assigned partition. On each tick it
    iterates the cron registry for that partition and writes timetable
    entries for fires within the materialization window that haven't been
    written yet.
    """

    app: KasprAppT
    monitor: KasprMonitor
    partition: int
    can_resume: Event
    flow_active: bool

    def __init__(
        self, app: KasprAppT, partition: int, monitor: KasprMonitor, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.monitor = monitor
        self.partition = partition
        self.can_resume = Event()
        self.flow_active = False

    def pause(self):
        """Pause ticking."""
        self.can_resume.clear()
        self.flow_active = False

    def resume(self):
        """Resume ticking."""
        self.can_resume.set()
        self.flow_active = True

    @Service.task
    async def _tick_crons(self):
        """Periodic loop that materializes upcoming cron fires."""
        tick_interval = self.app.conf.scheduler_cron_tick_interval_seconds
        buffer = self.app.conf.scheduler_cron_tick_buffer_seconds

        while not self.should_stop:
            if not self.flow_active:
                await self.wait(self.can_resume)
                if self.should_stop:
                    break

            await self.sleep(tick_interval)
            if self.should_stop:
                break

            try:
                self._materialize_window(buffer)
            except Exception as exc:
                self.log.error(
                    f"CronTicker(partition={self.partition}): error during tick: {exc!r}"
                )

    def _materialize_window(self, buffer: float):
        """Materialize fires for all active crons within the lookahead window."""
        cron_registry = self.app.scheduler.cron_registry
        timetable = self.app.scheduler.timetable
        schedule_index = self.app.scheduler.schedule_index
        partition = self.partition

        now = current_timekey()
        window_end = int(now + buffer)

        # Iterate all entries in this partition
        items = cron_registry.items_for_partition(partition)
        if not items:
            return

        for cron_id, entry in items:
            if entry.get("status") != "active":
                continue

            expr = entry.get("expr")
            if not expr:
                continue

            # Determine start of materialization window
            materialized_until = entry.get("materialized_until")
            after = materialized_until if materialized_until else entry.get("last_fire", now)

            if after >= window_end:
                # Already materialized past the window
                continue

            fires = compute_fires_in_window(expr, after, window_end)
            if not fires:
                continue

            dest = entry.get("dest")
            msg_key = entry.get("key")
            msg_value = entry.get("value")
            msg_headers = entry.get("headers") or {}

            for fire_epoch in fires:
                fire_request_id = f"{cron_id}:{fire_epoch}"
                time_key = str(fire_epoch)

                # Check if already written
                existing = schedule_index.get_for_partition(
                    fire_request_id, partition=partition
                )
                if existing:
                    continue

                message_total = (
                    timetable.get_for_partition(time_key, partition=partition) or 0
                )
                location = TTLocation(partition, fire_epoch, sequence=message_total)
                message_key = create_message_key(location)

                message_entry = {
                    "k": msg_key,
                    "v": msg_value,
                    "h": msg_headers,
                    "__kms": {"d": dest, "rid": fire_request_id},
                }

                live_key = f"{time_key}{TK_LIVE_SUFFIX}"
                live_value = timetable.get_for_partition(
                    live_key, partition=partition
                )
                live_count = self._live_count_from_value(live_value)

                timetable.update_for_partition(
                    {
                        time_key: message_total + 1,
                        live_key: self._next_live_value(
                            live_count + 1, existing=live_value
                        ),
                        message_key: message_entry,
                    },
                    partition=partition,
                )

                schedule_index.update_for_partition(
                    {
                        fire_request_id: {
                            "tk": fire_epoch,
                            "seq": message_total,
                        }
                    },
                    partition=partition,
                )

            # Update materialized_until
            new_materialized = max(fires)
            updated_entry = dict(entry)
            updated_entry["materialized_until"] = new_materialized
            cron_registry.update_for_partition(
                {cron_id: updated_entry}, partition=partition
            )

    def _live_count_from_value(self, live_value) -> int:
        """Extract live count integer from timetable live-key value."""
        if live_value is None:
            return 0
        if isinstance(live_value, int):
            return live_value
        if isinstance(live_value, str):
            return int(live_value.split("|")[0]) if "|" in live_value else int(live_value)
        return 0

    def _next_live_value(self, count: int, existing=None):
        """Build next live-key value preserving any trailing metadata."""
        if existing is None or isinstance(existing, int):
            return count
        if isinstance(existing, str) and "|" in existing:
            parts = existing.split("|", 1)
            return f"{count}|{parts[1]}"
        return count
