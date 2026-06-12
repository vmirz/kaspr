from mode import Service
from mode.utils.locks import Event
from typing import Any, Dict, List, Optional, Tuple
from kaspr.types import KasprAppT, CronTickerT, TTLocation
from kaspr.sensors.kaspr import KasprMonitor
from .utils import (
    current_timekey,
    create_message_key,
    compute_next_fire,
    compute_fires_in_window,
    due_index_key,
    due_index_prefix,
    TK_LIVE_SUFFIX,
)


class CronTicker(CronTickerT, Service):
    """Pre-materializes upcoming cron fires into the timetable.

    One CronTicker instance runs per assigned partition. On each tick it
    scans the due-index for crons whose next fire falls within the
    materialization window, writes timetable entries, then re-indexes
    each cron to its next fire time.

    The due-index is keyed by "{minute_bucket:010d}:{cron_id}" so that
    prefix_scan on a minute bucket returns only crons due in that minute.
    This makes tick cost O(due crons) instead of O(all crons).
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
        recovery_lookback = self.app.conf.scheduler_cron_recovery_lookback_seconds

        while not self.flow_active:
            await self.wait(self.can_resume)
            if self.should_stop:
                return

        # On first activation, catch up any fires missed during downtime
        # before entering the regular tick loop.
        try:
            self._catchup_missed_fires(
                buffer=buffer,
                lookback=recovery_lookback,
            )
        except Exception as exc:
            self.log.error(
                f"CronTicker(partition={self.partition}): error during catch-up: {exc!r}"
            )

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

    def _catchup_missed_fires(self, buffer: float, lookback: float):
        """Replay any cron fires missed during downtime.

        Scans the due-index across a bounded lookback window so recovery
        cost is proportional to crons that were actually due during
        downtime, rather than all crons in the registry.
        """
        now = current_timekey()
        recovery_start = max(0, int(now - lookback))
        window_end = int(now + buffer)
        due_entries = self._collect_due_entries(
            start_bucket=recovery_start // 60,
            end_bucket=window_end // 60,
            window_end=window_end,
        )
        if not due_entries:
            return

        self.log.info(
            f"CronTicker(partition={self.partition}): recovery scanning "
            f"minute buckets {recovery_start // 60}..{window_end // 60} "
            f"({len(due_entries)} due entry(s))"
        )
        self._process_due_entries(due_entries=due_entries, window_end=window_end)

    def _collect_due_entries(
        self,
        start_bucket: int,
        end_bucket: int,
        window_end: int,
    ) -> List[Tuple[str, str, int]]:
        """Collect current due-index entries for a minute-bucket range."""
        cron_due_index = self.app.scheduler.cron_due_index
        partition = self.partition
        due_entries: List[Tuple[str, str, int]] = []

        for bucket in range(start_bucket, end_bucket + 1):
            prefix = due_index_prefix(bucket)
            for key, fire_epoch in cron_due_index.prefix_scan(prefix, partition=partition):
                if fire_epoch > window_end:
                    continue
                cron_id = key.split(":", 1)[1] if ":" in key else key
                due_entries.append((key, cron_id, fire_epoch))

        return due_entries

    def _process_due_entries(
        self,
        due_entries: List[Tuple[str, str, int]],
        window_end: int,
    ) -> None:
        """Materialize due entries and advance the due-index."""
        cron_registry = self.app.scheduler.cron_registry
        cron_due_index = self.app.scheduler.cron_due_index
        partition = self.partition
        now = current_timekey()

        index_deletes: List[str] = []
        index_inserts: Dict[str, int] = {}

        for index_key, cron_id, _fire_epoch in due_entries:
            entry = cron_registry.get_for_partition(cron_id, partition=partition)
            if not entry or entry.get("status") != "active":
                index_deletes.append(index_key)
                continue

            expr = entry.get("expr")
            if not expr:
                index_deletes.append(index_key)
                continue

            materialized_until = entry.get("materialized_until")
            after = (
                materialized_until
                if materialized_until is not None
                else entry.get("last_fire", now)
            )
            fires = compute_fires_in_window(expr, after, window_end)

            if fires:
                self._materialize_fires_for_entry(
                    cron_id=cron_id,
                    entry=entry,
                    fires=fires,
                    partition=partition,
                )
                next_fire = compute_next_fire(expr, max(fires))
            else:
                next_fire = compute_next_fire(expr, after)

            index_deletes.append(index_key)
            if next_fire:
                index_inserts[due_index_key(next_fire, cron_id)] = next_fire

        for old_key in index_deletes:
            cron_due_index.del_for_partition(old_key, partition=partition)

        if index_inserts:
            cron_due_index.update_for_partition(index_inserts, partition=partition)

    def _materialize_window(self, buffer: float):
        """Scan due-index for crons firing in [now, now+buffer] and materialize them."""
        now = current_timekey()
        window_end = int(now + buffer)

        due_entries = self._collect_due_entries(
            start_bucket=now // 60,
            end_bucket=window_end // 60,
            window_end=window_end,
        )

        if not due_entries:
            return
        self._process_due_entries(due_entries=due_entries, window_end=window_end)

    def _materialize_fires_for_entry(
        self,
        cron_id: str,
        entry: dict,
        fires: List[int],
        partition: int,
    ) -> Optional[int]:
        """Write timetable + schedule_index entries for a list of fire epochs.

        Updates the cron registry entry's materialized_until and returns the
        next fire epoch for re-indexing, or None if there is no next fire.
        """
        cron_registry = self.app.scheduler.cron_registry
        timetable = self.app.scheduler.timetable
        schedule_index = self.app.scheduler.schedule_index

        dest = entry.get("dest")
        msg_key = entry.get("key")
        msg_value = entry.get("value")
        msg_headers = entry.get("headers") or {}
        expr = entry.get("expr")

        for fire_epoch in fires:
            fire_request_id = f"{cron_id}:{fire_epoch}"
            time_key = str(fire_epoch)

            # Skip if already written
            if schedule_index.get_for_partition(fire_request_id, partition=partition):
                continue

            message_total = (
                timetable.get_for_partition(time_key, partition=partition) or 0
            )
            location = TTLocation(partition, fire_epoch, sequence=message_total)
            message_key = create_message_key(location)

            # Add cron fire timestamp header
            msg_headers_with_fire = dict(msg_headers)
            msg_headers_with_fire["x-scheduler-cron-fire-timestamp"] = str(fire_epoch)

            message_entry = {
                "k": msg_key,
                "v": msg_value,
                "h": msg_headers_with_fire,
                "__kms": {"d": dest, "rid": fire_request_id},
            }

            live_key = f"{time_key}{TK_LIVE_SUFFIX}"
            live_value = timetable.get_for_partition(live_key, partition=partition)
            live_count = self._live_count_from_value(live_value)

            timetable.update_for_partition(
                {
                    time_key: message_total + 1,
                    live_key: self._next_live_value(live_count + 1, existing=live_value),
                    message_key: message_entry,
                },
                partition=partition,
            )

            schedule_index.update_for_partition(
                {fire_request_id: {"tk": fire_epoch, "seq": message_total}},
                partition=partition,
            )

        # Update registry with new materialized_until
        new_materialized = max(fires)
        next_fire = compute_next_fire(expr, new_materialized)
        updated_entry = dict(entry)
        updated_entry["materialized_until"] = new_materialized
        cron_registry.update_for_partition(
            {cron_id: updated_entry}, partition=partition
        )
        return next_fire

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
