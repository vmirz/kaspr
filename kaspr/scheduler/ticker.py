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

        while not self.flow_active:
            await self.wait(self.can_resume)
            if self.should_stop:
                return

        # On first activation, catch up any fires missed during downtime
        # before entering the regular tick loop.
        try:
            self._catchup_missed_fires(buffer=buffer)
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
            else:
                self.monitor.on_cron_ticker_tick(partition=self.partition)

    def _catchup_missed_fires(self, buffer: float):
        """Recover stale crons after arbitrary-length downtime.

        Scans the due-index for entries stranded in past minute buckets.
        For each stale cron, checks its missed_fire_policy from the registry:
        - "replay": materializes all missed fires through the window
        - "skip": advances the cron to now, skipping historical fires

        Keys are sorted chronologically (zero-padded bucket prefix), so
        iteration stops as soon as a current/future bucket is reached.
        """
        cron_due_index = self.app.scheduler.cron_due_index
        partition = self.partition
        now = current_timekey()
        now_bucket = int(now) // 60
        window_end = int(now + buffer)

        # Collect stale entries (due-index keys in past minute buckets).
        # Since keys are zero-padded "{bucket:010d}:{cron_id}", they sort
        # chronologically. We stop as soon as we hit a current/future bucket.
        stale_entries: List[Tuple[str, str, int]] = []
        for key, fire_epoch in cron_due_index.items_for_partition(partition):
            parts = key.split(":", 1)
            if len(parts) != 2:
                continue
            try:
                bucket = int(parts[0])
            except (ValueError, TypeError):
                continue
            if bucket >= now_bucket:
                break
            stale_entries.append((key, parts[1], fire_epoch))

        if not stale_entries:
            return

        self.log.info(
            f"CronTicker(partition={partition}): recovering "
            f"{len(stale_entries)} stale cron(s)"
        )

        # Separate by policy and handle accordingly.
        replay_entries: List[Tuple[str, str, int]] = []
        skip_entries: List[Tuple[str, str]] = []

        cron_registry = self.app.scheduler.cron_registry
        for index_key, cron_id, fire_epoch in stale_entries:
            entry = cron_registry.get_for_partition(cron_id, partition=partition)
            if not entry or entry.get("status") != "active":
                # Inactive cron, treat as replay (will be cleaned up).
                replay_entries.append((index_key, cron_id, fire_epoch))
                continue

            policy = entry.get("missed_fire_policy", "replay")
            if policy == "skip":
                skip_entries.append((index_key, cron_id))
            else:
                replay_entries.append((index_key, cron_id, fire_epoch))

        # Handle "skip" entries: advance them to now.
        if skip_entries:
            self.log.info(
                f"CronTicker(partition={partition}): advancing {len(skip_entries)} "
                f"cron(s) to now (missed_fire_policy=skip)"
            )
            self._advance_stale_entries_to_now(skip_entries)
            self.monitor.on_cron_fires_skipped(
                partition=partition, count=len(skip_entries)
            )

        # Handle "replay" entries: materialize all missed fires.
        if replay_entries:
            self.log.info(
                f"CronTicker(partition={partition}): replaying missed fires for "
                f"{len(replay_entries)} cron(s) (missed_fire_policy=replay)"
            )
            self._process_due_entries(due_entries=replay_entries, window_end=window_end)
            self.monitor.on_cron_fires_missed(
                partition=partition, count=len(replay_entries)
            )

        # Materialize the current buffer window (includes freshly-recovered crons).
        self._materialize_window(buffer)

    def _advance_stale_entries_to_now(
        self, stale_entries: List[Tuple[str, str]]
    ) -> None:
        """Advance stale due-index entries to now without replaying history."""
        cron_due_index = self.app.scheduler.cron_due_index
        cron_registry = self.app.scheduler.cron_registry
        partition = self.partition
        now = int(current_timekey())

        index_deletes: List[str] = []
        index_inserts: Dict[str, int] = {}

        for index_key, cron_id in stale_entries:
            entry = cron_registry.get_for_partition(cron_id, partition=partition)
            if not entry or entry.get("status") != "active":
                index_deletes.append(index_key)
                continue

            expr = entry.get("expr")
            if not expr:
                index_deletes.append(index_key)
                continue

            # Compute next fire from now, skip all historical fires.
            next_fire = compute_next_fire(expr, now)

            # Update registry so ticker doesn't try to materialize the gap.
            updated_entry = dict(entry)
            updated_entry["materialized_until"] = now
            cron_registry.update_for_partition(
                {cron_id: updated_entry}, partition=partition
            )

            index_deletes.append(index_key)
            if next_fire:
                index_inserts[due_index_key(next_fire, cron_id)] = next_fire

        for old_key in index_deletes:
            cron_due_index.del_for_partition(old_key, partition=partition)

        if index_inserts:
            cron_due_index.update_for_partition(index_inserts, partition=partition)

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

        Past fires (fire_epoch < now) are written to the timetable at the
        current time so the Dispatcher — which only scans forward from its
        checkpoint — will pick them up.  The original fire timestamp is
        preserved in the ``x-scheduler-cron-fire-timestamp`` header.
        """
        cron_registry = self.app.scheduler.cron_registry
        timetable = self.app.scheduler.timetable
        schedule_index = self.app.scheduler.schedule_index

        dest = entry.get("dest")
        msg_key = entry.get("key")
        msg_value = entry.get("value")
        msg_headers = entry.get("headers") or {}
        expr = entry.get("expr")
        now = int(current_timekey())

        for fire_epoch in fires:
            fire_request_id = f"{cron_id}:{fire_epoch}"

            # Skip if already written
            if schedule_index.get_for_partition(fire_request_id, partition=partition):
                continue

            # For past fires, place them at the current second so the
            # Dispatcher (which never revisits past time_keys) will find them.
            effective_tk = fire_epoch if fire_epoch >= now else now
            time_key = str(effective_tk)

            message_total = (
                timetable.get_for_partition(time_key, partition=partition) or 0
            )
            location = TTLocation(partition, effective_tk, sequence=message_total)
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
                {fire_request_id: {"tk": effective_tk, "seq": message_total}},
                partition=partition,
            )

            self.monitor.on_cron_fire_materialized(partition=partition)

        # Update registry with new materialized_until and last_fire
        new_materialized = max(fires)
        next_fire = compute_next_fire(expr, new_materialized)
        updated_entry = dict(entry)
        updated_entry["materialized_until"] = new_materialized
        updated_entry["last_fire"] = new_materialized
        cron_registry.update_for_partition(
            {cron_id: updated_entry}, partition=partition
        )
        return next_fire

    def _live_count_from_value(self, live_value) -> int:
        """Extract live count integer from timetable live-key value.

        Supports both legacy integer values and dict payloads ({"count": N}).
        """
        if isinstance(live_value, dict):
            live_value = live_value.get("count", 0)
        if live_value is None:
            return 0
        try:
            return max(0, int(live_value))
        except (TypeError, ValueError):
            return 0

    def _next_live_value(self, count: int, existing=None):
        """Build next live-key value preserving dict payload format and attributes."""
        next_count = max(0, int(count))
        if isinstance(existing, dict):
            payload = dict(existing)
            payload["count"] = next_count
            return payload
        return {"count": next_count}
