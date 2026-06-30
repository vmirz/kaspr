import asyncio
import hashlib
import json
from collections import defaultdict
from math import floor
from mode import Service
from typing import Any, Set, MutableMapping, Mapping, List, Sequence, Iterator, Optional
from mode.utils.objects import cached_property
from mode.utils.locks import Event
from faust.types import TP, StreamT, EventT, TopicT
from kaspr.types import (
    KasprAppT,
    KasprTableT,
    MessageSchedulerT,
    CheckpointT,
    DispatcherT,
    JanitorT,
    CronTickerT,
    TTLocation,
    PT,
)
from kaspr.sensors.kaspr import KasprMonitor
from kaspr.utils.functional import iso_datestr_to_datetime
from .checkpoint import Checkpoint
from .dispatcher import Dispatcher
from .janitor import Janitor
from .ticker import CronTicker
from .utils import (
    create_message_key,
    current_timekey,
    prettydate,
    locdiff,
    SchedulerPart,
    TK_LIVE_SUFFIX,
    validate_cron_expr,
    compute_next_fire,
    compute_fires_in_window,
    cron_min_interval,
    due_index_key,
)
from faust.serializers.codecs import get_codec
from faust.utils import terminal

TableDataT = Sequence[Sequence[str]]

json_codec = get_codec("json")

SCHEDULER_ACTION_ADD = "ADD"
SCHEDULER_ACTION_CANCEL = "CANCEL"
SCHEDULER_ACTION_REPLACE = "REPLACE"
SCHEDULER_ACTION_CRON_ADD = "CRON_ADD"
SCHEDULER_ACTION_CRON_CANCEL = "CRON_CANCEL"
SCHEDULER_ACTION_CRON_PAUSE = "CRON_PAUSE"
SCHEDULER_ACTION_CRON_RESUME = "CRON_RESUME"

H_SCHEDULER_ACTION = "x-scheduler-action"
H_SCHEDULER_DELIVER_AT = "x-scheduler-deliver-at"
H_SCHEDULER_DELIVER_TO = "x-scheduler-deliver-to"
H_SCHEDULER_REQUEST_ID = "x-scheduler-request-id"
H_SCHEDULER_CRON_EXPR = "x-scheduler-cron-expr"
H_SCHEDULER_CRON_MISSED_FIRE_POLICY = "x-scheduler-cron-missed-fire-policy"


class MessageScheduler(MessageSchedulerT, Service):
    """Kafka message scheduler service"""

    # Records all metrics related to message scheduling
    monitor: KasprMonitor

    _dispatchers: MutableMapping[int, Dispatcher]
    _janitors: MutableMapping[int, Janitor]

    topics_created: Event
    timetable_recovered: Event
    can_distribute: Event

    #: Number of messages added to timetable by partition
    #: since startup
    scheduled_total: Mapping[int, int]

    #: Number of messages immediately sent out
    #: by scheduler because the message was
    #: already past due (since startup by partition)
    instant_send_total: Mapping[int, int]

    #: Number of REPLACE actions that replaced an existing schedule
    #: (since startup by partition)
    replaced_total: Mapping[int, int]

    #: Number of REPLACE actions that were strict no-ops because
    #: the existing schedule was identical (since startup by partition)
    replace_noop_total: Mapping[int, int]

    #: Number of CANCEL actions that canceled an existing schedule
    #: (since startup by partition)
    canceled_total: Mapping[int, int]

    #: cached topics of delivery destinations
    _out_topics: Mapping[str, TopicT]

    def __init__(self, app: KasprAppT, **kwargs: Any) -> None:
        self.app = app
        self.monitor = self.app.monitor
        self.topics_created = Event()
        self.timetable_recovered = Event()
        self.can_distribute = Event()
        self.scheduled_total = defaultdict(int)
        self.instant_send_total = defaultdict(int)
        self.replaced_total = defaultdict(int)
        self.replace_noop_total = defaultdict(int)
        self.canceled_total = defaultdict(int)
        self._dispatchers = {}
        self._janitors = {}
        self._tickers = {}
        self._out_topics = {}
        super().__init__(**kwargs)

        # Attach event hooks to changes to partitions so
        # we can adjust dispatchers and janitors accordingly
        self.app.on_rebalance_started.connect(self.on_rebalance_started)
        self.app.on_partitions_assigned.connect(self.on_partitions_assigned)
        self.app.on_partitions_revoked.connect(self.on_partitions_revoked)
        self.timetable.on_table_recovery_completed.connect(
            self.on_timetable_recovery_completed
        )
        self.schedule_index.on_table_recovery_completed.connect(
            self.on_schedule_index_recovery_completed
        )
        if self.app.conf.scheduler_cron_enabled:
            self.cron_registry.on_table_recovery_completed.connect(
                self.on_cron_registry_recovery_completed
            )
            self.cron_due_index.on_table_recovery_completed.connect(
                self.on_cron_due_index_recovery_completed
            )

        # Attach streaming agents now
        self.app.agent(self.schedule_actions_topic, name=self.process_actions.__name__)(
            self.process_actions
        )
        self.app.agent(self.schedule_requests_topic, name=self.distribute.__name__)(
            self.distribute
        )

    def on_init_dependencies(self):
        return [self.checkpoints, *self._dispatchers.values(), *self._janitors.values(), *self._tickers.values()]

    async def on_start(self) -> None:
        pass

    async def on_started(self) -> None:
        pass

    async def on_stop(self) -> None:
        self.pause_dispatchers()
        self.pause_janitors()
        self.pause_tickers()
        await self.wait_empty_dispatchers_and_janitors()
        await self.checkpoints.flush()

    async def maybe_create_topics(self):
        """Create topics required for scheduling service."""

        # Ensure producer has starter before creating topics.
        await self.app.producer.maybe_start()

        topics = [
            self.schedule_rejections_topic.maybe_declare(),
            self.schedule_requests_topic.maybe_declare(),
            self.schedule_actions_topic.maybe_declare(),
        ]
        await asyncio.gather(*topics)
        self.topics_created.set()

    async def on_timetable_recovery_completed(
        self, sender: Any, actives, standbys, **kwargs
    ):
        self.timetable_recovered.set()
        self.can_distribute.set()
        self.checkpoints.resume()
        self.resume_dispatchers()
        self.resume_janitors()
        self.resume_tickers()

    async def on_schedule_index_recovery_completed(
            self, sender: Any, actives, standbys, **kwargs
    ):
        pass

    async def on_cron_registry_recovery_completed(
            self, sender: Any, actives, standbys, **kwargs
    ):
        pass

    async def on_cron_due_index_recovery_completed(
            self, sender: Any, actives, standbys, **kwargs
    ):
        pass

    def on_rebalance_started(self, sender: Any, **kwargs):
        self.timetable_recovered.clear()
        self.can_distribute.clear()
        self.checkpoints.on_rebalance_started()
        self.checkpoints.pause()
        self.pause_dispatchers()
        self.pause_janitors()
        self.pause_tickers()

    async def stop_and_revoke_all_dispatchers_and_janitors(self):
        """Stop and revoke all dispatchers, janitors, and tickers"""
        tasks = []
        for partition in self.dispatcher_partitions:
            tasks.append(self._revoke_dispatcher(partition))
        for partition in self.janitor_partitions:
            tasks.append(self._revoke_janitor(partition))
        for partition in list(self._tickers.keys()):
            tasks.append(self._revoke_ticker(partition))
        if tasks:
            await self.wait_many(tasks)

    def pause_dispatchers(self):
        """Pause all dispatcher work."""
        for dispatcher in self._dispatchers.values():
            dispatcher.pause()

    def pause_janitors(self):
        """Pause all janitor work."""
        for janitor in self._janitors.values():
            janitor.pause()

    def resume_dispatchers(self):
        """Resume all dispatcher work."""
        for dispatcher in self._dispatchers.values():
            dispatcher.resume()

    def resume_janitors(self):
        """Resume all janitor work."""
        for janitor in self._janitors.values():
            janitor.resume()

    def pause_tickers(self):
        """Pause all cron tickers."""
        for ticker in self._tickers.values():
            ticker.pause()

    def resume_tickers(self):
        """Resume all cron tickers."""
        for ticker in self._tickers.values():
            ticker.resume()

    async def wait_empty_dispatchers_and_janitors(self):
        """Wait for dispatchers and janitors to finish processing unacked messages."""
        await asyncio.gather(
            self._dispatchers_wait_empty(), self._janitors_wait_empty()
        )

    async def _dispatchers_wait_empty(self):
        """Wait for all dispatchers to finish processing unacked deliveries.
        Used during graceful shutdown.
        """
        tasks = []
        self.log.info("Waiting for dispatchers...")
        for partition in self.dispatcher_partitions:
            tasks.append(self._dispatchers[partition].wait_empty())
        if tasks:
            await asyncio.gather(*tasks)

    async def _janitors_wait_empty(self):
        """Wait for all janitors to finish processing unacked removals.
        Used during graceful shutdown.
        """
        tasks = []
        self.log.info("Waiting for janitors...")
        for partition in self.janitor_partitions:
            tasks.append(self._janitors[partition].wait_empty())
        if tasks:
            await asyncio.gather(*tasks)

    async def on_partitions_assigned(self, sender: Any, assigned: Set[TP], **kwargs):
        """Called when new topic partions are assigned to worker."""
        _tt_assigned = set(
            tp
            for tp in assigned
            if tp[0] == self.timetable.changelog_topic.get_topic_name()
        )
        await self._on_timetable_partitions_assigned(_tt_assigned)

    async def on_partitions_revoked(self, sender: Any, revoked: Set[TP], **kwargs):
        """Called when active topic partions are revoked from worker."""
        # Wait for any in-flight deliveries/removals to complete
        try:
            await asyncio.wait_for(
                self.wait_empty_dispatchers_and_janitors(),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            self.log.warning(
                "Timed out waiting for scheduler's in-flight work during partition revocation"
            )        
        await self.checkpoints.flush()
        await self.stop_and_revoke_all_dispatchers_and_janitors()
        self.checkpoints.pause()

    async def _on_timetable_partitions_assigned(self, assigned: Set[TP]):
        """Create dispatchers and janitors for newly assigned partitions."""
        timetable_actives = self.app.assignor.assigned_actives().intersection(assigned)
        for _, partition in timetable_actives:
            tasks = [
                self.assign_dispatcher(partition),
                self.assign_janitor(partition),
            ]
            if self.app.conf.scheduler_cron_enabled:
                tasks.append(self.assign_ticker(partition))
            await self.wait_many(tasks)
        self.log.info("Scheduler checkpoints:")
        self.log.info(self._checkpoints_logtable())

    def create_dispatcher(self, partition: int) -> DispatcherT:
        return Dispatcher(
            app=self.app,
            monitor=self.app.monitor,
            partition=partition,
            loop=self.loop,
            beacon=self.beacon,
        )

    def create_janitor(self, partition: int) -> JanitorT:
        return Janitor(
            app=self.app,
            monitor=self.app.monitor,
            partition=partition,
            loop=self.loop,
            beacon=self.beacon,
        )

    async def assign_dispatcher(self, partition: int):
        if partition not in self._dispatchers:
            dispatcher = self.create_dispatcher(partition)
            self._dispatchers[dispatcher.partition] = dispatcher
            self.monitor.on_dispatcher_assigned(dispatcher)
            await self.add_runtime_dependency(dispatcher)

    async def assign_janitor(self, partition: int):
        if partition not in self._janitors:
            janitor = self.create_janitor(partition)
            self._janitors[janitor.partition] = janitor
            self.monitor.on_janitor_assigned(janitor)
            await self.add_runtime_dependency(janitor)

    async def _revoke_dispatcher(self, partition: int):
        if partition in self._dispatchers:
            dispatcher = self._dispatchers[partition]
            await self.remove_dependency(dispatcher)
            self.monitor.on_dispatcher_revoked(dispatcher)
            self._dispatchers.pop(partition)

    async def _revoke_janitor(self, partition: int):
        if partition in self._janitors:
            janitor = self._janitors[partition]
            await self.remove_dependency(janitor)
            self.monitor.on_janitor_revoked(janitor)
            self._janitors.pop(partition)

    def create_ticker(self, partition: int) -> CronTickerT:
        return CronTicker(
            app=self.app,
            monitor=self.app.monitor,
            partition=partition,
            loop=self.loop,
            beacon=self.beacon,
        )

    async def assign_ticker(self, partition: int):
        if not self.app.conf.scheduler_cron_enabled:
            return
        if partition not in self._tickers:
            ticker = self.create_ticker(partition)
            self._tickers[ticker.partition] = ticker
            await self.add_runtime_dependency(ticker)

    async def _revoke_ticker(self, partition: int):
        if partition in self._tickers:
            ticker = self._tickers[partition]
            await self.remove_dependency(ticker)
            self._tickers.pop(partition)

    def _schedule_requests_topic_name(self) -> str:
        return f"{self.app.conf.id}-schedule-requests"

    def _schedule_actions_topic_name(self) -> str:
        return f"{self.app.conf.id}-schedule-actions"

    def _schedule_rejections_topic_name(self) -> str:
        return f"{self.app.conf.id}-schedule-rejections"

    def prepare_schedule_requests_topic(self) -> TopicT:
        """Prepare the requests topic."""
        return self.app.topic(
            self._schedule_requests_topic_name(),
            partitions=self.app.conf.scheduler_topic_partitions,
        )

    def prepare_schedule_actions_topic(self) -> TopicT:
        """Prepare the actions topic."""
        return self.app.topic(
            self._schedule_actions_topic_name(),
            partitions=self.app.conf.scheduler_topic_partitions,
        )

    def prepare_schedule_rejections_topic(self) -> TopicT:
        """Prepare the rejections topic."""
        return self.app.topic(self._schedule_rejections_topic_name())

    def prepare_timetable(self):
        """Prepare the timetable table."""
        return self.app.Table(
            "timetable",
            partitions=self.app.conf.scheduler_topic_partitions,
            options={
                "write_buffer_size": self.app.conf.store_rocksdb_write_buffer_size,
                "max_write_buffer_number": self.app.conf.store_rocksdb_max_write_buffer_number,
                "target_file_size_base": self.app.conf.store_rocksdb_target_file_size_base,
                "block_cache_size": self.app.conf.store_rocksdb_block_cache_size,
                "block_cache_compressed_size": self.app.conf.store_rocksdb_block_cache_compressed_size,
                "bloom_filter_size": self.app.conf.store_rocksdb_bloom_filter_size,
                "set_cache_index_and_filter_blocks": self.app.conf.store_rocksdb_set_cache_index_and_filter_blocks,
            },
        )

    def prepare_schedule_index(self):
        """Prepare the schedule index table.

        The index maps producer-supplied request IDs to their
        Timetable location so that CANCEL can look up messages
        without scanning.
        """
        return self.app.Table(
            "timetable-index",
            partitions=self.app.conf.scheduler_topic_partitions,
            options={
                "write_buffer_size": self.app.conf.store_rocksdb_write_buffer_size,
                "max_write_buffer_number": self.app.conf.store_rocksdb_max_write_buffer_number,
                "target_file_size_base": self.app.conf.store_rocksdb_target_file_size_base,
                "block_cache_size": self.app.conf.store_rocksdb_block_cache_size,
                "block_cache_compressed_size": self.app.conf.store_rocksdb_block_cache_compressed_size,
                "bloom_filter_size": self.app.conf.store_rocksdb_bloom_filter_size,
                "set_cache_index_and_filter_blocks": self.app.conf.store_rocksdb_set_cache_index_and_filter_blocks,
            },            
        )

    def prepare_cron_registry(self):
        """Prepare the cron registry table.

        The cron registry stores active cron schedule definitions
        keyed by the user-supplied request_id (cron_id).
        """
        return self.app.Table(
            "cron-registry",
            partitions=self.app.conf.scheduler_topic_partitions,
            options={
                "write_buffer_size": self.app.conf.store_rocksdb_write_buffer_size,
                "max_write_buffer_number": self.app.conf.store_rocksdb_max_write_buffer_number,
                "target_file_size_base": self.app.conf.store_rocksdb_target_file_size_base,
                "block_cache_size": self.app.conf.store_rocksdb_block_cache_size,
                "block_cache_compressed_size": self.app.conf.store_rocksdb_block_cache_compressed_size,
                "bloom_filter_size": self.app.conf.store_rocksdb_bloom_filter_size,
                "set_cache_index_and_filter_blocks": self.app.conf.store_rocksdb_set_cache_index_and_filter_blocks,
            },
        )

    def prepare_cron_due_index(self):
        """Prepare the cron due-time index table.

        Maps time-bucketed keys to cron IDs so the ticker can
        efficiently find only crons due within a window via
        prefix_scan. Key format: "{minute_bucket:010d}:{cron_id}".
        """
        return self.app.Table(
            "cron-due-index",
            partitions=self.app.conf.scheduler_topic_partitions,
            options={
                "write_buffer_size": self.app.conf.store_rocksdb_write_buffer_size,
                "max_write_buffer_number": self.app.conf.store_rocksdb_max_write_buffer_number,
                "target_file_size_base": self.app.conf.store_rocksdb_target_file_size_base,
                "block_cache_size": self.app.conf.store_rocksdb_block_cache_size,
                "block_cache_compressed_size": self.app.conf.store_rocksdb_block_cache_compressed_size,
                "bloom_filter_size": self.app.conf.store_rocksdb_bloom_filter_size,
                "set_cache_index_and_filter_blocks": self.app.conf.store_rocksdb_set_cache_index_and_filter_blocks,
            },
        )

    @cached_property
    def schedule_requests_topic(self) -> TopicT:
        """Topic for schedule requests."""
        return self.prepare_schedule_requests_topic()

    @cached_property
    def schedule_actions_topic(self) -> TopicT:
        """Topic for schedule actions."""
        return self.prepare_schedule_actions_topic()

    @cached_property
    def schedule_rejections_topic(self) -> TopicT:
        """Topic for schedule rejections."""
        return self.prepare_schedule_rejections_topic()

    @cached_property
    def timetable(self) -> KasprTableT:
        """Timetable table."""
        return self.prepare_timetable()

    @cached_property
    def schedule_index(self) -> KasprTableT:
        """Reverse index mapping request IDs to Timetable locations."""
        return self.prepare_schedule_index()

    @cached_property
    def cron_registry(self) -> KasprTableT:
        """Cron registry table mapping cron IDs to their definitions."""
        return self.prepare_cron_registry()

    @cached_property
    def cron_due_index(self) -> KasprTableT:
        """Due-time index for efficient cron tick lookups."""
        return self.prepare_cron_due_index()

    @property
    def dispatcher_partitions(self) -> Set[int]:
        """Return the set of known dispatcher partitions."""
        return set(self._dispatchers.keys())

    @property
    def janitor_partitions(self) -> Set[int]:
        """Return the set of known janitor partitions."""
        return set(self._janitors.keys())

    @cached_property
    def checkpoints(self) -> CheckpointT:
        """Checkpoint service."""
        return Checkpoint(
            app=self.app,
            monitor=self.app.monitor,
            loop=self.loop,
            beacon=self.beacon,
        )

    def _live_count_from_value(self, live_value: Any) -> int:
        """Extract live message count from timetable live record.

        Supports both legacy integer values and new dict values.
        """
        if isinstance(live_value, Mapping):
            live_value = live_value.get("count", 0)
        if live_value is None:
            return 0
        try:
            return max(0, int(live_value))
        except (TypeError, ValueError):
            return 0

    def _next_live_value(self, count: int, existing: Any = None) -> Mapping[str, Any]:
        """Build next live record while preserving future attributes in dict payload."""
        next_count = max(0, int(count))
        if isinstance(existing, Mapping):
            payload = dict(existing)
            payload["count"] = next_count
            return payload
        return {"count": next_count}

    def _request_partition(self, topic: TopicT, request_id: Any) -> Optional[int]:
        """Compute stable action partition for a request id."""
        if request_id is None:
            return None
        value = request_id.decode() if isinstance(request_id, bytes) else request_id
        if value is None:
            return None
        return self.app.producer.key_partition(
            topic.get_topic_name(), str(value).encode()
        ).partition

    def _decode_if_bytes(self, value: Any) -> Any:
        return value.decode() if isinstance(value, bytes) else value

    def _normalize_headers(self, headers: Optional[Mapping[Any, Any]]) -> Optional[Mapping[Any, Any]]:
        if not headers:
            return headers
        return {
            self._decode_if_bytes(k): self._decode_if_bytes(v)
            for k, v in headers.items()
        }

    def _build_message_entry(
        self,
        event: EventT,
        destination: str,
        request_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        kms_meta = {"d": destination}
        if request_id:
            kms_meta["rid"] = request_id
        return {
            "k": self._decode_if_bytes(event.key),
            "v": self._decode_if_bytes(event.value),
            "h": self._normalize_headers(event.headers),
            "__kms": kms_meta,
        }

    def _schedule_fingerprint(self, time_key: int, message_entry: Mapping[str, Any]) -> str:
        """Compute strict identity hash for schedule replacement no-op checks."""
        payload = {
            "tk": int(time_key),
            "d": (message_entry.get("__kms") or {}).get("d"),
            "k": self._decode_if_bytes(message_entry.get("k")),
            "v": self._decode_if_bytes(message_entry.get("v")),
            "h": self._normalize_headers(message_entry.get("h")),
        }
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _cancel_materialized_fires(
        self,
        entry: Mapping[str, Any],
        cron_id: str,
        partition: int,
        timetable,
        schedule_index,
    ):
        """Cancel all materialized timetable entries for a cron schedule.

        Recomputes fire times from last_fire to materialized_until
        using the cron expression and removes each from the timetable
        and schedule index.
        """
        materialized_until = entry.get("materialized_until")
        created_at = entry.get("created_at")
        expr = entry.get("expr")
        if not materialized_until or not created_at or not expr:
            return
        fires = compute_fires_in_window(expr, created_at, materialized_until)
        for fire_epoch in fires:
            fire_request_id = f"{cron_id}:{fire_epoch}"
            index_entry = schedule_index.get_for_partition(
                fire_request_id, partition=partition
            )
            if not index_entry:
                continue
            loc_time_key = index_entry["tk"]
            loc_sequence = index_entry["seq"]
            location = TTLocation(partition, loc_time_key, loc_sequence)
            message_key = create_message_key(location)
            timetable.del_for_partition(message_key, partition=partition)
            schedule_index.del_for_partition(fire_request_id, partition=partition)
            # Decrement live count
            live_key = f"{loc_time_key}{TK_LIVE_SUFFIX}"
            live_value = timetable.get_for_partition(live_key, partition=partition)
            live_count = self._live_count_from_value(live_value)
            if live_count > 0:
                timetable.update_for_partition(
                    {
                        live_key: self._next_live_value(
                            live_count - 1, existing=live_value
                        )
                    },
                    partition=partition,
                )

    def _consolidate_table_keys(self, data: TableDataT) -> Iterator[List[str]]:
        """Format terminal log table to reduce noise from duplicate keys.

        We log tables where the first row is the name of the topic,
        and it gets noisy when that name is repeated over and over.

        This function replaces repeating topic names
        with the ditto mark.

        Note:
            Data must be sorted.
        """
        prev_key: Optional[str] = None
        for key, *rest in data:
            if prev_key is not None and prev_key == key:
                yield ["〃", *rest]  # ditto
            else:
                yield [key, *rest]
            prev_key = key

    def _checkpoints_logtable(self) -> str:
        """Pretty table of scheduler checkpoint data."""
        _data: List[Set[Any]] = []
        for partition, dispatcher in self._dispatchers.items():
            pt = PT(SchedulerPart.dispatcher, partition)
            checkpoint = self.checkpoints.get(pt, default=dispatcher.default_checkpoint)
            _data.append(
                tuple(
                    [
                        dispatcher.type,
                        checkpoint.partition,
                        checkpoint.time_key,
                        checkpoint.sequence,
                        prettydate(checkpoint),
                        locdiff(dispatcher.highwater, checkpoint),
                    ]
                )
            )
        for partition, janitor in self._janitors.items():
            pt = PT(SchedulerPart.janitor, partition)
            checkpoint = self.checkpoints.get(pt, default=janitor.default_checkpoint)
            janitor_highwater = janitor.highwater
            _data.append(
                tuple(
                    [
                        janitor.type,
                        checkpoint.partition,
                        checkpoint.time_key,
                        checkpoint.sequence,
                        prettydate(checkpoint),
                        locdiff(janitor_highwater, checkpoint)
                        if janitor_highwater is not None
                        else "-",
                    ]
                )
            )
        return "\n" + terminal.logtable(
            self._consolidate_table_keys(_data),
            title="Timetable Partition Set",
            headers=[
                "process",
                "partition",
                "timekey",
                "sequence",
                "timestamp",
                "behind (seconds)",
            ],
        )

    def _stats_logtable(self) -> str:
        """Pretty table of scheduler checkpoint data."""
        _data: List[Set[Any]] = []
        for partition, dispatcher in self._dispatchers.items():
            last = dispatcher.last_location
            if last:
                _data.append(
                    tuple(
                        [
                            dispatcher.type,
                            last.partition,
                            last.time_key,
                            last.sequence,
                            prettydate(last),
                            locdiff(dispatcher.highwater, last),
                            self.monitor.dispatcher_state(
                                dispatcher
                            ).messages_delivered,
                            self.scheduled_total[partition],
                            self.instant_send_total[partition],
                            self.replaced_total[partition],
                            self.replace_noop_total[partition],
                            self.canceled_total[partition],
                        ]
                    )
                )
        for janitor in self._janitors.values():
            last = janitor.last_location
            if last:
                janitor_highwater = janitor.highwater
                _data.append(
                    tuple(
                        [
                            janitor.type,
                            last.partition,
                            last.time_key,
                            last.sequence,
                            prettydate(last),
                            locdiff(janitor_highwater, last)
                            if janitor_highwater is not None
                            else "-",
                            self.monitor.janitor_state(janitor).messages_removed,
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                        ]
                    )
                )
        return "\n" + terminal.logtable(
            self._consolidate_table_keys(_data),
            title="Lag",
            headers=[
                "process",
                "partition",
                "timekey",
                "sequence",
                "timestamp",
                "behind (seconds)",
                "deliveries",
                "total scheduled",
                "immediate sends",
                "replace hits",
                "replace no-ops",
                "cancel hits",
            ],
        )

    async def wait_until_topics_created(self):
        """Wait for all required topics to be created."""

        if not self.topics_created.is_set():
            self.log.info("Waiting for topics to be created...")
            await self.wait(self.topics_created)

    async def wait_until_timetable_recovered(self):
        """Wait for timetable to complete recovery."""

        if not self.timetable_recovered.is_set():
            self.log.info("Waiting for timetable to recover...")
            await self.wait(self.timetable_recovered)

    def on_instant_delivery(self, partition: int):
        """Call when message was delievered by scheduler.
        This happens when a message is past due at time of scheduling.

        Args:
            partition (int): the source partition that delivered the message
        """

        def _did_deliver(*args, **kwargs):
            self.monitor.on_message_delivered(partition=partition)

        return _did_deliver

    async def process_actions(self, stream: StreamT):
        """Schedule messages on the Timetable."""

        await self.wait_until_topics_created()
        out_topics = self._out_topics

        async for event in stream.events():
            event: EventT = event
            # Remove KMS related header keys
            action: bytes = event.headers.pop(H_SCHEDULER_ACTION)
            deliver_at: bytes = event.headers.pop(H_SCHEDULER_DELIVER_AT, None)
            deliver_to: bytes = event.headers.pop(H_SCHEDULER_DELIVER_TO, None)
            request_id: bytes = event.headers.pop(H_SCHEDULER_REQUEST_ID, None)

            _action = action.decode() if isinstance(action, bytes) else action
            partition = event.message.partition
            timetable = self.app.scheduler.timetable
            schedule_index = self.app.scheduler.schedule_index

            _request_id = (
                request_id.decode()
                if isinstance(request_id, bytes)
                else request_id
            )

            # --- CRON actions ---
            if _action in (
                SCHEDULER_ACTION_CRON_ADD,
                SCHEDULER_ACTION_CRON_CANCEL,
                SCHEDULER_ACTION_CRON_PAUSE,
                SCHEDULER_ACTION_CRON_RESUME,
            ):
                if not self.app.conf.scheduler_cron_enabled:
                    self.log.warning(
                        f"{_action}: cron scheduler is disabled; ignoring action"
                    )
                    continue
                cron_registry = self.app.scheduler.cron_registry
                cron_expr_raw: bytes = event.headers.pop(H_SCHEDULER_CRON_EXPR, None)

                if _action == SCHEDULER_ACTION_CRON_ADD:
                    _cron_expr = (
                        cron_expr_raw.decode()
                        if isinstance(cron_expr_raw, bytes)
                        else cron_expr_raw
                    )
                    if not validate_cron_expr(_cron_expr):
                        self.log.warning(
                            f"CRON_ADD: invalid cron expression '{_cron_expr}', skipping"
                        )
                        continue
                    min_interval = cron_min_interval(_cron_expr)
                    if min_interval < self.app.conf.scheduler_cron_min_interval_seconds:
                        self.log.warning(
                            f"CRON_ADD: cron interval {min_interval}s is below "
                            f"minimum {self.app.conf.scheduler_cron_min_interval_seconds}s, skipping"
                        )
                        continue
                    _deliver_to = self._decode_if_bytes(deliver_to)
                    now = current_timekey()
                    message_entry = self._build_message_entry(
                        event,
                        destination=_deliver_to,
                        request_id=_request_id,
                    )
                    # Extract missed_fire_policy, defaulting to "replay"
                    missed_fire_policy_raw = event.headers.pop(
                        H_SCHEDULER_CRON_MISSED_FIRE_POLICY, None
                    )
                    missed_fire_policy = (
                        missed_fire_policy_raw.decode()
                        if isinstance(missed_fire_policy_raw, bytes)
                        else missed_fire_policy_raw
                    )
                    if missed_fire_policy not in ("replay", "skip"):
                        missed_fire_policy = "replay"
                    registry_entry = {
                        "expr": _cron_expr,
                        "dest": _deliver_to,
                        "key": message_entry["k"],
                        "value": message_entry["v"],
                        "headers": message_entry["h"],
                        "status": "active",
                        "materialized_until": None,
                        "last_fire": now,
                        "created_at": now,
                        "missed_fire_policy": missed_fire_policy,
                    }
                    cron_registry.update_for_partition(
                        {_request_id: registry_entry}, partition=partition
                    )
                    # Write initial due-index entry for the first fire
                    next_fire = compute_next_fire(_cron_expr, now)
                    if next_fire:
                        cron_due_index = self.app.scheduler.cron_due_index
                        cron_due_index.update_for_partition(
                            {due_index_key(next_fire, _request_id): next_fire},
                            partition=partition,
                        )
                    self.log.info(
                        f"CRON_ADD: registered cron '{_cron_expr}' -> {_deliver_to} "
                        f"(id={_request_id}, partition={partition})"
                    )
                    self.monitor.on_cron_registered(partition=partition)

                elif _action == SCHEDULER_ACTION_CRON_PAUSE:
                    entry = cron_registry.get_for_partition(
                        _request_id, partition=partition
                    )
                    if not entry:
                        self.log.warning(
                            f"CRON_PAUSE: cron_id {_request_id} not found"
                        )
                        continue
                    if entry["status"] == "paused":
                        continue
                    # Remove due-index entry
                    cron_due_index = self.app.scheduler.cron_due_index
                    _after = entry.get("materialized_until") or entry.get("last_fire")
                    if _after:
                        _next = compute_next_fire(entry["expr"], _after)
                        if _next:
                            cron_due_index.del_for_partition(
                                due_index_key(_next, _request_id), partition=partition
                            )
                    # Cancel materialized fires
                    self._cancel_materialized_fires(
                        entry, _request_id, partition, timetable, schedule_index
                    )
                    now_pause = current_timekey()
                    updated_entry = dict(entry)
                    updated_entry["status"] = "paused"
                    updated_entry["materialized_until"] = None
                    updated_entry["paused_at"] = now_pause
                    cron_registry.update_for_partition(
                        {_request_id: updated_entry}, partition=partition
                    )
                    self.log.info(f"CRON_PAUSE: paused cron_id={_request_id}")
                    self.monitor.on_cron_paused(partition=partition)

                elif _action == SCHEDULER_ACTION_CRON_RESUME:
                    entry = cron_registry.get_for_partition(
                        _request_id, partition=partition
                    )
                    if not entry:
                        self.log.warning(
                            f"CRON_RESUME: cron_id {_request_id} not found"
                        )
                        continue
                    if entry["status"] == "active":
                        continue
                    now_resume = current_timekey()
                    updated_entry = dict(entry)
                    updated_entry["status"] = "active"
                    updated_entry["materialized_until"] = None

                    # Respect missed_fire_policy: "skip" or "replay" (default)
                    policy = entry.get("missed_fire_policy", "replay")
                    if policy == "skip":
                        # Skip policy: advance last_fire to now
                        updated_entry["last_fire"] = now_resume
                    else:
                        # Replay policy: set last_fire to pause time so ticker
                        # only backfills the paused gap (not since creation).
                        paused_at = entry.get("paused_at") or entry.get("last_fire")
                        updated_entry["last_fire"] = paused_at

                    cron_registry.update_for_partition(
                        {_request_id: updated_entry}, partition=partition
                    )
                    # Write due-index key at the next fire FROM NOW so it lands
                    # in a future bucket the ticker will scan. For replay, the
                    # ticker sees materialized_until=None, falls back to last_fire,
                    # and materializes the entire paused gap.
                    cron_due_index = self.app.scheduler.cron_due_index
                    next_due = compute_next_fire(entry["expr"], now_resume)
                    if next_due:
                        cron_due_index.update_for_partition(
                            {due_index_key(next_due, _request_id): next_due},
                            partition=partition,
                        )
                    policy_desc = "skip" if policy == "skip" else "replay"
                    self.log.info(
                        f"CRON_RESUME: resumed cron_id={_request_id} (policy={policy_desc})"
                    )
                    self.monitor.on_cron_resumed(partition=partition)

                elif _action == SCHEDULER_ACTION_CRON_CANCEL:
                    entry = cron_registry.get_for_partition(
                        _request_id, partition=partition
                    )
                    if not entry:
                        self.log.warning(
                            f"CRON_CANCEL: cron_id {_request_id} not found"
                        )
                        continue
                    # Remove due-index entry
                    cron_due_index = self.app.scheduler.cron_due_index
                    _after = entry.get("materialized_until") or entry.get("last_fire")
                    if _after:
                        _next = compute_next_fire(entry["expr"], _after)
                        if _next:
                            cron_due_index.del_for_partition(
                                due_index_key(_next, _request_id), partition=partition
                            )
                    # Cancel materialized fires
                    self._cancel_materialized_fires(
                        entry, _request_id, partition, timetable, schedule_index
                    )
                    cron_registry.del_for_partition(_request_id, partition=partition)
                    self.log.info(f"CRON_CANCEL: removed cron_id={_request_id}")
                    self.monitor.on_cron_canceled(partition=partition)

                continue

            replace_time_key: Optional[int] = None
            replace_destination: Optional[str] = None
            replace_message_entry: Optional[Mapping[str, Any]] = None
            replace_fingerprint: Optional[str] = None

            if _action == SCHEDULER_ACTION_REPLACE:
                if not deliver_at or not deliver_to:
                    self.log.warning(
                        "REPLACE: missing deliver_at or deliver_to, skipping"
                    )
                    continue
                try:
                    replace_time_key = int(self._decode_if_bytes(deliver_at))
                except (TypeError, ValueError):
                    self.log.warning("REPLACE: invalid deliver_at, skipping")
                    continue
                replace_destination = self._decode_if_bytes(deliver_to)
                replace_message_entry = self._build_message_entry(
                    event,
                    destination=replace_destination,
                    request_id=_request_id,
                )
                replace_fingerprint = self._schedule_fingerprint(
                    replace_time_key, replace_message_entry
                )

            if _action in (SCHEDULER_ACTION_CANCEL, SCHEDULER_ACTION_REPLACE):
                if not _request_id:
                    self.log.warning(f"{_action}: missing request_id, skipping")
                    continue
                index_entry = schedule_index.get_for_partition(
                    _request_id, partition=partition
                )
                if _action == SCHEDULER_ACTION_CANCEL and not index_entry:
                    self.log.warning(
                        f"Cancel: request_id {_request_id} not found"
                    )
                    continue
                if index_entry:
                    loc_time_key = index_entry["tk"]
                    loc_sequence = index_entry["seq"]
                    location = TTLocation(partition, loc_time_key, loc_sequence)
                    message_key = create_message_key(location)

                    if _action == SCHEDULER_ACTION_REPLACE:
                        existing_fingerprint = index_entry.get("fp")
                        if not existing_fingerprint:
                            existing_entry = timetable.get_for_partition(
                                message_key, partition=partition
                            )
                            if existing_entry:
                                existing_fingerprint = self._schedule_fingerprint(
                                    loc_time_key, existing_entry
                                )
                        if (
                            existing_fingerprint
                            and replace_fingerprint
                            and existing_fingerprint == replace_fingerprint
                        ):
                            if not index_entry.get("fp"):
                                updated_index_entry = dict(index_entry)
                                updated_index_entry["fp"] = existing_fingerprint
                                schedule_index.update_for_partition(
                                    {_request_id: updated_index_entry},
                                    partition=partition,
                                )
                            self.replace_noop_total[partition] += 1
                            self.monitor.on_message_replace_noop(
                                partition=partition
                            )
                            self.log.dev(
                                f"REPLACE: no-op for {_request_id}; identical schedule already exists"
                            )
                            continue

                    timetable.del_for_partition(message_key, partition=partition)
                    schedule_index.del_for_partition(_request_id, partition=partition)
                    # Decrement the live count for this timekey
                    live_key = f"{loc_time_key}{TK_LIVE_SUFFIX}"
                    live_value = timetable.get_for_partition(
                        live_key, partition=partition
                    )
                    live_count = self._live_count_from_value(live_value)
                    if live_count > 0:
                        timetable.update_for_partition(
                            {
                                live_key: self._next_live_value(
                                    live_count - 1, existing=live_value
                                )
                            },
                            partition=partition,
                        )
                    self.log.dev(
                        f"{_action}: removed existing scheduled message: {_request_id} at {location}"
                    )
                    if _action == SCHEDULER_ACTION_REPLACE:
                        self.replaced_total[partition] += 1
                        self.monitor.on_message_replaced(partition=partition)
                    elif _action == SCHEDULER_ACTION_CANCEL:
                        self.canceled_total[partition] += 1
                        self.monitor.on_message_canceled(partition=partition)

                if _action == SCHEDULER_ACTION_CANCEL:
                    continue

            if _action in (SCHEDULER_ACTION_ADD, SCHEDULER_ACTION_REPLACE):
                if not deliver_at or not deliver_to:
                    self.log.warning(
                        f"{_action}: missing deliver_at or deliver_to, skipping"
                    )
                    continue
                _topic_name = self._decode_if_bytes(deliver_to)
                time_key = str(self._decode_if_bytes(deliver_at))

                # a message attemping to be scheduled at or before the current timekey
                # is considered past due. We send past due messages immediately.
                # NOTE: `distribute` does this as well.
                if int(time_key) < current_timekey():
                    if not out_topics.get(_topic_name):
                        out_topics[_topic_name] = self.app.topic(_topic_name)
                    self.instant_send_total[partition] += 1
                    await out_topics[_topic_name].send(
                        key=event.message.key,
                        value=event.message.value,
                        headers=event.headers,
                        callback=self.on_instant_delivery(partition),
                    )
                    continue

                message_total = (
                    timetable.get_for_partition(time_key, partition=partition) or 0
                )
                location = TTLocation(partition, int(time_key), sequence=message_total)
                message_key = create_message_key(location)

                if _action == SCHEDULER_ACTION_REPLACE and replace_message_entry:
                    message_entry = replace_message_entry
                else:
                    message_entry = self._build_message_entry(
                        event,
                        destination=_topic_name,
                        request_id=_request_id,
                    )

                entry_fingerprint = self._schedule_fingerprint(int(time_key), message_entry)
                live_key = f"{time_key}{TK_LIVE_SUFFIX}"
                live_value = timetable.get_for_partition(live_key, partition=partition)
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

                # Store reverse index entry when request_id is provided
                if _request_id:
                    schedule_index.update_for_partition(
                        {
                            _request_id: {
                                "tk": int(time_key),
                                "seq": message_total,
                                "fp": entry_fingerprint,
                            }
                        },
                        partition=partition,
                    )

                self.monitor.on_message_scheduled(location)
                self.scheduled_total[partition] += 1

    async def distribute(self, stream: StreamT):
        """Transform requests into Timetable scheduling actions."""

        await self.wait_until_topics_created()

        # Wait for initial timetable recovery before processing events.
        # This ensures producer metadata is available for partition routing
        # on first startup. We MUST NOT block inside the event loop while
        # holding an unacked message, as that deadlocks wait_empty() during
        # partition revocation (rebalance). Flow control already prevents
        # new events from being delivered during rebalance, so the loop
        # pauses naturally after processing any in-flight event.
        if not self.can_distribute.is_set():
            await self.wait(self.can_distribute)

        out_topics = self._out_topics
        rejections_topic = self.schedule_rejections_topic
        actions_topic = self.schedule_actions_topic

        async for event in stream.events():
            event: EventT = event
            partition = event.message.partition
            action: bytes = event.headers.pop(
                H_SCHEDULER_ACTION, SCHEDULER_ACTION_ADD.encode()
            )
            deliver_at: bytes = event.headers.pop(H_SCHEDULER_DELIVER_AT, None)
            deliver_to: bytes = event.headers.pop(H_SCHEDULER_DELIVER_TO, None)
            request_id: bytes = event.headers.pop(H_SCHEDULER_REQUEST_ID, None)

            _action = action.decode() if isinstance(action, bytes) else action

            # --- CRON actions: route to actions topic ---
            if _action in (
                SCHEDULER_ACTION_CRON_ADD,
                SCHEDULER_ACTION_CRON_CANCEL,
                SCHEDULER_ACTION_CRON_PAUSE,
                SCHEDULER_ACTION_CRON_RESUME,
            ):
                if not self.app.conf.scheduler_cron_enabled:
                    error_entry = {
                        "key": event.key,
                        "value": event.value,
                        "headers": event.headers,
                        "errors": [
                            "Cron scheduler is disabled (SCHEDULER_CRON_ENABLED=false)"
                        ],
                    }
                    await rejections_topic.send(
                        key=event.key, value=json_codec.dumps(error_entry)
                    )
                    continue
                cron_expr: bytes = event.headers.pop(H_SCHEDULER_CRON_EXPR, None)
                # All cron actions require request_id
                if not request_id:
                    error_entry = {
                        "key": event.key,
                        "value": event.value,
                        "headers": event.headers,
                        "errors": [
                            f"Missing required header `{H_SCHEDULER_REQUEST_ID}` for {_action} action"
                        ],
                    }
                    await rejections_topic.send(
                        key=event.key, value=json_codec.dumps(error_entry)
                    )
                    continue
                # CRON_ADD requires cron_expr and deliver_to
                if _action == SCHEDULER_ACTION_CRON_ADD:
                    errors = []
                    if not cron_expr:
                        errors.append(
                            f"Missing required header `{H_SCHEDULER_CRON_EXPR}` for {_action} action"
                        )
                    if not deliver_to:
                        errors.append(
                            f"Missing required header `{H_SCHEDULER_DELIVER_TO}` for {_action} action"
                        )
                    if errors:
                        error_entry = {
                            "key": event.key,
                            "value": event.value,
                            "headers": event.headers,
                            "errors": errors,
                        }
                        await rejections_topic.send(
                            key=event.key, value=json_codec.dumps(error_entry)
                        )
                        continue
                scheduler_headers = {
                    H_SCHEDULER_ACTION: action,
                    H_SCHEDULER_REQUEST_ID: request_id,
                }
                if deliver_to:
                    scheduler_headers[H_SCHEDULER_DELIVER_TO] = deliver_to
                if cron_expr:
                    scheduler_headers[H_SCHEDULER_CRON_EXPR] = cron_expr
                headers = event.headers or {}
                headers.update(scheduler_headers)
                _partition = self._request_partition(actions_topic, request_id)
                send_kwargs = {"partition": _partition} if _partition is not None else {}

                await actions_topic.send(
                    key=event.message.key,
                    value=event.message.value,
                    headers=headers,
                    **send_kwargs,
                )
                continue

            # --- CANCEL: only requires request_id ---
            if _action == SCHEDULER_ACTION_CANCEL:
                if not request_id:
                    error_entry = {
                        "key": event.key,
                        "value": event.value,
                        "headers": event.headers,
                        "errors": [
                            f"Missing required header `{H_SCHEDULER_REQUEST_ID}` for CANCEL action"
                        ],
                    }
                    await rejections_topic.send(
                        key=event.key, value=json_codec.dumps(error_entry)
                    )
                    continue
                scheduler_headers = {
                    H_SCHEDULER_ACTION: action,
                    H_SCHEDULER_REQUEST_ID: request_id,
                }
                headers = event.headers or {}
                headers.update(scheduler_headers)
                _partition = self._request_partition(actions_topic, request_id)
                send_kwargs = {"partition": _partition} if _partition is not None else {}

                await actions_topic.send(
                    key=event.message.key,
                    value=event.message.value,
                    headers=headers,
                    **send_kwargs,
                )
                continue

            # --- REPLACE: request_id required ---
            if _action == SCHEDULER_ACTION_REPLACE and not request_id:
                error_entry = {
                    "key": event.key,
                    "value": event.value,
                    "headers": event.headers,
                    "errors": [
                        f"Missing required header `{H_SCHEDULER_REQUEST_ID}` for REPLACE action"
                    ],
                }
                await rejections_topic.send(
                    key=event.key, value=json_codec.dumps(error_entry)
                )
                continue

            # --- ADD (default): requires deliver_at and deliver_to ---
            if not deliver_at or not deliver_to:
                errors = []
                if not deliver_at:
                    errors.append(f"Missing required header `{H_SCHEDULER_DELIVER_AT}`")
                if not deliver_to:
                    errors.append(f"Missing required header `{H_SCHEDULER_DELIVER_TO}`")
                error_entry = {
                    "key": event.key,
                    "value": event.value,
                    "headers": event.headers,
                    "errors": errors,
                }
                await rejections_topic.send(
                    key=event.key, value=json_codec.dumps(error_entry)
                )
                continue

            try:
                timekey = floor(
                    iso_datestr_to_datetime(deliver_at.decode()).timestamp()
                )
            except Exception as ex:
                error_entry = {
                    "key": event.key,
                    "value": event.value,
                    "headers": event.headers,
                    "errors": [ex],
                }
                await rejections_topic.send(
                    key=event.key, value=json_codec.dumps(error_entry)
                )
                continue

            # a message attemping to be scheduled at or before the current timekey
            # is considered past due. We send past due messages immediately.
            # NOTE: `process_actions` does this as well
            if timekey < current_timekey():
                _topic_name = deliver_to.decode()
                if not out_topics.get(_topic_name):
                    out_topics[_topic_name] = self.app.topic(_topic_name)
                self.instant_send_total[partition] += 1
                await out_topics[_topic_name].send(
                    key=event.message.key,
                    value=event.message.value,
                    headers=event.headers,
                    callback=self.on_instant_delivery(partition),
                )
                continue

            scheduler_headers = {
                H_SCHEDULER_ACTION: action,
                H_SCHEDULER_DELIVER_AT: f"{timekey}".encode(),
                H_SCHEDULER_DELIVER_TO: deliver_to,
            }
            if request_id:
                scheduler_headers[H_SCHEDULER_REQUEST_ID] = request_id
            headers = event.headers or {}
            headers.update(scheduler_headers)
            _partition = self._request_partition(actions_topic, request_id)
            send_kwargs = {"partition": _partition} if _partition is not None else {}

            await actions_topic.send(
                key=event.message.key,
                value=event.message.value,
                headers=headers,
                **send_kwargs,
            )

    @Service.task
    async def _print_stats(self):
        """Periodically print scheduler stats to terminal."""
        if not self.app.conf.scheduler_debug_stats_enabled:
            return
        while not self.should_stop:
            self.log.info(self._stats_logtable())
            await self.sleep(5)
