import asyncio
from collections import defaultdict
from math import floor
from mode import Service
from typing import Any, Set, MutableMapping, Mapping, List, Sequence, Iterator, Optional
from mode.utils.objects import cached_property
from mode.utils.locks import Event
from faust.types import TP, StreamT, EventT, TopicT
from kaspr.types import (
    KasprAppT,
    MessageSchedulerT,
    CheckpointT,
    DispatcherT,
    JanitorT,
    TTLocation,
    PT,
)
from kaspr.sensors.kaspr import KasprMonitor
from kaspr.utils.functional import iso_datestr_to_datetime
from kaspr.scheduler import Checkpoint, Dispatcher, Janitor
from .utils import (
    create_message_key,
    current_timekey,
    prettydate,
    locdiff,
    SchedulerPart,
)
from faust.serializers.codecs import get_codec
from faust.utils import terminal

TableDataT = Sequence[Sequence[str]]

json_codec = get_codec("json")

KMS_ACTION_ADD = "ADD"

H_KMS_ACTION = "x-kms-action"
H_KMS_DELIVER_AT = "x-kms-deliver-at"
H_KMS_DELIVER_TO = "x-kms-deliver-to"


class MessageScheduler(MessageSchedulerT, Service):
    """Kafka message scheduler service"""

    # Records all metrics related to message scheduling
    monitor: KasprMonitor

    _dispatchers: MutableMapping[int, Dispatcher]
    _janitors: MutableMapping[int, Janitor]

    topics_created: Event
    timetable_recovered: Event

    #: Number of messages added to timetable by partition
    #: since startup
    scheduled_total: Mapping[int, int]

    #: Number of messages immediately sent out
    #: by scheduler because the message was
    #: already past due (since startup by partition)
    instant_send_total: Mapping[int, int]

    #: cached topics of delivery destinations
    _out_topics: Mapping[str, TopicT]

    def __init__(self, app: KasprAppT, **kwargs: Any) -> None:
        self.app = app
        self.monitor = self.app.monitor
        self.topics_created = Event()
        self.timetable_recovered = Event()
        self.scheduled_total = defaultdict(int)
        self.instant_send_total = defaultdict(int)
        self._dispatchers = {}
        self._janitors = {}
        self._out_topics = {}
        super().__init__(**kwargs)

        topic_prefix = app.conf.topic_prefix
        topic_partitions = (
            self.app.conf.kms_topic_partitions or self.app.conf.topic_partitions
        )
        self.topic_dlq = app.topic(f"{topic_prefix}kms-dlq", partitions=1)
        self.topic_input = app.topic(
            f"{topic_prefix}kms-input", partitions=topic_partitions
        )
        self.topic_timetable_changelog = app.topic(
            self.timetable_changelog_topic_name,
            compacting=True,
            deleting=True,
            partitions=topic_partitions,
        )
        self.topic_actions = app.topic(
            f"{topic_prefix}kms-actions", partitions=topic_partitions
        )
        self.timetable = app.Table(
            self.timetable_changelog_topic_name,
            changelog_topic=self.topic_timetable_changelog,
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

        # Attach event hooks to changes to partitions so
        # we can adjust dispatchers and janitors accordingly
        self.app.on_rebalance_started.connect(self.on_rebalance_started)
        self.app.on_partitions_assigned.connect(self.on_partitions_assigned)
        self.app.on_partitions_revoked.connect(self.on_partitions_revoked)
        self.timetable.on_table_recovery_completed.connect(
            self.on_timetable_recovery_completed
        )

        # Attach streaming agents now
        self.app.agent(self.topic_actions, name=self.process_actions.__name__)(
            self.process_actions
        )
        self.app.agent(self.topic_input, name=self.distribute.__name__)(self.distribute)

    def on_init_dependencies(self):
        return [self.checkpoints, *self._dispatchers.values(), *self._janitors.values()]

    async def on_start(self) -> None:
        pass

    async def on_started(self) -> None:
        pass

    async def on_stop(self) -> None:
        self.pause_dispatchers()
        self.pause_janitors()
        await self.wait_empty_dispatchers_and_janitors()
        await self.checkpoints.flush()

    async def maybe_create_topics(self):
        """Create topics required for scheduling service."""

        # Ensure producer has starter before creating topics.
        await self.app.producer.maybe_start()

        topics = [
            self.topic_dlq.maybe_declare(),
            self.topic_input.maybe_declare(),
            self.topic_actions.maybe_declare(),
        ]
        await asyncio.gather(*topics)
        self.topics_created.set()

    async def on_timetable_recovery_completed(
        self, sender: Any, actives, standbys, **kwargs
    ):
        self.timetable_recovered.set()
        self.checkpoints.resume()
        self.resume_dispatchers()
        self.resume_janitors()

    def on_rebalance_started(self, sender: Any, **kwargs):
        self.timetable_recovered.clear()
        self.checkpoints.on_rebalance_started()
        self.checkpoints.pause()
        self.pause_dispatchers()
        self.pause_janitors()

    async def stop_and_revoke_all_dispatchers_and_janitors(self):
        """Stop and revoke all dispatchers and janitors"""
        tasks = []
        for partition in self.dispatcher_partitions:
            tasks.append(self._revoke_dispatcher(partition))
        for partition in self.janitor_partitions:
            tasks.append(self._revoke_janitor(partition))
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
        for partition in self.dispatcher_partitions:
            tasks.append(self._dispatchers[partition].wait_empty())
        if tasks:
            await asyncio.gather(*tasks)

    async def on_partitions_assigned(self, sender: Any, assigned: Set[TP], **kwargs):
        """Called when new topic partions are assigned to worker."""
        _tt_assigned = set(
            tp for tp in assigned if tp[0] == self.timetable_changelog_topic_name
        )
        await self._on_timetable_partitions_assigned(_tt_assigned)

    async def on_partitions_revoked(self, sender: Any, revoked: Set[TP], **kwargs):
        """Called when active topic partions are revoked from worker."""
        await self.checkpoints.flush()
        await self.stop_and_revoke_all_dispatchers_and_janitors()
        self.checkpoints.pause()

    async def _on_timetable_partitions_assigned(self, assigned: Set[TP]):
        """Create dispatchers and janitors for newly assigned partitions."""
        timetable_actives = self.app.assignor.assigned_actives().intersection(assigned)
        for _, partition in timetable_actives:
            await self.wait_many(
                [
                    self.assign_dispatcher(partition),
                    self.assign_janitor(partition),
                ]
            )
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

    @cached_property
    def timetable_changelog_topic_name(self) -> str:
        return f"{self.app.conf.topic_prefix}kms-timetable-changelog"

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
            _data.append(
                tuple(
                    [
                        janitor.type,
                        checkpoint.partition,
                        checkpoint.time_key,
                        checkpoint.sequence,
                        prettydate(checkpoint),
                        locdiff(dispatcher.highwater, checkpoint),
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
                        ]
                    )
                )
        for janitor in self._janitors.values():
            last = janitor.last_location
            if last:
                _data.append(
                    tuple(
                        [
                            janitor.type,
                            last.partition,
                            last.time_key,
                            last.sequence,
                            prettydate(last),
                            locdiff(janitor.highwater, last),
                            self.monitor.janitor_state(janitor).messages_removed,
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
            action: bytes = event.headers.pop(H_KMS_ACTION)
            deliver_at: bytes = event.headers.pop(H_KMS_DELIVER_AT, None)
            deliver_to: bytes = event.headers.pop(H_KMS_DELIVER_TO, None)

            _action = action.decode()
            _topic_name = deliver_to.decode()
            partition = event.message.partition
            timetable = self.app.scheduler.timetable

            if _action == KMS_ACTION_ADD:
                time_key = str(deliver_at.decode())

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
                message_entry = {
                    "k": event.key.decode()
                    if isinstance(event.key, bytes)
                    else event.key,
                    "v": event.value.decode()
                    if isinstance(event.value, bytes)
                    else event.value,
                    "h": {header.decode() for header in event.headers}
                    if event.headers
                    else event.headers,
                    "__kms": {"d": deliver_to.decode()},
                }
                timetable.update_for_partition(
                    {
                        time_key: message_total + 1,
                        message_key: message_entry,
                    },
                    partition=partition,
                )
                self.monitor.on_message_scheduled(location)
                self.scheduled_total[partition] += 1

    async def distribute(self, stream: StreamT):
        """Transform requests into Timetable scheduling actions."""

        await self.wait_until_topics_created()
        out_topics = self._out_topics
        dlq_topic = self.topic_dlq
        topic_actions = self.topic_actions

        async for event in stream.events():
            event: EventT = event
            partition = event.message.partition
            action: bytes = event.headers.pop(H_KMS_ACTION, KMS_ACTION_ADD.encode())
            deliver_at: bytes = event.headers.pop(H_KMS_DELIVER_AT, None)
            deliver_to: bytes = event.headers.pop(H_KMS_DELIVER_TO, None)

            if not deliver_at or not deliver_to:
                errors = []
                if not deliver_at:
                    errors.append(f"Missing required header `{H_KMS_DELIVER_AT}`")
                if not deliver_to:
                    errors.append(f"Missing required header `{H_KMS_DELIVER_TO}`")
                error_entry = {
                    "key": event.key,
                    "value": event.value,
                    "headers": event.headers,
                    "errors": errors,
                }
                await dlq_topic.send(key=event.key, value=json_codec.dumps(error_entry))
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
                await dlq_topic.send(key=event.key, value=json_codec.dumps(error_entry))
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

            kms_headers = {
                H_KMS_ACTION: action,
                H_KMS_DELIVER_AT: f"{timekey}".encode(),
                H_KMS_DELIVER_TO: deliver_to,
            }
            headers = event.headers or {}
            headers.update(kms_headers)
            await topic_actions.send(
                key=event.message.key, value=event.message.value, headers=headers
            )

    @Service.task
    async def _print_stats(self):
        """Periodically print scheduler stats to terminal."""
        if not self.app.conf.kms_debug_stats_enabled:
            return
        while not self.should_stop:
            self.log.info(self._stats_logtable())
            await self.sleep(5)
