import faust
from datetime import datetime
from kaspr.utils.functional import utc_now
from faust.types.tuples import TP, MessageSentCallback
from typing import Set, Any
from mode import Signal


class KasprTable(faust.Table):
    """Implements custom behavior for faust table"""

    # Timestamp of when table received an app shutdown event.
    shutdown_begin_ts: int = None

    # Timestamp of last time table handled a rebalance event
    last_rebalanced_at: datetime = None

    #: Signal
    on_table_recovery_completed: Signal = Signal()

    def __init__(self, *args, **kwargs):
        self.last_rebalanced_at = utc_now()
        super().__init__(*args, **kwargs)
        self._init_signals()

    def _init_signals(self) -> None:
        self.on_table_recovery_completed = (
            self.on_table_recovery_completed.with_default_sender(self)
        )

    async def on_rebalance(
        self, assigned: Set[TP], revoked: Set[TP], newly_assigned: Set[TP]
    ) -> None:
        """Call when cluster is rebalancing."""
        await super().on_rebalance(assigned, revoked, newly_assigned)
        self.last_rebalanced_at = utc_now()

    async def on_recovery_completed(
        self, active_tps: Set[TP], standby_tps: Set[TP]
    ) -> None:
        """Call when recovery has completed after rebalancing."""

        await self.on_table_recovery_completed.send(
            actives=active_tps, standbys=standby_tps
        )
        return await super().on_recovery_completed(
            active_tps=active_tps, standby_tps=standby_tps
        )

    # def _changelog_topic_name(self):
    #     """Overrides the internal auto generated changelog topic name
    #     This means, the 'name' specified with app.Table is the actual
    #     changelog topic name"""
    #     return self.name

    def update_for_partition(
        self,
        *args: Any,
        partition: int,
        callback: MessageSentCallback = None,
        **kwargs: Any,
    ) -> None:
        """Update a specific partition of table"""
        for d in args:
            for key, value in d.items():
                self.on_key_set(key, value, partition=partition, callback=callback)
        for key, value in kwargs.items():
            self.on_key_set(key, value, partition=partition, callback=callback)
        self.data.update(*args, partition=partition, **kwargs)

    def get_for_partition(self, key, partition: int):
        """Get key in specific partition of table"""
        self.on_key_get(key, partition)
        return self.data.get_for_partition(key, partition)

    def del_for_partition(
        self, key, partition: int, callback: MessageSentCallback = None
    ):
        """Delete key in specific partition of table"""
        self.on_key_del(key, partition, callback)
        return self.data.del_for_partition(key, partition)


class KasprGlobalTable(faust.GlobalTable):
    """Implements custom behavior for faust table"""

    # Timestamp of when table received an app shutdown event.
    shutdown_begin_ts: int = None

    # Timestamp of last time table handled a rebalance event
    last_rebalanced_at: datetime = None

    def __init__(self, *args, **kwargs):
        self.last_rebalanced_at = utc_now()
        return super().__init__(*args, **kwargs)

    async def on_rebalance(
        self, assigned: Set[TP], revoked: Set[TP], newly_assigned: Set[TP]
    ) -> None:
        """Call when cluster is rebalancing."""
        self.last_rebalanced_at = utc_now()
        return await super().on_rebalance(assigned, revoked, newly_assigned)

    async def on_recovery_completed(
        self, active_tps: Set[TP], standby_tps: Set[TP]
    ) -> None:
        """Call when recovery has completed after rebalancing."""
        return await super().on_recovery_completed(
            active_tps=active_tps, standby_tps=standby_tps
        )

    # def _changelog_topic_name(self):
    #     """Overrides the internal auto generated changelog topic name
    #     This means, the 'name' specified with app.Table is the actual
    #     changelog topic name"""
    #     return self.name
