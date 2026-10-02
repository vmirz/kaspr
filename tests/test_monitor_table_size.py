from types import SimpleNamespace

from kaspr.sensors.kaspr import KasprMonitor


class FakeTable:
    """Table that fails the test if its keys are scanned."""

    def __init__(self, name, size):
        self.name = name
        self.size = size

    def size_estimate(self):
        return self.size

    def keys(self):
        raise AssertionError("table keys must not be scanned")

    def __len__(self):
        raise AssertionError("len(table) must not be used")


class RecordingMonitor(KasprMonitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.refreshed = []
        self.timetable_refreshed = []

    def on_table_key_count_refreshed(self, table):
        self.refreshed.append((table.name, self.count_table_keys[table]))

    def on_timetable_size_refreshed(self, table):
        self.timetable_refreshed.append((table.name, self.count_timetable_keys))


def make_monitor(tables, *, scheduler_timetable=None):
    scheduler_enabled = scheduler_timetable is not None
    app = SimpleNamespace(
        conf=SimpleNamespace(scheduler_enabled=scheduler_enabled),
        tables={table.name: table for table in tables},
    )
    if scheduler_enabled:
        changelog_topic = SimpleNamespace(get_topic_name=lambda: scheduler_timetable)
        app.scheduler = SimpleNamespace(
            timetable=SimpleNamespace(changelog_topic=changelog_topic)
        )
    return RecordingMonitor(app)


def test_sample_tables_counts_keys_from_size_estimate():
    orders, items = FakeTable("orders", 5_400_000), FakeTable("items", 19_000)
    monitor = make_monitor([orders, items])

    monitor._sample_tables()

    assert monitor.count_table_keys[orders] == 5_400_000
    assert monitor.count_table_keys[items] == 19_000
    assert monitor.refreshed == [("orders", 5_400_000), ("items", 19_000)]


def test_sample_tables_updates_counts_on_each_sample():
    orders = FakeTable("orders", 10)
    monitor = make_monitor([orders])
    monitor._sample_tables()

    orders.size = 25
    monitor._sample_tables()

    assert monitor.count_table_keys[orders] == 25


def test_sample_tables_reports_timetable_size_separately():
    timetable, orders = FakeTable("timetable-changelog", 70), FakeTable("orders", 5)
    monitor = make_monitor([timetable, orders], scheduler_timetable="timetable-changelog")

    monitor._sample_tables()

    assert monitor.count_timetable_keys == 70
    assert monitor.timetable_refreshed == [("timetable-changelog", 70)]
    assert timetable not in monitor.count_table_keys
    assert monitor.count_table_keys[orders] == 5
