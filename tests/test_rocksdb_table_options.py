import inspect
import logging
from types import SimpleNamespace

import pytest

from kaspr.types import settings
from kaspr.types.models.table.table import TableSpec
from kaspr.utils import rocksdb as rocksdb_utils
from kaspr.utils.rocksdb import rocksdb_table_options, shared_block_cache

MiB = 1024**2


@pytest.fixture
def conf():
    return SimpleNamespace(
        store_rocksdb_write_buffer_size=64 * MiB,
        store_rocksdb_max_write_buffer_number=3,
        store_rocksdb_target_file_size_base=64 * MiB,
        store_rocksdb_block_cache_size=128 * MiB,
        store_rocksdb_block_cache_compressed_size=256 * MiB,
        store_rocksdb_bloom_filter_size=3,
        store_rocksdb_set_cache_index_and_filter_blocks=False,
    )


@pytest.fixture(autouse=True)
def fresh_block_caches(monkeypatch):
    monkeypatch.setattr(rocksdb_utils, "_block_caches", {})


# --- container memory ---------------------------------------------------------


@pytest.mark.parametrize(
    "content, expected",
    [
        ("1073741824\n", 1073741824),  # cgroup v2 limit
        ("max\n", None),  # cgroup v2 without limit
        ("9223372036854771712", 9223372036854771712),  # cgroup v1 without limit
        ("", None),
    ],
)
def test_cgroup_memory_limit(tmp_path, content, expected):
    limit_file = tmp_path / "memory.max"
    limit_file.write_text(content)

    assert settings._cgroup_memory_limit([str(limit_file)]) == expected


def test_cgroup_memory_limit_falls_back_to_next_file(tmp_path):
    v1 = tmp_path / "memory.limit_in_bytes"
    v1.write_text("536870912")

    assert settings._cgroup_memory_limit([str(tmp_path / "missing"), str(v1)]) == 536870912


def test_cgroup_memory_limit_without_cgroup(tmp_path):
    assert settings._cgroup_memory_limit([str(tmp_path / "missing")]) is None


@pytest.mark.parametrize(
    "limit, expected",
    [
        (1 * 1024 * MiB, 1 * 1024 * MiB),  # container limit is below the host memory
        (None, 64 * 1024 * MiB),  # no container limit
        (9223372036854771712, 64 * 1024 * MiB),  # "unlimited" cgroup v1 value
    ],
)
def test_getmem_honors_container_limit(monkeypatch, limit, expected):
    monkeypatch.setattr(settings.psutil, "virtual_memory", lambda: SimpleNamespace(total=64 * 1024 * MiB))
    monkeypatch.setattr(settings, "_cgroup_memory_limit", lambda: limit)

    assert settings._getmem() == expected


# --- table options --------------------------------------------------------------


def test_options_default_to_app_settings(conf):
    options = rocksdb_table_options(conf)

    assert options["write_buffer_size"] == 64 * MiB
    assert options["max_write_buffer_number"] == 3
    assert options["target_file_size_base"] == 64 * MiB
    assert options["block_cache_size"] == 128 * MiB
    assert options["block_cache_compressed_size"] == 256 * MiB
    assert options["bloom_filter_size"] == 3
    assert options["set_cache_index_and_filter_blocks"] is False


def test_options_share_one_block_cache_between_tables(conf):
    first, second = rocksdb_table_options(conf), rocksdb_table_options(conf)

    assert first["block_cache"] is second["block_cache"]
    assert first["block_cache"] is shared_block_cache(128 * MiB)


def test_options_overrides_are_parsed_from_strings(conf):
    options = rocksdb_table_options(
        conf,
        {
            "bloom_filter_size": "10",
            "write_buffer_size": "33554432",
            "set_cache_index_and_filter_blocks": "true",
            "prefix_extractor_enabled": "False",
        },
    )

    assert options["bloom_filter_size"] == 10
    assert options["write_buffer_size"] == 33554432
    assert options["set_cache_index_and_filter_blocks"] is True
    assert options["prefix_extractor_enabled"] is False
    # untouched settings keep the app level value
    assert options["block_cache_size"] == 128 * MiB


def test_options_block_cache_size_override_gets_its_own_cache(conf):
    default = rocksdb_table_options(conf)
    dedicated = rocksdb_table_options(conf, {"block_cache_size": str(32 * MiB)})

    assert dedicated["block_cache_size"] == 32 * MiB
    assert dedicated["block_cache"] is not default["block_cache"]
    assert dedicated["block_cache"] is rocksdb_table_options(conf, {"block_cache_size": 32 * MiB})["block_cache"]


def test_options_ignore_unknown_and_invalid_overrides(conf, caplog):
    with caplog.at_level(logging.WARNING):
        options = rocksdb_table_options(
            conf, {"no_such_option": "1", "bloom_filter_size": "lots", "prefix_extractor_enabled": "maybe"}
        )

    assert options["bloom_filter_size"] == 3
    assert "prefix_extractor_enabled" not in options
    assert "no_such_option" not in options
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 3


def test_options_without_rocksdict_omit_block_cache(conf, monkeypatch):
    monkeypatch.setattr(rocksdb_utils, "rocksdict", None)

    assert "block_cache" not in rocksdb_table_options(conf)


@pytest.mark.parametrize(
    "setting, expected",
    [("true", True), ("1", True), ("false", False), ("0", False), (True, True), ("maybe", False)],
)
def test_options_normalize_the_index_and_filter_blocks_setting(conf, setting, expected):
    # the setting is only converted to a bool for "true"/"false" in the environment
    conf.store_rocksdb_set_cache_index_and_filter_blocks = setting

    assert rocksdb_table_options(conf)["set_cache_index_and_filter_blocks"] is expected


# --- wiring ---------------------------------------------------------------------


def make_table_spec(app, **overrides):
    values = dict(
        name="orders",
        description=None,
        is_global=False,
        default_selector=None,
        key_serializer="json",
        value_serializer="json",
        partitions=4,
        extra_topic_configs={},
        options={},
        window=None,
        app=app,
    )
    values.update(overrides)
    return TableSpec(**values)


@pytest.mark.parametrize("is_global", [False, True])
def test_prepare_table_passes_rocksdb_options(conf, is_global):
    created = {}

    def create(kind):
        def factory(**kwargs):
            created[kind] = kwargs
            return SimpleNamespace(**kwargs)

        return factory

    app = SimpleNamespace(conf=conf, Table=create("table"), GlobalTable=create("global"))
    spec = make_table_spec(app, is_global=is_global, options={"bloom_filter_size": "10"})

    spec.prepare_table()

    options = created["global" if is_global else "table"]["options"]
    assert options["bloom_filter_size"] == 10
    assert options["block_cache_size"] == 128 * MiB
    assert options["block_cache"] is shared_block_cache(128 * MiB)


def test_scheduler_tables_use_the_same_options(conf):
    from kaspr.scheduler.manager import MessageScheduler

    created = []
    app = SimpleNamespace(
        conf=SimpleNamespace(scheduler_topic_partitions=3, **vars(conf)),
        Table=lambda name, **kwargs: created.append((name, kwargs)),
    )
    scheduler = SimpleNamespace(app=app)

    for prepare in ("timetable", "schedule_index", "cron_registry", "cron_due_index"):
        getattr(MessageScheduler, f"prepare_{prepare}")(scheduler)

    assert [name for name, _ in created] == ["timetable", "timetable-index", "cron-registry", "cron-due-index"]
    for _, kwargs in created:
        assert kwargs["options"]["block_cache"] is shared_block_cache(128 * MiB)


# --- end to end with RocksDB ------------------------------------------------------


def test_tables_share_the_block_cache_in_rocksdb(conf, tmp_path):
    faust_rocksdb = pytest.importorskip("faust.stores.rocksdb")
    if faust_rocksdb.rocksdict is None or "block_cache" not in inspect.signature(
        faust_rocksdb.RocksDBOptions.__init__
    ).parameters:
        pytest.skip("twm-faust without shared block cache support")

    dbs = []
    try:
        for table in ("orders", "items"):
            options = faust_rocksdb.RocksDBOptions(use_rocksdict=True, **rocksdb_table_options(conf))
            dbs.append(options.open(tmp_path / f"{table}-0.db"))

        for db in dbs:
            assert db.property_int_value("rocksdb.block-cache-capacity") == 128 * MiB
        for i in range(2000):
            dbs[0].put(f"key{i}".encode(), b"v" * 512)
        dbs[0].flush()
        for i in range(2000):
            dbs[0].get(f"key{i}".encode())
        usage = dbs[0].property_int_value("rocksdb.block-cache-usage")
        assert usage > 100 * 1024
        assert dbs[1].property_int_value("rocksdb.block-cache-usage") == usage
    finally:
        for db in dbs:
            db.close()
