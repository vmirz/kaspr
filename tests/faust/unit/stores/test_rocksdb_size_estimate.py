import pytest
from kaspr.faust.stores import rocksdb
from kaspr.faust.stores.rocksdb import Store
from kaspr.faust.types import TP
from kaspr.mode.utils.mocks import Mock


@pytest.mark.skipif(
    rocksdb.rocksdict is None, reason='rocksdict not installed')
class test_Store__rocksdict:
    """Run against real RocksDB databases (no mocks)."""

    @pytest.fixture()
    def table(self):
        table = Mock(name='table')
        table.name = 'table1'
        table.changelog_topic_name = 'clog'
        table.is_global = False
        return table

    @pytest.fixture()
    def store(self, *, app, table, tmp_path, monkeypatch):
        monkeypatch.setattr(Store, 'path', property(lambda self: tmp_path))
        store = Store('rocksdb://', app, table)
        app.assignor.assigned_actives = Mock(
            return_value={TP('clog', 0), TP('clog', 1)})
        yield store
        for db in store._dbs.values():
            db.close()

    def populate(self, store, partition, count):
        for i in range(count):
            store._set(f'{partition}:{i}'.encode(), b'value',
                       partition=partition)
        store.set_persisted_offset(TP('clog', partition), count)
        store._db_for_partition(partition).flush()

    def test_size_estimate__matches_len(self, *, store):
        self.populate(store, 0, 1000)
        self.populate(store, 1, 500)

        assert len(store) == 1500
        assert store.size_estimate() == 1500

    def test_size_estimate__empty(self, *, store):
        store._db_for_partition(0)
        assert len(store) == 0
        assert store.size_estimate() == 0

    def test_size_estimate__does_not_scan_keys(self, *, store):
        self.populate(store, 0, 100)
        store._visible_keys = Mock(side_effect=AssertionError('scanned'))

        assert store.size_estimate() == 100


@pytest.mark.skipif(
    rocksdb.rocksdict is None, reason='rocksdict not installed')
def test_store__driver_and_read_only_options(*, app, tmp_path, monkeypatch):
    """A table naming its driver, or asking to be read only, which rocksdict
    does not do, opens as before, and the store writes nothing but the database."""
    table = Mock(name='table')
    table.name = 'table1'
    monkeypatch.setattr(Store, 'path', property(lambda self: tmp_path))
    store = Store('rocksdb://', app, table, options={
        'driver': 'rocksdict', 'read_only': True, 'block_cache_size': 8 << 20})

    assert store.driver == 'rocksdict'
    assert store.read_only
    assert store.rocksdb_options.block_cache_size == 8 << 20
    db = store._db_for_partition(0)
    try:
        db.put(b'key', b'value')
        assert db.get(b'key') == b'value'
        assert sorted(p.name for p in tmp_path.iterdir()) == ['table1-0.db']
    finally:
        db.close()
