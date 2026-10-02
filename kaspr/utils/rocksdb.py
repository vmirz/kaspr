from typing import Any, Callable, Dict, Mapping, Optional

from kaspr.utils.logging import get_logger

try:
    import rocksdict
except ImportError:  # pragma: no cover
    rocksdict = None

logger = get_logger(__name__)


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"not a boolean: {value!r}")


#: Table options that can be overridden per table, with the parser of their values.
TABLE_OPTION_PARSERS: Mapping[str, Callable[[Any], Any]] = {
    "max_open_files": int,
    "write_buffer_size": int,
    "max_write_buffer_number": int,
    "target_file_size_base": int,
    "block_cache_size": int,
    "block_cache_compressed_size": int,
    "bloom_filter_size": int,
    "set_cache_index_and_filter_blocks": _to_bool,
    "prefix_extractor_enabled": _to_bool,
    "prefix_max_length": int,
}

_block_caches: Dict[int, Any] = {}


def shared_block_cache(size: int) -> Optional[Any]:
    """Return the process wide block cache of `size` bytes, created on first use.

    Sharing one cache between all RocksDB instances (every partition of every
    table) bounds their total block cache memory to `size`.
    """
    if rocksdict is None:
        return None
    cache = _block_caches.get(size)
    if cache is None:
        cache = _block_caches[size] = rocksdict.Cache(size)
        logger.info("RocksDB block cache: %.0f MiB shared by all tables", size / 2**20)
    return cache


def rocksdb_table_options(
    conf: Any, overrides: Optional[Mapping[str, Any]] = None
) -> Dict[str, Any]:
    """Return the RocksDB store options of a table.

    The defaults come from the app's `store_rocksdb_*` settings. `overrides`
    (e.g. the `options` of a KasprTable, whose values are strings) take
    precedence; unknown or invalid ones are ignored with a warning.
    """
    options: Dict[str, Any] = {
        "write_buffer_size": conf.store_rocksdb_write_buffer_size,
        "max_write_buffer_number": conf.store_rocksdb_max_write_buffer_number,
        "target_file_size_base": conf.store_rocksdb_target_file_size_base,
        "block_cache_size": conf.store_rocksdb_block_cache_size,
        "block_cache_compressed_size": conf.store_rocksdb_block_cache_compressed_size,
        "bloom_filter_size": conf.store_rocksdb_bloom_filter_size,
        "set_cache_index_and_filter_blocks": conf.store_rocksdb_set_cache_index_and_filter_blocks,
    }
    try:
        # the env var is only converted to a bool for "true"/"false"
        options["set_cache_index_and_filter_blocks"] = _to_bool(options["set_cache_index_and_filter_blocks"])
    except ValueError:
        logger.warning("Ignoring invalid store_rocksdb_set_cache_index_and_filter_blocks setting")
        options["set_cache_index_and_filter_blocks"] = False
    for name, value in (overrides or {}).items():
        parse = TABLE_OPTION_PARSERS.get(name)
        if parse is None:
            logger.warning("Ignoring unknown table option %r", name)
            continue
        try:
            options[name] = parse(value)
        except (TypeError, ValueError):
            logger.warning("Ignoring invalid value %r for table option %r", value, name)
    block_cache = shared_block_cache(options["block_cache_size"])
    if block_cache is not None:
        options["block_cache"] = block_cache
    return options
