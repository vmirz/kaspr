import socket
import re
import random
import os
import psutil
import math
import ssl
from pathlib import Path
from typing import Any, Sequence, Optional, Union, Type, cast
from faust import SASLCredentials, SSLCredentials
from faust.types.settings import Settings
from faust.types.auth import SASLMechanism, CredentialsT, AuthProtocol
from faust.exceptions import ImproperlyConfigured
from mode import Seconds, want_seconds
from mode.utils.imports import SymbolArg, symbol_by_name
from kaspr.types.builder import AppBuilderT

PREFICES: Sequence[str] = ["KASPR_", "K_"]
_TRUE, _FALSE = {"True", "true"}, {"False", "false"}


def _getenv(name: str, *default: Any, prefices: Sequence[str] = PREFICES) -> Any:
    for prefix in prefices:
        try:
            v = os.environ[prefix + name]
            if v in _TRUE:
                return True
            elif v in _FALSE:
                return False
            else:
                return v
        except KeyError:
            pass
    if default:
        return default[0]
    raise KeyError(prefices[0] + name)


def _subenv(input: str):
    """
    Substitutes dynamic variables found in input with environment variable(s).

    For example, my-app-{APP_NAME} converts to my-app-0 if ORDINAL_NUM
    is a defined variable.
    """
    environ = os.environ
    found = re.findall(r"{([^{}]*?)}", input)
    for v in found:
        if v in environ:
            input = input.replace(f"{{{v}}}", str(environ[v]))
    return input


def _getmem():
    """Return total available memory (RAM) in bytes"""
    return psutil.virtual_memory().total


# ------------------------------------------------
# ---- Defaults and environment variables ----
# ------------------------------------------------

#: Number that differentiates this worker from other workers in a multi-worker application
WORKER_ORDINAL_NUMBER = _getenv("WORKER_ORDINAL_NUMBER", None)

#: Default transport used when no scheme specified.
DEFAULT_BROKER_SCHEME = "kafka"

#: Kafka bootstram server URLs
KAFKA_BOOTSTRAP_SERVERS = (
    f"kafka://{_getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}"
)

#: Kafka security protocol
KAFKA_SECURITY_PROTOCOL = _getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

#: Kafka SASL mechanism
KAFKA_SASL_MECHANISM = _getenv("KAFKA_SASL_MECHANISM", None)

#: Kafka authentication username
KAFKA_AUTH_USERNAME = _getenv("KAFKA_AUTH_USERNAME", None)

#: Kafka authentication password
KAFKA_AUTH_PASSWORD = _getenv("KAFKA_AUTH_PASSWORD", None)

#: Kafka TLS authentication cert
KAFKA_AUTH_CAFILE = _getenv("KAFKA_AUTH_CAFILE", None)

#: Kafka TLS authentication cert
KAFKA_AUTH_CAPATH = _getenv("KAFKA_AUTH_CAPATH", None)

#: Kafka TLS authentication cert
KAFKA_AUTH_CADATA = _getenv("KAFKA_AUTH_CADATA", None)

#: Table state directory path used as default
#: This path will be treated as relative to datadir, unless the provided
#: poth is absolute.
TABLE_DIR = _getenv("TABLE_DIR", "/var/lib/stream/rocksdb")

# Prefix applied to all internal Kafka topics.
TOPIC_PREFIX = _subenv(_getenv("TOPIC_PREFIX", "{K_APP_NAME}."))

#: The default replication factor for topics created by the application.
TOPIC_REPLICATION_FACTOR = int(_getenv("TOPIC_REPLICATION_FACTOR", 3))

#: Default number of partitions for new topics.
TOPIC_PARTITIONS = int(_getenv("TOPIC_PARTITIONS", 3))

#: This setting disables auto creation of internal topics.
TOPIC_ALLOW_DECLARE = bool(_getenv("TOPIC_ALLOW_DECLARE", True))

#: RocksDB configirations
#: (https://github.com/EighteenZi/rocksdb_wiki/blob/master/Memory-usage-in-RocksDB.md)
# ------------------------------------------------
#: This is the maximum write buffer size.
#: It represents the amount of data to build up in memory
#: before converting to a sorted on-disk file. The default is 64 MB.
STORE_ROCKSDB_WRITE_BUFFER_SIZE = int(
    _getenv("STORE_ROCKSDB_WRITE_BUFFER_SIZE", 64 << 20)
)

#: Maximum number of write buffers (memtables)
#: that can be built in memory at the same time.
STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER = int(
    _getenv("STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER", 3)
)

#: Target size for files at level-1 in the LSM tree
#: Used to determine the size of the SST (Sorted String Table)
#: files that RocksDB generates during compactions.
STORE_ROCKSDB_TARGET_FILE_SIZE_BASE = int(
    _getenv("STORE_ROCKSDB_TARGET_FILE_SIZE_BASE", 64 << 20)
)

#: Size for caching uncompressed data.
#: Defauls to about 1/3 of your total memory budget
STORE_ROCKSDB_BLOCK_CACHE_SIZE = int(
    _getenv("STORE_ROCKSDB_BLOCK_CACHE_SIZE", math.floor(_getmem() / 3))
)

#: Size for caching compressed data.
#: Defaults to 254MB.
STORE_ROCKSDB_BLOCK_CACHE_COMPRESSED_SIZE = int(
    _getenv("STORE_ROCKSDB_BLOCK_CACHE_COMPRESSED_SIZE", 256 << 20)
)

#: A Bloom filter in RocksDB is used to quickly check whether a
#: key might be in an SST (Sorted String Table) file without
#: actually reading the file, which can significantly improve
#: read performance. Defaults to 3.
STORE_ROCKSDB_BLOOM_FILTER_SIZE = int(
    _getenv("STORE_ROCKSDB_BLOCK_CACHE_COMPRESSED_SIZE", 3)
)
#: If set to true, index and filter blocks will be stored in block cache,
#: together with all other data blocks.
STORE_ROCKSDB_SET_CACHE_INDEX_AND_FILTER_BLOCKS = _getenv(
    "STORE_ROCKSDB_SET_CACHE_INDEX_AND_FILTER_BLOCKS", False
)

#: Enable kafka message scheduler service
SCHEDULER_ENABLED = bool(_getenv("SCHEDULER_ENABLED", False))

#: Enable printing runtime scheduler statistics to log
SCHEDULER_DEBUG_STATS_ENABLED = bool(_getenv("SCHEDULER_DEBUG_STATS_ENABLED", False))

#: Default number of partitions for KMS related topics
#: If not specified, will use app's default topic partition configuration.
_scheduler_tps = _getenv("SCHEDULER_TOPIC_PARTITIONS", None)
SCHEDULER_TOPIC_PARTITIONS = int(_scheduler_tps) if _scheduler_tps is not None else None

#: How often we save checkpoint to storage (and to changelog topic)
SCHEDULER_CHECKPOINT_SAVE_INTERVAL_SECONDS = float(
    _getenv("SCHEDULER_CHECKPOINT_SAVE_INTERVAL_SECONDS", 1.3)
)

#: Number of days dispatcher looks back to build a default checkpoint
SCHEDULER_DISPATCHER_DEFAULT_CHECKPOINT_LOOKBACK_DAYS = int(
    _getenv("SCHEDULER_DISPATCHER_DEFAULT_CHECKPOINT_LOOKBACK_DAYS", 7)
)

#: How often we checkpoint the dispacher's location in the timetable.
SCHEDULER_DISPATCHER_CHECKPOINT_INTERVAL = float(
    _getenv("SCHEDULER_DISPATCHER_CHECKPOINT_INTERVAL", 10.0)
)

#: How often we checkpoint the janitor's location in the timetable.
SCHEDULER_JANITOR_CHECKPOINT_INTERVAL = float(
    _getenv("SCHEDULER_JANITOR_CHECKPOINT_INTERVAL", 10.0)
)

#: How oftwn we attempt to run the cleaning process.
SCHEDULER_JANITOR_CLEAN_INTERVAL_SECONDS = float(
    _getenv("SCHEDULER_JANITOR_CLEAN_INTERVAL_SECONDS", 3.0)
)

#: Number of seconds to offset the janitor highwater Timetable location.
SCHEDULER_JANITOR_HIGHWATER_OFFSET_SECONDS = float(
    _getenv("SCHEDULER_JANITOR_HIGHWATER_OFFSET_SECONDS", 3600 * 4.0)
)

#: Base http path for serving web requests
WEB_BASE_PATH = _getenv("WEB_BASE_PATH", "")

#: Port number between 1024 and 65535 to use for the web server.
WEB_PORT: int = int(_getenv("WEB_PORT", "6066"))

#: Base http path for serving metrics
WEB_METRICS_BASE_PATH = _getenv("WEB_METRICS_BASE_PATH", "")

#: Directory path to app component definition file(s).
#: This path will be treated as relative to appdir, unless the provided
#: path is absolute.
DEFINITIONS_DIR = _getenv("DEFINITIONS_DIR", "builders")

#: Enable building of stream processors from definition file(s).
# Set this to False if you don't want to allow defining stream processors with configuration.
APP_BUILDER_ENABLED = bool(_getenv("APP_BUILDER_ENABLED", True))

#: Path to app builder class, used as default for :setting:`AppBuilder`.
APP_BUILDER_TYPE = "kaspr.core.builder.AppBuilder"


class CustomSettings(Settings):
    """Application settings"""

    worker_ordinal_number: int = WORKER_ORDINAL_NUMBER
    worker_name_format: str = "{app_name}-{ordinal_number}"

    kafka_boostrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    kafka_security_protocol: str = KAFKA_SECURITY_PROTOCOL
    kafka_sasl_mechanism: str = KAFKA_SASL_MECHANISM
    kafka_auth_username: str = KAFKA_AUTH_USERNAME
    kafka_auth_password: str = KAFKA_AUTH_PASSWORD
    kafka_auth_cafile: str = KAFKA_AUTH_CAFILE
    kafka_auth_capath: str = KAFKA_AUTH_CAPATH
    kafka_auth_cadata: str = KAFKA_AUTH_CADATA

    table_dir: str = TABLE_DIR

    topic_prefix: str = TOPIC_PREFIX
    topic_replication_factor: int = TOPIC_REPLICATION_FACTOR
    topic_partitions: int = TOPIC_PARTITIONS
    topic_allow_declare: bool = TOPIC_ALLOW_DECLARE

    store_rocksdb_write_buffer_size: int = STORE_ROCKSDB_WRITE_BUFFER_SIZE
    store_rocksdb_max_write_buffer_number: int = STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER
    store_rocksdb_target_file_size_base: int = STORE_ROCKSDB_TARGET_FILE_SIZE_BASE
    store_rocksdb_block_cache_size: int = STORE_ROCKSDB_BLOCK_CACHE_SIZE
    store_rocksdb_block_cache_compressed_size: int = (
        STORE_ROCKSDB_BLOCK_CACHE_COMPRESSED_SIZE
    )
    store_rocksdb_bloom_filter_size: int = STORE_ROCKSDB_BLOOM_FILTER_SIZE
    store_rocksdb_set_cache_index_and_filter_blocks: bool = (
        STORE_ROCKSDB_SET_CACHE_INDEX_AND_FILTER_BLOCKS
    )

    scheduler_enabled: bool = SCHEDULER_ENABLED
    scheduler_debug_stats_enabled: bool = SCHEDULER_DEBUG_STATS_ENABLED
    scheduler_topic_partitions: Optional[int] = SCHEDULER_TOPIC_PARTITIONS
    scheduler_checkpoint_save_interval_seconds: float = SCHEDULER_CHECKPOINT_SAVE_INTERVAL_SECONDS
    scheduler_dispatcher_default_checkpoint_lookback_days: int = (
        SCHEDULER_DISPATCHER_DEFAULT_CHECKPOINT_LOOKBACK_DAYS
    )
    scheduler_dispatcher_checkpoint_interval: float = SCHEDULER_DISPATCHER_CHECKPOINT_INTERVAL
    scheduler_janitor_checkpoint_interval: float = SCHEDULER_JANITOR_CHECKPOINT_INTERVAL
    scheduler_janitor_clean_interval_seconds: float = SCHEDULER_JANITOR_CLEAN_INTERVAL_SECONDS
    scheduler_janitor_highwater_offset_seconds: float = SCHEDULER_JANITOR_HIGHWATER_OFFSET_SECONDS

    web_base_path: str = WEB_BASE_PATH
    web_port: int = WEB_PORT
    web_metrics_base_path: str = WEB_METRICS_BASE_PATH

    app_builder_enabled: bool = APP_BUILDER_ENABLED

    _worker_name: str = None
    _kafka_credentials: CredentialsT = None
    _definitionsdir: Path = None

    def __init__(
        self,
        *args,
        worker_ordinal_number: int = None,
        worker_name_format: str = None,
        kafka_boostrap_servers: str = None,
        kafka_security_protocol: str = None,
        kafka_sasl_mechanism: str = None,
        kafka_auth_username: str = None,
        kafka_auth_password: str = None,
        kafka_auth_cafile: str = None,
        kafka_auth_capath: str = None,
        kafka_auth_cadata: str = None,
        table_dir: str = None,
        topic_prefix: str = None,
        store_rocksdb_write_buffer_size: int = None,
        store_rocksdb_max_write_buffer_number: int = None,
        store_rocksdb_target_file_size_base: int = None,
        store_rocksdb_block_cache_size: int = None,
        store_rocksdb_block_cache_compressed_size: int = None,
        store_rocksdb_bloom_filter_size: int = None,
        store_rocksdb_set_cache_index_and_filter_blocks: bool = None,
        kms_enabled: bool = None,
        kms_debug_stats_enabled: bool = None,
        kms_topic_partitions: int = None,
        kms_checkpoint_save_interval_seconds: Seconds = None,
        kms_dispatcher_default_checkpoint_lookback_days: int = None,
        kms_dispatcher_checkpoint_interval: float = None,
        kms_janitor_checkpoint_interval: float = None,
        kms_janitor_clean_interval_seconds: Seconds = None,
        kms_janitor_highwater_offset_seconds: Seconds = None,
        web_base_path: str = None,
        web_port: int = None,
        web_metrics_base_path: str = None,
        definitions_dir: str = None,
        app_builder_enabled: bool = None,
        AppBuilder: SymbolArg[Type[AppBuilderT]] = None,
        **kwargs,
    ):
        # Apply settings that exist in base class before we pass them down.
        if kafka_boostrap_servers is not None:
            self.kafka_boostrap_servers = kafka_boostrap_servers

        if kafka_security_protocol is not None:
            self.kafka_security_protocol = kafka_security_protocol

        if kafka_sasl_mechanism is not None:
            self.kafka_sasl_mechanism = kafka_sasl_mechanism

        if kafka_auth_username is not None:
            self.kafka_auth_username = kafka_auth_username

        if kafka_auth_password is not None:
            self.kafka_auth_password = kafka_auth_password

        if kafka_auth_cafile is not None:
            self.kafka_auth_cafile = kafka_auth_cafile

        if kafka_auth_capath is not None:
            self.kafka_auth_capath = kafka_auth_capath

        if kafka_auth_cadata is not None:
            self.kafka_auth_cadata = kafka_auth_cadata

        self.kafka_credentials = self._prepare_kafka_credentials()

        if table_dir is not None:
            self.table_dir = table_dir

        if web_port is not None:
            self.web_port = web_port

        super().__init__(
            *args,
            tabledir=self.table_dir,
            broker=self.kafka_boostrap_servers,
            broker_credentials=self.kafka_credentials,
            web_port=self.web_port,
            **kwargs,
        )

        self.definitionssdir = cast(Path, definitions_dir or DEFINITIONS_DIR)

        if app_builder_enabled is not None:
            self.app_builder_enabled = app_builder_enabled

        if worker_ordinal_number is not None:
            self.worker_ordinal_number = int(worker_ordinal_number)

        if worker_name_format is not None:
            self.worker_name_format = worker_name_format

        self.worker_name = self._prepare_worker_name(name=self.name)

        if topic_prefix is not None:
            self.topic_prefix = str(topic_prefix)

        if store_rocksdb_write_buffer_size is not None:
            self.store_rocksdb_write_buffer_size = store_rocksdb_write_buffer_size

        if store_rocksdb_max_write_buffer_number is not None:
            self.store_rocksdb_max_write_buffer_number = (
                store_rocksdb_max_write_buffer_number
            )

        if store_rocksdb_target_file_size_base is not None:
            self.store_rocksdb_target_file_size_base = (
                store_rocksdb_target_file_size_base
            )

        if store_rocksdb_block_cache_size is not None:
            self.store_rocksdb_block_cache_size = store_rocksdb_block_cache_size

        if store_rocksdb_block_cache_compressed_size is not None:
            self.store_rocksdb_block_cache_compressed_size = (
                store_rocksdb_block_cache_compressed_size
            )

        if store_rocksdb_bloom_filter_size is not None:
            self.store_rocksdb_bloom_filter_size = store_rocksdb_bloom_filter_size

        if store_rocksdb_set_cache_index_and_filter_blocks is not None:
            self.store_rocksdb_set_cache_index_and_filter_blocks = (
                store_rocksdb_set_cache_index_and_filter_blocks
            )

        if kms_enabled is not None:
            self.scheduler_enabled = kms_enabled

        if kms_debug_stats_enabled is not None:
            self.scheduler_debug_stats_enabled = kms_debug_stats_enabled

        if kms_topic_partitions is not None:
            self.scheduler_topic_partitions = kms_topic_partitions

        if kms_checkpoint_save_interval_seconds is not None:
            self.scheduler_checkpoint_save_interval_seconds = want_seconds(
                kms_checkpoint_save_interval_seconds
            )

        if kms_dispatcher_default_checkpoint_lookback_days is not None:
            self.scheduler_dispatcher_default_checkpoint_lookback_days = (
                kms_dispatcher_default_checkpoint_lookback_days
            )

        if kms_dispatcher_checkpoint_interval is not None:
            self.scheduler_dispatcher_checkpoint_interval = want_seconds(
                kms_dispatcher_checkpoint_interval
            )

        if kms_janitor_checkpoint_interval is not None:
            self.scheduler_janitor_checkpoint_interval = want_seconds(
                kms_janitor_checkpoint_interval
            )

        if kms_janitor_clean_interval_seconds is not None:
            self.scheduler_janitor_clean_interval_seconds = want_seconds(
                kms_janitor_clean_interval_seconds
            )

        if kms_janitor_highwater_offset_seconds is not None:
            self.scheduler_janitor_highwater_offset_seconds = want_seconds(
                kms_janitor_highwater_offset_seconds
            )

        if web_base_path is not None:
            self.web_base_path = web_base_path

        if web_metrics_base_path is not None:
            self.web_metrics_base_path = web_metrics_base_path

        self.AppBuilder = cast(
            Type[AppBuilderT], AppBuilder or APP_BUILDER_TYPE
        )

    def _prepare_kafka_credentials(self) -> CredentialsT:
        security_protocol = AuthProtocol(self.kafka_security_protocol)
        if security_protocol == AuthProtocol.PLAINTEXT:
            return None
        elif security_protocol in [
            AuthProtocol.SASL_PLAINTEXT,
            AuthProtocol.SASL_SSL,
        ]:
            return SASLCredentials(
                username=self.kafka_auth_username,
                password=self.kafka_auth_password,
                ssl_context=ssl.create_default_context()
                if security_protocol == AuthProtocol.SASL_SSL
                else None,
                mechanism=SASLMechanism(self.kafka_sasl_mechanism)
                if self.kafka_sasl_mechanism
                else None,
            )
        elif security_protocol in [AuthProtocol.SSL]:
            ssl_auth = {
                "cafile": self.kafka_auth_cafile,
                "capath": self.kafka_auth_capath,
                "cadata": self.kafka_auth_cadata,
            }
            return SSLCredentials(context=ssl.create_default_context(), **ssl_auth)
        else:
            raise ImproperlyConfigured(
                f"Unknown or unsupported auth protocol: {security_protocol}"
            )

    def _prepare_worker_name(self, name: str):
        return self.worker_name_format.format(
            app_name=name, ordinal_number=self._get_ordinal_number()
        )

    def _get_ordinal_number(self) -> int:
        if self.worker_ordinal_number is not None:
            return self.worker_ordinal_number
        else:
            # try to extract from hostname (k8s statefulsets)
            result = re.match(".*-([0-9]+)$", socket.gethostname())
            if result is not None:
                try:
                    return int(result.group(1))
                except Exception:
                    pass
            # last resort: generate a random number
            return random.randint(1000, 1999)

    def _prepare_definitionsdir(self, definitionsdir: Union[str, Path]) -> Path:
        return self._appdir_path(self._Path(definitionsdir))

    @property
    def kafka_credentials(self) -> CredentialsT:
        return self._kafka_credentials

    @kafka_credentials.setter
    def kafka_credentials(self, credentials: CredentialsT):
        self._kafka_credentials = credentials

    @property
    def definitionssdir(self) -> Path:
        return self._definitionsdir

    @definitionssdir.setter
    def definitionssdir(self, definitionsdir: Union[Path, str]) -> None:
        self._definitionsdir = self._prepare_definitionsdir(definitionsdir)

    @property
    def worker_name(self):
        """Unique name for worker in a multi-worker application."""
        return self._worker_name

    @worker_name.setter
    def worker_name(self, name: str):
        self._worker_name = name

    @property
    def AppBuilder(self) -> Type[AppBuilderT]:
        return self._AppBuilder

    @AppBuilder.setter
    def AppBuilder(self, AppBuilder: SymbolArg[Type[AppBuilderT]]) -> None:
        self._AppBuilder = symbol_by_name(AppBuilder)
