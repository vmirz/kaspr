from datetime import timedelta
from faust.exceptions import ImproperlyConfigured
from kaspr import KasprApp
from kaspr.sensors.prometheus_monitor import PrometheusMonitor
from kaspr.types.settings import _getenv


app_name = _getenv("APP_NAME", None)
if not app_name:
    raise ImproperlyConfigured("Missing required configuration: `APP_NAME`")

app = KasprApp(
    app_name,
    autodiscover=["kaspr"],
    origin="kaspr",
    store="rocksdb://",
    processing_guarantee="exactly_once",
    # How often we commit messages that have been fully processed (acked).
    broker_commit_interval=timedelta(seconds=2),
    broker_commit_every=500,  # 2000,
    # How long to wait for a node to finish rebalancing before the broker
    # will consider it dysfunctional and remove it from the cluster.
    # Increase this if you experience the cluster being in a state of
    # constantly rebalancing, but make sure you also increase the
    # broker_heartbeat_interval at the same time.
    broker_session_timeout=120,
    # Kafka client request timeout
    broker_request_timeout=180,
    # How often we send heartbeats to the broker, and also how often we
    # expect to receive heartbeats from the broker.
    broker_heartbeat_interval=9,
    # How long time it takes before we warn that the Kafka commit offset
    # has not advanced (only when processing messages).
    broker_commit_livelock_soft_timeout=timedelta(seconds=30),
    # The number of acknowledgments the producer requires the leader to
    # have received before considering a request complete (0,1,-1)
    producer_acks=-1,
    # Time to wait before continuing after rebalance
    # To prevent issues with premature processing
    stream_recovery_delay=timedelta(seconds=20),
    # This setting controls whether the worker should wait for the currently
    # processing task in an agent to complete before rebalancing or shutting down.
    # On rebalance/shut down we clear the stream buffers. Those events will be
    # reprocessed after the rebalance anyway, but we may have already started
    # processing one event in every agent, and if we rebalance we will process that event again.
    stream_wait_empty=True,
    # The maximum number of records returned in a single call to poll()
    broker_max_poll_records=100,
    table_cleanup_interval=10,
    Agent="kaspr.core.agent.KasprAgent",
    Table="kaspr.core.table.KasprTable",
    GlobalTable="kaspr.core.table.KasprGlobalTable",
    Stream="kaspr.core.stream.KasprStream",
    LeaderAssignor="kaspr.core.leader_assignor.KasprLeaderAssignor",
)

app.monitor = PrometheusMonitor(
    app,
    pattern=f"{app.conf.web_metrics_base_path}/metrics",
)