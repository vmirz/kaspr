import logging
from types import SimpleNamespace

import pytest
from faust.exceptions import ImproperlyConfigured

from kaspr.core.builder import AppBuilder
from kaspr.types.models.agent.agent import AgentSpec
from kaspr.types.models.agent.input import AgentInputSpec
from kaspr.types.models.agent.operations import AgentProcessorOperation
from kaspr.types.models.agent.processor import AgentProcessorSpec
from kaspr.types.models.channel import ChannelSpec
from kaspr.types.models.join import JoinSpec
from kaspr.types.models.tableref import TableRefSpec
from kaspr.types.models.topicsrc import TopicSrcSpec


def _table(name, key_serializer="json", value_serializer="json"):
    # What a KasprTable reports: 'raw' when its spec says raw, else 'json'.
    return SimpleNamespace(
        name=name,
        key_serializer=key_serializer,
        value_serializer=value_serializer,
    )


def _app(*tables, key_serializer="raw"):
    return SimpleNamespace(
        conf=SimpleNamespace(key_serializer=key_serializer),
        tables={table.name: table for table in tables},
    )


def _agent(app, name, tables, topic="source", key_serializer=None,
           value_serializer=None, channel=None):
    if channel is None:
        input = AgentInputSpec(
            topic_spec=TopicSrcSpec(
                name=topic,
                pattern=None,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                app=app,
            ),
            channel_spec=None,
            app=app,
        )
    else:
        input = AgentInputSpec(
            topic_spec=None,
            channel_spec=ChannelSpec(name=channel, app=app),
            app=app,
        )
    operation = AgentProcessorOperation(
        name="write",
        table_refs=[TableRefSpec(name=t, param_name=t) for t in tables],
        app=app,
    )
    return AgentSpec(
        name=name,
        input=input,
        processors=AgentProcessorSpec(operations=[operation], app=app),
        app=app,
    )


def _join(name, left, right):
    return JoinSpec(name=name, left_table=left, right_table=right)


def _builder(app, agents, joins):
    builder = AppBuilder(app)
    builder._agents = agents
    builder._joins = joins
    return builder


JOIN = _join("orders-products", "orders", "products")


def test_defaults_do_not_join():
    # Unset, the topic's keys are raw and the table's json.
    app = _app(_table("orders"), _table("products"))
    agents = [
        _agent(app, "store-orders", ["orders"], topic="orders"),
        _agent(app, "store-products", ["products"], topic="products"),
    ]

    with pytest.raises(ImproperlyConfigured) as raised:
        _builder(app, agents, [JOIN]).verify_serializers()

    message = str(raised.value)
    assert "Agent 'store-orders' uses table 'orders'" in message
    assert "Agent 'store-products' uses table 'products'" in message
    assert "joined by 'orders-products'" in message
    assert "input topic 'orders' has key serializer 'raw'" in message
    assert "and the table 'json'" in message


@pytest.mark.parametrize("topic_serializer,table_serializer", [
    ("json", "json"),
    ("raw", "raw"),
])
def test_matching_serializers_join(topic_serializer, table_serializer):
    app = _app(
        _table("orders", key_serializer=table_serializer),
        _table("products", key_serializer=table_serializer),
    )
    agents = [
        _agent(app, "store-orders", ["orders"],
               key_serializer=topic_serializer),
        _agent(app, "store-products", ["products"],
               key_serializer=topic_serializer),
    ]

    _builder(app, agents, [JOIN]).verify_serializers()


def test_app_default_key_serializer_counts():
    app = _app(_table("orders"), _table("products"), key_serializer="json")
    agents = [_agent(app, "store-orders", ["orders"])]

    _builder(app, agents, [JOIN]).verify_serializers()


def test_every_joined_table_is_checked():
    # Not only the first table an agent uses.
    app = _app(_table("audit"), _table("orders"), _table("products"))
    agents = [_agent(app, "store", ["audit", "orders"], key_serializer="raw")]

    with pytest.raises(ImproperlyConfigured) as raised:
        _builder(app, agents, [JOIN]).verify_serializers()

    assert "uses table 'orders'" in str(raised.value)
    assert "'audit'" not in str(raised.value)


def test_tables_outside_joins_only_warn(caplog):
    app = _app(_table("audit"))
    agents = [_agent(app, "store", ["audit"], key_serializer="raw")]

    with caplog.at_level(logging.WARNING):
        _builder(app, agents, [JOIN]).verify_serializers()

    assert "key_serializer 'raw' does not match table 'audit'" in caplog.text


def test_joined_table_is_not_also_warned_about(caplog):
    app = _app(_table("orders"), _table("products"))
    agents = [_agent(app, "store", ["orders"], key_serializer="raw")]

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ImproperlyConfigured):
            _builder(app, agents, [JOIN]).verify_serializers()

    assert "does not match table 'orders' key_serializer" not in caplog.text


def test_channel_input_is_not_checked():
    # A join's output channel carries no topic serializer.
    app = _app(_table("orders"), _table("products"))
    agents = [_agent(app, "enrich", ["orders"], channel="joined")]

    _builder(app, agents, [JOIN]).verify_serializers()


def test_without_joins_nothing_is_refused():
    app = _app(_table("orders"))
    agents = [_agent(app, "store", ["orders"])]

    _builder(app, agents, []).verify_serializers()


def test_build_verifies_after_every_definition(monkeypatch):
    app = _app(_table("orders"), _table("products"))
    builder = _builder(
        app, [_agent(app, "store", ["orders"], key_serializer="raw")], [JOIN])
    builder._apps = []
    builder._webviews = []
    monkeypatch.setattr(
        "kaspr.core.builder.WebViewSpec.prepare_webviews", lambda views: None)

    with pytest.raises(ImproperlyConfigured):
        builder.build()


def test_builder_lists_the_loaded_tables():
    app = _app()
    builder = AppBuilder(app)
    specs = [SimpleNamespace(name="orders"), SimpleNamespace(name="products")]
    builder._apps = [SimpleNamespace(tables_spec=specs[:1]),
                     SimpleNamespace(tables_spec=specs[1:])]

    assert builder.tables == specs
