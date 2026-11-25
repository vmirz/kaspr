"""K_KAFKA_BOOTSTRAP_SERVERS is a comma-separated list, as a KasprApp's bootstrapServers is."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PROGRAM = """
import json
from kaspr.app import app
from kaspr.faust.transport.drivers.aiokafka import server_list
print(json.dumps(server_list(app.conf.broker, 9092)))
"""


@pytest.mark.parametrize("value, servers", [
    ("a:9092", ["a:9092"]),
    ("a:9092,b:9093", ["a:9092", "b:9093"]),
    ("a:9092;b:9093", ["a:9092", "b:9093"]),
    ("a,b", ["a:9092", "b:9092"]),
])
def test_bootstrap_servers(value, servers):
    """Read from the environment once, as kaspr.types.settings is imported."""
    env = {**os.environ, "K_APP_NAME": "app", "K_KAFKA_BOOTSTRAP_SERVERS": value}
    result = subprocess.run(
        [sys.executable, "-c", PROGRAM], cwd=REPO, env=env, capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == servers
