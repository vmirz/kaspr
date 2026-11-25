"""Faust is part of the kaspr package."""

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path

from kaspr.faust.models.base import registry

REPO = Path(__file__).resolve().parents[1]

#: What Faust's own models are called in Kafka, where members that still run Faust as a package of
#: its own read and write them too.
NAMES_IN_KAFKA = {
    "agents.models": {
        "@ReqRepRequest",
        "@ReqRepResponse",
        "faust.agents.models.ModelReqRepRequest",
        "faust.agents.models.ModelReqRepResponse",
    },
    "assignor.client_assignment": {"@ClientAssignment", "@ClientMetadata"},
    "assignor.cluster_assignment": {"@ClusterAssignment"},
    "joins": {"faust.joins.ResponseMessage", "faust.joins.SubscriptionMessage"},
    "livecheck.models": {
        "faust.livecheck.models.SignalEvent",
        "faust.livecheck.models.TestExecution",
        "faust.livecheck.models.TestReport",
    },
    "tables.sets": {"@SetManagerOperation"},
    "web.apps.tables": {"@TableInfo"},
}


def imports(path):
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level:
            yield node.module


def test_nothing_imports_faust_as_a_package_of_its_own():
    """It would work wherever twm-faust is still installed, and not in the image."""
    sources = [*(REPO / "kaspr").rglob("*.py"), *(REPO / "tests").rglob("*.py")]
    assert not [str(p) for p in sources if any(name.split(".")[0] == "faust" for name in imports(p))]


def test_models_keep_their_names_in_kafka():
    for module, names in NAMES_IN_KAFKA.items():
        module = importlib.import_module(f"kaspr.faust.{module}").__name__
        assert {name for name, model in registry.items() if model.__module__ == module} == names


def run(tmp_path, *command):
    env = {
        **os.environ,
        "K_APP_NAME": "app",
        "K_DATA_DIR": str(tmp_path / "data"),
        "K_DEFINITIONS_DIR": str(tmp_path / "definitions"),
    }
    result = subprocess.run(
        [sys.executable, *command], cwd=REPO, env=env, capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_autodiscovery_stays_out_of_faust(tmp_path):
    """Every Kaspr command imports the modules of kaspr first; Faust's would run its command-line."""
    assert "Agents" in run(tmp_path, "-m", "kaspr", "agents")


def test_faust_command_line(tmp_path):
    assert "Faust" in run(tmp_path, "-m", "kaspr.faust", "--version")
