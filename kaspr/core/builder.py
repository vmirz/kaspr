"""Build stream processors from external definition files."""

import yaml
from typing import Dict, List
from pathlib import Path
from kaspr.types import KasprAppT, AppBuilderT, KasprStreamT
from kaspr.types.models import AppSpec, AgentSpec
from kaspr.types.schemas import AppSpecSchema

from mode.utils.objects import cached_property


class AppBuilder(AppBuilderT):
    """Build stream processors from external definition files."""

    app: KasprAppT = None

    _apps: List[AppSpec] = None
    _agents: List[AgentSpec] = None

    def __init__(self, app: KasprAppT) -> None:
        self.app = app

    def _files(self, directory: Path) -> List[str]:
        """Find all JSON files in the given directory and its subdirectories."""
        return list(directory.rglob("*.yml")) + list(directory.rglob("*.yaml"))

    def _load(self, files: List[Path]) -> List[AppSpec]:
        """Load the content of definition files and convert them to dictionaries."""
        apps = []
        for file in files:
            with file.open("r") as file:
                try:
                    apps.append(AppSpecSchema().load(yaml.safe_load(file)))
                except yaml.YAMLError as exc:
                    print(f"Error loading file `{file}`: {exc}")
        return apps

    def _load_apps(self) -> List[AppSpec]:
        """Load apps specs from files."""
        return self._load(self._files(self.app.conf.buildersdir))

    def _prepare_agents(self) -> List[AgentSpec]:
        """Prepare agents from loaded definitions."""
        agents = []
        for app in self.apps:
            agents.extend(app.agents)
        return agents

    async def __process(self, stream):
        async for event in stream:
            print(event)

    def build(self) -> None:
        """Build agents, tasks, etc. from external definition files."""
        self._build_agents()

    def _build_agents(self) -> None:
        async def _root_processor(stream: KasprStreamT):
            pass
        
        for agent in self.agents:
            _topic = agent.inputs.topic
            if _topic and _topic.names or _topic.pattern:
                channel = self.app.topic(
                    *_topic.names,
                    pattern=_topic.pattern,
                    key_serializer=_topic.key_serializer,
                    value_serializer=_topic.value_serializer,
                )
            elif agent.inputs.channel and agent.inputs.channel.name:
                channel = self.app.channel(agent.inputs.channel.name)
            else:
                raise ValueError("No input channel or topic defined for agent")
            self.app.agent(channel, name=agent.name)(_root_processor)

    @cached_property
    def apps(self) -> List[AppSpec]:
        if self._apps is None:
            self._apps = self._load_apps()
        return self._apps

    @cached_property
    def agents(self) -> List[AgentSpec]:
        if self._agents is None:
            self._agents = self._prepare_agents()
        return self._agents
