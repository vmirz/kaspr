from typing import Optional
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.agent.input import AgentInputSpec
from kaspr.types.models.agent.output import AgentOutputSpec
from kaspr.types.models.agent.processor import AgentProcessorSpec
from kaspr.types.app import KasprAppT
from kaspr.types.agent import KasprAgentT


class AgentSpec(SpecComponent):
    name: str
    description: Optional[str]
    isolated_partitions: Optional[bool]
    input: AgentInputSpec
    output: AgentOutputSpec
    processors: AgentProcessorSpec

    app: KasprAppT = None

    _agent: KasprAgentT = None

    def prepare_agent(self) -> KasprAgentT:
        self.log.info("Preparing...")
        processors = self.processors
        processors.input = self.input
        processors.output = self.output
        return self.app.agent(
            self.input.channel,
            name=self.name,
            isolated_partitions=self.isolated_partitions,
        )(processors.processor)

    @property
    def agent(self) -> KasprAgentT:
        if self._agent is None:
            self._agent = self.prepare_agent()
        return self._agent

    @property
    def label(self) -> str:
        """Return description of component, used in logs."""
        return f"{type(self).__name__}: {self.__repr__()}"

    @property
    def shortlabel(self) -> str:
        """Return short description of processor."""
        return f"{type(self).__name__}: {self.name}"
