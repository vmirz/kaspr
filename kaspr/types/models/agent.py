import asyncio
from typing import Optional
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.input import AgentInputSpec
from kaspr.types.models.processor import AgentProcessorSpec
from kaspr.types.app import KasprAppT
from kaspr.types.agent import KasprAgentT


class AgentSpec(SpecComponent):
    name: str
    description: Optional[str]
    inputs: AgentInputSpec
    processors: AgentProcessorSpec

    app: KasprAppT = None

    _agent: KasprAgentT = None

    def prepare_agent(self) -> KasprAgentT:
        self.log.info("Preparing...")
        return self.app.agent(
            self.inputs.channel, name=self.name
        )(self.processors.processor)

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
