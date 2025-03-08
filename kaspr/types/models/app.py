from typing import Optional, List
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.agent import AgentSpec
from kaspr.types.app import KasprAppT
from kaspr.types.agent import KasprAgentT


class AppSpec(SpecComponent):
    agents_spec: Optional[List[AgentSpec]]
    app: KasprAppT = None

    _agents: List[KasprAgentT] = None

    @property
    def agents(self) -> List[KasprAgentT]:
        if self.app:
            if self._agents is None:
                self._agents = [agent.agent for agent in self.agents_spec]

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f'{type(self).__name__}: {self.app.conf.name}'

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label