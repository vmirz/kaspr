from typing import Optional, List
from kaspr.types.models.base import BaseModel
from kaspr.types.models.input import AgentInputSpec
from kaspr.types.models.processor import AgentProcessorSpec
from kaspr.types.models.output import AgentOutput
from kaspr.types.app import KasprAppT
from kaspr.types.agent import KasprAgentT

class AgentSpec(BaseModel):

    name: str
    description: Optional[str]
    inputs: AgentInputSpec
    processors: AgentProcessorSpec
    #outputs: List[AgentOutput]

    app: KasprAppT = None
    _agent: KasprAgentT = None


    def prepare_agent(self) -> KasprAgentT:
        return self.app.agent(self.inputs.channel, name=self.name)(self.processors.processor)    
    
    @property
    def agent(self) -> KasprAgentT:
        if self._agent is None:
            self._agent = self.prepare_agent()
        return self._agent
