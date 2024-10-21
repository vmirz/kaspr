from kaspr.types.models.base import BaseModel
from kaspr.types.models.input import AgentInput
from kaspr.types.models.output import AgentOutput
from typing import Optional, List

class AgentSpec(BaseModel):

    name: str
    description: Optional[str]
    input: AgentInput
    output: List[AgentOutput]