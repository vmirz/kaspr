from kaspr.types.models.base import BaseModel
from kaspr.types.models.agent import AgentSpec
from typing import Optional, List

class AppSpec(BaseModel):

    agents: Optional[List[AgentSpec]]