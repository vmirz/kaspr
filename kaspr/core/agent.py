import faust


class KasprAgent(faust.Agent):
    """
    Adds customization to faust.Agent.
    """

    def _agent_label(self, name_suffix: str = '') -> str:
        s = f'{self.name}{name_suffix}: '
        return s
    
    @property
    def shortlabel(self) -> str:
        """Return short description of agent."""
        return f"{type(self).__name__}: {self._agent_label()}"