from typing import Optional, List
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.agent import AgentSpec
from kaspr.types.models.webview import WebViewSpec
from kaspr.types.models.table import TableSpec
from kaspr.types.app import KasprAppT
from kaspr.types.agent import KasprAgentT
from kaspr.types.webview import KasprWebViewT
from kaspr.types.table import KasprTableT


class AppSpec(SpecComponent):
    agents_spec: Optional[List[AgentSpec]]
    webviews_spec: Optional[List[WebViewSpec]]
    tables_spec: Optional[List[TableSpec]]

    app: KasprAppT = None
    _agents: List[KasprAgentT] = None
    _webviews: List[KasprWebViewT] = None
    _tables: List[KasprTableT] = None

    @property
    def agents(self) -> List[KasprAgentT]:
        if self.app:
            if self._agents is None:
                self._agents = [agent.agent for agent in self.agents_spec]
            return self._agents

    @property
    def webviews(self) -> List[WebViewSpec]:

        if self.app:
            if self._webviews is None:
                self._webviews = [webview.webview for webview in self.webviews_spec]
            return self._webviews
        
    @property
    def tables(self) -> List[TableSpec]:
        if self.app:
            if self._tables is None:
                self._tables = [table.table for table in self.tables_spec]
            return self._tables

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f"{type(self).__name__}: {self.app.conf.name}"

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label
