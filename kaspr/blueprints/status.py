"""HTTP endpoint showing health/status of various components the app."""

from collections import defaultdict
from typing import List, MutableMapping, Set
from faust import web
from faust.types.tuples import TP

__all__ = ["Status", "blueprint"]

TPMap = MutableMapping[str, List[int]]

blueprint = web.Blueprint("status")

@blueprint.route("/", name="status")
class Status(web.View):
    """App status information."""

    @classmethod
    def _topic_grouped(cls, assignment: Set[TP]) -> TPMap:
        tps: MutableMapping[str, List[int]] = defaultdict(list)
        for tp in sorted(assignment):
            tps[tp.topic].append(tp.partition)
        return dict(tps)

    async def get(self, request: web.Request) -> web.Response:
        """Return current app state as a JSON response."""
        assignor = self.app.assignor
        return self.json(
            {
                "rebalancing": self.app.rebalancing,
                "recovering": self.app.tables.recovery.in_recovery,
                "assignment": {
                    "actives": self._topic_grouped(assignor.assigned_actives()),
                    "standbys": self._topic_grouped(assignor.assigned_standbys()),
                },
                "table_metadata": self.app.router.tables_metadata()
            }
        )
