"""HTTP endpoint to handle actionable signals for the app."""

from collections import defaultdict
from typing import Any, List, MutableMapping, Set
from faust import web
from faust.types.tuples import TP

__all__ = ["Signal", "blueprint"]

TPMap = MutableMapping[str, List[int]]

blueprint = web.Blueprint("signal")

@blueprint.route("/rebalance", name="rebalance")
class Signal(web.View):
    """App signal information."""

    async def post(self, request: web.Request, **kwargs: Any) -> Any:
        """Handle signals sent to the app."""
        self.app.log.info("Received rebalance signal!")
        self.app.consumer.request_rejoin()