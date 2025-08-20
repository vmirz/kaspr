# failfast_assignor.py
import os
import asyncio
from faust.assignor import PartitionAssignor as BaseAssignor

EXIT_CODE = 42  # any non-zero

class KasprPartitionAssignor(BaseAssignor):
    def on_assignment(self, *args, **kwargs):
        try:
            return super().on_assignment(*args, **kwargs)
        except AssertionError as e:
            self.app.log.error("Faust assignment assertion failed: %r", e, exc_info=True)
            self.app.log.error("Exiting with code %d", EXIT_CODE)
            os._exit(EXIT_CODE)