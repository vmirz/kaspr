from faust.assignor import LeaderAssignor
from mode.utils.objects import cached_property

class KasprLeaderAssignor(LeaderAssignor):

    @cached_property
    def _leader_topic_name(self) -> str:
        return f'{self.app.conf.id}-assignor-leader'
