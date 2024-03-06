from faust.assignor import LeaderAssignor
from mode.utils.objects import cached_property
from kaspr.app import KasprApp

class KasprLeaderAssignor(LeaderAssignor):
        
    @cached_property
    def _leader_topic_name(self) -> str:
        app: KasprApp = self.app
        return f'{app.conf.topic_prefix}assignor-leader'
