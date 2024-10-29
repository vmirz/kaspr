from kaspr.types.models.base import BaseModel
from kaspr.types.app import KasprAppT
from kaspr.types.channel import KasprChannelT


class ChannelSpec(BaseModel):
    name: str

    app: KasprAppT = None
    _channel: KasprChannelT = None

    def prepare_channel(self) -> KasprChannelT:
        return self.app.channel(self.name)

    @property
    def channel(self) -> KasprChannelT:
        if self._channel is None:
            self._channel = self.prepare_channel()
        return self._channel
