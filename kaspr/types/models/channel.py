from kaspr.types.models.base import SpecComponent
from kaspr.types.app import KasprAppT
from kaspr.types.channel import KasprChannelT


class ChannelSpec(SpecComponent):
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

    @property
    def label(self) -> str:
        """Return description, used in graphs and logs."""
        return f'{type(self).__name__}: {self.name}'

    @property
    def shortlabel(self) -> str:
        """Return short description."""
        return self.label