from typing import Callable, TypeVar, Union, Awaitable, Type, Optional
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.webview.request import WebViewRequestSpec
from kaspr.types.models.webview.response import WebViewResponseSpec
from kaspr.types.models.webview.processor import WebViewProcessorSpec
from kaspr.types.app import KasprAppT
from kaspr.types.webview import KasprWebViewT, View

T = TypeVar("T")
Function = Callable[[T], Union[T, Awaitable[T]]]


class WebViewSpec(SpecComponent):
    name: str
    description: Optional[str]
    request: WebViewRequestSpec
    response: WebViewResponseSpec
    processors: WebViewProcessorSpec

    app: KasprAppT = None

    _webview: KasprWebViewT = None

    def prepare_webview(self) -> KasprWebViewT:
        self.log.info("Preparing...")
        processors = self.processors
        processors.response = self.response
        return self.app.page(self.request.path, name=self.name)(
            self.prepare_request_handler()
        )

    def prepare_request_handler(self) -> Type[View]:
        """Returns a class type that implements handler required HTTP verb."""
    
        return type("KasprWebView", (View,), {
            self.request.method.lower(): self.processors.processor
        })            

    @property
    def webview(self) -> KasprWebViewT:
        if self._webview is None:
            self._webview = self.prepare_webview()
        return self._webview

    @property
    def label(self) -> str:
        """Return description of component, used in logs."""
        return f"{type(self).__name__}: {self.__repr__()}"

    @property
    def shortlabel(self) -> str:
        """Return short description of processor."""
        return f"{type(self).__name__}: {self.name}"
