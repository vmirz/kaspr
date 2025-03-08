from faust.types.web import Request, Response, Web
from faust.web.views import View


class KasprWeb(Web):
    pass

class KasprWebResponse(Response):
    pass

class KasprWebRequest(Request):
    pass

class KasprWebViewT(View):
    pass
