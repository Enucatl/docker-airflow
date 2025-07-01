from typing import Any, Callable

from flask import current_app
from flask_appbuilder.const import AUTH_REMOTE_USER


class CustomMiddleware:
    def __init__(self, wsgi_app: Callable) -> None:
        self.wsgi_app = wsgi_app

    def __call__(self, environ: dict, start_response: Callable) -> Any:
        if "HTTP_REMOTE_USER" in environ:
            environ["REMOTE_USER"] = environ["HTTP_REMOTE_USER"]
        return self.wsgi_app(environ, start_response)


current_app.wsgi_app = CustomMiddleware(current_app.wsgi_app)

AUTH_TYPE = AUTH_REMOTE_USER
