"""Keep the status code and headers of HTML responses in the Astro Runtime.

Problem (Astro Runtime 3.3-7, astronomer/runtime/plugin.py):
``CustomizationMiddleware`` adds Astro scripts to each ``text/html`` response.
It makes a new ``StreamingResponse`` with only the body and the media type,
so it drops the status code and all the headers of the original response.

With the FAB auth manager (DEPLOY_VPS.md, step 16) the login pages are HTML:
- ``GET /auth/login/`` loses its ``Set-Cookie: session=...``, so the login
  form always fails with "The CSRF session token is missing".
- The redirect after a login (302 with an HTML body) becomes a 200 with no
  ``Location`` header.

Fix: replace ``CustomizationMiddleware.dispatch`` with a version that keeps
the status code and the headers (without ``Content-Length``, because the
injected scripts change the length). The Astro scripts are still added.

The patch runs when Airflow imports this file (plugin loading), before the
API server builds its middleware stack. If a later Runtime version removes
or renames the middleware, this file does nothing.
"""

from __future__ import annotations

import logging

from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


def _patch_astro_customization_middleware() -> bool:
    try:
        from astronomer.runtime import plugin as astro_plugin
        from starlette.responses import StreamingResponse
    except ImportError:
        return False

    middleware = getattr(astro_plugin, "CustomizationMiddleware", None)
    inject_scripts = getattr(astro_plugin, "inject_scripts", None)
    if middleware is None or inject_scripts is None:
        return False
    if getattr(middleware, "_spp_keeps_headers", False):
        return True

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if "text/html" not in response.headers.get("content-type", ""):
            return response
        new_response = StreamingResponse(
            inject_scripts(response.body_iterator),
            status_code=response.status_code,
            background=response.background,
        )
        # raw_headers keeps repeated headers (more than one Set-Cookie).
        new_response.raw_headers = [
            (name, value) for name, value in response.raw_headers if name.lower() != b"content-length"
        ]
        return new_response

    middleware.dispatch = dispatch
    middleware._spp_keeps_headers = True
    return True


if _patch_astro_customization_middleware():
    log.debug("Astro CustomizationMiddleware patched: HTML responses keep status code and headers")


class AstroHtmlHeadersFixPlugin(AirflowPlugin):
    """Empty plugin: Airflow imports this module, and the import applies the patch."""

    name = "spp_astro_html_headers_fix"
