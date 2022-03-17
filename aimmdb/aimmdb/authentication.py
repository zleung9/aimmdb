from pathlib import Path

from tiled.authenticators import OIDCAuthenticator
from fastapi import APIRouter, Request, Security, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from tiled.server.utils import get_base_url
from tiled.server.authentication import get_current_principal

from .utils import SHARE_AIMMDB_PATH


def get_code_url(request):
    base_url = get_base_url(request)
    path = request.url.path.split("/")
    provider = path[path.index("provider") + 1]
    return f"{base_url}/auth/provider/{provider}/code"


class AIMMAuthenticator(OIDCAuthenticator):
    def __init__(
        self,
        client_id,
        client_secret,
        redirect_uri,
        public_keys,
        token_uri,
        authorization_endpoint,
        confirmation_message,
    ):
        super().__init__(
            client_id,
            client_secret,
            redirect_uri,
            public_keys,
            token_uri,
            authorization_endpoint,
            confirmation_message,
        )

        router = APIRouter()
        templates = Jinja2Templates(Path(SHARE_AIMMDB_PATH, "templates"))

        @router.get("/login", response_class=HTMLResponse)
        async def login(
            request: Request,
            # This dependency is here because it runs the code that moves
            # API key from the query parameter to a cookie (if it is valid).
            principal=Security(get_current_principal, scopes=[]),
        ):
            if request.headers.get("user-agent", "").startswith("python-tiled"):
                # This results in an error message like
                # ClientError: 400: To connect from a Python client, use
                # http://localhost:8000/api not http://localhost:8000/?root_path=true
                raise HTTPException(
                    status_code=400,
                    detail=f"To connect from a Python client, use {get_base_url(request)} not",
                )

            orcid_config = {
                "authorization_endpoint": self.authorization_endpoint,
                "tiled_code_url": get_code_url(request),
            }

            return templates.TemplateResponse(
                "login.html",
                {"request": request, "orcid_config": orcid_config},
            )

        self.include_routers = [router]
