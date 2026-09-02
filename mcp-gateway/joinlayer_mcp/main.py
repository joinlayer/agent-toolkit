from .config import Settings
from .api import JoinLayerAPI
from .auth import OAuthTokenVerifier
from .guard import GatewayGuard
from .server import create_server, create_streamable_http_app

import uvicorn


def main() -> None:
    settings = Settings.from_env()
    api = JoinLayerAPI(settings)
    server = create_server(settings, api)
    app = GatewayGuard(create_streamable_http_app(server, settings), settings, OAuthTokenVerifier(api, settings.public_url + "/mcp"))
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info", proxy_headers=False)


if __name__ == "__main__":
    main()
