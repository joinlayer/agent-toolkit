from __future__ import annotations

import time
from contextvars import ContextVar, Token
from dataclasses import dataclass

from mcp.server.auth.provider import AccessToken, TokenVerifier

from .api import JoinLayerAPI, JoinLayerAPIError

_CURRENT_API_TOKEN: ContextVar[str | None] = ContextVar("joinlayer_mcp_api_token", default=None)
_CURRENT_OAUTH_PRINCIPAL: ContextVar[str | None] = ContextVar("joinlayer_mcp_oauth_principal", default=None)


def current_api_token() -> str:
    token = _CURRENT_API_TOKEN.get()
    if not token:
        raise PermissionError("No authenticated JoinLayer delegated session is available")
    return token


def set_current_api_token(token: str) -> Token[str | None]:
    return _CURRENT_API_TOKEN.set(token)


def reset_current_api_token(context_token: Token[str | None]) -> None:
    _CURRENT_API_TOKEN.reset(context_token)


def current_oauth_principal() -> str | None:
    return _CURRENT_OAUTH_PRINCIPAL.get()


def set_current_oauth_principal(principal: str) -> Token[str | None]:
    return _CURRENT_OAUTH_PRINCIPAL.set(principal)


def reset_current_oauth_principal(context_token: Token[str | None]) -> None:
    _CURRENT_OAUTH_PRINCIPAL.reset(context_token)


@dataclass(frozen=True)
class OAuthVerification:
    access: AccessToken
    api_token: str
    principal_key: str


class OAuthTokenVerifier(TokenVerifier):
    """Fail-closed verifier backed by JoinLayer's OAuth authorization server."""

    def __init__(self, api: JoinLayerAPI, resource: str) -> None:
        self._api = api
        self._resource = resource

    async def verify(self, token: str) -> OAuthVerification | None:
        if not token.startswith("jlo_at_") or len(token) < 48:
            return None
        try:
            principal = await self._api.introspect(token)
        except JoinLayerAPIError as exc:
            if exc.status_code in {400, 401, 403}:
                return None
            raise
        if principal.get("active") is not True:
            return None
        resource = str(principal.get("resource") or "").strip()
        client_id = str(principal.get("client_id") or "").strip()
        api_token = str(principal.get("api_token") or "").strip()
        agent_id = str(principal.get("agent_id") or "").strip()
        org_id = str(principal.get("org_id") or "").strip()
        user_id = str(principal.get("user_id") or "").strip()
        grant_id = str(principal.get("grant_id") or "").strip()
        try:
            expires_at = int(principal.get("expires_at") or 0)
        except (TypeError, ValueError):
            return None
        if (
            resource != self._resource
            or not client_id
            or not api_token.startswith("jli_")
            or not agent_id
            or not org_id
            or not user_id
            or not grant_id
            or expires_at <= int(time.time())
        ):
            return None
        scopes = [scope for scope in str(principal.get("scope") or "").split() if scope]
        access = AccessToken(
            token=token,
            client_id=client_id,
            scopes=scopes,
            expires_at=expires_at,
            resource=resource,
            subject=user_id,
            claims={"act": agent_id, "org_id": org_id, "grant_id": grant_id},
        )
        return OAuthVerification(
            access=access,
            api_token=api_token,
            principal_key="\x1f".join((grant_id, client_id, user_id, agent_id, org_id)),
        )

    async def verify_token(self, token: str) -> AccessToken | None:
        verification = await self.verify(token)
        return verification.access if verification is not None else None
