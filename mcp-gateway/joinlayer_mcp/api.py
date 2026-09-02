from __future__ import annotations

import json
import uuid
import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

from .config import Settings

MCP_CONTRACT_VERSION = "2026-07-28"


@dataclass(frozen=True)
class JoinLayerAPIError(Exception):
    status_code: int
    code: str
    message: str
    details: Any = None

    def __str__(self) -> str:
        return f"JoinLayer API {self.status_code} {self.code}: {self.message}"


class JoinLayerAPI:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        timeout = httpx.Timeout(settings.api_timeout_seconds, connect=min(settings.api_timeout_seconds, 5.0))
        self._client = httpx.AsyncClient(
            base_url=f"{settings.api_base_url}/api/v1",
            timeout=timeout,
            follow_redirects=False,
            headers={"Accept": "application/json", "User-Agent": "joinlayer-mcp/0.1"},
        )
        self._root_client = httpx.AsyncClient(
            base_url=settings.api_base_url,
            timeout=timeout,
            follow_redirects=False,
            headers={"Accept": "application/json", "User-Agent": "joinlayer-mcp/0.1"},
        )
        self._readiness_lock = asyncio.Lock()
        self._readiness_valid_until = 0.0

    async def close(self) -> None:
        await self._client.aclose()
        await self._root_client.aclose()

    async def verify_readiness(self) -> None:
        if time.monotonic() < self._readiness_valid_until:
            return
        async with self._readiness_lock:
            if time.monotonic() < self._readiness_valid_until:
                return
            try:
                ready = await self._root_client.get("/readyz")
                auth_boundary = await self._root_client.get("/api/v1/me")
            except httpx.HTTPError as exc:
                raise JoinLayerAPIError(503, "api_unreachable", "JoinLayer API is unreachable") from exc
            if ready.status_code not in {200, 204}:
                raise JoinLayerAPIError(503, "api_not_ready", "JoinLayer API is not ready")
            if auth_boundary.status_code != 401:
                raise JoinLayerAPIError(503, "api_auth_contract_invalid", "JoinLayer API authentication boundary is not enforced")
            self._readiness_valid_until = time.monotonic() + 5.0

    async def request(
        self,
        api_token: str,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        approval_id: str | None = None,
        tool_name: str | None = None,
    ) -> Any:
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("API path must be absolute and host-relative")
        headers = {
            "Authorization": f"Bearer {api_token}",
            "X-JoinLayer-Gateway-Token": self._settings.gateway_token,
            "X-Request-ID": f"mcp_{uuid.uuid4().hex}",
        }
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        if approval_id:
            headers["X-JoinLayer-Approval-ID"] = approval_id
        if tool_name:
            headers["X-JoinLayer-Agent-Tool"] = tool_name
            headers["X-JoinLayer-MCP-Version"] = MCP_CONTRACT_VERSION
        request = self._client.build_request(method, path, json=body, params=params, headers=headers)
        response: httpx.Response | None = None
        try:
            response = await self._client.send(request, stream=True)
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    declared_size = int(content_length)
                except ValueError:
                    declared_size = 0
                if declared_size > self._settings.max_response_bytes:
                    raise JoinLayerAPIError(502, "response_too_large", "JoinLayer API response exceeded the MCP safety limit")
            raw = bytearray()
            async for chunk in response.aiter_bytes():
                if len(raw) + len(chunk) > self._settings.max_response_bytes:
                    raise JoinLayerAPIError(502, "response_too_large", "JoinLayer API response exceeded the MCP safety limit")
                raw.extend(chunk)
            payload = _decode_json(bytes(raw))
            if response.is_error:
                error = payload.get("error", {}) if isinstance(payload, dict) else {}
                raise JoinLayerAPIError(
                    response.status_code,
                    str(error.get("code") or "api_error"),
                    str(error.get("message") or f"JoinLayer API returned HTTP {response.status_code}"),
                    error.get("details"),
                )
            return payload
        except httpx.HTTPError as exc:
            raise JoinLayerAPIError(502, "api_transport_error", "JoinLayer API request failed") from exc
        finally:
            if response is not None:
                await response.aclose()

    async def introspect(self, token: str) -> dict[str, Any]:
        """Validate an MCP audience token and mint a separate API-audience token."""
        request = self._root_client.build_request(
            "POST",
            "/api/v1/internal/oauth/introspect",
            json={"token": token, "resource": self._settings.public_url + "/mcp"},
            headers={
                "X-JoinLayer-Gateway-Token": self._settings.gateway_token,
                "Content-Type": "application/json",
                "Cache-Control": "no-store",
            },
        )
        response: httpx.Response | None = None
        try:
            response = await self._root_client.send(request, stream=True)
            raw = bytearray()
            async for chunk in response.aiter_bytes():
                if len(raw) + len(chunk) > min(self._settings.max_response_bytes, 64 * 1024):
                    raise JoinLayerAPIError(502, "invalid_introspection_response", "OAuth introspection response is too large")
                raw.extend(chunk)
            payload = _decode_json(bytes(raw))
            if response.is_error:
                error = payload.get("error", {}) if isinstance(payload, dict) else {}
                raise JoinLayerAPIError(
                    response.status_code,
                    str(error.get("code") or "oauth_introspection_error"),
                    str(error.get("message") or "OAuth introspection failed"),
                )
            if not isinstance(payload, dict):
                raise JoinLayerAPIError(502, "invalid_introspection_response", "OAuth introspection returned invalid JSON")
            return payload
        except httpx.HTTPError as exc:
            raise JoinLayerAPIError(502, "oauth_introspection_transport_error", "OAuth introspection request failed") from exc
        finally:
            if response is not None:
                await response.aclose()


def _decode_json(raw: bytes) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise JoinLayerAPIError(502, "invalid_api_response", "JoinLayer API returned invalid JSON") from exc
