from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from .api import JoinLayerAPIError
from .auth import (
    OAuthTokenVerifier,
    reset_current_api_token,
    reset_current_oauth_principal,
    set_current_api_token,
    set_current_oauth_principal,
)
from .metrics import (
    AUTH_DURATION,
    AUTHENTICATIONS,
    CONCURRENCY_LIMIT,
    DURATION,
    IN_FLIGHT,
    RATE_LIMIT_KEYS,
    RATE_LIMIT_KEYS_LIMIT,
    REJECTIONS,
    REQUESTS,
)

TOOL_SCOPES = {
    "get_workspace_context": "workspace:read",
    "get_workspace_capacity": "usage:read",
    "list_connector_types": "workspace:read",
    "list_connections": "connections:read",
    "get_connection": "connections:read",
    "create_connection_setup": "connections:test",
    "get_connection_setup_status": "connections:test",
    "list_connection_setups": "connections:test",
    "cancel_connection_setup": "connections:test",
    "test_connection": "connections:test",
    "discover_connection_schema": "connections:test",
    "list_pipelines": "pipelines:read",
    "get_pipeline": "pipelines:read",
    "create_pipeline_draft": "pipelines:write",
    "update_pipeline_draft": "pipelines:write",
    "validate_pipeline": "pipelines:validate",
    "preview_pipeline": "pipelines:validate",
    "request_run_start_approval": "runs:execute",
    "request_run_stop_approval": "runs:control",
    "list_agent_approvals": "runs:read",
    "cancel_agent_approval": "runs:read",
    "start_pipeline": "runs:execute",
    "list_pipeline_runs": "runs:read",
    "get_run": "runs:read",
    "stop_run": "runs:control",
    "list_activity": "runs:read",
    "diagnose_run_failure": "diagnostics:read",
    "get_usage_report": "usage:read",
}


@dataclass
class _Bucket:
    tokens: float
    updated_at: float
    last_seen_at: float


class TokenBucketRegistry:
    def __init__(self, rate: float, burst: int, max_keys: int, idle_ttl_seconds: float = 900, *, name: str = "other") -> None:
        self.rate = rate
        self.burst = burst
        self.max_keys = max_keys
        self.name = name
        self.idle_ttl_seconds = idle_ttl_seconds
        self._buckets: dict[str, _Bucket] = {}
        self._lock = asyncio.Lock()
        self._calls = 0
        RATE_LIMIT_KEYS.labels(self.name).set(0)
        RATE_LIMIT_KEYS_LIMIT.labels(self.name).set(max_keys)

    async def allow(self, key: str, now: float | None = None) -> tuple[bool, int]:
        current = time.monotonic() if now is None else now
        async with self._lock:
            self._calls += 1
            if self._calls % 100 == 0:
                cutoff = current - self.idle_ttl_seconds
                self._buckets = {name: bucket for name, bucket in self._buckets.items() if bucket.last_seen_at >= cutoff}
            bucket = self._buckets.get(key)
            if bucket is None:
                if len(self._buckets) >= self.max_keys:
                    RATE_LIMIT_KEYS.labels(self.name).set(len(self._buckets))
                    return False, 1
                bucket = _Bucket(tokens=float(self.burst), updated_at=current, last_seen_at=current)
                self._buckets[key] = bucket
            elapsed = max(0.0, current - bucket.updated_at)
            bucket.tokens = min(float(self.burst), bucket.tokens + elapsed * self.rate)
            bucket.updated_at = current
            bucket.last_seen_at = current
            RATE_LIMIT_KEYS.labels(self.name).set(len(self._buckets))
            if bucket.tokens >= 1:
                bucket.tokens -= 1
                return True, 0
            wait = max(1, math.ceil((1 - bucket.tokens) / self.rate))
            return False, wait


class GatewayGuard:
    def __init__(self, app: Any, settings: Any, token_verifier: OAuthTokenVerifier) -> None:
        self.app = app
        self.token_verifier = token_verifier
        self.global_bucket = TokenBucketRegistry(settings.global_rate_limit_rps, settings.global_rate_limit_burst, 1, name="global")
        self.client_buckets = TokenBucketRegistry(
            settings.client_rate_limit_rps,
            settings.client_rate_limit_burst,
            settings.rate_limit_max_keys,
            name="client",
        )
        self.anonymous_buckets = TokenBucketRegistry(
            settings.anonymous_rate_limit_rps,
            settings.anonymous_rate_limit_burst,
            settings.rate_limit_max_keys,
            name="anonymous",
        )
        self.auth_attempt_buckets = TokenBucketRegistry(
            settings.auth_attempt_rate_limit_rps,
            settings.auth_attempt_rate_limit_burst,
            settings.rate_limit_max_keys,
            name="auth_attempt",
        )
        self.max_concurrent = settings.max_concurrent_requests
        CONCURRENCY_LIMIT.set(self.max_concurrent)
        self.max_request_bytes = settings.max_request_bytes
        self.max_response_bytes = settings.max_response_bytes
        self.request_body_timeout_seconds = settings.request_body_timeout_seconds
        self.trust_proxy_headers = settings.trust_proxy_headers
        self.allowed_hosts = frozenset(host.lower() for host in settings.allowed_hosts)
        self.allowed_origins = frozenset(origin.lower() for origin in settings.allowed_origins)
        self.resource_metadata_url = settings.public_url + "/.well-known/oauth-protected-resource/mcp"
        self.initial_scope = "workspace:read"
        self._active = 0
        self._active_lock = asyncio.Lock()

    async def __call__(self, scope: dict[str, Any], receive: Callable[..., Awaitable[Any]], send: Callable[..., Awaitable[Any]]) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path", ""))
        route = _route_label(path)
        method = str(scope.get("method", "GET")).upper()
        # Health and the private, independently authenticated scrape endpoint
        # must remain observable while public MCP admission is saturated. They
        # also must not inflate the user-traffic in-flight gauge themselves.
        if path in {"/healthz", "/metrics"}:
            await self._observe(scope, receive, send, method, route)
            return

        headers = _headers(scope)
        if path == "/mcp" or path.startswith("/skills/") or path.startswith("/.well-known/oauth-protected-resource"):
            reason = _transport_error(headers, self.allowed_hosts, self.allowed_origins, path == "/mcp")
            if reason:
                status = 421 if reason == "invalid_host" else 403
                await _error(send, status, reason)
                REJECTIONS.labels(reason, route).inc()
                REQUESTS.labels(method, route, str(status)).inc()
                return

        allowed, retry_after = await self.global_bucket.allow("global")
        if not allowed:
            await _reject(send, 429, "global_rate_limited", retry_after)
            REJECTIONS.labels("global_rate_limit", route).inc()
            REQUESTS.labels(method, route, "429").inc()
            return

        async with self._active_lock:
            if self._active >= self.max_concurrent:
                REJECTIONS.labels("concurrency_limit", route).inc()
                REQUESTS.labels(method, route, "503").inc()
                await _reject(send, 503, "gateway_busy", 1)
                return
            self._active += 1
            IN_FLIGHT.set(self._active)
        context_token = None
        principal_context_token = None
        try:
            if path == "/mcp":
                source_key = "ip:" + _client_ip(scope, headers, self.trust_proxy_headers)
                allowed, retry_after = await self.auth_attempt_buckets.allow(source_key)
                if not allowed:
                    await _reject(send, 429, "authentication_rate_limited", retry_after)
                    REJECTIONS.labels("auth_attempt_rate_limit", route).inc()
                    REQUESTS.labels(method, route, "429").inc()
                    return
                authorization = headers.get("authorization", "")
                scheme, _, credential = authorization.partition(" ")
                access = None
                if scheme.lower() == "bearer" and credential:
                    auth_started = time.monotonic()
                    try:
                        verification = await self.token_verifier.verify(credential)
                        access = verification.access if verification is not None else None
                    except JoinLayerAPIError:
                        AUTHENTICATIONS.labels("dependency_error").inc()
                        AUTH_DURATION.labels("dependency_error").observe(time.monotonic() - auth_started)
                        await _error(send, 503, "authentication_dependency_unavailable")
                        REJECTIONS.labels("auth_dependency", route).inc()
                        REQUESTS.labels(method, route, "503").inc()
                        return
                    auth_outcome = "success" if access is not None else "invalid"
                    AUTHENTICATIONS.labels(auth_outcome).inc()
                    AUTH_DURATION.labels(auth_outcome).observe(time.monotonic() - auth_started)
                if access is None:
                    allowed, retry_after = await self.anonymous_buckets.allow(source_key)
                    if not allowed:
                        await _reject(send, 429, "rate_limited", retry_after)
                        REJECTIONS.labels("anonymous_rate_limit", route).inc()
                        REQUESTS.labels(method, route, "429").inc()
                        return
                    challenge = _oauth_challenge(
                        self.resource_metadata_url,
                        _challenge_scope(headers, self.initial_scope),
                        "invalid_token",
                    )
                    await _error(send, 401, "invalid_token", [(b"www-authenticate", challenge.encode())])
                    REJECTIONS.labels("authentication", route).inc()
                    REQUESTS.labels(method, route, "401").inc()
                    return
                # One popular MCP client must not make unrelated users or
                # workspaces share a throttle bucket. The key is the stable
                # delegated grant/client/user/agent/workspace principal and
                # never contains bearer-token material.
                allowed, retry_after = await self.client_buckets.allow("agent:" + verification.principal_key)
                if not allowed:
                    await _reject(send, 429, "rate_limited", retry_after)
                    REJECTIONS.labels("client_rate_limit", route).inc()
                    REQUESTS.labels(method, route, "429").inc()
                    return
                context_token = set_current_api_token(verification.api_token)
                principal_context_token = set_current_oauth_principal(verification.principal_key)
            elif path.startswith("/skills/"):
                key = "ip:" + _client_ip(scope, headers, self.trust_proxy_headers)
                allowed, retry_after = await self.anonymous_buckets.allow(key)
                if not allowed:
                    await _reject(send, 429, "rate_limited", retry_after)
                    REJECTIONS.labels("anonymous_rate_limit", route).inc()
                    REQUESTS.labels(method, route, "429").inc()
                    return

            bounded_receive = receive
            if method in {"POST", "PUT", "PATCH"}:
                try:
                    async with asyncio.timeout(self.request_body_timeout_seconds):
                        body = await _bounded_body(receive, headers.get("content-length"), self.max_request_bytes)
                except TimeoutError:
                    await _error(send, 408, "request_body_timeout")
                    REJECTIONS.labels("request_body_timeout", route).inc()
                    REQUESTS.labels(method, route, "408").inc()
                    return
                if body is None:
                    await _error(send, 413, "request_too_large")
                    REJECTIONS.labels("request_too_large", route).inc()
                    REQUESTS.labels(method, route, "413").inc()
                    return
                bounded_receive = _body_receiver(body)
                if path == "/mcp" and access is not None:
                    required_scope = _required_tool_scope(body)
                    if required_scope and required_scope not in access.scopes:
                        granted_scopes = sorted(set(access.scopes))
                        message = f"Additional authorization is required for scope {required_scope}"
                        challenge = _oauth_challenge(
                            self.resource_metadata_url,
                            required_scope,
                            "insufficient_scope",
                            message,
                        )
                        await _error(
                            send,
                            403,
                            "insufficient_scope",
                            [(b"www-authenticate", challenge.encode())],
                            message=message,
                            details={
                                "required_scopes": [required_scope],
                                "granted_scopes": granted_scopes,
                            },
                        )
                        REJECTIONS.labels("insufficient_scope", route).inc()
                        REQUESTS.labels(method, route, "403").inc()
                        return
            await self._observe(scope, bounded_receive, send, method, route)
        finally:
            if principal_context_token is not None:
                reset_current_oauth_principal(principal_context_token)
            if context_token is not None:
                reset_current_api_token(context_token)
            async with self._active_lock:
                self._active -= 1
                IN_FLIGHT.set(self._active)

    async def _observe(self, scope: dict[str, Any], receive: Callable[..., Awaitable[Any]], send: Callable[..., Awaitable[Any]], method: str, route: str) -> None:
        started = time.monotonic()
        status = 500
        response_messages: list[dict[str, Any]] = []
        response_bytes = 0
        oversized = False

        async def observed_send(message: dict[str, Any]) -> None:
            nonlocal status, response_bytes, oversized
            if message.get("type") == "http.response.start":
                status = int(message.get("status", 500))
            if message.get("type") == "http.response.body":
                response_bytes += len(message.get("body", b""))
                if response_bytes > self.max_response_bytes:
                    oversized = True
                    return
            if not oversized:
                response_messages.append(message)

        try:
            await self.app(scope, receive, observed_send)
            if oversized:
                status = 502
                REJECTIONS.labels("response_too_large", route).inc()
                await _error(send, status, "response_too_large")
            else:
                for message in response_messages:
                    await send(message)
        finally:
            REQUESTS.labels(method, route, str(status)).inc()
            DURATION.labels(method, route).observe(time.monotonic() - started)


def _route_label(path: str) -> str:
    if path == "/mcp":
        return "mcp"
    if path == "/healthz":
        return "health"
    if path == "/readyz":
        return "ready"
    if path == "/metrics":
        return "metrics"
    if path.startswith("/skills/"):
        return "skill"
    if path.startswith("/.well-known/oauth-protected-resource"):
        return "oauth_metadata"
    return "other"


def _oauth_challenge(metadata_url: str, scopes: str, error: str | None = None, error_description: str | None = None) -> str:
    values = [f'realm="joinlayer-mcp"', f'resource_metadata="{metadata_url}"', f'scope="{scopes}"']
    if error:
        values.append(f'error="{error}"')
    if error_description:
        values.append(f'error_description="{error_description}"')
    return "Bearer " + ", ".join(values)


def _required_tool_scope(body: bytes) -> str | None:
    try:
        message = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(message, dict) or message.get("method") != "tools/call":
        return None
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    name = params.get("name")
    return TOOL_SCOPES.get(name) if isinstance(name, str) else None


def _challenge_scope(headers: dict[str, str], default: str) -> str:
    if headers.get("mcp-method", "").strip().lower() == "tools/call":
        name = headers.get("mcp-name", "").strip()
        if required := TOOL_SCOPES.get(name):
            return required
    return default


def _headers(scope: dict[str, Any]) -> dict[str, str]:
    return {bytes(key).decode("latin-1").lower(): bytes(value).decode("latin-1") for key, value in scope.get("headers", [])}


def _client_ip(scope: dict[str, Any], headers: dict[str, str], trust_proxy: bool) -> str:
    if trust_proxy:
        forwarded = headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
        if forwarded:
            return forwarded
    client = scope.get("client")
    return str(client[0]) if isinstance(client, (tuple, list)) and client else "unknown"


def _transport_error(headers: dict[str, str], allowed_hosts: frozenset[str], allowed_origins: frozenset[str], check_origin: bool) -> str | None:
    if headers.get("host", "").strip().lower() not in allowed_hosts:
        return "invalid_host"
    origin = headers.get("origin", "").strip().lower()
    if check_origin and origin and origin not in allowed_origins:
        return "invalid_origin"
    return None


async def _bounded_body(receive: Callable[..., Awaitable[Any]], content_length: str | None, limit: int) -> bytes | None:
    if content_length:
        try:
            if int(content_length) > limit:
                return None
        except ValueError:
            return None
    body = bytearray()
    more = True
    while more:
        message = await receive()
        if message.get("type") == "http.disconnect":
            return bytes(body)
        chunk = message.get("body", b"")
        if len(body) + len(chunk) > limit:
            return None
        body.extend(chunk)
        more = bool(message.get("more_body", False))
    return bytes(body)


def _body_receiver(body: bytes) -> Callable[..., Awaitable[dict[str, Any]]]:
    sent = False

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return receive


async def _reject(send: Callable[..., Awaitable[Any]], status: int, code: str, retry_after: int) -> None:
    body = json.dumps({"error": {"code": code, "message": "Request capacity is temporarily unavailable"}}, separators=(",", ":")).encode("utf-8")
    await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"application/json"), (b"retry-after", str(retry_after).encode("ascii")), (b"cache-control", b"no-store")]})
    await send({"type": "http.response.body", "body": body})


async def _error(
    send: Callable[..., Awaitable[Any]],
    status: int,
    code: str,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
    *,
    message: str = "Request rejected by the MCP gateway",
    details: dict[str, Any] | None = None,
) -> None:
    error: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
    body = json.dumps({"error": error}, separators=(",", ":")).encode("utf-8")
    headers = [(b"content-type", b"application/json"), (b"cache-control", b"no-store")]
    headers.extend(extra_headers or [])
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})
