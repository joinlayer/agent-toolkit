from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


def _env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} must not be empty")
    return value


def _positive_float(name: str, default: str) -> float:
    try:
        value = float(_env(name, default))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_int(name: str, default: str) -> int:
    try:
        value = int(_env(name, default))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _boolean(name: str, default: str = "false") -> bool:
    value = _env(name, default).lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _optional_sha256(name: str) -> str:
    value = os.getenv(name, "").strip().lower()
    if value and (len(value) != 64 or any(character not in "0123456789abcdef" for character in value)):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return value


def _secret(name: str, default: str = "") -> str:
    file_path = os.getenv(f"{name}_FILE", "").strip()
    if file_path:
        try:
            value = Path(file_path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ValueError(f"{name}_FILE could not be read") from exc
    else:
        value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} or {name}_FILE must not be empty")
    return value


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    public_url: str
    oauth_issuer: str
    gateway_token: str
    request_state_keys: tuple[str, ...]
    host: str
    port: int
    api_timeout_seconds: float
    max_request_bytes: int
    max_response_bytes: int
    environment: str
    skill_directory: str
    docs_url: str = "https://docs.joinlayer.app/agent-integrations"
    global_rate_limit_rps: float = 100
    global_rate_limit_burst: int = 200
    client_rate_limit_rps: float = 10
    client_rate_limit_burst: int = 30
    anonymous_rate_limit_rps: float = 2
    anonymous_rate_limit_burst: int = 10
    auth_attempt_rate_limit_rps: float = 10
    auth_attempt_rate_limit_burst: int = 30
    rate_limit_max_keys: int = 10000
    max_concurrent_requests: int = 100
    request_body_timeout_seconds: float = 15
    trust_proxy_headers: bool = False
    metrics_token_sha256: str = ""
    allowed_hosts: tuple[str, ...] = ()
    allowed_origins: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "Settings":
        api_base_url = _validated_http_url("MCP_API_BASE_URL", _env("MCP_API_BASE_URL", "http://api:8080"))
        public_url = _validated_http_url("MCP_PUBLIC_URL", _env("MCP_PUBLIC_URL", "http://127.0.0.1:8092"))
        public = urlparse(public_url)
        if public.path not in {"", "/"}:
            raise ValueError("MCP_PUBLIC_URL must not contain a path")
        try:
            port = int(_env("MCP_PORT", "8092"))
            max_request_bytes = int(_env("MCP_MAX_REQUEST_BYTES", str(1024 * 1024)))
            max_response_bytes = int(_env("MCP_MAX_RESPONSE_BYTES", str(8 * 1024 * 1024)))
        except ValueError as exc:
            raise ValueError("MCP_PORT, MCP_MAX_REQUEST_BYTES, and MCP_MAX_RESPONSE_BYTES must be integers") from exc
        if not 1 <= port <= 65535:
            raise ValueError("MCP_PORT must be between 1 and 65535")
        if max_request_bytes < 1024 or max_response_bytes < 1024:
            raise ValueError("MCP request and response byte limits must be at least 1024")
        if _env("DATAFLOW_ENV", "development").lower() in {"production", "prodlike", "demo", "staging"}:
            if public.scheme != "https" and public.hostname not in {"127.0.0.1", "localhost", "::1"}:
                raise ValueError("MCP_PUBLIC_URL must use HTTPS outside local development")
        default_host = public.netloc
        default_origin = f"{public.scheme}://{public.netloc}"
        environment = _env("DATAFLOW_ENV", "development")
        oauth_issuer = _validated_http_url(
            "MCP_OAUTH_ISSUER",
            _env("MCP_OAUTH_ISSUER", "http://127.0.0.1:5173"),
        ).rstrip("/")
        issuer = urlparse(oauth_issuer)
        if issuer.path not in {"", "/"}:
            raise ValueError("MCP_OAUTH_ISSUER must not contain a path")
        if environment.lower() in {"production", "prodlike", "demo", "staging"}:
            if issuer.scheme != "https" and issuer.hostname not in {"127.0.0.1", "localhost", "::1"}:
                raise ValueError("MCP_OAUTH_ISSUER must use HTTPS outside local development")
        gateway_default = "" if environment.lower() in {"production", "prodlike", "demo", "staging"} else "development-mcp-gateway-token-change-me"
        request_state_default = "" if environment.lower() in {"production", "prodlike", "demo", "staging"} else "development-mcp-request-state-key-change-me"
        request_state_keys = tuple(
            dict.fromkeys(
                part.strip()
                for part in _secret("MCP_REQUEST_STATE_KEYS", request_state_default).replace("\n", ",").split(",")
                if part.strip()
            )
        )
        if not request_state_keys or any(len(key.encode("utf-8")) < 32 for key in request_state_keys):
            raise ValueError("every MCP_REQUEST_STATE_KEYS entry must contain at least 32 bytes")
        gateway_token = _secret("MCP_GATEWAY_TOKEN", gateway_default)
        if len(gateway_token.encode("utf-8")) < 32:
            raise ValueError("MCP_GATEWAY_TOKEN must contain at least 32 bytes")
        return cls(
            api_base_url=api_base_url.rstrip("/"),
            public_url=public_url.rstrip("/"),
            oauth_issuer=oauth_issuer,
            gateway_token=gateway_token,
            request_state_keys=request_state_keys,
            host=_env("MCP_HOST", "0.0.0.0"),
            port=port,
            api_timeout_seconds=_positive_float("MCP_API_TIMEOUT_SECONDS", "20"),
            max_request_bytes=max_request_bytes,
            max_response_bytes=max_response_bytes,
            environment=environment,
            skill_directory=_env("MCP_SKILL_DIRECTORY", "/workspace/skills/joinlayer-pipelines"),
            docs_url=_validated_http_url(
                "MCP_DOCS_URL",
                _env("MCP_DOCS_URL", "https://docs.joinlayer.app/agent-integrations"),
            ),
            global_rate_limit_rps=_positive_float("MCP_GLOBAL_RATE_LIMIT_RPS", "100"),
            global_rate_limit_burst=_positive_int("MCP_GLOBAL_RATE_LIMIT_BURST", "200"),
            client_rate_limit_rps=_positive_float("MCP_CLIENT_RATE_LIMIT_RPS", "10"),
            client_rate_limit_burst=_positive_int("MCP_CLIENT_RATE_LIMIT_BURST", "30"),
            anonymous_rate_limit_rps=_positive_float("MCP_ANONYMOUS_RATE_LIMIT_RPS", "2"),
            anonymous_rate_limit_burst=_positive_int("MCP_ANONYMOUS_RATE_LIMIT_BURST", "10"),
            auth_attempt_rate_limit_rps=_positive_float("MCP_AUTH_ATTEMPT_RATE_LIMIT_RPS", "10"),
            auth_attempt_rate_limit_burst=_positive_int("MCP_AUTH_ATTEMPT_RATE_LIMIT_BURST", "30"),
            rate_limit_max_keys=_positive_int("MCP_RATE_LIMIT_MAX_KEYS", "10000"),
            max_concurrent_requests=_positive_int("MCP_MAX_CONCURRENT_REQUESTS", "100"),
            request_body_timeout_seconds=_positive_float("MCP_REQUEST_BODY_TIMEOUT_SECONDS", "15"),
            trust_proxy_headers=_boolean("MCP_TRUST_PROXY_HEADERS"),
            metrics_token_sha256=_optional_sha256("DATAFLOW_METRICS_TOKEN_SHA256"),
            allowed_hosts=_csv("MCP_ALLOWED_HOSTS", default_host),
            allowed_origins=_csv("MCP_ALLOWED_ORIGINS", default_origin),
        )


def _validated_http_url(name: str, value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError(f"{name} must be an http(s) URL without embedded credentials")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{name} must not contain query parameters or a fragment")
    return value


def _csv(name: str, default: str) -> tuple[str, ...]:
    values = tuple(dict.fromkeys(part.strip() for part in os.getenv(name, default).split(",") if part.strip()))
    if not values:
        raise ValueError(f"{name} must contain at least one value")
    return values
