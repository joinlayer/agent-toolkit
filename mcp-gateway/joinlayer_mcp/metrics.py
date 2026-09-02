from __future__ import annotations

import time
from typing import Any

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from prometheus_client import Counter, Gauge, Histogram


REQUESTS = Counter("joinlayer_mcp_requests_total", "MCP gateway HTTP requests", ["method", "route", "status"])
DURATION = Histogram("joinlayer_mcp_request_duration_seconds", "MCP gateway HTTP request latency", ["method", "route"])
REJECTIONS = Counter("joinlayer_mcp_rejections_total", "MCP gateway rejected requests", ["reason", "route"])
AUTHENTICATIONS = Counter(
    "joinlayer_mcp_authentications_total",
    "MCP delegated bearer verification outcomes",
    ["outcome"],
)
AUTH_DURATION = Histogram(
    "joinlayer_mcp_authentication_duration_seconds",
    "MCP delegated bearer verification latency",
    ["outcome"],
)
IN_FLIGHT = Gauge("joinlayer_mcp_in_flight_requests", "MCP gateway HTTP requests currently executing")
CONCURRENCY_LIMIT = Gauge("joinlayer_mcp_concurrency_limit", "Configured MCP gateway concurrent request limit")
RATE_LIMIT_KEYS = Gauge(
    "joinlayer_mcp_rate_limit_keys",
    "Active MCP gateway token buckets by bounded registry",
    ["registry"],
)
RATE_LIMIT_KEYS_LIMIT = Gauge(
    "joinlayer_mcp_rate_limit_keys_limit",
    "Configured maximum MCP gateway token buckets by bounded registry",
    ["registry"],
)
TOOL_CALLS = Counter(
    "joinlayer_mcp_tool_calls_total",
    "MCP tool calls by bounded tool name and outcome",
    ["tool", "outcome"],
)
TOOL_DURATION = Histogram(
    "joinlayer_mcp_tool_call_duration_seconds",
    "MCP tool call latency by bounded tool name and outcome",
    ["tool", "outcome"],
)


class MCPToolMetricsMiddleware:
    """Observe one complete MCP tool invocation, including SDK-level errors.

    HTTP transport status is commonly 200 for a valid JSON-RPC response whose
    tool result has ``isError=true``. Measuring at this boundary keeps those
    product failures visible without parsing request/response bodies or adding
    tenant, resource, credential, or user-controlled labels.
    """

    def __init__(self, tool_names: frozenset[str]) -> None:
        self._tool_names = tool_names

    async def __call__(self, ctx: ServerRequestContext[Any, Any], call_next: CallNext) -> HandlerResult:
        if ctx.method != "tools/call":
            return await call_next(ctx)

        requested_name = ctx.params.get("name") if isinstance(ctx.params, dict) else None
        tool = requested_name if isinstance(requested_name, str) and requested_name in self._tool_names else "unknown"
        started = time.monotonic()
        outcome = "protocol_error"
        try:
            result = await call_next(ctx)
            outcome = "tool_error" if _is_tool_error(result) else "success"
            return result
        finally:
            TOOL_CALLS.labels(tool, outcome).inc()
            TOOL_DURATION.labels(tool, outcome).observe(time.monotonic() - started)


def _is_tool_error(result: HandlerResult) -> bool:
    if isinstance(result, dict):
        return result.get("isError") is True
    return getattr(result, "is_error", False) is True
