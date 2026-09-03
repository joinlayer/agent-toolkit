from __future__ import annotations

import json
import re
import io
import zipfile
import hashlib
import hmac
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from mcp.server import MCPServer
from mcp.server.request_state import RequestStateSecurity
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from .api import JoinLayerAPI, JoinLayerAPIError
from .auth import current_api_token, current_oauth_principal
from .config import Settings
from .guard import MCPToolAuthorizationMiddleware, SUPPORTED_SCOPES, TOOL_SCOPES
from .metrics import MCPToolMetricsMiddleware
from .models import (
    ActivityQuery,
    ActivityQueryInput,
    ConnectionSetupDraftInput,
    PipelineDraft,
    PipelineDraftInput,
    StartRunOptionsInput,
)

IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")

READ_ONLY = ToolAnnotations(
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=False,
)
READ_EXTERNAL = ToolAnnotations(
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=True,
)
WRITE_ADDITIVE = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=False,
)
WRITE_DESTRUCTIVE = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=True,
    idempotent_hint=True,
    open_world_hint=False,
)
WRITE_EXTERNAL = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=True,
    idempotent_hint=True,
    open_world_hint=True,
)
VALIDATE_EXTERNAL = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=False,
    open_world_hint=True,
)


def create_server(settings: Settings, api: JoinLayerAPI | None = None) -> MCPServer:
    api = api or JoinLayerAPI(settings)

    @asynccontextmanager
    async def lifespan(_):
        try:
            yield {"api": api, "settings": settings}
        finally:
            await api.close()

    mcp = MCPServer(
        "JoinLayer",
        title="JoinLayer Agentic Product Interface",
        description="Secure workspace-scoped data integration tools for delegated agents.",
        instructions=(
            "On the first JoinLayer request, call get_workspace_context and get_workspace_capacity, then use "
            "list_connections and list_pipelines to inspect current state before proposing a mutation. Report the "
            "authenticated workspace, identity, scopes, blockers, and whether state changed. Validate and preview "
            "every pipeline before starting it. Never ask for or transmit database, SSH, cloud, or API credentials "
            f"through MCP tool arguments. Setup and operating guide: {settings.docs_url}"
        ),
        website_url=settings.docs_url,
        version="2026-07-28",
        lifespan=lifespan,
        request_state_security=RequestStateSecurity(
            keys=list(settings.request_state_keys),
            ttl=600,
            audience=settings.public_url + "/mcp",
            bind_principal=lambda _: current_oauth_principal(),
        ),
        middleware=[
            MCPToolMetricsMiddleware(frozenset(TOOL_SCOPES)),
            MCPToolAuthorizationMiddleware(settings.public_url + "/.well-known/oauth-protected-resource/mcp"),
        ],
    )

    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "joinlayer-mcp"})

    @mcp.custom_route("/readyz", methods=["GET"])
    async def readyz(_: Request) -> JSONResponse:
        try:
            await api.verify_readiness()
        except JoinLayerAPIError as exc:
            return JSONResponse(
                {"status": "unavailable", "service": "joinlayer-mcp", "dependency": "joinlayer-api", "code": exc.code},
                status_code=503,
                headers={"Cache-Control": "no-store"},
            )
        return JSONResponse({"status": "ready", "service": "joinlayer-mcp", "dependency": "joinlayer-api"})

    async def protected_resource_metadata() -> JSONResponse:
        return JSONResponse(
            {
                "resource": settings.public_url + "/mcp",
                "authorization_servers": [settings.oauth_issuer],
                "bearer_methods_supported": ["header"],
                "scopes_supported": list(SUPPORTED_SCOPES),
                "resource_documentation": settings.docs_url,
            },
            headers={"Cache-Control": "public, max-age=300", "X-Content-Type-Options": "nosniff"},
        )

    @mcp.custom_route("/.well-known/oauth-protected-resource/mcp", methods=["GET"])
    async def oauth_protected_resource_path(_: Request) -> JSONResponse:
        return await protected_resource_metadata()

    @mcp.custom_route("/metrics", methods=["GET"])
    async def metrics(request: Request) -> Response:
        expected = settings.metrics_token_sha256
        token = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        actual = hashlib.sha256(token.encode("utf-8")).hexdigest() if token else ""
        if not expected:
            return Response(status_code=404)
        if not token or not hmac.compare_digest(actual, expected):
            return JSONResponse({"error": {"code": "unauthorized", "message": "invalid metrics token"}}, status_code=401, headers={"Cache-Control": "no-store"})
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST, headers={"Cache-Control": "no-store"})

    @mcp.custom_route("/skills/joinlayer-pipelines.zip", methods=["GET"])
    async def download_skill(_: Request) -> Response:
        try:
            archive = _skill_archive(Path(settings.skill_directory), settings.public_url + "/mcp", settings.docs_url)
        except (OSError, ValueError):
            return JSONResponse({"error": {"code": "skill_unavailable", "message": "JoinLayer skill package is unavailable"}}, status_code=503)
        return Response(
            archive,
            media_type="application/zip",
            headers={
                "Content-Disposition": 'attachment; filename="joinlayer-pipelines.zip"',
                "Cache-Control": "public, max-age=300",
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def call(
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        approval_id: str | None = None,
        tool_name: str,
    ) -> Any:
        token = current_api_token()
        try:
            return await api.request(
                token,
                method,
                path,
                body=body,
                params=params,
                idempotency_key=_idempotency_key(idempotency_key),
                approval_id=_resource_id(approval_id) if approval_id else None,
                tool_name=tool_name,
            )
        except JoinLayerAPIError as exc:
            details = f" Details: {json.dumps(exc.details, default=str)}" if exc.details is not None else ""
            raise ValueError(f"{exc.code}: {exc.message}.{details}") from exc

    @mcp.tool(annotations=READ_ONLY)
    async def get_workspace_context() -> dict[str, Any]:
        """Return identity, workspace, role, scopes, and whether workspace policy requires approvals for agent run starts/stops."""
        return await call("GET", "/me", tool_name="get_workspace_context")

    @mcp.tool(annotations=READ_ONLY)
    async def get_workspace_capacity() -> dict[str, Any]:
        """Return current workspace plan capacity, usage, remaining limits, and run blockers."""
        return await call("GET", "/billing/usage", tool_name="get_workspace_capacity")

    @mcp.tool(annotations=READ_ONLY)
    async def list_connector_types() -> dict[str, Any]:
        """List providers, roles, capabilities, and exact config_schema fields/defaults/options/secret flags. Call before setup."""
        return await call("GET", "/providers", tool_name="list_connector_types")

    @mcp.tool(annotations=READ_ONLY)
    async def list_connections(limit: int = 50, offset: int = 0) -> dict[str, Any]:
        """List workspace connections in a paginated object without stored credentials or encrypted configuration."""
        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError("limit must be 1..200 and offset must be non-negative")
        return await call(
            "GET",
            "/connections",
            params={"limit": limit, "offset": offset},
            tool_name="list_connections",
        )

    @mcp.tool(annotations=READ_ONLY)
    async def get_connection(connection_id: str) -> dict[str, Any]:
        """Get one connection's safe metadata and current configuration capabilities."""
        return await call("GET", f"/connections/{_resource_id(connection_id)}", tool_name="get_connection")

    @mcp.tool(annotations=WRITE_ADDITIVE)
    async def create_connection_setup(setup: ConnectionSetupDraftInput, idempotency_key: str) -> dict[str, Any]:
        """Create a browser setup from non-secret config_schema fields. Omit every secret-marked field, credential, and secret placeholder."""
        return await call(
            "POST",
            "/agent-setup-sessions",
            body=setup.model_dump(),
            idempotency_key=idempotency_key,
            tool_name="create_connection_setup",
        )

    @mcp.tool(annotations=READ_ONLY)
    async def get_connection_setup_status(setup_session_id: str) -> dict[str, Any]:
        """Poll a secure connection setup session; completed responses expose only the durable connection ID."""
        return await call("GET", f"/agent-setup-sessions/{_resource_id(setup_session_id)}", tool_name="get_connection_setup_status")

    @mcp.tool(annotations=READ_ONLY)
    async def list_connection_setups(limit: int = 50) -> dict[str, Any]:
        """List this identity's recent setup sessions to recover safely after an uncertain create response."""
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        return await call("GET", "/agent-setup-sessions", params={"limit": limit}, tool_name="list_connection_setups")

    @mcp.tool(annotations=WRITE_DESTRUCTIVE)
    async def cancel_connection_setup(setup_session_id: str, idempotency_key: str) -> dict[str, Any]:
        """Cancel an unused secure connection setup link."""
        return await call(
            "POST",
            f"/agent-setup-sessions/{_resource_id(setup_session_id)}/cancel",
            idempotency_key=idempotency_key,
            tool_name="cancel_connection_setup",
        )

    @mcp.tool(annotations=READ_EXTERNAL)
    async def test_connection(connection_id: str) -> dict[str, Any]:
        """Test one stored connection using server-side secrets. Kafka verifies broker/topic/Registry; ClickHouse verifies transport, credentials, and database access."""
        return await call(
            "POST",
            "/connections/test",
            body={"connection_id": _resource_id(connection_id)},
            tool_name="test_connection",
        )

    @mcp.tool(annotations=READ_EXTERNAL)
    async def discover_connection_schema(
        connection_id: str,
        schema: str | None = None,
        table: str | None = None,
    ) -> dict[str, Any]:
        """Discover relational schemas, ClickHouse databases, BigQuery datasets and tables, or sample Kafka fields with topic as table and no schema."""
        safe_id = _resource_id(connection_id)
        schema = schema.strip() if schema is not None else None
        table = table.strip() if table is not None else None
        if table and not schema:
            return await call(
                "POST",
                f"/connections/{safe_id}/discover-schema",
                body={"table": table},
                tool_name="discover_connection_schema",
            )
        if not schema:
            return await call(
                "GET",
                f"/connections/{safe_id}/catalog/schemas",
                tool_name="discover_connection_schema",
            )
        if not table:
            return await call(
                "GET",
                f"/connections/{safe_id}/catalog/tables",
                params={"schema": schema},
                tool_name="discover_connection_schema",
            )
        return await call(
            "POST",
            f"/connections/{safe_id}/discover-schema",
            body={"schema": schema, "table": table},
            tool_name="discover_connection_schema",
        )

    @mcp.tool(annotations=READ_ONLY)
    async def list_pipelines(limit: int = 50, offset: int = 0, state: str | None = None) -> dict[str, Any]:
        """List pipelines with one authoritative inventory_state, plus setup, runtime, validation, and matching state summaries."""
        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError("limit must be 1..200 and offset must be non-negative")
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if state:
            params["state"] = state
        result = await call("GET", "/pipelines", params=params, tool_name="list_pipelines")
        return _pipeline_inventory_contract(result)

    @mcp.tool(annotations=READ_ONLY)
    async def get_pipeline(pipeline_id: str) -> dict[str, Any]:
        """Get the complete safe pipeline definition, mappings, transforms, checks, setup state, and runtime summary."""
        result = await call("GET", f"/pipelines/{_resource_id(pipeline_id)}", tool_name="get_pipeline")
        return _pipeline_detail_contract(result)

    @mcp.tool(annotations=WRITE_ADDITIVE)
    async def create_pipeline_draft(pipeline: PipelineDraftInput, idempotency_key: str) -> dict[str, Any]:
        """Create a pipeline draft. Inspect connections first; validate and preview before any run."""
        result = await call("POST", "/pipelines", body=pipeline.model_dump(exclude_none=True), idempotency_key=idempotency_key, tool_name="create_pipeline_draft")
        return _pipeline_detail_contract(result)

    @mcp.tool(annotations=WRITE_DESTRUCTIVE)
    async def update_pipeline_draft(pipeline_id: str, pipeline: PipelineDraftInput, idempotency_key: str) -> dict[str, Any]:
        """Replace a pipeline draft contract with an explicitly supplied full configuration."""
        result = await call(
            "PUT",
            f"/pipelines/{_resource_id(pipeline_id)}",
            body=pipeline.model_dump(exclude_none=True),
            idempotency_key=idempotency_key,
            tool_name="update_pipeline_draft",
        )
        return _pipeline_detail_contract(result)

    @mcp.tool(annotations=VALIDATE_EXTERNAL)
    async def validate_pipeline(pipeline_id: str) -> dict[str, Any]:
        """Run non-destructive pipeline checks and return stable issues, sections, and remediation actions."""
        return await call("POST", f"/pipelines/{_resource_id(pipeline_id)}/validate", tool_name="validate_pipeline")

    @mcp.tool(annotations=READ_EXTERNAL)
    async def preview_pipeline(pipeline_id: str) -> dict[str, Any]:
        """Preview transformed and enriched target rows without starting the pipeline."""
        return await call("POST", f"/pipelines/{_resource_id(pipeline_id)}/preview", tool_name="preview_pipeline")

    @mcp.tool(annotations=WRITE_ADDITIVE)
    async def request_run_start_approval(
        pipeline_id: str,
        options: StartRunOptionsInput,
        idempotency_key: str,
        expires_in_minutes: int = 15,
    ) -> dict[str, Any]:
        """Request exact human approval when workspace policy requires it. If approval_required=false, start without approval_id; otherwise present approval_url and poll."""
        if not 5 <= expires_in_minutes <= 30:
            raise ValueError("expires_in_minutes must be between 5 and 30")
        safe_id = _resource_id(pipeline_id)
        operation = options.model_dump(exclude_none=True)
        return await call(
            "POST",
            "/agent-approvals",
            body={
                "action": "run.start",
                "pipeline_id": safe_id,
                "run_mode": options.mode,
                "operation": operation,
                "expires_in_minutes": expires_in_minutes,
            },
            idempotency_key=idempotency_key,
            tool_name="request_run_start_approval",
        )

    @mcp.tool(annotations=WRITE_ADDITIVE)
    async def request_run_stop_approval(
        pipeline_id: str,
        run_id: str,
        idempotency_key: str,
        expires_in_minutes: int = 15,
    ) -> dict[str, Any]:
        """Request human approval to stop one exact run when workspace policy requires it. Honor approval_required and return approval_url/next_action when required."""
        if not 5 <= expires_in_minutes <= 30:
            raise ValueError("expires_in_minutes must be between 5 and 30")
        return await call(
            "POST",
            "/agent-approvals",
            body={
                "action": "run.stop",
                "pipeline_id": _resource_id(pipeline_id),
                "run_id": _resource_id(run_id),
                "operation": {},
                "expires_in_minutes": expires_in_minutes,
            },
            idempotency_key=idempotency_key,
            tool_name="request_run_stop_approval",
        )

    @mcp.tool(annotations=READ_ONLY)
    async def list_agent_approvals(limit: int = 50) -> dict[str, Any]:
        """List this agent identity's pending and historical approval requests."""
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        return await call("GET", "/agent-approvals", params={"limit": limit}, tool_name="list_agent_approvals")

    @mcp.tool(annotations=WRITE_DESTRUCTIVE)
    async def cancel_agent_approval(approval_id: str, idempotency_key: str) -> dict[str, Any]:
        """Cancel one pending approval request owned by this agent identity."""
        return await call(
            "POST",
            f"/agent-approvals/{_resource_id(approval_id)}/cancel",
            idempotency_key=idempotency_key,
            tool_name="cancel_agent_approval",
        )

    @mcp.tool(annotations=WRITE_EXTERNAL)
    async def start_pipeline(
        pipeline_id: str,
        options: StartRunOptionsInput,
        idempotency_key: str,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        """Start or resume a validated pipeline using an approved request or matching automation policy."""
        return await call(
            "POST",
            f"/pipelines/{_resource_id(pipeline_id)}/runs",
            body=options.model_dump(exclude_none=True),
            idempotency_key=idempotency_key,
            approval_id=approval_id,
            tool_name="start_pipeline",
        )

    @mcp.tool(annotations=READ_ONLY)
    async def list_pipeline_runs(pipeline_id: str) -> dict[str, Any]:
        """List recent runs and recovery state for a pipeline."""
        return await call("GET", f"/pipelines/{_resource_id(pipeline_id)}/runs", tool_name="list_pipeline_runs")

    @mcp.tool(annotations=READ_ONLY)
    async def get_run(run_id: str) -> dict[str, Any]:
        """Get run progress, counters, worker lease, recovery state, and curated failure details."""
        return await call("GET", f"/runs/{_resource_id(run_id)}", tool_name="get_run")

    @mcp.tool(annotations=WRITE_EXTERNAL)
    async def stop_run(run_id: str, idempotency_key: str, approval_id: str | None = None) -> dict[str, Any]:
        """Stop an active run using an approved request or matching automation policy."""
        return await call(
            "POST",
            f"/runs/{_resource_id(run_id)}/stop",
            idempotency_key=idempotency_key,
            approval_id=approval_id,
            tool_name="stop_run",
        )

    @mcp.tool(annotations=READ_ONLY)
    async def list_activity(query: ActivityQueryInput = ActivityQuery()) -> dict[str, Any]:
        """List workspace run activity using explicit status, mode, period, and pipeline filters."""
        params = query.model_dump(exclude_none=True)
        params["include_usage"] = "true"
        params["include_summary"] = "true"
        return await call("GET", "/activity/runs", params=params, tool_name="list_activity")

    @mcp.tool(annotations=READ_ONLY)
    async def diagnose_run_failure(run_id: str, limit: int = 100) -> dict[str, Any]:
        """Return run state, structured errors, events, and dead-letter summaries for diagnosis."""
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        safe_id = _resource_id(run_id)
        run = await call("GET", f"/runs/{safe_id}", tool_name="diagnose_run_failure")
        errors = await call("GET", f"/runs/{safe_id}/errors", params={"limit": limit}, tool_name="diagnose_run_failure")
        events = await call("GET", f"/runs/{safe_id}/events", params={"limit": limit}, tool_name="diagnose_run_failure")
        dead_letters = await call("GET", f"/runs/{safe_id}/dead-letters", params={"limit": min(limit, 100)}, tool_name="diagnose_run_failure")
        return {"run": run, "errors": errors, "events": events, "dead_letters": dead_letters}

    @mcp.tool(annotations=READ_ONLY)
    async def get_usage_report(from_date: str, to_date: str) -> dict[str, Any]:
        """Return workspace and per-pipeline usage attribution for an ISO-8601 date range."""
        return await call("GET", "/usage/report", params={"from": from_date, "to": to_date}, tool_name="get_usage_report")

    @mcp.resource("joinlayer://concepts/execution-model")
    def execution_model() -> str:
        """Explain the safe JoinLayer pipeline lifecycle."""
        return (
            "A JoinLayer pipeline connects a source and target, maps fields, optionally filters, computes, "
            "and enriches rows, then writes them using an explicit target strategy. The safe lifecycle is: "
            "inspect connections; create or update a draft; validate; preview; review capacity; start or resume; "
            "monitor the run. Realtime-with-backfill keeps CDC live from the captured boundary while spare workers load coordinated history."
        )

    @mcp.resource("joinlayer://guides/first-session")
    def first_session_guide() -> str:
        """Return the mandatory discovery sequence for a new JoinLayer agent session."""
        return (
            "First call get_workspace_context and verify the workspace, identity, role, and scopes. Then call "
            "get_workspace_capacity, list_connections, and list_pipelines. For a read-only first check, summarize "
            "those results and make no mutations. Never substitute guessed IDs, direct database access, or requests "
            f"for credentials. Full guide: {settings.docs_url}"
        )

    @mcp.resource("joinlayer://schemas/pipeline-draft")
    def pipeline_draft_schema() -> str:
        """Return the machine-readable pipeline draft input schema."""
        return json.dumps(PipelineDraft.model_json_schema(), indent=2, sort_keys=True)

    @mcp.prompt()
    def create_enriched_pipeline(goal: str) -> str:
        """Guide an agent through creating an enriched pipeline safely."""
        return (
            f"Goal: {goal}\n\n"
            "Use JoinLayer tools in this order: inspect workspace capacity and connector capabilities; list and "
            "inspect source, target, and lookup connections; discover schemas; draft mappings and enrichment; "
            "create the draft with a stable idempotency key; validate; preview representative rows; explain any "
            "warnings and capacity impact; ask the user before starting realtime or scheduled execution. Never ask "
            "the user to paste credentials into chat."
        )

    @mcp.prompt()
    def diagnose_failed_run(run_id: str) -> str:
        """Guide an agent through diagnosing a failed pipeline run."""
        return (
            f"Diagnose JoinLayer run {run_id}. Fetch structured diagnostics, separate configuration/data failures "
            "from infrastructure recovery, explain the user-visible impact, and propose the least destructive fix. "
            "Do not clear checkpoints or restart from current unless the user explicitly approves data-loss risk."
        )

    return mcp


def create_streamable_http_app(server: MCPServer, settings: Settings):
    return server.streamable_http_app(
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
        max_request_body_size=settings.max_request_bytes,
        host=settings.host,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=list(settings.allowed_hosts),
            allowed_origins=list(settings.allowed_origins),
        ),
    )


def _pipeline_inventory_contract(payload: Any) -> dict[str, Any]:
    """Return only the computed, customer-facing pipeline lifecycle.

    The API still carries a historical persisted ``status`` for internal
    compatibility paths. It is not the inventory lifecycle and can remain
    ``DRAFT`` after a definition is complete, so exposing both is ambiguous.
    """
    if not isinstance(payload, dict):
        raise ValueError("invalid_pipeline_inventory: JoinLayer API returned a non-object pipeline inventory")
    pipelines = payload.get("pipelines")
    if not isinstance(pipelines, list):
        raise ValueError("invalid_pipeline_inventory: JoinLayer API response is missing pipelines")
    normalized = dict(payload)
    normalized["pipelines"] = []
    for item in pipelines:
        normalized["pipelines"].append(_pipeline_detail_contract(item))
    return normalized


def _pipeline_detail_contract(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("invalid_pipeline_response: JoinLayer API returned a non-object pipeline")
    normalized = dict(payload)
    normalized.pop("status", None)
    return normalized


def _skill_archive(directory: Path, mcp_url: str | None = None, docs_url: str | None = None) -> bytes:
    required = [
        "SKILL.md",
        "agents/openai.yaml",
        "references/execution-model.md",
        "references/data-shaping.md",
        "references/operations.md",
        "references/product-capabilities.md",
        "references/connector-contracts.md",
    ]
    if not directory.is_dir():
        raise ValueError("skill directory does not exist")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in required:
            source = directory / relative
            if not source.is_file() or source.is_symlink():
                raise ValueError(f"missing skill file: {relative}")
            if relative in {"SKILL.md", "agents/openai.yaml"}:
                content = source.read_text(encoding="utf-8")
                if mcp_url:
                    content = content.replace("https://replace-with-your-joinlayer-host.example/mcp", mcp_url)
                if docs_url:
                    content = content.replace(
                        "https://replace-with-your-joinlayer-docs.example/agent-integrations", docs_url
                    )
                archive.writestr(f"joinlayer-pipelines/{relative}", content)
            else:
                archive.write(source, f"joinlayer-pipelines/{relative}")
    return output.getvalue()


def _resource_id(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{2,127}", value):
        raise ValueError("resource ID has an invalid format")
    return value


def _idempotency_key(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not IDEMPOTENCY_KEY.fullmatch(value):
        raise ValueError("idempotency_key must be 8..128 safe characters")
    return value
