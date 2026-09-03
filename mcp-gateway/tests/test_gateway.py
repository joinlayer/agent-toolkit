from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import re
import tempfile
import time
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from jsonschema import Draft202012Validator
from mcp.server.auth.provider import AccessToken
from pydantic import ValidationError
from prometheus_client import REGISTRY
from starlette.testclient import TestClient

from joinlayer_mcp.api import JoinLayerAPI, JoinLayerAPIError
from joinlayer_mcp.auth import OAuthTokenVerifier
from joinlayer_mcp.config import Settings
from joinlayer_mcp.guard import SUPPORTED_SCOPES, TOOL_SCOPES, GatewayGuard, TokenBucketRegistry, _bounded_body, _challenge_scope, _transport_error, _uses_openai_tool_level_authorization
from joinlayer_mcp.metrics import IN_FLIGHT
from joinlayer_mcp.models import ConnectionSetupDraft, PipelineDraft, StartRunOptions
from joinlayer_mcp.server import _idempotency_key, _pipeline_detail_contract, _pipeline_inventory_contract, _resource_id, _skill_archive, create_server, create_streamable_http_app


class FakeAPI:
    def __init__(self, principal: dict | None = None, error: Exception | None = None) -> None:
        self.principal = principal
        self.error = error
        self.request = AsyncMock(side_effect=self._request)
        self.introspect = AsyncMock(side_effect=self._introspect)
        self.close = AsyncMock()

    async def _request(self, *_args, **_kwargs):
        if self.error:
            raise self.error
        return self.principal

    async def _introspect(self, *_args, **_kwargs):
        if self.error:
            raise self.error
        return self.principal


class ConnectionListAPI(FakeAPI):
    async def _request(self, _token, method, path, **kwargs):
        if method == "GET" and path == "/connections":
            params = kwargs.get("params")
            if params == {"limit": 50, "offset": 0}:
                return {
                    "connections": [{"id": "conn_demo", "name": "Demo source", "type": "postgres_cdc"}],
                    "pagination": {"limit": 50, "offset": 0, "total": 1, "has_more": False},
                    "summary": {"total": 1, "sources": 1, "targets": 0, "types": {"postgres_cdc": 1}},
                }
            return [{"id": "conn_demo"}]
        return await super()._request(_token, method, path, **kwargs)


class ProviderListAPI(FakeAPI):
    async def _request(self, _token, method, path, **kwargs):
        if method == "GET" and path == "/providers":
            return {
                "sources": [{
                    "kind": "kafka_source",
                    "role": "source",
                    "capabilities": ["connection_test", "schema_discovery", "stream_read"],
                    "config_schema": [
                        {"name": "value_format", "label": "Message Format", "kind": "select", "secret": False, "options": [{"value": "json_object"}, {"value": "avro"}]},
                        {"name": "avro_schema_source", "label": "Schema Source", "kind": "select", "secret": False, "options": [{"value": "inline"}, {"value": "schema_registry"}]},
                        {"name": "schema_registry_url", "label": "Schema Registry URL", "kind": "string", "secret": False},
                        {"name": "schema_registry_password", "label": "Registry Password", "kind": "password", "secret": True},
                    ],
                }],
                "targets": [{
                    "kind": "bigquery",
                    "role": "target",
                    "capabilities": ["connection_test", "schema_discovery", "batch_write", "stream_write"],
                    "config_schema": [
                        {"name": "dataset", "label": "Dataset", "kind": "string", "secret": False},
                        {"name": "table", "label": "Table", "kind": "string", "secret": False},
                        {"name": "write_mode", "label": "Write Method", "kind": "select", "secret": False, "options": [{"value": "storage_write"}, {"value": "insert_all"}, {"value": "batch_load"}]},
                        {"name": "service_account_json", "label": "Service Account Key (JSON)", "kind": "textarea", "secret": True},
                    ],
                }],
            }
        return await super()._request(_token, method, path, **kwargs)


class ConnectionSchemaAPI(FakeAPI):
    async def _request(self, _token, method, path, **kwargs):
        if method == "GET" and path == "/connections/conn_demo/catalog/schemas":
            return {"schemas": ["public"]}
        if method == "GET" and path == "/connections/conn_demo/catalog/tables":
            return {"tables": [{"schema": "public", "name": "orders"}]}
        if method == "POST" and path == "/connections/conn_demo/discover-schema":
            return {"fields": [{"name": "id", "type": "int8", "primary_key": True}]}
        return await super()._request(_token, method, path, **kwargs)


class ConnectionTestAPI(FakeAPI):
    async def _request(self, _token, method, path, **kwargs):
        if method == "POST" and path == "/connections/test":
            return {
                "ok": True,
                "message": 'Kafka topic "events" is reachable with 3 partitions. Schema Registry subject "events-value" version "latest" was loaded and parsed as Avro.',
                "latency_ms": 42,
                "checked_at": "2026-08-28T12:00:00Z",
            }
        return await super()._request(_token, method, path, **kwargs)


class PipelineListAPI(FakeAPI):
    async def _request(self, _token, method, path, **kwargs):
        if method == "GET" and path == "/pipelines":
            return {
                "pipelines": [
                    {"id": "pipe_operational", "status": "DRAFT", "inventory_state": "operational"},
                    {"id": "pipe_ready", "status": "DRAFT", "inventory_state": "ready"},
                ],
                "pagination": {"limit": 50, "offset": 0, "total": 2, "has_more": False},
                "summary": {"all": 2, "operational": 1, "ready": 1, "attention": 0, "draft": 0},
                "overview_summary": {"active": 1, "realtime": 0, "failed_24h": 0, "rows_written_24h": 0},
            }
        return await super()._request(_token, method, path, **kwargs)


class PipelineDraftAPI(FakeAPI):
    async def _request(self, _token, method, path, **kwargs):
        if method == "POST" and path == "/pipelines":
            return {"id": "pipe_demo", "org_id": "org-demo", "status": "DRAFT", **kwargs["body"]}
        return await super()._request(_token, method, path, **kwargs)


def settings(**overrides) -> Settings:
    values = {
        "api_base_url": "http://api:8080",
        "public_url": "https://mcp.example.com",
        "oauth_issuer": "https://joinlayer.example.com",
        "gateway_token": "gateway-token-value-with-at-least-32-chars",
        "request_state_keys": ("request-state-key-value-with-at-least-32-chars",),
        "host": "0.0.0.0",
        "port": 8092,
        "api_timeout_seconds": 5,
        "max_request_bytes": 1024,
        "max_response_bytes": 1024,
        "environment": "test",
        "skill_directory": "/workspace/skills/joinlayer-pipelines",
        "allowed_hosts": ("mcp.example.com",),
        "allowed_origins": ("https://mcp.example.com",),
    }
    values.update(overrides)
    return Settings(**values)


class OAuthTokenVerifierTests(unittest.IsolatedAsyncioTestCase):
    async def test_accepts_only_active_audience_bound_oauth_principal(self) -> None:
        verifier = OAuthTokenVerifier(FakeAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "pipelines:read",
            "expires_at": int(time.time()) + 300,
        }), "https://mcp.example.com/mcp")
        access = await verifier.verify_token("jlo_at_" + "a" * 64)
        self.assertIsNotNone(access)
        assert access is not None
        self.assertEqual(access.client_id, "https://client.example/client.json")
        self.assertEqual(access.scopes, ["pipelines:read"])
        self.assertEqual(access.subject, "user_demo")
        verification = await verifier.verify("jlo_at_" + "a" * 64)
        assert verification is not None
        self.assertIn("ogr_demo", verification.principal_key)
        self.assertNotIn("jlo_at_", verification.principal_key)

        wrong_audience = OAuthTokenVerifier(FakeAPI({
            "active": True,
            "resource": "https://api.example.com",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "expires_at": int(time.time()) + 300,
        }), "https://mcp.example.com/mcp")
        self.assertIsNone(await wrong_audience.verify_token("jlo_at_" + "b" * 64))

    async def test_rejects_invalid_and_revoked_tokens_but_surfaces_api_outage(self) -> None:
        api = FakeAPI(error=JoinLayerAPIError(401, "unauthorized", "invalid bearer token"))
        self.assertIsNone(await OAuthTokenVerifier(api, "https://mcp.example.com/mcp").verify_token("jlo_at_" + "a" * 64))
        with self.assertRaises(JoinLayerAPIError):
            await OAuthTokenVerifier(FakeAPI(error=JoinLayerAPIError(503, "unavailable", "API unavailable")), "https://mcp.example.com/mcp").verify_token(
                "jlo_at_" + "a" * 64
            )


class ContractTests(unittest.TestCase):
    def test_pipeline_contract_is_strict(self) -> None:
        pipeline = PipelineDraft(
            name="Orders enriched",
            source_connection_id="conn_source",
            target_connection_id="conn_target",
            target_write_strategy="upsert",
            target_primary_key_field="id",
            max_rows_per_second=1_000,
            throttle_policy="delay",
            error_policy="stop",
        )
        self.assertEqual(pipeline.run_mode, "one_time")
        self.assertEqual(pipeline.target_write_strategy, "upsert")
        self.assertEqual(pipeline.max_rows_per_second, 1_000)
        with self.assertRaises(ValidationError):
            PipelineDraft(
                name="Non-canonical",
                source_connection_id="conn_source",
                target_connection_id="conn_target",
                target_write_strategy="merge",
            )
        with self.assertRaises(ValidationError):
            PipelineDraft(
                name="Bad",
                source_connection_id="conn_source",
                target_connection_id="conn_target",
                target_write_strategy="insert",
                invented=True,
            )

    def test_bigquery_target_layout_contract_is_strict(self) -> None:
        pipeline = PipelineDraft(
            name="Partitioned orders",
            source_connection_id="conn_source",
            target_connection_id="conn_bigquery",
            target_write_strategy="connector_default",
            target_schema_options={
                "bigquery": {
                    "partitioning": {
                        "mode": "field",
                        "field": "event_at",
                        "granularity": "DAY",
                        "require_partition_filter": True,
                    },
                    "clustering_fields": ["customer_id", "region"],
                }
            },
        )
        self.assertEqual(
            pipeline.model_dump(exclude_none=True)["target_schema_options"]["bigquery"]["partitioning"]["field"],
            "event_at",
        )
        invalid_options = [
            {"bigquery": {"partitioning": {"mode": "field"}}},
            {"bigquery": {"partitioning": {"mode": "none", "field": "event_at"}}},
            {"bigquery": {"partitioning": {"mode": "none", "require_partition_filter": True}}},
            {"bigquery": {"partitioning": {"mode": "field", "field": "DATE(event_at)"}}},
            {"bigquery": {"clustering_fields": ["a", "b", "c", "d", "e"]}},
            {"bigquery": {"clustering_fields": ["customer_id", "customer_id"]}},
            {"bigquery": {"invented": True}},
            {"postgres": {}},
        ]
        for options in invalid_options:
            with self.subTest(options=options), self.assertRaises(ValidationError):
                PipelineDraft(
                    name="Invalid BigQuery layout",
                    source_connection_id="conn_source",
                    target_connection_id="conn_bigquery",
                    target_write_strategy="connector_default",
                    target_schema_options=options,
                )

    def test_pipeline_transform_contract_is_strict_and_matches_runtime(self) -> None:
        transforms = [
            {"type": "cast", "rules": [{"field": "amount", "type": "float64"}]},
            {
                "type": "derive",
                "derive_rules": [{"source_field": "amount", "target_field": "amount_taxed", "function": "multiply", "value": 1.2}],
            },
            {"type": "derive", "derive_rules": [{"source_field": "amount_taxed", "target_field": "amount_rounded", "function": "round", "places": 2}]},
            {"type": "filter", "field": "status", "op": "in", "values": ["paid", "shipped"]},
            {
                "type": "enrich",
                "source_field": "customer_id",
                "target_fields": ["customer_tier"],
                "lookup_connection_id": "conn_lookup",
                "lookup_table": "customers",
                "lookup_key_field": "id",
                "lookup_cache_mode": "materialized",
                "lookup_cache_ttl_seconds": 600,
            },
        ]
        pipeline = PipelineDraft(
            name="Orders enriched",
            source_connection_id="conn_source",
            target_connection_id="conn_target",
            target_write_strategy="upsert",
            transforms=transforms,
            field_mappings=[{"source_field": "id", "target_field": "order_id", "position": 0, "transforms": []}],
        )
        dumped = pipeline.model_dump(exclude_none=True)
        self.assertEqual(dumped["transforms"][0]["rules"][0]["type"], "float64")
        self.assertEqual(dumped["field_mappings"][0]["transforms"], ())

        invalid_transforms = [
            {"type": "unknown"},
            {"type": "cast", "rules": [{"field": "amount", "type": "uuid"}]},
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "total", "function": "multiply"}]},
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "total", "function": "multiply", "value": {"factor": 2}}]},
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "total", "function": "lower", "value": "ignored"}]},
            {
                "type": "derive",
                "derive_rules": [
                    {"source_field": "amount", "target_field": "total", "function": "round"},
                    {"source_field": "tax", "target_field": "total", "function": "round"},
                ],
            },
            {"type": "filter", "field": "status", "op": "eq", "values": ["paid", "shipped"]},
            {"type": "filter", "field": "status", "op": "in", "values": [" "]},
            {"type": "enrich", "source_field": "customer_id", "target_fields": ["tier"]},
            {
                "type": "cast",
                "rules": [{"field": "amount", "type": "float64"}, {"field": "amount", "type": "string"}],
            },
            {"type": "mapping", "fields": [{"from": "id", "to": "order_id"}]},
        ]
        for transform in invalid_transforms:
            with self.subTest(transform=transform), self.assertRaises(ValidationError):
                PipelineDraft(
                    name="Invalid transform",
                    source_connection_id="conn_source",
                    target_connection_id="conn_target",
                    target_write_strategy="insert",
                    transforms=[transform],
                )
        with self.assertRaises(ValidationError):
            PipelineDraft(
                name="Ignored per-field transform",
                source_connection_id="conn_source",
                target_connection_id="conn_target",
                target_write_strategy="insert",
                field_mappings=[{
                    "source_field": "amount",
                    "target_field": "amount",
                    "position": 0,
                    "transforms": [{"type": "cast", "rules": [{"field": "amount", "type": "float64"}]}],
                }],
            )

    def test_start_contract_cannot_discard_checkpoint(self) -> None:
        with self.assertRaises(ValidationError):
            StartRunOptions(mode="realtime", checkpoint_mode="restart")

    def test_start_contract_matches_api_retry_limits(self) -> None:
        standard = StartRunOptions(mode="realtime")
        self.assertEqual((standard.max_retries, standard.retry_backoff_ms), (3, 1000))
        with self.assertRaises(ValidationError):
            StartRunOptions(mode="realtime", max_retries=6)
        with self.assertRaises(ValidationError):
            StartRunOptions(mode="realtime", retry_backoff_ms=30001)

    def test_connection_setup_contract_rejects_unknown_and_long_lived_sessions(self) -> None:
        setup = ConnectionSetupDraft(name="Orders source", connection_type="postgres_cdc", config_template={"host": "db.internal"})
        self.assertEqual(setup.expires_in_minutes, 15)
        with self.assertRaises(ValidationError):
            ConnectionSetupDraft(name="Orders source", connection_type="postgres_cdc", expires_in_minutes=60)
        with self.assertRaises(ValidationError):
            ConnectionSetupDraft(name="Orders source", connection_type="postgres_cdc", secret="password")

    def test_nested_tool_inputs_are_renderable_without_json_schema_refs(self) -> None:
        server = create_server(settings(), FakeAPI())
        expected_nested_properties = {
            "create_connection_setup": ("setup", {"name", "connection_type", "config_template", "expires_in_minutes"}),
            "create_pipeline_draft": ("pipeline", {"name", "source_connection_id", "target_connection_id", "target_write_strategy"}),
            "update_pipeline_draft": ("pipeline", {"name", "source_connection_id", "target_connection_id", "target_write_strategy"}),
            "request_run_start_approval": ("options", {"mode", "checkpoint_mode", "max_retries"}),
            "start_pipeline": ("options", {"mode", "checkpoint_mode", "max_retries"}),
            "list_activity": ("query", {"status", "mode", "pipeline", "period", "limit", "offset"}),
        }
        for tool_name in TOOL_SCOPES:
            tool = server._tool_manager.get_tool(tool_name)
            self.assertIsNotNone(tool)
            assert tool is not None
            rendered = json.dumps(tool.parameters)
            self.assertNotIn('"$ref"', rendered, tool_name)
            self.assertNotIn('"$defs"', rendered, tool_name)

        for tool_name, (argument_name, expected_fields) in expected_nested_properties.items():
            tool = server._tool_manager.get_tool(tool_name)
            self.assertIsNotNone(tool)
            assert tool is not None
            nested = tool.parameters["properties"][argument_name]
            self.assertEqual(nested["type"], "object", tool_name)
            self.assertTrue(expected_fields.issubset(nested["properties"]), tool_name)
            if argument_name == "pipeline":
                self.assertIn("target_write_strategy", nested["required"], tool_name)

        pipeline_schema = server._tool_manager.get_tool("create_pipeline_draft").parameters["properties"]["pipeline"]
        transform_schema = pipeline_schema["properties"]["transforms"]["items"]
        variants = {variant["properties"]["type"]["const"]: variant for variant in transform_schema["oneOf"]}
        self.assertEqual(set(variants), {"cast", "derive", "filter", "enrich"})
        self.assertTrue(all(variant["additionalProperties"] is False for variant in variants.values()))
        self.assertFalse(variants["cast"]["properties"]["rules"]["items"]["additionalProperties"])
        self.assertIn("unique", variants["cast"]["properties"]["rules"]["description"])
        derive_rules = variants["derive"]["properties"]["derive_rules"]
        self.assertTrue(all(rule["additionalProperties"] is False for rule in derive_rules["items"]["oneOf"]))
        self.assertIn("unique", derive_rules["description"])
        derive_functions: set[str] = set()
        for rule in derive_rules["items"]["oneOf"]:
            function_schema = rule["properties"]["function"]
            derive_functions.update(function_schema.get("enum", [function_schema.get("const")]))
        self.assertEqual(
            derive_functions,
            {"date_trunc_day", "date_trunc_hour", "trim", "lower", "upper", "coalesce", "multiply", "divide", "round"},
        )
        field_mapping_transforms = pipeline_schema["properties"]["field_mappings"]["items"]["properties"]["transforms"]
        self.assertEqual(field_mapping_transforms["minItems"], 0)
        self.assertEqual(field_mapping_transforms["maxItems"], 0)
        self.assertNotIn("items", field_mapping_transforms)
        target_schema_options = pipeline_schema["properties"]["target_schema_options"]
        serialized_target_options = json.dumps(target_schema_options)
        self.assertIn("bigquery", serialized_target_options)
        self.assertIn("partitioning", serialized_target_options)
        self.assertIn("clustering_fields", serialized_target_options)
        self.assertNotIn('"$ref"', serialized_target_options)
        config_template = server._tool_manager.get_tool("create_connection_setup").parameters["properties"]["setup"]["properties"]["config_template"]
        self.assertTrue(config_template["additionalProperties"])

    def test_every_tool_has_explicit_reviewable_safety_annotations(self) -> None:
        server = create_server(settings(), FakeAPI())
        expected = {
            **{
                name: (True, False, True, False)
                for name in (
                    "get_workspace_context",
                    "get_workspace_capacity",
                    "list_connector_types",
                    "list_connections",
                    "get_connection",
                    "get_connection_setup_status",
                    "list_connection_setups",
                    "list_pipelines",
                    "get_pipeline",
                    "list_agent_approvals",
                    "list_pipeline_runs",
                    "get_run",
                    "list_activity",
                    "diagnose_run_failure",
                    "get_usage_report",
                )
            },
            **{
                name: (True, False, True, True)
                for name in ("test_connection", "discover_connection_schema", "preview_pipeline")
            },
            **{
                name: (False, False, True, False)
                for name in ("create_connection_setup", "create_pipeline_draft", "request_run_start_approval", "request_run_stop_approval")
            },
            **{
                name: (False, True, True, False)
                for name in ("cancel_connection_setup", "update_pipeline_draft", "cancel_agent_approval")
            },
            **{name: (False, True, True, True) for name in ("start_pipeline", "stop_run")},
            "validate_pipeline": (False, False, False, True),
        }
        self.assertEqual(set(expected), set(TOOL_SCOPES))
        for name, contract in expected.items():
            tool = server._tool_manager.get_tool(name)
            self.assertIsNotNone(tool, name)
            assert tool is not None
            annotations = tool.annotations
            self.assertIsNotNone(annotations, name)
            assert annotations is not None
            self.assertEqual(
                (
                    annotations.read_only_hint,
                    annotations.destructive_hint,
                    annotations.idempotent_hint,
                    annotations.open_world_hint,
                ),
                contract,
                name,
            )

    def test_identifiers_and_idempotency_keys_are_constrained(self) -> None:
        self.assertEqual(_resource_id("pipe_demo"), "pipe_demo")
        self.assertEqual(_idempotency_key("pipeline-demo-001"), "pipeline-demo-001")
        for invalid in ("../secret", "x", "https://example.com"):
            with self.assertRaises(ValueError):
                _resource_id(invalid)
        with self.assertRaises(ValueError):
            _idempotency_key("short")

    def test_pipeline_inventory_contract_omits_legacy_status(self) -> None:
        payload = _pipeline_inventory_contract({
            "pipelines": [{"id": "pipe_demo", "status": "DRAFT", "inventory_state": "operational"}],
            "summary": {"all": 1, "operational": 1, "ready": 0, "attention": 0, "draft": 0},
        })
        self.assertNotIn("status", payload["pipelines"][0])
        self.assertEqual(payload["pipelines"][0]["inventory_state"], "operational")
        self.assertEqual(payload["summary"]["operational"], 1)
        with self.assertRaisesRegex(ValueError, "non-object"):
            _pipeline_inventory_contract([])
        self.assertEqual(
            _pipeline_detail_contract({"id": "pipe_demo", "status": "DRAFT", "setup_state": {"ready_to_run": True}}),
            {"id": "pipe_demo", "setup_state": {"ready_to_run": True}},
        )

    def test_server_is_stateless_streamable_http(self) -> None:
        server = create_server(settings())
        create_streamable_http_app(server, settings())
        self.assertTrue(server.session_manager.stateless)
        self.assertTrue(server.session_manager.json_response)
        self.assertEqual(server.session_manager.max_request_body_size, settings().max_request_bytes)

    def test_public_health_does_not_disclose_environment(self) -> None:
        configured = settings(environment="production")
        app = create_streamable_http_app(create_server(configured, FakeAPI()), configured)
        with TestClient(app, base_url="https://mcp.example.com") as client:
            response = client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "service": "joinlayer-mcp"})

    def test_private_metrics_require_scrape_token_and_export_mcp_families(self) -> None:
        token = "metrics-scrape-token-value"
        configured = settings(
            metrics_token_sha256=hashlib.sha256(token.encode("utf-8")).hexdigest(),
            max_response_bytes=256 * 1024,
        )
        api = FakeAPI()
        server = create_server(configured, api)
        app = GatewayGuard(
            create_streamable_http_app(server, configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        with TestClient(app, base_url="https://mcp.example.com") as client:
            self.assertEqual(client.get("/metrics").status_code, 401)
            response = client.get("/metrics", headers={"Authorization": f"Bearer {token}"})

        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("joinlayer_mcp_requests_total", response.text)
        self.assertIn("joinlayer_mcp_authentications_total", response.text)
        self.assertIn("joinlayer_mcp_tool_calls_total", response.text)
        self.assertIn("joinlayer_mcp_concurrency_limit", response.text)

    def test_private_metrics_bypass_public_concurrency_and_do_not_count_the_scrape(self) -> None:
        token = "metrics-scrape-token-value"
        configured = settings(
            metrics_token_sha256=hashlib.sha256(token.encode("utf-8")).hexdigest(),
            max_concurrent_requests=1,
            max_response_bytes=256 * 1024,
        )
        api = FakeAPI()
        gateway = GatewayGuard(
            create_streamable_http_app(create_server(configured, api), configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        gateway._active = configured.max_concurrent_requests
        IN_FLIGHT.set(configured.max_concurrent_requests)
        try:
            with TestClient(gateway, base_url="https://mcp.example.com") as client:
                response = client.get("/metrics", headers={"Authorization": f"Bearer {token}"})
        finally:
            gateway._active = 0
            IN_FLIGHT.set(0)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("joinlayer_mcp_in_flight_requests 1.0", response.text)
        self.assertNotIn("joinlayer_mcp_in_flight_requests 2.0", response.text)

    def test_skill_archive_contains_only_declared_package_files(self) -> None:
        files = [
            "SKILL.md",
            "agents/openai.yaml",
            "references/execution-model.md",
            "references/data-shaping.md",
            "references/operations.md",
            "references/product-capabilities.md",
            "references/connector-contracts.md",
        ]
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            for relative in files:
                path = directory / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(relative, encoding="utf-8")
            (directory / "secret.env").write_text("do-not-package", encoding="utf-8")
            with zipfile.ZipFile(BytesIO(_skill_archive(directory))) as archive:
                self.assertEqual(sorted(archive.namelist()), sorted(f"joinlayer-pipelines/{name}" for name in files))

    def test_skill_archive_injects_deployment_endpoints_and_first_session_playbook(self) -> None:
        files = [
            "SKILL.md",
            "agents/openai.yaml",
            "references/execution-model.md",
            "references/data-shaping.md",
            "references/operations.md",
            "references/product-capabilities.md",
            "references/connector-contracts.md",
        ]
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            for relative in files:
                path = directory / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                if relative == "agents/openai.yaml":
                    content = "https://replace-with-your-joinlayer-host.example/mcp"
                elif relative == "SKILL.md":
                    content = (
                        "Start Here In Every New Session\n"
                        "https://replace-with-your-joinlayer-host.example/mcp\n"
                        "https://replace-with-your-joinlayer-docs.example/agent-integrations"
                    )
                else:
                    content = relative
                path.write_text(content, encoding="utf-8")
            with zipfile.ZipFile(BytesIO(_skill_archive(directory, "https://mcp.customer.example/mcp", "https://docs.customer.example/agent-integrations"))) as archive:
                manifest = archive.read("joinlayer-pipelines/agents/openai.yaml").decode("utf-8")
                skill = archive.read("joinlayer-pipelines/SKILL.md").decode("utf-8")
                self.assertIn("https://mcp.customer.example/mcp", manifest)
                self.assertNotIn("replace-with-your-joinlayer-host", manifest)
                self.assertIn("Start Here In Every New Session", skill)
                self.assertIn("https://mcp.customer.example/mcp", skill)
                self.assertNotIn("replace-with-your-joinlayer-host", skill)
                self.assertIn("https://docs.customer.example/agent-integrations", skill)
                self.assertNotIn("replace-with-your-joinlayer-docs", skill)

    def test_public_skill_download_contains_complete_deployment_guide_without_authentication(self) -> None:
        configured = settings(
            skill_directory=str(Path(__file__).resolve().parents[2] / "skills" / "joinlayer-pipelines"),
            max_response_bytes=256 * 1024,
        )
        api = FakeAPI()
        app = GatewayGuard(
            create_streamable_http_app(create_server(configured, api), configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        with TestClient(app, base_url="https://mcp.example.com") as client:
            response = client.get("/skills/joinlayer-pipelines.zip")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["content-type"], "application/zip")
        self.assertEqual(response.headers["content-disposition"], 'attachment; filename="joinlayer-pipelines.zip"')
        with zipfile.ZipFile(BytesIO(response.content)) as archive:
            capabilities = archive.read("joinlayer-pipelines/references/product-capabilities.md").decode("utf-8")
            connector_contracts = archive.read("joinlayer-pipelines/references/connector-contracts.md").decode("utf-8")
            data_shaping = archive.read("joinlayer-pipelines/references/data-shaping.md").decode("utf-8")
            skill = archive.read("joinlayer-pipelines/SKILL.md").decode("utf-8")
        self.assertIn("Complete MCP Tool Map", capabilities)
        self.assertIn("Schema Registry", connector_contracts)
        self.assertIn("target_schema_options", connector_contracts)
        for strategy in ("connector_default", "insert", "upsert", "replace"):
            self.assertIn(f"`{strategy}`", data_shaping)
        self.assertNotIn("`merge`", data_shaping)
        self.assertNotIn("`update`", data_shaping)
        self.assertIn("https://mcp.example.com/mcp", skill)
        self.assertIn("https://docs.joinlayer.app/agent-integrations", skill)


class SettingsTests(unittest.TestCase):
    def test_rejects_embedded_api_credentials(self) -> None:
        with patch.dict(os.environ, {"MCP_API_BASE_URL": "https://user:pass@example.com"}, clear=False):
            with self.assertRaises(ValueError):
                Settings.from_env()

    def test_rejects_invalid_metrics_hash(self) -> None:
        with patch.dict(os.environ, {"DATAFLOW_METRICS_TOKEN_SHA256": "not-a-digest"}, clear=False):
            with self.assertRaises(ValueError):
                Settings.from_env()

    def test_rejects_short_request_state_rotation_keys(self) -> None:
        with patch.dict(os.environ, {"MCP_REQUEST_STATE_KEYS": "valid-request-state-key-value-000001,short"}, clear=False):
            with self.assertRaises(ValueError):
                Settings.from_env()

    def test_rejects_short_gateway_credential(self) -> None:
        with patch.dict(os.environ, {"MCP_GATEWAY_TOKEN": "short"}, clear=False):
            with self.assertRaises(ValueError):
                Settings.from_env()

    def test_derives_transport_allowlists_and_requires_oauth_dependencies(self) -> None:
        with patch.dict(os.environ, {
            "DATAFLOW_ENV": "production",
            "MCP_PUBLIC_URL": "https://mcp.example.com",
            "MCP_API_BASE_URL": "http://api:8080",
            "MCP_OAUTH_ISSUER": "https://joinlayer.example.com",
            "MCP_GATEWAY_TOKEN": "gateway-token-value-with-at-least-32-chars",
            "MCP_REQUEST_STATE_KEYS": "request-state-key-value-with-at-least-32-chars",
        }, clear=False):
            configured = Settings.from_env()
        self.assertEqual(configured.allowed_hosts, ("mcp.example.com",))
        self.assertEqual(configured.allowed_origins, ("https://mcp.example.com",))

    def test_rejects_public_plaintext_url_in_production(self) -> None:
        with patch.dict(os.environ, {
            "DATAFLOW_ENV": "production",
            "MCP_PUBLIC_URL": "http://mcp.example.com",
            "MCP_API_BASE_URL": "http://api:8080",
            "MCP_OAUTH_ISSUER": "https://joinlayer.example.com",
            "MCP_GATEWAY_TOKEN": "gateway-token-value-with-at-least-32-chars",
        }, clear=False):
            with self.assertRaises(ValueError):
                Settings.from_env()


class TokenBucketTests(unittest.IsolatedAsyncioTestCase):
    async def test_enforces_burst_refill_and_bounded_keys(self) -> None:
        registry = TokenBucketRegistry(rate=1, burst=2, max_keys=2)
        self.assertEqual(await registry.allow("agent-a", now=10), (True, 0))
        self.assertEqual(await registry.allow("agent-a", now=10), (True, 0))
        allowed, retry_after = await registry.allow("agent-a", now=10)
        self.assertFalse(allowed)
        self.assertEqual(retry_after, 1)
        self.assertEqual(await registry.allow("agent-a", now=11), (True, 0))
        self.assertEqual(await registry.allow("agent-b", now=11), (True, 0))
        self.assertEqual(await registry.allow("agent-c", now=11), (False, 1))


class GatewaySecurityTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_published_tool_has_an_explicit_scope(self) -> None:
        registered = set(re.findall(
            r"@mcp\.tool\(annotations=[A-Z_]+\)\s+async def\s+([a-z_][a-z0-9_]*)",
            inspect.getsource(create_server),
        ))
        self.assertEqual(registered, set(TOOL_SCOPES))

    async def test_read_only_tool_scope_challenges_are_operation_specific(self) -> None:
        self.assertEqual({
            name: TOOL_SCOPES[name]
            for name in (
                "discover_connection_schema",
                "test_connection",
                "get_connection_setup_status",
                "list_connection_setups",
                "list_activity",
                "list_agent_approvals",
                "validate_pipeline",
                "preview_pipeline",
                "list_pipeline_runs",
                "get_run",
                "diagnose_run_failure",
            )
        }, {
            "discover_connection_schema": "connections:test",
            "test_connection": "connections:test",
            "get_connection_setup_status": "connections:test",
            "list_connection_setups": "connections:test",
            "list_activity": "runs:read",
            "list_agent_approvals": "runs:read",
            "validate_pipeline": "pipelines:validate",
            "preview_pipeline": "pipelines:validate",
            "list_pipeline_runs": "runs:read",
            "get_run": "runs:read",
            "diagnose_run_failure": "diagnostics:read",
        })

    async def test_transport_allowlist_rejects_wrong_host_and_origin(self) -> None:
        allowed_hosts = frozenset({"mcp.example.com"})
        allowed_origins = frozenset({"https://mcp.example.com"})
        self.assertEqual(_transport_error({"host": "evil.example"}, allowed_hosts, allowed_origins, True), "invalid_host")
        self.assertEqual(
            _transport_error({"host": "mcp.example.com", "origin": "https://evil.example"}, allowed_hosts, allowed_origins, True),
            "invalid_origin",
        )
        self.assertIsNone(_transport_error({"host": "mcp.example.com"}, allowed_hosts, allowed_origins, True))

    async def test_request_body_is_bounded_without_trusting_content_length(self) -> None:
        messages = iter([
            {"type": "http.request", "body": b"a" * 700, "more_body": True},
            {"type": "http.request", "body": b"b" * 700, "more_body": False},
        ])

        async def receive():
            return next(messages)

        self.assertIsNone(await _bounded_body(receive, None, 1024))

    async def test_slow_request_body_times_out_and_releases_concurrency_slot(self) -> None:
        async def app(_scope, _receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"{}"})

        verifier = AsyncMock()
        access = AccessToken(
            token="jlo_at_" + "a" * 64, client_id="https://client.example/client.json", scopes=["workspace:read"], resource="https://mcp.example.com/mcp"
        )
        verifier.verify.return_value = SimpleNamespace(
            access=access, api_token="jli_" + "i" * 64, principal_key="ogr_demo\x1fclient\x1fuser\x1fagent\x1forg"
        )
        guard = GatewayGuard(app, settings(request_body_timeout_seconds=0.01), verifier)

        async def receive():
            await asyncio.sleep(1)
            return {"type": "http.request", "body": b"{}", "more_body": False}

        responses = []
        async def send(message):
            responses.append(message)
        await guard({
            "type": "http", "method": "POST", "path": "/mcp", "client": ("192.0.2.10", 1000),
            "headers": [(b"host", b"mcp.example.com"), (b"authorization", ("Bearer jlo_at_" + "a" * 64).encode())],
        }, receive, send)
        self.assertEqual(responses[0]["status"], 408)
        self.assertEqual(guard._active, 0)

    async def test_final_serialized_response_is_bounded(self) -> None:
        async def app(_scope, _receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"x" * 1500})

        guard = GatewayGuard(app, settings(max_response_bytes=1024), AsyncMock())
        responses = []
        async def send(message):
            responses.append(message)
        await guard({
            "type": "http", "method": "GET", "path": "/readyz", "client": ("192.0.2.10", 1000),
            "headers": [(b"host", b"mcp.example.com")],
        }, AsyncMock(), send)
        self.assertEqual(responses[0]["status"], 502)

    async def test_rotating_invalid_tokens_never_allocate_credential_buckets(self) -> None:
        async def app(_scope, _receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"{}"})

        verifier = AsyncMock()
        verifier.verify.return_value = None
        guard = GatewayGuard(app, settings(anonymous_rate_limit_burst=100, rate_limit_max_keys=10), verifier)

        async def receive():
            return {"type": "http.request", "body": b"{}", "more_body": False}

        for index in range(25):
            responses = []
            async def send(message):
                responses.append(message)
            scope = {
                "type": "http",
                "method": "POST",
                "path": "/mcp",
                "client": ("192.0.2.10", 1000),
                "headers": [(b"host", b"mcp.example.com"), (b"authorization", f"Bearer invalid-{index:04d}".encode())],
            }
            await guard(scope, receive, send)
            self.assertEqual(responses[0]["status"], 401)
        self.assertEqual(len(guard.client_buckets._buckets), 0)
        self.assertEqual(len(guard.anonymous_buckets._buckets), 1)

    async def test_rate_limit_isolated_by_delegated_principal_not_client_id(self) -> None:
        async def app(_scope, _receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"{}"})

        access = AccessToken(
            token="jlo_at_" + "a" * 64,
            client_id="https://shared-client.example/client.json",
            scopes=["workspace:read"],
            resource="https://mcp.example.com/mcp",
        )
        verifier = AsyncMock()
        verifier.verify.side_effect = [
            SimpleNamespace(access=access, api_token="jli_" + "a" * 64, principal_key="grant-a\x1fclient\x1fuser-a\x1fagent-a\x1forg-a"),
            SimpleNamespace(access=access, api_token="jli_" + "b" * 64, principal_key="grant-b\x1fclient\x1fuser-b\x1fagent-b\x1forg-b"),
        ]
        guard = GatewayGuard(app, settings(client_rate_limit_burst=1, client_rate_limit_rps=0.01), verifier)

        async def request_once():
            responses = []
            async def send(message):
                responses.append(message)
            await guard({
                "type": "http", "method": "POST", "path": "/mcp", "client": ("192.0.2.50", 1000),
                "headers": [(b"host", b"mcp.example.com"), (b"authorization", ("Bearer jlo_at_" + "a" * 64).encode())],
            }, AsyncMock(return_value={"type": "http.request", "body": b"{}", "more_body": False}), send)
            return responses

        self.assertEqual((await request_once())[0]["status"], 200)
        self.assertEqual((await request_once())[0]["status"], 200)
        self.assertEqual(len(guard.client_buckets._buckets), 2)

    async def test_auth_attempt_limit_precedes_authoritative_token_verification(self) -> None:
        async def app(_scope, _receive, _send):
            raise AssertionError("request must not reach app")

        verifier = AsyncMock()
        verifier.verify.return_value = None
        guard = GatewayGuard(app, settings(auth_attempt_rate_limit_burst=1, auth_attempt_rate_limit_rps=0.01), verifier)

        async def request_once():
            responses = []
            async def send(message):
                responses.append(message)
            await guard({
                "type": "http", "method": "POST", "path": "/mcp", "client": ("192.0.2.20", 1000),
                "headers": [(b"host", b"mcp.example.com"), (b"authorization", ("Bearer jlo_at_" + "x" * 64).encode())],
            }, AsyncMock(return_value={"type": "http.request", "body": b"{}"}), send)
            return responses

        self.assertEqual((await request_once())[0]["status"], 401)
        self.assertEqual((await request_once())[0]["status"], 429)
        self.assertEqual(verifier.verify.await_count, 1)

    async def test_authentication_challenge_points_to_path_specific_resource_metadata(self) -> None:
        async def app(_scope, _receive, _send):
            raise AssertionError("unauthenticated request must not reach MCP")

        verifier = AsyncMock()
        verifier.verify.return_value = None
        guard = GatewayGuard(app, settings(), verifier)
        responses = []

        async def send(message):
            responses.append(message)

        await guard({
            "type": "http", "method": "POST", "path": "/mcp", "client": ("192.0.2.30", 1000),
            "headers": [(b"host", b"mcp.example.com")],
        }, AsyncMock(return_value={"type": "http.request", "body": b"{}", "more_body": False}), send)

        self.assertEqual(responses[0]["status"], 401)
        headers = dict(responses[0]["headers"])
        challenge = headers[b"www-authenticate"].decode()
        self.assertIn('resource_metadata="https://mcp.example.com/.well-known/oauth-protected-resource/mcp"', challenge)
        self.assertIn(f'scope="{" ".join(SUPPORTED_SCOPES)}"', challenge)

    async def test_unauthenticated_tool_challenge_requests_only_current_operation_scope(self) -> None:
        self.assertEqual(_challenge_scope({"mcp-method": "tools/call", "mcp-name": "create_pipeline_draft"}, "workspace:read"), "pipelines:write")
        self.assertEqual(_challenge_scope({"mcp-method": "tools/call", "mcp-name": "cancel_agent_approval"}, "workspace:read"), "runs:read")
        self.assertEqual(_challenge_scope({"mcp-method": "server/discover"}, "workspace:read"), "workspace:read")
        self.assertEqual(_challenge_scope({"mcp-method": "tools/call", "mcp-name": "unknown"}, "workspace:read"), "workspace:read")

    async def test_tool_level_authorization_is_bound_to_validated_openai_client_identity(self) -> None:
        self.assertTrue(_uses_openai_tool_level_authorization("https://chatgpt.com/oauth/client.json"))
        self.assertTrue(_uses_openai_tool_level_authorization("https://chatgpt.com/oauth/codex/session/client.json"))
        self.assertFalse(_uses_openai_tool_level_authorization("jlo_client_claude"))
        self.assertFalse(_uses_openai_tool_level_authorization("https://chatgpt.com.evil.example/oauth/client.json"))
        self.assertFalse(_uses_openai_tool_level_authorization("https://user@chatgpt.com/oauth/client.json"))

    async def test_non_openai_client_keeps_transport_scope_challenge(self) -> None:
        async def app(_scope, _receive, _send):
            raise AssertionError("insufficiently scoped request must not reach MCP")

        verifier = AsyncMock()
        verifier.verify.return_value = SimpleNamespace(
            access=AccessToken(
                token="jlo_at_" + "a" * 64,
                client_id="jlo_client_claude",
                scopes=["workspace:read"],
                resource="https://mcp.example.com/mcp",
            ),
            api_token="jli_" + "i" * 64,
            principal_key="ogr_demo\x1fclient\x1fuser\x1fagent\x1forg",
        )
        guard = GatewayGuard(app, settings(), verifier)
        body = b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_pipeline_draft","arguments":{}}}'
        responses = []

        async def send(message):
            responses.append(message)

        await guard({
            "type": "http", "method": "POST", "path": "/mcp", "client": ("192.0.2.31", 1000),
            "headers": [
                (b"host", b"mcp.example.com"),
                (b"content-length", str(len(body)).encode()),
                (b"authorization", ("Bearer jlo_at_" + "a" * 64).encode()),
            ],
        }, AsyncMock(return_value={"type": "http.request", "body": body, "more_body": False}), send)

        self.assertEqual(responses[0]["status"], 403)
        challenge = dict(responses[0]["headers"])[b"www-authenticate"].decode()
        self.assertIn('error="insufficient_scope"', challenge)
        self.assertIn('scope="workspace:read pipelines:write"', challenge)
        error = json.loads(responses[1]["body"])["error"]
        self.assertEqual(error["details"]["required_scopes"], ["pipelines:write"])
        self.assertEqual(error["details"]["granted_scopes"], ["workspace:read"])


class MCPProtocolTests(unittest.TestCase):
    def test_actual_tools_list_schemas_enforce_conditional_transform_contracts(self) -> None:
        api = FakeAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "workspace:read",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=512 * 1024)
        server = create_server(configured, api)
        app = GatewayGuard(
            create_streamable_http_app(server, configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/list",
        }
        request_meta = {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name": "schema-contract-test", "version": "1"},
        }
        with TestClient(app, base_url="https://mcp.example.com") as client:
            response = client.post(
                "/mcp",
                headers=headers,
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"_meta": request_meta}},
            )

        self.assertEqual(response.status_code, 200, response.text)
        tools = response.json()["result"]["tools"]
        self.assertEqual(len(tools), len(TOOL_SCOPES))
        self.assertEqual({tool["name"] for tool in tools}, set(TOOL_SCOPES))
        for tool in tools:
            expected = [{"type": "oauth2", "scopes": [TOOL_SCOPES[tool["name"]]]}]
            self.assertEqual(tool["securitySchemes"], expected)
            self.assertEqual(tool["_meta"]["securitySchemes"], expected)
            Draft202012Validator.check_schema(tool["inputSchema"])
        pipeline_schema = next(tool for tool in tools if tool["name"] == "create_pipeline_draft")["inputSchema"]["properties"]["pipeline"]
        transform_schema = pipeline_schema["properties"]["transforms"]["items"]
        validator = Draft202012Validator(transform_schema)

        valid_transforms = [
            {"type": "cast", "rules": [{"field": "amount", "type": "float64"}]},
            {"type": "derive", "derive_rules": [{"source_field": "name", "target_field": "normalized_name", "function": "lower"}]},
            {"type": "derive", "derive_rules": [{"source_field": "nickname", "target_field": "display_name", "function": "coalesce", "value": "Anonymous"}]},
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "total", "function": "multiply", "value": 1.2}]},
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "rounded", "function": "round", "places": 2}]},
            {"type": "filter", "field": "status", "op": "eq", "values": ["paid"]},
            {"type": "filter", "field": "status", "op": "in", "values": ["paid", "shipped"]},
            {"type": "enrich", "source_field": "customer_id", "target_fields": ["tier"], "lookup": {"42": "gold"}},
            {
                "type": "enrich",
                "source_field": "customer_id",
                "target_fields": ["tier"],
                "lookup_connection_id": "conn_lookup",
                "lookup_table": "customers",
                "lookup_key_field": "id",
                "lookup_cache_mode": "materialized",
            },
        ]
        for transform in valid_transforms:
            with self.subTest(valid=transform):
                self.assertEqual(list(validator.iter_errors(transform)), [])

        invalid_transforms = [
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "total", "function": "multiply"}]},
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "total", "function": "divide", "value": {"divisor": 2}}]},
            {"type": "derive", "derive_rules": [{"source_field": "name", "target_field": "normalized_name", "function": "lower", "value": "ignored"}]},
            {"type": "derive", "derive_rules": [{"source_field": "amount", "target_field": "rounded", "function": "round", "places": 10}]},
            {"type": "filter", "field": "status", "op": "eq", "values": ["paid", "shipped"]},
            {"type": "filter", "field": "status", "op": "in", "values": []},
            {"type": "filter", "field": "status", "op": "in", "values": [""]},
            {"type": "enrich", "source_field": "customer_id", "target_fields": ["tier"]},
            {
                "type": "enrich",
                "source_field": "customer_id",
                "target_fields": ["tier"],
                "lookup": {"42": "gold"},
                "lookup_connection_id": "conn_lookup",
                "lookup_table": "customers",
                "lookup_key_field": "id",
            },
            {
                "type": "enrich",
                "source_field": "customer_id",
                "target_fields": ["tier"],
                "lookup": {"42": "gold"},
                "lookup_cache_mode": "materialized",
            },
            {
                "type": "enrich",
                "source_field": "customer_id",
                "target_fields": ["tier"],
                "lookup_connection_id": "conn_lookup",
                "lookup_table": "customers",
            },
        ]
        for transform in invalid_transforms:
            with self.subTest(invalid=transform):
                self.assertNotEqual(list(validator.iter_errors(transform)), [])

    def test_delegated_oauth_token_discovers_and_calls_tool_on_2026_protocol(self) -> None:
        api = FakeAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "workspace:read",
            "agent_governance": {
                "require_run_start_approval": False,
                "require_run_stop_approval": True,
            },
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        server = create_server(configured, api)
        verifier = OAuthTokenVerifier(api, configured.public_url + "/mcp")
        app = GatewayGuard(create_streamable_http_app(server, configured), configured, verifier)
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
        }
        request_meta = {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name": "security-test", "version": "1"},
        }
        discover = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {"_meta": request_meta},
        }
        with TestClient(app, base_url="https://mcp.example.com") as client:
            discovered = client.post("/mcp", headers={**headers, "Mcp-Method": "server/discover"}, json=discover)
            self.assertEqual(discovered.status_code, 200, discovered.text)
            self.assertIn("2026-07-28", discovered.json()["result"]["supportedVersions"])
            called = client.post(
                "/mcp",
                headers={**headers, "Mcp-Method": "tools/call", "Mcp-Name": "get_workspace_context"},
                json={
                    "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": "get_workspace_context", "arguments": {}, "_meta": request_meta},
                },
            )
            self.assertEqual(called.status_code, 200, called.text)
            self.assertIn("agt_demo", called.text)
            self.assertIn('"require_run_start_approval":false', called.text)
            self.assertIn('"require_run_stop_approval":true', called.text)
            metadata = client.get("/.well-known/oauth-protected-resource/mcp")
            self.assertEqual(metadata.status_code, 200)
            self.assertEqual(metadata.json()["resource"], "https://mcp.example.com/mcp")
            self.assertEqual(metadata.json()["authorization_servers"], ["https://joinlayer.example.com"])
            self.assertEqual(metadata.json()["scopes_supported"], list(SUPPORTED_SCOPES))
            self.assertEqual(client.get("/.well-known/oauth-protected-resource").status_code, 404)

    def test_under_scoped_tool_call_returns_incremental_oauth_result_without_dispatch(self) -> None:
        api = FakeAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://chatgpt.com/oauth/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "workspace:read",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        app = GatewayGuard(
            create_streamable_http_app(create_server(configured, api), configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers={
                    "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
                    "Accept": "application/json, text/event-stream",
                    "Origin": "https://mcp.example.com",
                    "MCP-Protocol-Version": "2026-07-28",
                    "Mcp-Method": "tools/call",
                    "Mcp-Name": "get_workspace_capacity",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "get_workspace_capacity",
                        "arguments": {},
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities": {},
                            "io.modelcontextprotocol/clientInfo": {"name": "oauth-step-up-test", "version": "1"},
                        },
                    },
                },
            )
            self.assertEqual(called.status_code, 200, called.text)
            result = called.json()["result"]
            self.assertTrue(result["isError"])
            self.assertIn("scope usage:read", result["content"][0]["text"])
            challenge = result["_meta"]["mcp/www_authenticate"][0]
            self.assertEqual(
                result["_meta"]["io.modelcontextprotocol/serverInfo"],
                {
                    "name": "JoinLayer",
                    "version": "2026-07-28",
                    "title": "JoinLayer Agentic Product Interface",
                    "description": "Secure workspace-scoped data integration tools for delegated agents.",
                    "websiteUrl": configured.docs_url,
                },
            )
            self.assertIn('resource_metadata="https://mcp.example.com/.well-known/oauth-protected-resource/mcp"', challenge)
            self.assertIn('scope="workspace:read usage:read"', challenge)
            self.assertIn('error="insufficient_scope"', challenge)
            self.assertIn('error_description="Additional authorization is required for scope usage:read"', challenge)
            api.request.assert_not_awaited()

            api.principal["scope"] = "workspace:read usage:read"
            authorized = client.post(
                "/mcp",
                headers={
                    "Authorization": "Bearer " + "jlo_at_" + "b" * 64,
                    "Accept": "application/json, text/event-stream",
                    "Origin": "https://mcp.example.com",
                    "MCP-Protocol-Version": "2026-07-28",
                    "Mcp-Method": "tools/call",
                    "Mcp-Name": "get_workspace_capacity",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "get_workspace_capacity",
                        "arguments": {},
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities": {},
                            "io.modelcontextprotocol/clientInfo": {"name": "oauth-step-up-test", "version": "1"},
                        },
                    },
                },
            )
            self.assertEqual(authorized.status_code, 200, authorized.text)
            self.assertFalse(authorized.json()["result"].get("isError", False))
            api.request.assert_awaited_once()

    def test_list_connector_types_exposes_kafka_registry_and_bigquery_contracts(self) -> None:
        api = ProviderListAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "workspace:read",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        app = GatewayGuard(
            create_streamable_http_app(create_server(configured, api), configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers={
                    "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
                    "Accept": "application/json, text/event-stream",
                    "Origin": "https://mcp.example.com",
                    "MCP-Protocol-Version": "2026-07-28",
                    "Mcp-Method": "tools/call",
                    "Mcp-Name": "list_connector_types",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "list_connector_types",
                        "arguments": {},
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities": {},
                            "io.modelcontextprotocol/clientInfo": {"name": "provider-contract-test", "version": "1"},
                        },
                    },
                },
            )
        self.assertEqual(called.status_code, 200, called.text)
        payload = called.json()["result"]["structuredContent"]
        kafka_fields = {field["name"]: field for field in payload["sources"][0]["config_schema"]}
        self.assertEqual(kafka_fields["avro_schema_source"]["options"][1]["value"], "schema_registry")
        self.assertFalse(kafka_fields["schema_registry_url"]["secret"])
        self.assertTrue(kafka_fields["schema_registry_password"]["secret"])
        bigquery_fields = {field["name"]: field for field in payload["targets"][0]["config_schema"]}
        self.assertEqual([option["value"] for option in bigquery_fields["write_mode"]["options"]], ["storage_write", "insert_all", "batch_load"])
        self.assertTrue(bigquery_fields["service_account_json"]["secret"])
        api.request.assert_awaited_once_with(
            "jli_" + "i" * 64,
            "GET",
            "/providers",
            body=None,
            params=None,
            idempotency_key=None,
            approval_id=None,
            tool_name="list_connector_types",
        )

    def test_list_connections_returns_object_structured_content(self) -> None:
        api = ConnectionListAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "connections:read",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        server = create_server(configured, api)
        verifier = OAuthTokenVerifier(api, configured.public_url + "/mcp")
        app = GatewayGuard(create_streamable_http_app(server, configured), configured, verifier)
        request_meta = {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name": "shape-test", "version": "1"},
        }
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "list_connections",
        }

        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "list_connections", "arguments": {}, "_meta": request_meta},
                },
            )

        self.assertEqual(called.status_code, 200, called.text)
        result = called.json()["result"]
        self.assertFalse(result["isError"])
        self.assertEqual(result["structuredContent"]["connections"][0]["id"], "conn_demo")
        self.assertEqual(result["structuredContent"]["pagination"]["total"], 1)
        api.request.assert_awaited_once_with(
            "jli_" + "i" * 64,
            "GET",
            "/connections",
            body=None,
            params={"limit": 50, "offset": 0},
            idempotency_key=None,
            approval_id=None,
            tool_name="list_connections",
        )

    def test_test_connection_uses_stored_server_side_configuration(self) -> None:
        api = ConnectionTestAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "connections:test",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        app = GatewayGuard(
            create_streamable_http_app(create_server(configured, api), configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "test_connection",
        }
        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "test_connection",
                        "arguments": {"connection_id": "conn_kafka"},
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities": {},
                            "io.modelcontextprotocol/clientInfo": {"name": "connection-test", "version": "1"},
                        },
                    },
                },
            )
        self.assertEqual(called.status_code, 200, called.text)
        result = called.json()["result"]
        self.assertFalse(result["isError"], called.text)
        self.assertTrue(result["structuredContent"]["ok"])
        self.assertIn("Schema Registry subject", result["structuredContent"]["message"])
        api.request.assert_awaited_once_with(
            "jli_" + "i" * 64,
            "POST",
            "/connections/test",
            body={"connection_id": "conn_kafka"},
            params=None,
            idempotency_key=None,
            approval_id=None,
            tool_name="test_connection",
        )

    def test_discover_connection_schema_sends_required_table_coordinates(self) -> None:
        api = ConnectionSchemaAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "connections:test",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        server = create_server(configured, api)
        verifier = OAuthTokenVerifier(api, configured.public_url + "/mcp")
        app = GatewayGuard(create_streamable_http_app(server, configured), configured, verifier)
        request_meta = {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name": "schema-shape-test", "version": "1"},
        }
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "discover_connection_schema",
        }
        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "discover_connection_schema",
                        "arguments": {"connection_id": "conn_demo", "schema": " public ", "table": " orders "},
                        "_meta": request_meta,
                    },
                },
            )
        self.assertEqual(called.status_code, 200, called.text)
        self.assertFalse(called.json()["result"]["isError"], called.text)
        self.assertEqual(called.json()["result"]["structuredContent"]["fields"][0]["name"], "id")
        api.request.assert_awaited_once_with(
            "jli_" + "i" * 64,
            "POST",
            "/connections/conn_demo/discover-schema",
            body={"schema": "public", "table": "orders"},
            params=None,
            idempotency_key=None,
            approval_id=None,
            tool_name="discover_connection_schema",
        )

    def test_discover_connection_schema_supports_kafka_topic_without_schema(self) -> None:
        api = ConnectionSchemaAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "connections:test",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        app = GatewayGuard(
            create_streamable_http_app(create_server(configured, api), configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "discover_connection_schema",
        }
        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "discover_connection_schema",
                        "arguments": {"connection_id": "conn_demo", "table": " events.v1 "},
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities": {},
                            "io.modelcontextprotocol/clientInfo": {"name": "kafka-schema-test", "version": "1"},
                        },
                    },
                },
            )
        self.assertEqual(called.status_code, 200, called.text)
        self.assertFalse(called.json()["result"]["isError"], called.text)
        api.request.assert_awaited_once_with(
            "jli_" + "i" * 64,
            "POST",
            "/connections/conn_demo/discover-schema",
            body={"table": "events.v1"},
            params=None,
            idempotency_key=None,
            approval_id=None,
            tool_name="discover_connection_schema",
        )

    def test_discover_connection_schema_progresses_from_schemas_to_tables(self) -> None:
        api = ConnectionSchemaAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "connections:test",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        server = create_server(configured, api)
        app = GatewayGuard(
            create_streamable_http_app(server, configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        request_meta = {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name": "catalog-progression-test", "version": "1"},
        }
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "discover_connection_schema",
        }
        with TestClient(app, base_url="https://mcp.example.com") as client:
            schemas = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "discover_connection_schema", "arguments": {"connection_id": "conn_demo"}, "_meta": request_meta},
                },
            )
            tables = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": "discover_connection_schema", "arguments": {"connection_id": "conn_demo", "schema": "public"}, "_meta": request_meta},
                },
            )
        self.assertEqual(schemas.status_code, 200, schemas.text)
        self.assertEqual(tables.status_code, 200, tables.text)
        self.assertEqual(schemas.json()["result"]["structuredContent"]["schemas"], ["public"])
        self.assertEqual(tables.json()["result"]["structuredContent"]["tables"][0]["name"], "orders")
        self.assertEqual(
            [call.args[1:3] for call in api.request.await_args_list],
            [("GET", "/connections/conn_demo/catalog/schemas"), ("GET", "/connections/conn_demo/catalog/tables")],
        )
        self.assertEqual(api.request.await_args_list[1].kwargs["params"], {"schema": "public"})

    def test_create_pipeline_draft_round_trips_canonical_write_and_policy_fields(self) -> None:
        api = PipelineDraftAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "pipelines:write",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        server = create_server(configured, api)
        app = GatewayGuard(
            create_streamable_http_app(server, configured),
            configured,
            OAuthTokenVerifier(api, configured.public_url + "/mcp"),
        )
        body = {
            "name": "Orders copy",
            "source_connection_id": "conn_source",
            "target_connection_id": "conn_target",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "public",
            "target_table": "orders_copy",
            "target_write_strategy": "upsert",
            "target_primary_key_field": "id",
            "max_rows_per_second": 1_000,
            "throttle_policy": "delay",
            "error_policy": "stop",
            "target_schema_options": {
                "bigquery": {
                    "partitioning": {"mode": "field", "field": "created_at", "granularity": "DAY", "require_partition_filter": False},
                    "clustering_fields": ["customer_id"],
                }
            },
        }
        request_meta = {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name": "draft-round-trip-test", "version": "1"},
        }
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "create_pipeline_draft",
        }
        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "create_pipeline_draft",
                        "arguments": {"pipeline": body, "idempotency_key": "draft-round-trip-001"},
                        "_meta": request_meta,
                    },
                },
            )
        self.assertEqual(called.status_code, 200, called.text)
        result = called.json()["result"]
        self.assertFalse(result["isError"], called.text)
        created = result["structuredContent"]
        self.assertNotIn("status", created)
        self.assertEqual(created["target_write_strategy"], "upsert")
        self.assertEqual(created["throttle_policy"], "delay")
        self.assertEqual(created["error_policy"], "stop")
        expected_body = {
            **body,
            "auto_sync_target_schema": False,
            "transforms": [],
            "run_mode": "one_time",
            "schedule_enabled": False,
            "throttle_policy": "delay",
            "error_policy": "stop",
            "field_mappings": [],
        }
        api.request.assert_awaited_once_with(
            "jli_" + "i" * 64,
            "POST",
            "/pipelines",
            body=expected_body,
            params=None,
            idempotency_key="draft-round-trip-001",
            approval_id=None,
            tool_name="create_pipeline_draft",
        )

    def test_tool_metrics_observe_success_and_mcp_error_outcomes(self) -> None:
        principal = {
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "workspace:read",
            "expires_at": int(time.time()) + 300,
        }
        api = FakeAPI(principal)
        configured = settings(max_response_bytes=64 * 1024)
        server = create_server(configured, api)
        verifier = OAuthTokenVerifier(api, configured.public_url + "/mcp")
        app = GatewayGuard(create_streamable_http_app(server, configured), configured, verifier)
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "get_workspace_context",
        }
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "get_workspace_context",
                "arguments": {},
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": {},
                    "io.modelcontextprotocol/clientInfo": {"name": "metrics-test", "version": "1"},
                },
            },
        }
        success_before = REGISTRY.get_sample_value(
            "joinlayer_mcp_tool_calls_total",
            {"tool": "get_workspace_context", "outcome": "success"},
        ) or 0
        error_before = REGISTRY.get_sample_value(
            "joinlayer_mcp_tool_calls_total",
            {"tool": "get_workspace_context", "outcome": "tool_error"},
        ) or 0
        unknown_before = REGISTRY.get_sample_value(
            "joinlayer_mcp_tool_calls_total",
            {"tool": "unknown", "outcome": "tool_error"},
        ) or 0

        with TestClient(app, base_url="https://mcp.example.com") as client:
            success = client.post("/mcp", headers=headers, json=request)
            self.assertEqual(success.status_code, 200, success.text)
            self.assertFalse(success.json()["result"]["isError"])
            api.request.side_effect = JoinLayerAPIError(503, "api_unavailable", "JoinLayer API is unavailable")
            request["id"] = 2
            failed = client.post("/mcp", headers=headers, json=request)
            request["id"] = 3
            request["params"]["name"] = "tenant-controlled-unbounded-tool-name"
            unknown_headers = {**headers, "Mcp-Name": "tenant-controlled-unbounded-tool-name"}
            unknown = client.post("/mcp", headers=unknown_headers, json=request)

        self.assertEqual(failed.status_code, 200, failed.text)
        self.assertTrue(failed.json()["result"]["isError"])
        self.assertEqual(unknown.status_code, 200, unknown.text)
        self.assertTrue(unknown.json()["result"]["isError"])
        self.assertEqual(
            REGISTRY.get_sample_value(
                "joinlayer_mcp_tool_calls_total",
                {"tool": "get_workspace_context", "outcome": "success"},
            ),
            success_before + 1,
        )
        self.assertEqual(
            REGISTRY.get_sample_value(
                "joinlayer_mcp_tool_calls_total",
                {"tool": "get_workspace_context", "outcome": "tool_error"},
            ),
            error_before + 1,
        )
        self.assertEqual(
            REGISTRY.get_sample_value(
                "joinlayer_mcp_tool_calls_total",
                {"tool": "unknown", "outcome": "tool_error"},
            ),
            unknown_before + 1,
        )
        self.assertIsNone(
            REGISTRY.get_sample_value(
                "joinlayer_mcp_tool_calls_total",
                {"tool": "tenant-controlled-unbounded-tool-name", "outcome": "tool_error"},
            )
        )

    def test_list_pipelines_exposes_only_authoritative_inventory_state(self) -> None:
        api = PipelineListAPI({
            "active": True,
            "resource": "https://mcp.example.com/mcp",
            "client_id": "https://client.example/client.json",
            "api_token": "jli_" + "i" * 64,
            "agent_id": "agt_demo",
            "grant_id": "ogr_demo",
            "user_id": "user_demo",
            "org_id": "org-demo",
            "scope": "pipelines:read",
            "expires_at": int(time.time()) + 300,
        })
        configured = settings(max_response_bytes=64 * 1024)
        server = create_server(configured, api)
        verifier = OAuthTokenVerifier(api, configured.public_url + "/mcp")
        app = GatewayGuard(create_streamable_http_app(server, configured), configured, verifier)
        headers = {
            "Authorization": "Bearer " + "jlo_at_" + "a" * 64,
            "Accept": "application/json, text/event-stream",
            "Origin": "https://mcp.example.com",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "list_pipelines",
        }
        request_meta = {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {"name": "state-test", "version": "1"},
        }

        with TestClient(app, base_url="https://mcp.example.com") as client:
            called = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "list_pipelines", "arguments": {}, "_meta": request_meta},
                },
            )

        self.assertEqual(called.status_code, 200, called.text)
        result = called.json()["result"]["structuredContent"]
        self.assertEqual([item["inventory_state"] for item in result["pipelines"]], ["operational", "ready"])
        self.assertTrue(all("status" not in item for item in result["pipelines"]))
        self.assertEqual(result["summary"], {"all": 2, "operational": 1, "ready": 1, "attention": 0, "draft": 0})


class JoinLayerAPIStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_mcp_token_is_used_only_in_private_introspection_body(self) -> None:
        captured: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            if request.url.path == "/api/v1/internal/oauth/introspect":
                return httpx.Response(200, json={"active": False})
            return httpx.Response(200, json={"ok": True})

        api = JoinLayerAPI(settings())
        await api._client.aclose()
        await api._root_client.aclose()
        transport = httpx.MockTransport(handler)
        api._client = httpx.AsyncClient(base_url="http://api/api/v1", transport=transport)
        api._root_client = httpx.AsyncClient(base_url="http://api", transport=transport)
        public_token = "jlo_at_" + "p" * 64
        internal_token = "jli_" + "i" * 64
        try:
            await api.introspect(public_token)
            await api.request(internal_token, "GET", "/me")
        finally:
            await api.close()

        introspection, downstream = captured
        self.assertNotIn("authorization", introspection.headers)
        self.assertEqual(introspection.headers["x-joinlayer-gateway-token"], settings().gateway_token)
        self.assertEqual(json.loads(introspection.content)["token"], public_token)
        self.assertEqual(downstream.headers["authorization"], f"Bearer {internal_token}")
        self.assertEqual(downstream.headers["x-joinlayer-gateway-token"], settings().gateway_token)
        self.assertNotIn(public_token, str(downstream.url))
        self.assertNotIn(public_token, downstream.headers.values())

    async def test_rejects_declared_oversized_response_before_reading_body(self) -> None:
        async def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"content-length": "2048"}, content=b"{}")

        api = JoinLayerAPI(settings())
        await api._client.aclose()
        api._client = httpx.AsyncClient(base_url="http://api/api/v1", transport=httpx.MockTransport(handler))
        try:
            with self.assertRaisesRegex(JoinLayerAPIError, "response_too_large"):
                await api.request("jli_" + "x" * 64, "GET", "/me")
        finally:
            await api.close()

    async def test_aborts_chunked_response_once_limit_is_crossed(self) -> None:
        class OversizedStream(httpx.AsyncByteStream):
            closed = False

            async def __aiter__(self):
                yield b"a" * 700
                yield b"b" * 700
                raise AssertionError("gateway read beyond configured limit")

            async def aclose(self):
                self.closed = True

        stream = OversizedStream()

        async def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, stream=stream)

        api = JoinLayerAPI(settings())
        await api._client.aclose()
        api._client = httpx.AsyncClient(base_url="http://api/api/v1", transport=httpx.MockTransport(handler))
        try:
            with self.assertRaisesRegex(JoinLayerAPIError, "response_too_large"):
                await api.request("jli_" + "x" * 64, "GET", "/me")
            self.assertTrue(stream.closed)
        finally:
            await api.close()

    async def test_readiness_requires_api_health_and_auth_boundary(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/readyz":
                return httpx.Response(204)
            if request.url.path == "/api/v1/me":
                return httpx.Response(401, json={"error": {"code": "unauthorized"}})
            return httpx.Response(404)

        api = JoinLayerAPI(settings())
        await api._root_client.aclose()
        api._root_client = httpx.AsyncClient(base_url="http://api", transport=httpx.MockTransport(handler))
        try:
            await api.verify_readiness()
        finally:
            await api.close()


if __name__ == "__main__":
    unittest.main()
