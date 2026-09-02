# JoinLayer Product Capabilities

Use this reference to choose a complete workflow without guessing the product surface.

## Contents

- [Product Model](#product-model)
- [Capability Map](#capability-map)
- [Complete MCP Tool Map](#complete-mcp-tool-map)
- [Task Playbooks](#task-playbooks)
- [State And Evidence Rules](#state-and-evidence-rules)
- [Authorization And Errors](#authorization-and-errors)
- [Completion Contract](#completion-contract)

## Product Model

### Workspace And Identity

Every MCP session is delegated by one user to one OAuth client for one workspace and one exact scope set. JoinLayer creates a separately attributed agent identity. The effective permission is always the narrowest of:

1. the user's current workspace membership and role;
2. consented OAuth scopes;
3. the requested tool and resource;
4. the workspace approval requirement, plus an exact run approval or pipeline-specific automation permission when that requirement is enabled;
5. pipeline state and workspace capacity.

Do not infer access from a tool appearing in the client. Call `get_workspace_context` and use returned identity, role, workspace, and scopes.

### Connections

Connections are adapter-based stored endpoints. A connection can support source, target, lookup, schema discovery, testing, CDC, or a subset of those capabilities. Call `list_connector_types`; never hardcode a provider capability. Call `list_connections` and `get_connection` for safe metadata.

If a required connection is missing, call `create_connection_setup` with nonsecret configuration only. The user opens the returned short-lived browser link and enters credentials directly into JoinLayer. Poll `get_connection_setup_status`; the agent receives only safe status and the durable connection ID.

### Pipelines

A pipeline draft binds source and target connections, source/target objects, mappings, transforms, optional lookup enrichment, run mode, schedule, and target write strategy. It is durable configuration, not active execution.

Always discover schemas before using object or field names. Validate after every material draft change. Preview after validation to inspect representative target rows. A valid preview does not authorize or start a run.

### Runs

A run is one execution of a pipeline:

- `one_time`: finite current-data load;
- `scheduled`: finite run launched by saved schedule policy;
- `realtime`: future CDC from a current or durable checkpoint;
- `realtime_with_backfill`: CDC from a captured boundary runs concurrently with coordinated historical chunks on spare capacity.

Use `checkpoint_mode=resume`. Accepted, queued, running, recovering, succeeded, failed, stopped, and cancelled are different outcomes. Verify durable state after every action.

### Usage And Operations

Capacity and historical usage answer different questions. `get_workspace_capacity` reports the current billing period, remaining limits, admission decisions, and blockers. `get_usage_report` reports an explicit date range with workspace and per-pipeline attribution. Do not compare mismatched periods without saying so.

## Capability Map

| User outcome | JoinLayer capabilities | Required proof before mutation or completion |
|---|---|---|
| Inspect a workspace | Identity, capacity, connection inventory, pipeline inventory | Expected workspace/identity; current scopes; explicit statement that nothing changed |
| Add a connection | Capability discovery, secure browser setup, connection test, schema discovery | User completed browser setup; safe connection ID; returned capabilities/schema |
| Build a pipeline | Route selection, mappings, constrained transforms, filters, enrichment, target write strategy | Real connection IDs and schemas; complete draft; unique idempotency key |
| Prove a design | Structured validation and representative preview | No blockers; warnings explained; target rows reviewed against business intent |
| Run data movement | Four run modes, checkpoint resume, retries, allocation options | Current capacity; exact options; effective workspace governance and, when required, one-time approval or matching automation permission |
| Monitor operations | Workspace activity, pipeline runs, progress, worker/queue/recovery state | Current run evidence, not only historical inventory status |
| Diagnose failures | Structured errors, events, run state, dead-letter summary | Stable error/stage; affected scope; classified data/configuration vs infrastructure cause |
| Explain usage | Current capacity plus arbitrary ISO date-range usage report | Period boundaries and independent used/remaining/limit dimensions |

## Complete MCP Tool Map

All names are stable MCP tools. Tool schemas are authoritative for exact arguments.

### Context, Capacity, And Discovery

| Tool | Scope | Use | Follow-up |
|---|---|---|---|
| `get_workspace_context` | `workspace:read` | Read workspace, delegated identity, user role, granted scopes, and effective start/stop approval settings | Stop on unexpected workspace; report scope gaps and follow `agent_governance` |
| `get_workspace_capacity` | `usage:read` | Read current-period limits, usage, remaining capacity, and run blockers | Call before proposing or starting execution |
| `list_connector_types` | `workspace:read` | Discover supported providers and source/target capabilities | Select only returned capabilities |
| `list_connections` | `connections:read` | Read paginated safe connection inventory and summary | Use returned IDs; fetch details when needed |
| `get_connection` | `connections:read` | Read one connection's safe metadata and capabilities | Discover schema before designing fields |
| `discover_connection_schema` | `connections:test` | List relational/BigQuery/ClickHouse namespaces and tables, inspect one table, or sample a Kafka topic | Relational/ClickHouse/BigQuery: progress from ID to schema/database/dataset to table. Kafka: pass topic as `table` with no schema |

### Secure Connection Setup

| Tool | Scope | Use | Follow-up |
|---|---|---|---|
| `create_connection_setup` | `connections:test` | Create a short-lived browser handoff using nonsecret fields | Give URL to user; never request credentials in chat |
| `get_connection_setup_status` | `connections:test` | Poll setup status and receive completed connection ID | Discover the completed connection schema |
| `list_connection_setups` | `connections:test` | Recover recent setup sessions after an uncertain response | Reuse the existing session instead of duplicating it |
| `cancel_connection_setup` | `connections:test` | Cancel an unused or unexpected setup capability | Confirm cancellation; create a new setup only if intended |
| `test_connection` | `connections:test` | Test a stored connection without exposing its secrets | Require every configured boundary: Kafka broker/topic/Registry, or ClickHouse transport/database and configured table access |

### Pipeline Inventory And Design

| Tool | Scope | Use | Follow-up |
|---|---|---|---|
| `list_pipelines` | `pipelines:read` | Read paginated pipeline inventory and lifecycle summary | Treat `inventory_state` as authoritative |
| `get_pipeline` | `pipelines:read` | Read complete safe pipeline design, setup, and runtime summary | Compare durable configuration with requested outcome |
| `create_pipeline_draft` | `pipelines:write` | Save a complete new pipeline contract | Validate; do not start from create response |
| `update_pipeline_draft` | `pipelines:write` | Replace the complete saved draft contract | Use a new idempotency key when body/intent changes; validate again |
| `validate_pipeline` | `pipelines:validate` | Run non-destructive setup and contract checks | Resolve blockers; explain warnings |
| `preview_pipeline` | `pipelines:validate` | Return representative transformed/enriched target rows | Check filters, nulls, types, mappings, enrichment, and business meaning |

### Approvals And Execution

| Tool | Scope | Use | Follow-up |
|---|---|---|---|
| `request_run_start_approval` | `runs:execute` | Request human approval for one exact pipeline revision and start options when workspace policy requires it | Honor `approval_required`; poll and do not alter approved options |
| `request_run_stop_approval` | `runs:control` | Request human approval to stop one exact run when workspace policy requires it | Honor `approval_required`; identify operational impact |
| `list_agent_approvals` | `runs:read` | Read this identity's pending and historical requests | Continue only with an approved, unexpired exact match |
| `cancel_agent_approval` | `runs:read` | Cancel this identity's stale or no-longer-needed pending request | Confirm cancellation |
| `start_pipeline` | `runs:execute` | Start/resume under workspace policy, with exact approved options or matching pipeline permission when required | Fetch returned run and verify durable state |
| `stop_run` | `runs:control` | Stop one active run under workspace policy, with approval or matching permission when required | Check `state_changed` and `stop_outcome`; `already_terminal` is an explicit no-op |

### Monitoring, Diagnosis, And Usage

| Tool | Scope | Use | Follow-up |
|---|---|---|---|
| `list_pipeline_runs` | `runs:read` | Read recent runs and recovery state for one pipeline | Select exact run ID; distinguish current from historical state |
| `get_run` | `runs:read` | Read progress, counters, lease/worker, recovery, and curated failures | Poll deliberately; do not create an unbounded loop |
| `list_activity` | `runs:read` | Read workspace activity with period/status/mode/pipeline filters | Narrow to pipeline and run evidence |
| `diagnose_run_failure` | `diagnostics:read` | Read run, structured errors/events, and dead-letter summaries | Classify cause and propose least destructive correction |
| `get_usage_report` | `usage:read` | Read workspace/per-pipeline usage for explicit ISO dates | State exact returned time boundaries |

## Task Playbooks

### Read-Only Workspace Inspection

1. Call `get_workspace_context`.
2. Call `get_workspace_capacity`.
3. Call `list_connections` and `list_pipelines`.
4. Fetch individual resources only when needed to answer the question.
5. Report identity, scopes, capacity, inventory, blockers, recommendation, and that no state changed.

### Add And Discover A Connection

1. Call `list_connector_types` and verify the requested role/capability exists.
2. Call `list_connections` to avoid duplicates.
3. Call `create_connection_setup` with a nonsecret name, type, and configuration template. Give the returned `setup_url` to the user; no standalone setup token is returned.
4. Ask the user to complete the browser form; do not ask them to paste secrets.
5. Poll `get_connection_setup_status` at a bounded interval.
6. Call `test_connection` with the completed connection ID. Stop on failure; do not bypass a failed provider or Registry check.
7. Call `get_connection`, then discover coordinates. For relational/ClickHouse/BigQuery, use ID only for schemas/databases/datasets, ID + returned namespace for tables, and ID + returned namespace/table for fields. For Kafka, pass the topic as `table` with no schema. Read `connector-contracts.md` for the complete provider-specific contract.

### Build Or Change A Pipeline

1. Establish context/capacity and translate the business outcome into records, keys, target behavior, and mode.
2. Resolve source, target, and lookup connections from inventory.
3. Discover all involved schemas.
4. Read `execution-model.md` and, for shaping, `data-shaping.md`.
5. Create/update the complete draft with a stable idempotency key.
6. Validate, correct with a new key, and validate again until no blockers remain.
7. Preview representative output and explain surprises/limitations.
8. Report the saved pipeline ID and evidence. Do not start unless requested and authorized.

The draft input uses the same canonical values returned by `get_pipeline`: `target_write_strategy` is one of `connector_default`, `insert`, `upsert`, or `replace`; `throttle_policy` is `delay`; and `error_policy` is `stop` or `skip`. Optional rate limits are `max_rows_per_second`, `max_rows_per_minute`, `max_bytes_per_second`, and `max_bytes_per_minute`; use `0` on update to clear a saved limit. Do not translate `upsert` to a client-only alias.

### Start Or Resume A Pipeline

1. Re-fetch pipeline, validation evidence, preview when relevant, and current capacity.
2. Explain source/target, write strategy, mode, checkpoint resume, retry/allocation options, and material risk.
3. Read the workspace governance policy. Request exact start approval when required unless a matching pipeline-specific permission exists; otherwise start without `approval_id`.
4. Poll `list_agent_approvals` without asking the user to copy an approval token.
5. Call `start_pipeline` once with the exact options, approval ID when required, and stable idempotency key.
6. Call `get_run`; report durable state and monitor to a meaningful finite or stable streaming state as requested.

### Monitor Or Diagnose

1. Use `list_activity` for workspace scope or `list_pipeline_runs` for one pipeline.
2. Call `get_run` for current progress, freshness, worker/queue, checkpoint, recovery attempts/limit, and failure state. A live lease does not clear previously consumed recovery attempts; checkpoint progress or 30 minutes of stable ownership does.
3. On failure, call `diagnose_run_failure` and read `operations.md`.
4. Correct deterministic configuration/data causes before retrying. Let bounded automatic recovery handle classified transient interruption.
5. Validate/preview any changed draft before another start.

## State And Evidence Rules

- In pipeline inventory, `inventory_state` is the authoritative lifecycle: `operational`, `ready`, `attention`, or `draft`.
- Runtime status belongs to runs/runtime summaries, not pipeline inventory lifecycle.
- A saved draft is not validated; validation is not preview; preview is not approval; approval is not execution; accepted execution is not completion.
- `scheduled` means a persisted recurring launch policy. A scheduled run is still a finite run with its own status.
- `realtime_with_backfill` is not plain realtime: a live CDC child and bounded historical children run together. Report both lanes, and never describe queued/running history as blocking live delivery when the realtime child is healthy.
- Report resource IDs returned by JoinLayer so another agent can resume without rediscovery.
- Use returned timestamps and period boundaries. Never call stale historical usage “current capacity.”

## Authorization And Errors

- `insufficient_scope` means authentication succeeded. Read `required_scopes` and `granted_scopes`, explain why the extra action is needed, and use incremental OAuth consent. Never request a token.
- `approval_required` means request an exact approval and wait for the user in JoinLayer.
- An invalid/expired/mismatched approval requires a new request for the current exact operation; never modify and reuse it.
- A capacity blocker is authoritative. Report its dimension/remediation and stop retrying.
- An invalid input or contract error means inspect the MCP tool schema and returned details. Never guess an undocumented field.
- A transport/auth dependency error is not proof that a mutation failed or succeeded. Re-read durable resource state before deciding whether an idempotent retry is safe.
- If MCP is unavailable or OAuth cannot complete, stop. Do not bypass the gateway with curl, direct API calls, database access, or user credentials.

## Completion Contract

Return a concise evidence-based summary containing:

1. workspace and agent identity;
2. requested outcome and constraints;
3. observed connections/pipelines/runs and their IDs;
4. durable changes, if any;
5. validation and preview evidence;
6. authorization path: read-only, workspace policy without approval, one-time approval, or named automation permission;
7. current capacity and runtime result;
8. warnings, untested cases, blockers, and next action;
9. an explicit statement of whether state changed.

Never include credentials, tokens, unrestricted logs, or unnecessary customer rows.
