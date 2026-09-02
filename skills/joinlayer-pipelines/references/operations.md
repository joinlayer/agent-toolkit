# Operations

## Diagnose A Failure

1. Fetch the run with `get_run`.
2. Call `diagnose_run_failure` for structured errors, events, and dead-letter summary.
3. Separate configuration/data errors from infrastructure interruption.
4. Explain affected rows, durable progress, and whether automatic recovery is active.
5. Propose the least destructive correction.
6. Validate and preview changed configuration before another run.

Do not paste unrestricted logs or customer rows into the conversation. Use stable error codes and curated hints.

## Capacity

Call `get_workspace_capacity` before execution and when a run is queued or throttled. Report dimensions independently: worker slots, runs, rows read/written, cache bytes, target concurrency, or another returned limiter. Distinguish **used**, **remaining**, and **limit** values.

`remaining.worker_slots` is the workspace billing entitlement; it is not proof that a compatible process can claim a run. Check `placement.realtime.available_slots` for `realtime/stream` work and the corresponding one-time or batch placement for other modes. Respect `capabilities.can_start_realtime` and report `realtime_worker_capacity_unavailable` even when unrelated batch slots are free.

Do not repeatedly start runs against a hard blocker. Ask the user to stop competing work, reduce the operation, wait for period reset, or change plan/capacity according to the returned remediation.

## Recovery

- Resume from the durable checkpoint after transient worker/network failure.
- Treat repeated deterministic data failures as configuration incidents, not retryable infrastructure errors.
- Stop an active run before changing configuration when required by validation.
- Explain whether a stop is graceful and whether a future resume preserves progress.
- Never promise exactly-once behavior unless the source checkpoint and target write strategy reported by JoinLayer support it.

## Monitoring

Use `list_activity` for workspace outcomes and `list_pipeline_runs` for one pipeline. Historical failed outcomes do not necessarily mean the current realtime stream is unhealthy. Verify current run status, `runtime_health`, recent progress, errors, and checkpoint/recovery state together. `stalled` means a run marked running has no assigned worker, no current lease, or an expired lease without completed recovery. `waiting_for_worker` means a realtime run remained unclaimed for more than 90 seconds. Neither is operational success, and a durable `running` status never overrides these runtime-health incidents. Do not start a duplicate realtime run while diagnosing either condition.

Read `recovery_attempts` together with `recovery_limit` even after ownership is
restored. Attempts remain consumed until the durable checkpoint advances or an
idle stream holds uninterrupted ownership for 30 minutes. At half the budget,
report a crash-loop risk and ask the operator to inspect worker crash, OOM, and
runtime connectivity evidence. `realtime_recovery_exhausted` is terminal: fix
the cause before requesting approval for a deliberate replacement from the
committed checkpoint.

Read `agent_governance.require_run_start_approval` and `require_run_stop_approval` from `get_workspace_context` before requesting approval. If the applicable value is false, the workspace administrator has deliberately enabled immediate execution: call the exact start or stop tool without `approval_id`, while still enforcing user intent, scopes, role, capacity, validation, idempotency, and verification. If an approval tool returns `approval_required=false`, follow its `next_action` instead of creating another request.

When an approval request is required and created, present its `approval_url` and `next_action` to the workspace administrator. State explicitly that approving authorizes the agent's next exact submission and does not execute the command by itself. JoinLayer also surfaces pending commands in the workspace banner, but never make the administrator search for one when the direct URL is available. If `start_pipeline` returns `approval_pending`, keep the exact operation and idempotency key unchanged, poll `list_agent_approvals` at a bounded interval, and retry only after the approval is `approved`. Do not treat a pending request as expired or request a duplicate approval.

For `stop_run`, inspect `state_changed` and `stop_outcome`. `already_terminal` means the run had already completed and the stop was an idempotent no-op; report that outcome instead of claiming that the call stopped it.

For pipeline inventory, treat `inventory_state` as the authoritative lifecycle. The `state` filter and `summary` counts use the same values: `operational`, `ready`, `attention`, and `draft`. Runtime status is separate and comes from runtime summaries and run tools.
