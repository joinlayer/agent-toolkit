# Capacity And Recovery

Use [`explain-capacity.md`](../prompts/explain-capacity.md), [`diagnose-failed-run.md`](../prompts/diagnose-failed-run.md), or [`recover-realtime-pipeline.md`](../prompts/recover-realtime-pipeline.md).

The agent separates subscription limits, usage-period limits, active-run limits, physical worker availability, worker compatibility, and pipeline-specific validation. It reports returned values instead of guessing infrastructure state. Recovery must preserve the latest durable checkpoint and avoid duplicate active runs.

A `running` durable status is insufficient evidence for realtime health. Current worker ownership, heartbeat, lease, and checkpoint progress are required. A queued run without ownership beyond the product threshold is an incident even when workspace capacity otherwise says ready.
