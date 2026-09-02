# Run Approval And Control

Use [`control-a-run.md`](../prompts/control-a-run.md) for starts, checkpoint-safe resumes, and stops.

The agent reads the workspace approval policy rather than assuming every command needs approval. When approval is enabled, the request binds the exact pipeline revision, action, mode, checkpoint behavior, and options. The agent gives the administrator the returned approval URL and waits for the durable request to become approved. A pending approval is not permission and an approved request has not executed anything by itself.

When approval is disabled or a matching pipeline-specific automation permission applies, the agent submits the exact command without inventing a fake approval. OAuth scopes, current membership, capacity, validation, idempotency, and audit enforcement still apply.

Completion requires the durable run ID and current state. Realtime success additionally requires current worker ownership, heartbeat, lease, and checkpoint evidence. A terminal stop that changes nothing must be reported as a no-op rather than a new state transition.
