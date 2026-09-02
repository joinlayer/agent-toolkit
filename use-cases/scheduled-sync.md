# Scheduled Synchronization

Use this for recurring finite loads.

The agent must confirm schedule timezone, cadence, expected source window, write idempotency, overlap behavior, and capacity. A saved schedule is not proof that a run executed. After enabling it, verify the durable pipeline schedule and inspect the first scheduled run before declaring success.

Use [`validate-before-run.md`](../prompts/validate-before-run.md) before enabling the schedule. Stop if the selected write strategy can duplicate or replace data in a way the user has not explicitly accepted.
