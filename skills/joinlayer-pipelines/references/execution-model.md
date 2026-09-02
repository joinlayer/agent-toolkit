# Execution Model

## Mode Selection

| Need | Mode | Meaning |
|---|---|---|
| Copy current source rows once | `one_time` | Finite full load; rerun only by explicit user action |
| Run a recurring snapshot | `scheduled` | Finite run started by the scheduler using the saved cron/timezone policy |
| Process only future source changes | `realtime` | Long-lived CDC from a valid current or saved checkpoint |
| Keep new changes flowing while loading history | `realtime_with_backfill` | CDC starts at the captured boundary while spare workers copy coordinated historical chunks |

Do not use plain realtime when the user expects historical rows. Do not describe scheduled pipelines as idle or waiting for setup when their next run is valid.

## Authoring Sequence

1. Inspect connection capabilities and schemas.
2. Identify source/target tables and target write strategy.
3. Define mappings and shaping operations.
4. Create or update the complete draft contract.
5. Validate and preview.
6. Check workspace capacity.
7. Start only after approval when execution is persistent or high impact.

Keep a draft separate from a run. Updating a draft must not be represented as changing an already executing run unless the API reports that behavior.

## Idempotency

Generate an opaque key of 8–128 safe characters for each intended mutation. Reuse it after timeout only when method, resource, and payload are identical. Generate a new key when user intent or payload changes.

## Checkpoints

Use `checkpoint_mode=resume`. A resumed run continues from durable progress saved after successful target writes. Never clear or bypass a checkpoint to make an error disappear. Escalate when recovery would intentionally skip source history or duplicate target writes.

For `realtime_with_backfill`, verify that the returned group contains a
`realtime` child immediately as well as `backfill_chunk` children. Realtime may
remain healthy when one history chunk fails; retry only the failed chunks. If
the realtime child fails, treat the whole group as unsafe until JoinLayer has
fenced history and the user has completed the documented recovery.
