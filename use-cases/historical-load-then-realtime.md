# Historical Load Then Realtime

Use this when history must be copied before continuing from database change events.

## Start Prompt

```text
Use $joinlayer-pipelines. Design a realtime-with-backfill pipeline for the
source and target I identify. Discover CDC and target capabilities, verify the
single-table stream contract, explain snapshot-to-stream handoff and checkpoint
semantics, then draft, validate, and preview. Do not start until I review the
target effects, capacity, and approval path.
```

## Required Evidence

- source connector advertises the required CDC mode for the exact table;
- source stream identity and target write key are unambiguous;
- validation and preview succeed;
- realtime capacity is available and persistent entitlement is present;
- the user understands that resume preserves the durable checkpoint;
- a healthy run eventually has a current worker, heartbeat, lease, advancing checkpoint, and no recovery exhaustion.

Never substitute restart-from-current for resume or start a second run while one is queued, running, waiting for a worker, or stalled.
