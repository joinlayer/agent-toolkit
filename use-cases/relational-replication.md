# Relational Table Replication

Use this when a customer wants a one-time or scheduled copy between supported relational connections.

## Start Prompt

Use [`create-new-target-table.md`](../prompts/create-new-target-table.md) for a new table, or [`create-pipeline.md`](../prompts/create-pipeline.md) for an existing destination.

## Agent Path

1. Verify workspace identity, scopes, and capacity.
2. Discover source and target connector capabilities; never infer table-creation support from provider name.
3. List stored connections and discover schema, table, and fields using returned identifiers.
4. Clarify destination table, write strategy, row identity, mappings, and schedule.
5. Save a draft with a stable idempotency key.
6. Validate, preview, and explain output and target effects.
7. Start only after the user requests execution and the workspace approval policy is satisfied.
8. Verify durable run completion and destination row evidence.

Stop on ambiguous table identity, unsupported creation/evolution, validation blockers, surprising preview output, insufficient capacity, or an approval mismatch.
