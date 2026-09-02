# Data Shaping

## Mapping

Map every value that must reach target output. A source field is read from the source row. An enriched field exists only after a lookup. A calculated field exists only after its function/expression. Verify target names and types through discovery or target schema planning; do not infer them from labels.

## Filters

Filters decide whether a source record proceeds to shaping and target output. Confirm operator and value types. Multiple conditions currently use AND semantics unless the product capability explicitly reports otherwise. Preview both matching and excluded cases when possible.

## Enrichment

An enrichment uses:

- a source lookup key;
- a stored lookup connection and table;
- a lookup key field;
- explicit output fields;
- an on-miss policy;
- direct or materialized lookup mode when supported.

The lookup key does not need to appear in target output. Every desired lookup result does need a target field mapping. Use direct lookup for live point reads and materialized DuckDB lookup for a refreshed snapshot when the configured plan/capacity permits it. Never present cached lookup data as continuously current; state its refresh interval.

## Calculated Fields

Use only functions exposed by the pipeline contract. Preserve null semantics and validate output types. Do not invent SQL expressions or connector-specific syntax when the schema offers a constrained function contract.

## Target Writes

- `connector_default`: defer to the target connector's declared default strategy.
- `insert`: append new rows; unsuitable when retries can create duplicates without a durable key.
- `upsert`: insert or update by the configured target key.
- `replace`: destructive replacement; require explicit approval and validation of target scope.

Check required target columns and constraints before execution. A successful preview does not prove a target write will satisfy every database constraint unless validation reports those constraints as checked.
