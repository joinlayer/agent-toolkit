# Kafka, BigQuery, And ClickHouse Connector Contracts

Use this reference after `list_connector_types` returns `kafka_source`, `kafka_target`, `bigquery`, `clickhouse_source`, or `clickhouse_target`. The returned provider `config_schema` is authoritative for current field names, allowed values, defaults, capabilities, and `secret` flags.

## Secure Connection Setup

1. Call `list_connector_types` and select the exact provider role.
2. Put only non-secret fields returned by that provider's `config_schema` into `create_connection_setup.config_template`.
3. Omit every field marked `secret`, even if the user already has a credential. Never send `***` or another placeholder.
4. Give the short-lived setup URL to the user. They enter Kafka SASL, Schema Registry, Google, ClickHouse, and certificate credentials in JoinLayer's browser form.
5. Poll setup status and fetch the completed connection ID.
6. Call `test_connection(connection_id)`. It uses the stored server-side secrets; never reconstruct credentials in MCP arguments.
7. Only after the test passes, discover the connection's schema/topic contract.

## Kafka

### Coordinates And Modes

- Kafka has no route-level schema. In a pipeline, omit `source_schema`/`target_schema` and put the topic in `source_table`/`target_table`.
- To discover Kafka fields, call `discover_connection_schema(connection_id, table=topic)` with no `schema`.
- A Kafka source supports realtime execution, not one-time/scheduled snapshots or preview. Do not use preview as a substitute for discovery.
- A Kafka target can receive any mode supported by its source.
- Topics, partitions, retention, and ACLs must already exist; JoinLayer does not auto-create topics.

### JSON And Avro

Read `value_format` from the provider schema:

- `json_object`: values must decode to JSON objects. Discovery samples bounded messages without joining a consumer group or committing offsets. An empty topic returns `schema_sample_not_found`.
- `avro`: `value_mode` is `after`. Select `avro_schema_source=inline` or `schema_registry` and choose `avro_wire_format=raw` or `confluent`.

For `inline`, supply the non-secret `avro_schema_json`. Confluent framing also requires the positive `avro_schema_id` that JoinLayer must validate/write.

For `schema_registry`, the safe template can include `schema_registry_url`, `schema_registry_auth_mode`, `avro_subject`, `avro_schema_version`, and `avro_wire_format`. Omit registry password/token/custom-CA fields because the provider marks them secret. The URL must use HTTPS except for loopback development. Version is `latest` or a positive number.

The subject/version must already exist. `test_connection` first verifies broker/topic metadata, then loads and parses this subject/version from Registry. JoinLayer repeats resolution when opening the connection; it does not register or mutate schemas. A Confluent-framed source resolves the schema ID carried by each message and caches parsed schemas, so compatible producer evolution can use multiple registered IDs. A target resolves the configured subject/version and writes its returned schema ID.

### Consistency

Kafka source processing is fetch -> target write -> durable JoinLayer checkpoint -> Kafka offset commit. Delivery is at-least-once across a crash between target write and offset commit. On recovery, the durable JoinLayer checkpoint is authoritative: the prior consumer group must be inactive, all checkpointed offsets must still be retained, and the complete group offset map is reconciled before reading. `start_offset` applies only to the initial durable fence; partitions added later begin at their first retained record and receive their own durable fence before data is returned. Never advise changing JoinLayer-managed group offsets externally. Prefer an idempotent target/upsert or downstream dedupe when duplicates are unacceptable. Kafka target uses synchronous acknowledgements; prefer `required_acks=all` and a stable `key_field`.

## BigQuery

### Connection And Coordinates

- Use `bigquery` as a target. `target_schema` is the dataset and `target_table` is the table.
- Discover datasets with connection ID only, tables with connection ID + dataset, and fields with connection ID + dataset + table.
- The dataset must already exist. With `auto_sync_target_schema=true`, JoinLayer can create a missing table and add compatible nullable columns.
- Use `target_write_strategy=connector_default`; BigQuery delivery method belongs to the connection (`storage_write`, `insert_all`, or `batch_load`). Read current values from `list_connector_types` rather than guessing them.
- For batch load, non-secret Cloud Storage bucket/path fields may be templated; the browser collects Google credentials.

### Partitioning And Clustering

Set create-time table layout through the strict pipeline field:

```json
{
  "target_schema_options": {
    "bigquery": {
      "partitioning": {
        "mode": "field",
        "field": "event_at",
        "granularity": "DAY",
        "require_partition_filter": true
      },
      "clustering_fields": ["customer_id", "region"]
    }
  }
}
```

Partition mode is `none`, `ingestion_time`, or `field`. Field partitioning requires one simple mapped DATE/TIMESTAMP target field; `HOUR` requires TIMESTAMP. Granularity is `HOUR`, `DAY`, `MONTH`, or `YEAR`. Clustering accepts at most four unique simple mapped target fields.

Validate after setting layout. Existing incompatible schema/layout follows the workspace BigQuery evolution policy: validation either blocks it or plans a new versioned table while retaining the old table. Never claim that JoinLayer creates datasets, drops/renames/narrows columns, or auto-registers an external schema.

## ClickHouse

### Source And Target Roles

- Use `clickhouse_source` only for complete `one_time` or `scheduled` snapshots. It advertises `batch_read` and `preview`, never `stream_read`, `confirm`, realtime, or realtime-with-backfill.
- For a source, `source_schema` is the ClickHouse database and `source_table` is the table. Every scheduled occurrence rereads the full table; do not describe it as incremental polling.
- One source run is one consistent query session. If `clickhouse_snapshot_restart_required` occurs, explain that JoinLayer deliberately refused a cross-session resume. Inspect partial target writes and start a new run; never clear or forge a checkpoint to continue.
- `target_schema` is the ClickHouse database and `target_table` is the table.
- Discover databases with the connection ID only, tables with connection ID + database, and fields with connection ID + database + table.
- Use `target_write_strategy=connector_default`. JoinLayer's ClickHouse contract is append-only and preserves `_operation`, `_deleted`, `_pipeline_id`, `_source`, `_lsn`, and `_event_time` metadata.
- A ClickHouse target can receive historical batches or realtime events from another supported source. That does not make `clickhouse_source` a realtime connector.

### Connection And Schema

Read `protocol` from the provider schema: `native` is recommended; `http` is supported. TLS is on by default, a custom CA is a secret setup field, and certificate verification must remain on for production data. The browser form automatically aligns conventional ports when protocol/TLS changes; agents must still use the values returned by `config_schema` and the stored connection test.

The database must already exist. With `auto_sync_target_schema=true`, JoinLayer can create a missing table as `MergeTree ORDER BY tuple()`, enforce the minimum non-replicated insert-deduplication window, and add missing nullable columns. Existing writable targets must use a MergeTree-family engine so retry deduplication is available. Incompatible types, drops, renames, narrowing, engine changes, ordering-key changes, and partitioning changes are blocked rather than silently mutated.

### Consistency

JoinLayer uses synchronous acknowledged inserts and a stable ClickHouse `insert_deduplication_token` derived from the durable pipeline checkpoint. A retry of the same batch is deduplicated by a MergeTree-family target within ClickHouse's effective replicated/shared or non-replicated deduplication window. Direct ClickHouse-to-ClickHouse mappings preserve numeric width, signedness, precision, and scale. Incompatible types or nullability, and unmapped non-nullable columns without an explicit ClickHouse default, block the run instead of coercing values. The runtime refreshes the live target schema before each batch and disables implicit NULL-to-default conversion. Do not claim global exactly-once delivery outside that provider contract. Do not model ClickHouse `ReplacingMergeTree` as synchronous upsert: background merges are eventual and queries may observe multiple versions.

A ClickHouse target supports standalone historical and realtime writes from capable sources. A ClickHouse source remains finite. Do not request `realtime_with_backfill` for ClickHouse: concurrent history plus realtime currently requires a CDC-capable source and a transactional PostgreSQL or MySQL target fence.
