from __future__ import annotations

import copy
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, WithJsonSchema, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CastRule(StrictModel):
    field: str = Field(min_length=1, max_length=256)
    type: Literal["string", "int64", "float64", "bool"]


class CastTransform(StrictModel):
    type: Literal["cast"]
    rules: list[CastRule] = Field(description="Cast rules; each field value must be unique within this transform.")

    @field_validator("rules")
    @classmethod
    def require_unique_fields(cls, value: list[CastRule]) -> list[CastRule]:
        fields = [rule.field for rule in value]
        if len(set(fields)) != len(fields):
            raise ValueError("cast rules must not contain duplicate fields")
        return value


class DeriveRuleBase(StrictModel):
    source_field: str = Field(min_length=1, max_length=256)
    target_field: str = Field(min_length=1, max_length=256)


class UnaryDeriveRule(DeriveRuleBase):
    function: Literal[
        "date_trunc_day",
        "date_trunc_hour",
        "trim",
        "lower",
        "upper",
    ]


JSONValue = Annotated[
    Any,
    WithJsonSchema({
        "oneOf": [
            {"type": "string"},
            {"type": "number"},
            {"type": "boolean"},
            {"type": "object", "additionalProperties": True},
            {"type": "array"},
        ]
    }),
]


class CoalesceDeriveRule(DeriveRuleBase):
    function: Literal["coalesce"]
    value: JSONValue | None = Field(default=None, description="Fallback JSON value used when the source value is null or blank.")


class NumericDeriveRule(DeriveRuleBase):
    function: Literal["multiply", "divide"]
    value: StrictInt | StrictFloat = Field(description="Required numeric multiplier or divisor.")


class RoundDeriveRule(DeriveRuleBase):
    function: Literal["round"]
    places: int = Field(default=0, ge=0, le=9)


DeriveRule = Annotated[
    UnaryDeriveRule | CoalesceDeriveRule | NumericDeriveRule | RoundDeriveRule,
    Field(discriminator="function"),
]


class DeriveTransform(StrictModel):
    type: Literal["derive"]
    derive_rules: list[DeriveRule] = Field(
        min_length=1,
        description="Calculated-field rules; each target_field value must be unique within this transform.",
    )

    @field_validator("derive_rules")
    @classmethod
    def require_unique_target_fields(cls, value: list[DeriveRule]) -> list[DeriveRule]:
        target_fields = [rule.target_field for rule in value]
        if len(set(target_fields)) != len(target_fields):
            raise ValueError("derive rules must not contain duplicate target fields")
        return value


class FilterTransform(StrictModel):
    model_config = ConfigDict(json_schema_extra={
        "allOf": [{
            "if": {
                "properties": {"op": {"enum": ["eq", "neq"]}},
                "required": ["op"],
            },
            "then": {"properties": {"values": {"maxItems": 1}}},
        }]
    })

    type: Literal["filter"]
    field: str = Field(min_length=1, max_length=256)
    op: Literal["eq", "neq", "in", "not_in"]
    values: list[Annotated[str, Field(min_length=1)]] = Field(
        min_length=1,
        description="One nonblank value for eq/neq; one or more nonblank values for in/not_in.",
    )

    @field_validator("values")
    @classmethod
    def require_nonblank_values(cls, value: list[str]) -> list[str]:
        if any(not item for item in value):
            raise ValueError("filter values must not contain blank strings")
        return value

    @model_validator(mode="after")
    def require_single_equality_value(self) -> FilterTransform:
        if self.op in {"eq", "neq"} and len(self.values) != 1:
            raise ValueError(f"{self.op} requires exactly one value")
        return self


class EnrichTransform(StrictModel):
    model_config = ConfigDict(json_schema_extra={
        "oneOf": [
            {
                "properties": {"lookup": {"type": "object"}},
                "required": ["lookup"],
                "not": {
                    "anyOf": [
                        {"properties": {"lookup_connection_id": {"type": "string"}}, "required": ["lookup_connection_id"]},
                        {"properties": {"lookup_schema": {"type": "string"}}, "required": ["lookup_schema"]},
                        {"properties": {"lookup_table": {"type": "string"}}, "required": ["lookup_table"]},
                        {"properties": {"lookup_key_field": {"type": "string"}}, "required": ["lookup_key_field"]},
                    ]
                },
            },
            {
                "properties": {
                    "lookup_connection_id": {"type": "string"},
                    "lookup_table": {"type": "string"},
                    "lookup_key_field": {"type": "string"},
                },
                "required": ["lookup_connection_id", "lookup_table", "lookup_key_field"],
                "not": {"properties": {"lookup": {"type": "object"}}, "required": ["lookup"]},
            },
        ],
        "allOf": [{
            "if": {
                "properties": {"lookup_cache_mode": {"const": "materialized"}},
                "required": ["lookup_cache_mode"],
            },
            "then": {
                "properties": {
                    "lookup_connection_id": {"type": "string"},
                    "lookup_table": {"type": "string"},
                    "lookup_key_field": {"type": "string"},
                },
                "required": ["lookup_connection_id", "lookup_table", "lookup_key_field"],
            },
        }],
    })

    type: Literal["enrich"]
    source_field: str = Field(min_length=1, max_length=256)
    target_fields: list[Annotated[str, Field(min_length=1)]] = Field(
        min_length=1,
        description="Nonblank, unique fields copied from the lookup result.",
    )
    lookup: dict[str, Any] | None = Field(
        default=None,
        description="Inline lookup keyed by source value; each value is a scalar for one target field or an object keyed by target field.",
    )
    lookup_connection_id: str | None = Field(default=None, min_length=1, max_length=128)
    lookup_schema: str | None = Field(default=None, min_length=1, max_length=256)
    lookup_table: str | None = Field(default=None, min_length=1, max_length=256)
    lookup_key_field: str | None = Field(default=None, min_length=1, max_length=256)
    on_miss: Literal["leave", "null", "fail", "skip"] = "leave"
    lookup_cache_mode: Literal["direct", "materialized"] = "direct"
    lookup_cache_ttl_seconds: int = Field(default=0, ge=0)

    @field_validator("target_fields")
    @classmethod
    def require_nonempty_target_fields(cls, value: list[str]) -> list[str]:
        if any(not field for field in value):
            raise ValueError("target_fields must not contain empty field names")
        if len(set(value)) != len(value):
            raise ValueError("target_fields must not contain duplicates")
        return value

    @model_validator(mode="after")
    def require_one_lookup_source(self) -> EnrichTransform:
        has_inline_lookup = self.lookup is not None
        has_table_lookup = self.lookup_connection_id is not None
        if has_inline_lookup == has_table_lookup:
            raise ValueError("provide exactly one of lookup or lookup_connection_id")
        if has_table_lookup:
            if self.lookup_table is None or self.lookup_key_field is None:
                raise ValueError("table enrichment requires lookup_table and lookup_key_field")
        elif any(value is not None for value in (self.lookup_schema, self.lookup_table, self.lookup_key_field)):
            raise ValueError("lookup_schema, lookup_table, and lookup_key_field require lookup_connection_id")
        if self.lookup_cache_mode == "materialized" and not has_table_lookup:
            raise ValueError("materialized lookup cache requires lookup_connection_id")
        return self


TransformConfig = Annotated[
    CastTransform | DeriveTransform | FilterTransform | EnrichTransform,
    Field(discriminator="type"),
]


class FieldMapping(StrictModel):
    source_field: str = Field(min_length=1, max_length=256)
    target_field: str = Field(min_length=1, max_length=256)
    transforms: tuple[()] = ()
    position: int = Field(ge=0, le=10000)


SimpleTargetField = Annotated[
    str,
    Field(
        min_length=1,
        max_length=256,
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
        description="A simple mapped target field name, not a SQL expression or qualified path.",
    ),
]


class BigQueryPartitioningOptions(StrictModel):
    mode: Literal["none", "ingestion_time", "field"] = "none"
    field: SimpleTargetField | None = Field(
        default=None,
        description="Required only for field partitioning; must be a mapped DATE or TIMESTAMP target field.",
    )
    granularity: Literal["HOUR", "DAY", "MONTH", "YEAR"] = "DAY"
    require_partition_filter: bool = False

    @model_validator(mode="after")
    def require_consistent_partitioning(self) -> BigQueryPartitioningOptions:
        if self.mode == "field" and self.field is None:
            raise ValueError("BigQuery field partitioning requires field")
        if self.mode != "field" and self.field is not None:
            raise ValueError("BigQuery partition field is allowed only when mode is field")
        if self.mode == "none" and self.require_partition_filter:
            raise ValueError("require_partition_filter requires BigQuery partitioning")
        return self


class BigQueryTargetSchemaOptions(StrictModel):
    partitioning: BigQueryPartitioningOptions | None = Field(
        default=None,
        description="Create-time BigQuery partition layout. Existing incompatible layouts follow workspace schema-evolution policy.",
    )
    clustering_fields: list[SimpleTargetField] | None = Field(
        default=None,
        max_length=4,
        description="Up to four unique mapped target fields used for BigQuery clustering.",
    )

    @field_validator("clustering_fields")
    @classmethod
    def require_unique_clustering_fields(cls, value: list[str] | None) -> list[str] | None:
        if value is not None and len(set(value)) != len(value):
            raise ValueError("BigQuery clustering fields must be unique")
        return value


class TargetSchemaOptions(StrictModel):
    bigquery: BigQueryTargetSchemaOptions | None = Field(
        default=None,
        description="BigQuery partitioning and clustering overrides for this pipeline.",
    )


class PipelineDraft(StrictModel):
    name: str = Field(min_length=1, max_length=256)
    source_connection_id: str = Field(min_length=1, max_length=128)
    target_connection_id: str = Field(min_length=1, max_length=128)
    source_schema: str | None = Field(
        default=None,
        max_length=256,
        description="Source database schema or ClickHouse database. Omit for Kafka.",
    )
    source_table: str | None = Field(
        default=None,
        max_length=256,
        description="Source table, including a ClickHouse snapshot table, or the Kafka source topic when source_schema is omitted.",
    )
    target_schema: str | None = Field(
        default=None,
        max_length=256,
        description="Target database schema, ClickHouse database, or BigQuery dataset. Omit for Kafka.",
    )
    target_table: str | None = Field(
        default=None,
        max_length=256,
        description="Target table, or the Kafka target topic when target_schema is omitted.",
    )
    auto_sync_target_schema: bool = Field(
        default=False,
        description="Plan and apply supported target table creation/additive schema evolution before execution.",
    )
    target_schema_options: TargetSchemaOptions | None = Field(
        default=None,
        description="Connector-specific target layout. Currently supports strict BigQuery partitioning and clustering options.",
    )
    max_rows_per_second: int | None = Field(default=None, ge=0)
    max_rows_per_minute: int | None = Field(default=None, ge=0)
    max_bytes_per_second: int | None = Field(default=None, ge=0)
    max_bytes_per_minute: int | None = Field(default=None, ge=0)
    throttle_policy: Literal["delay"] | None = None
    error_policy: Literal["stop", "skip"] | None = None
    transforms: list[TransformConfig] = Field(default_factory=list)
    run_mode: Literal["one_time", "realtime", "realtime_with_backfill", "scheduled"] = "one_time"
    schedule_enabled: bool = False
    schedule_interval_seconds: int | None = Field(default=None, ge=60)
    schedule_cron_expression: str | None = Field(default=None, max_length=256)
    schedule_timezone: str | None = Field(default=None, max_length=128)
    target_write_strategy: Literal["connector_default", "insert", "upsert", "replace"]
    target_primary_key_field: str | None = Field(default=None, max_length=256)
    field_mappings: list[FieldMapping] = Field(default_factory=list)

    @field_validator("source_schema", "source_table", "target_schema", "target_table", "schedule_cron_expression", "schedule_timezone", "target_primary_key_field")
    @classmethod
    def trim_optional_string(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None


class StartRunOptions(StrictModel):
    mode: Literal["one_time", "realtime", "realtime_with_backfill", "scheduled"]
    checkpoint_mode: Literal["resume"] = "resume"
    max_retries: int = Field(default=3, ge=0, le=5)
    retry_backoff_ms: int = Field(default=1000, ge=100, le=30000)
    requested_pool: str | None = Field(default=None, max_length=128)
    resource_class: str | None = Field(default=None, max_length=128)
    backfill_requested_pool: str | None = Field(default=None, max_length=128)
    backfill_resource_class: str | None = Field(default=None, max_length=128)
    backfill_chunk_column: str | None = Field(default=None, max_length=256)
    backfill_chunk_size: int | None = Field(default=None, ge=100, le=10_000_000)


class ActivityQuery(StrictModel):
    status: Literal["queued", "running", "succeeded", "failed", "cancelled", "stopped"] | None = None
    mode: Literal["one_time", "realtime", "realtime_with_backfill", "scheduled"] | None = None
    pipeline: str | None = Field(default=None, max_length=256)
    period: Literal["24h", "7d", "30d"] = "24h"
    limit: int = Field(default=50, ge=1, le=200)
    offset: int = Field(default=0, ge=0)


class ConnectionSetupDraft(StrictModel):
    name: str = Field(min_length=1, max_length=256)
    connection_type: str = Field(min_length=1, max_length=128)
    config_template: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-secret fields from list_connector_types.config_schema only. Omit every field marked secret and all secret placeholders.",
    )
    expires_in_minutes: int = Field(default=15, ge=5, le=30)


def _inline_model_schema(model: type[BaseModel]) -> dict[str, Any]:
    """Inline local JSON Schema refs for MCP clients that do not render $defs."""
    schema = model.model_json_schema()
    definitions = schema.get("$defs", {})

    def inline(value: Any) -> Any:
        if isinstance(value, list):
            return [inline(item) for item in value]
        if not isinstance(value, dict):
            return value
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            name = ref.removeprefix("#/$defs/")
            definition = definitions.get(name)
            if isinstance(definition, dict):
                replacement = copy.deepcopy(definition)
                replacement.update({key: item for key, item in value.items() if key != "$ref"})
                return inline(replacement)
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key == "$defs":
                continue
            if key == "discriminator" and isinstance(item, dict):
                # Pydantic's discriminator mapping points back into $defs. The
                # inlined oneOf branches already carry an exact `type` const,
                # so propertyName retains the discriminator without dangling
                # references that MCP clients cannot resolve.
                item = {nested_key: nested for nested_key, nested in item.items() if nested_key != "mapping"}
            result[key] = inline(item)
        return result

    result = inline(schema)
    if not isinstance(result, dict):  # pragma: no cover - Pydantic always returns an object schema
        raise TypeError("model JSON schema must be an object")
    return result


PipelineDraftInput = Annotated[PipelineDraft, WithJsonSchema(_inline_model_schema(PipelineDraft))]
StartRunOptionsInput = Annotated[StartRunOptions, WithJsonSchema(_inline_model_schema(StartRunOptions))]
ConnectionSetupDraftInput = Annotated[ConnectionSetupDraft, WithJsonSchema(_inline_model_schema(ConnectionSetupDraft))]
ActivityQueryInput = Annotated[ActivityQuery, WithJsonSchema(_inline_model_schema(ActivityQuery))]
