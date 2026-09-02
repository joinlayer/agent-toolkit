from __future__ import annotations

import hashlib
import json
import urllib.request
from pathlib import Path

from jsonschema import Draft7Validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_URL = "https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json"
SCHEMA_SHA256 = "3fba09590c99f61735d234822279f4223fab9e300c0a81e81c91ab62a4114de0"  # pragma: allowlist secret


def main() -> None:
    with urllib.request.urlopen(SCHEMA_URL, timeout=20) as response:
        raw_schema = response.read(512 * 1024 + 1)
    if len(raw_schema) > 512 * 1024:
        raise SystemExit("MCP Registry schema exceeds the validation size limit")
    if hashlib.sha256(raw_schema).hexdigest() != SCHEMA_SHA256:
        raise SystemExit("MCP Registry schema digest does not match the pinned version")

    schema = json.loads(raw_schema)
    server = json.loads((ROOT / "server.json").read_text(encoding="utf-8"))
    errors = sorted(Draft7Validator(schema).iter_errors(server), key=lambda error: list(error.path))
    if errors:
        rendered = [f"/{'/'.join(map(str, error.path))}: {error.message}" for error in errors]
        raise SystemExit("server.json validation failed:\n- " + "\n- ".join(rendered))
    print("server.json validation passed")


if __name__ == "__main__":
    main()
