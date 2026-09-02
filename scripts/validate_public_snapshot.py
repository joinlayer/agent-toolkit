from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {"", ".json", ".lock", ".md", ".py", ".txt", ".yaml", ".yml"}
REVIEWED_BINARY_SHA256 = {
    "assets/joinlayer-glyph-dark.png": "7bf3871f7f36cdefd7c7ac672e9a359269127a905c66432a3a1622306d4a8078",  # pragma: allowlist secret
    "assets/joinlayer-glyph-white.png": "dcc6663e77c3a52f687e955dc0418e8f4b36d54eaefb72d62a1c6edf284b1d66",  # pragma: allowlist secret
    "plugins/joinlayer/assets/joinlayer-glyph-dark.png": "7bf3871f7f36cdefd7c7ac672e9a359269127a905c66432a3a1622306d4a8078",  # pragma: allowlist secret
    "plugins/joinlayer/assets/joinlayer-glyph-white.png": "dcc6663e77c3a52f687e955dc0418e8f4b36d54eaefb72d62a1c6edf284b1d66",  # pragma: allowlist secret
}
FORBIDDEN_PATH_PARTS = {"__pycache__", ".venv", ".env"}
MAX_PUBLIC_FILE_BYTES = 1024 * 1024
FORBIDDEN_PATTERNS = {
    "private host address": re.compile(r"\b(?:10|169\.254|172\.(?:1[6-9]|2\d|3[01])|192\.168)\.\d{1,3}\.\d{1,3}\b"),
    "JoinLayer demo identifier": re.compile(
        r"\b(?:org-" + "demo|user_" + "demo|agt_" + "demo|conn_" + "demo|pipe_" + "demo|run_[0-9a-f]{12,})\b"
    ),
    "demo fixture key": re.compile(r"\bpipeline-" + "demo(?:-|\b)"),
    "legacy bearer": re.compile(r"\bjla_[A-Za-z0-9_-]{12,}\b"),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
}
MCP_FIXTURE_LABELS = {"JoinLayer demo identifier", "demo fixture key"}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_sha256(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): sha256(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def main() -> None:
    failures: list[str] = []
    for path in sorted(ROOT.rglob("*")):
        relative = path.relative_to(ROOT)
        if ".git" in relative.parts:
            continue
        if path.is_symlink():
            failures.append(f"symbolic link is not allowed: {relative}")
            continue
        if path.is_dir():
            if any(part in FORBIDDEN_PATH_PARTS for part in relative.parts):
                failures.append(f"forbidden generated path: {relative}")
            continue
        if any(part in FORBIDDEN_PATH_PARTS for part in relative.parts):
            failures.append(f"forbidden file path: {relative}")
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES:
            expected_digest = REVIEWED_BINARY_SHA256.get(relative.as_posix())
            if expected_digest is None:
                failures.append(f"unreviewed binary file type: {relative}")
            elif sha256(path) != expected_digest:
                failures.append(f"reviewed binary digest changed: {relative}")
            continue
        if path.stat().st_size > MAX_PUBLIC_FILE_BYTES:
            failures.append(f"public file exceeds size limit: {relative}")
            continue
        text = path.read_text(encoding="utf-8")
        for label, pattern in FORBIDDEN_PATTERNS.items():
            if relative.parts[0] == "mcp-gateway" and label in MCP_FIXTURE_LABELS:
                # Gateway tests are published byte-for-byte with the private
                # build source. Their explicit demo/example fixture names are
                # synthetic contract data, not deployment or customer data.
                continue
            if pattern.search(text):
                failures.append(f"{label}: {relative}")

    plugin = json.loads((ROOT / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8"))
    claude_plugin = json.loads((ROOT / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))
    claude_marketplace = json.loads((ROOT / ".claude-plugin" / "marketplace.json").read_text(encoding="utf-8"))
    codex_marketplace = json.loads((ROOT / ".agents" / "plugins" / "marketplace.json").read_text(encoding="utf-8"))
    mcp = json.loads((ROOT / ".mcp.json").read_text(encoding="utf-8"))
    server = json.loads((ROOT / "server.json").read_text(encoding="utf-8"))
    expected_url = "https://mcp.joinlayer.app/mcp"
    if plugin.get("name") != "joinlayer" or plugin.get("mcpServers") != "./.mcp.json":
        failures.append("plugin manifest does not bind the JoinLayer MCP companion")
    if not re.fullmatch(r"\d+\.\d+\.\d+", str(plugin.get("version") or "")):
        failures.append("plugin version is not strict semantic versioning")
    for field in ("homepage", "repository"):
        if not str(plugin.get(field) or "").startswith("https://"):
            failures.append(f"plugin {field} is not an absolute HTTPS URL")
    interface = plugin.get("interface") or {}
    for field in ("displayName", "shortDescription", "longDescription", "developerName", "category", "defaultPrompt"):
        if not interface.get(field):
            failures.append(f"plugin interface is missing {field}")
    if not (ROOT / str(plugin.get("skills") or "missing")).is_dir():
        failures.append("plugin skills directory does not exist")
    if not (ROOT / str(plugin.get("mcpServers") or "missing")).is_file():
        failures.append("plugin MCP companion does not exist")
    for field in ("composerIcon", "logo", "logoDark"):
        asset = ROOT / str(interface.get(field) or "missing")
        if not asset.is_file():
            failures.append(f"plugin interface asset does not exist: {field}")
    if not re.fullmatch(r"#[0-9A-Fa-f]{6}", str(interface.get("brandColor") or "")):
        failures.append("plugin interface brandColor is not a six-digit hex color")
    for field in ("name", "version", "description", "homepage", "repository", "license", "skills", "mcpServers"):
        if claude_plugin.get(field) != plugin.get(field):
            failures.append(f"Claude and Codex plugin metadata differ for {field}")
    claude_source = (claude_marketplace.get("plugins") or [{}])[0]
    if claude_marketplace.get("name") != "joinlayer" or claude_source.get("name") != "joinlayer":
        failures.append("Claude marketplace identity is not canonical")
    if claude_source.get("source") != {"source": "github", "repo": "joinlayer/agent-toolkit"}:
        failures.append("Claude marketplace does not install the canonical repository")
    expected_codex_entry = {
        "name": "joinlayer",
        "source": {"source": "local", "path": "./plugins/joinlayer"},
        "policy": {"installation": "AVAILABLE", "authentication": "ON_INSTALL"},
        "category": "Developer Tools",
    }
    if codex_marketplace.get("name") != "joinlayer":
        failures.append("Codex marketplace identity is not canonical")
    if (codex_marketplace.get("interface") or {}).get("displayName") != "JoinLayer":
        failures.append("Codex marketplace display name is not canonical")
    if codex_marketplace.get("plugins") != [expected_codex_entry]:
        failures.append("Codex marketplace plugin contract is not canonical")
    if mcp.get("mcpServers", {}).get("joinlayer", {}).get("url") != expected_url:
        failures.append("plugin MCP URL is not canonical")
    if server.get("remotes", [{}])[0].get("url") != expected_url:
        failures.append("server.json MCP URL is not canonical")
    if server.get("version") != plugin.get("version"):
        failures.append("MCP Registry and plugin versions differ")

    plugin_root = ROOT / "plugins" / "joinlayer"
    synchronized_files = (
        (ROOT / ".codex-plugin" / "plugin.json", plugin_root / ".codex-plugin" / "plugin.json"),
        (ROOT / ".mcp.json", plugin_root / ".mcp.json"),
        (ROOT / "assets" / "joinlayer-glyph-dark.png", plugin_root / "assets" / "joinlayer-glyph-dark.png"),
        (ROOT / "assets" / "joinlayer-glyph-white.png", plugin_root / "assets" / "joinlayer-glyph-white.png"),
    )
    for root_file, plugin_file in synchronized_files:
        if not plugin_file.is_file() or sha256(root_file) != sha256(plugin_file):
            failures.append(f"repo plugin copy differs: {plugin_file.relative_to(ROOT)}")
    if tree_sha256(ROOT / "skills") != tree_sha256(plugin_root / "skills"):
        failures.append("repo plugin skill copy differs from canonical root skill")

    gateway_server = (ROOT / "mcp-gateway" / "joinlayer_mcp" / "server.py").read_text(encoding="utf-8")
    if "@mcp.tool()" in gateway_server:
        failures.append("MCP tool is missing explicit safety annotations")

    skill_path = ROOT / "skills" / "joinlayer-pipelines" / "SKILL.md"
    skill_text = skill_path.read_text(encoding="utf-8")
    match = re.match(r"^---\n(.*?)\n---\n", skill_text, flags=re.DOTALL)
    if not match:
        failures.append("skill frontmatter is missing")
    else:
        metadata = yaml.safe_load(match.group(1)) or {}
        if set(metadata) != {"name", "description"} or metadata.get("name") != "joinlayer-pipelines":
            failures.append("skill frontmatter contract is invalid")
    if "[TODO:" in "\n".join(path.read_text(encoding="utf-8") for path in ROOT.rglob("*.md")):
        failures.append("public content contains TODO placeholders")

    if failures:
        raise SystemExit("public snapshot validation failed:\n- " + "\n- ".join(failures))
    print("public snapshot validation passed")


if __name__ == "__main__":
    main()
