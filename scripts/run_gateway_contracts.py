from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_MCP_URL = "https://mcp.joinlayer.app/mcp"
DEPLOYMENT_MCP_URL = "https://replace-with-your-joinlayer-host.example/mcp"


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="joinlayer-public-contract-") as temporary:
        fixture_root = Path(temporary)
        shutil.copytree(ROOT / "mcp-gateway", fixture_root / "mcp-gateway")
        shutil.copytree(ROOT / "skills", fixture_root / "skills")

        for relative in (
            Path("skills/joinlayer-pipelines/SKILL.md"),
            Path("skills/joinlayer-pipelines/agents/openai.yaml"),
        ):
            path = fixture_root / relative
            content = path.read_text(encoding="utf-8")
            replaced = content.replace(PUBLIC_MCP_URL, DEPLOYMENT_MCP_URL)
            if replaced == content:
                raise SystemExit(f"public MCP URL is missing from {relative}")
            path.write_text(replaced, encoding="utf-8")

        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(fixture_root / "mcp-gateway")
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "-s",
                str(fixture_root / "mcp-gateway" / "tests"),
                "-v",
            ],
            cwd=fixture_root,
            env=environment,
            check=False,
        )
        if completed.returncode != 0:
            raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
