from __future__ import annotations

import hashlib
import json
import subprocess
import sys


REVIEWED_SYNTHETIC_FINDINGS = {
    (
        "mcp-gateway/tests/test_gateway.py",
        "Secret Keyword",
        hashlib.sha1(("pass" + "word").encode()).hexdigest(),  # noqa: S324 - scanner fingerprint
    ),
    (
        "mcp-gateway/tests/test_gateway.py",
        "Basic Auth Credentials",
        hashlib.sha1(("pa" + "ss").encode()).hexdigest(),  # noqa: S324 - scanner fingerprint
    ),
}


def main() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "detect_secrets",
            "scan",
            "--all-files",
            "--exclude-files",
            r"(^|/)\.git(/|$)",
            ".",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no diagnostic output"
        raise SystemExit(f"secret scanner failed to execute:\n{detail}")
    result = json.loads(completed.stdout)
    findings = result.get("results", {})
    observed = {
        (path, item.get("type", ""), item.get("hashed_secret", ""))
        for path, items in findings.items()
        for item in items
    }
    unexpected = observed - REVIEWED_SYNTHETIC_FINDINGS
    missing = REVIEWED_SYNTHETIC_FINDINGS - observed
    if unexpected or missing:
        rendered = [f"unexpected finding: {item}" for item in sorted(unexpected)]
        rendered.extend(f"stale reviewed finding: {item}" for item in sorted(missing))
        raise SystemExit("secret scan failed:\n- " + "\n- ".join(rendered))
    print(f"secret scan passed ({len(observed)} reviewed synthetic gateway findings)")


if __name__ == "__main__":
    main()
