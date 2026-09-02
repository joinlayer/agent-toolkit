from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_PROMPTS = {
    "inspect-workspace.md",
    "create-pipeline.md",
    "create-new-target-table.md",
    "enrich-pipeline.md",
    "validate-before-run.md",
    "diagnose-failed-run.md",
    "recover-realtime-pipeline.md",
    "explain-capacity.md",
    "control-a-run.md",
    "connect-a-database.md",
    "report-usage.md",
}
REQUIRED_USE_CASES = {
    "relational-replication.md",
    "historical-load-then-realtime.md",
    "scheduled-sync.md",
    "lookup-enrichment.md",
    "capacity-and-recovery.md",
    "run-approval-and-control.md",
    "secure-connection-setup.md",
    "usage-and-capacity.md",
}
FORBIDDEN_GUIDANCE = ("paste your token", "send me your password", "authorization: bearer", "checkpoint_mode=restart")
MARKDOWN_LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
START_HERE_LINKS = {
    "prompts/create-pipeline.md",
    "prompts/connect-a-database.md",
    "prompts/create-new-target-table.md",
    "use-cases/historical-load-then-realtime.md",
    "prompts/enrich-pipeline.md",
    "prompts/validate-before-run.md",
    "prompts/control-a-run.md",
    "prompts/diagnose-failed-run.md",
    "prompts/recover-realtime-pipeline.md",
    "prompts/explain-capacity.md",
    "prompts/report-usage.md",
}


def main() -> None:
    failures: list[str] = []
    prompt_files = {path.name for path in (ROOT / "prompts").glob("*.md")}
    use_case_files = {path.name for path in (ROOT / "use-cases").glob("*.md")}
    if missing := REQUIRED_PROMPTS - prompt_files:
        failures.append(f"missing prompts: {sorted(missing)}")
    if missing := REQUIRED_USE_CASES - use_case_files:
        failures.append(f"missing use cases: {sorted(missing)}")

    start_here = (ROOT / "START_HERE.md").read_text(encoding="utf-8")
    if "$joinlayer-pipelines" not in start_here:
        failures.append("START_HERE.md does not provide a directly copyable skill prompt")
    for relative in sorted(START_HERE_LINKS):
        if relative not in start_here:
            failures.append(f"START_HERE.md does not link {relative}")
        if not (ROOT / relative).is_file():
            failures.append(f"START_HERE.md target is missing: {relative}")
    if "START_HERE.md" not in (ROOT / "README.md").read_text(encoding="utf-8"):
        failures.append("README.md does not lead users to START_HERE.md")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    required_install_fragments = (
        "codex plugin marketplace add joinlayer/agent-toolkit",
        "codex plugin add joinlayer@joinlayer",
        "codex mcp add joinlayer",
        "claude plugin marketplace add joinlayer/agent-toolkit",
        "claude plugin install joinlayer@joinlayer",
        "npx skills add https://github.com/joinlayer/agent-toolkit --skill joinlayer-pipelines",
    )
    for fragment in required_install_fragments:
        if fragment not in readme:
            failures.append(f"README.md is missing install path: {fragment}")

    for directory in (ROOT / "prompts", ROOT / "use-cases"):
        for path in sorted(directory.glob("*.md")):
            text = path.read_text(encoding="utf-8")
            lowered = text.lower()
            if "$joinlayer-pipelines" not in text and directory.name == "prompts":
                failures.append(f"prompt does not invoke the skill: {path.relative_to(ROOT)}")
            if directory.name == "prompts" and "```text" not in text:
                failures.append(f"prompt is not directly copyable: {path.relative_to(ROOT)}")
            for phrase in FORBIDDEN_GUIDANCE:
                if phrase in lowered:
                    failures.append(f"unsafe guidance {phrase!r}: {path.relative_to(ROOT)}")

    for path in sorted(ROOT.rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK.findall(text):
            target = raw_target.strip().split(maxsplit=1)[0].strip("<>")
            if target.startswith(("https://", "http://", "mailto:", "#")):
                continue
            relative_target = unquote(target.split("#", 1)[0])
            if relative_target and not (path.parent / relative_target).resolve().exists():
                failures.append(
                    f"broken relative link {target!r}: {path.relative_to(ROOT)}"
                )

    if failures:
        raise SystemExit("content validation failed:\n- " + "\n- ".join(failures))
    print("content validation passed")


if __name__ == "__main__":
    main()
