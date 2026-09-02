from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INCLUDED_ROOTS = (Path("mcp-gateway"),)


def compute_tree_digest(root: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    file_count = 0
    for relative_root in INCLUDED_ROOTS:
        source_root = root / relative_root
        if not source_root.is_dir():
            raise SystemExit(f"missing synchronized source root: {source_root}")
        for path in sorted(source_root.rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            relative = path.relative_to(root).as_posix().encode("utf-8")
            content = path.read_bytes()
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            digest.update(len(content).to_bytes(8, "big"))
            digest.update(content)
            file_count += 1
    if file_count == 0:
        raise SystemExit("synchronized source tree is empty")
    return digest.hexdigest(), file_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Digest the synchronized JoinLayer MCP gateway source tree.")
    parser.add_argument(
        "--compare-root",
        type=Path,
        help="Fail unless this repository root has the same normalized source tree.",
    )
    args = parser.parse_args()

    current_digest, current_files = compute_tree_digest(ROOT)
    print(f"sha256:{current_digest} files:{current_files}")
    if args.compare_root is None:
        return

    compare_root = args.compare_root.expanduser().resolve()
    compare_digest, compare_files = compute_tree_digest(compare_root)
    print(f"compare sha256:{compare_digest} files:{compare_files} root:{compare_root}")
    if (current_digest, current_files) != (compare_digest, compare_files):
        raise SystemExit("synchronized MCP gateway source trees differ")
    print("synchronized MCP gateway source trees match")


if __name__ == "__main__":
    main()
