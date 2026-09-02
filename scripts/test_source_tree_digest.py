from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.source_tree_digest import compute_tree_digest


class SourceTreeDigestTests(unittest.TestCase):
    def test_only_mcp_gateway_source_affects_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            gateway_file = root / "mcp-gateway" / "joinlayer_mcp" / "server.py"
            gateway_file.parent.mkdir(parents=True)
            gateway_file.write_text("source-v1\n", encoding="utf-8")
            skill_file = root / "skills" / "joinlayer-pipelines" / "SKILL.md"
            skill_file.parent.mkdir(parents=True)
            skill_file.write_text("skill-v1\n", encoding="utf-8")

            original = compute_tree_digest(root)
            skill_file.write_text("skill-v2\n", encoding="utf-8")
            self.assertEqual(compute_tree_digest(root), original)

            cache_file = root / "mcp-gateway" / "joinlayer_mcp" / "__pycache__" / "server.pyc"
            cache_file.parent.mkdir(parents=True)
            cache_file.write_bytes(b"generated")
            self.assertEqual(compute_tree_digest(root), original)

            gateway_file.write_text("source-v2\n", encoding="utf-8")
            self.assertNotEqual(compute_tree_digest(root), original)


if __name__ == "__main__":
    unittest.main()
