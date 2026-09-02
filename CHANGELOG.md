# Changelog

## 0.1.1 — 2026-09-02

- Exclude Git implementation metadata from the committed-tree secret scan so
  checkout commit IDs cannot be misclassified as leaked high-entropy values.

## 0.1.0 — 2026-09-02

- Add the `joinlayer-pipelines` operating skill for Codex, Claude Code, and
  skill-compatible agents.
- Add browser-OAuth MCP configuration for the hosted JoinLayer service.
- Add directly copyable prompts and outcome-based runbooks for connection
  setup, pipeline creation, validation, execution, usage, and recovery.
- Publish the synchronized MCP gateway source and protocol/security contracts
  for review.
- Add explicit safety annotations for every hosted MCP tool and a regression
  contract that keeps their read, mutation, external-side-effect, and
  destructive semantics reviewable.
- Add Codex and Claude marketplace packaging, reviewed brand assets, and
  release material for OpenAI, MCP Registry, skills.sh, and community catalogs.

Production OAuth E2E is complete for Codex and Claude Code. The release date is
assigned only after the public repository security controls, final source
boundary review, and authorized initial public release are complete.
