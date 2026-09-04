# Start With One Prompt

You do not need to learn JoinLayer's API or read this repository before using
it. Connect an OAuth-capable MCP client, then copy the prompt for your goal.
The JoinLayer skill guides the agent through discovery, validation, approvals,
and durable result checks.

## 1. Connect JoinLayer

### Codex

```bash
codex plugin marketplace add joinlayer/agent-toolkit
codex plugin add joinlayer@joinlayer
```

The plugin installs the skill and hosted MCP companion. Complete browser OAuth
when prompted. A direct MCP-only setup remains available in the README.

### Claude Code

```bash
claude plugin marketplace add joinlayer/agent-toolkit
claude plugin install joinlayer@joinlayer --scope user
claude mcp login joinlayer
```

Complete browser OAuth with `claude mcp login joinlayer` when available, or
choose **Authenticate** in `/mcp`. Never paste a JoinLayer token or add an
`Authorization` header. Start a fresh agent session after authentication so
the MCP tools and skill are both loaded.

## 2. Prove The Connection Safely

Copy this first:

```text
Use $joinlayer-pipelines. Inspect the authenticated JoinLayer workspace.
Report the workspace identity, agent identity, granted scopes, capacity,
connections, and pipelines. Do not create, change, start, or stop anything.
Recommend one next action based only on returned MCP state.
```

The response should name the authenticated workspace and agent identity, list
the granted scopes, report capacity and blockers, summarize connections and
pipelines, and explicitly say that no state changed.

For this broad first inspection, the agent should call
`get_workspace_overview` first. It requests the four required read-only scopes
in one browser consent and returns the complete overview in one result.

## 3. Choose A Goal

| Goal | Copy this prompt |
|---|---|
| Connect a source, target, or lookup database | [Connect a database](prompts/connect-a-database.md) |
| Create a pipeline | [Create a pipeline](prompts/create-pipeline.md) |
| Copy into a new destination table | [Create a new target table](prompts/create-new-target-table.md) |
| Load history, then continue realtime | [Historical load then realtime](use-cases/historical-load-then-realtime.md) |
| Add lookup enrichment | [Enrich a pipeline](prompts/enrich-pipeline.md) |
| Check a draft before running | [Validate before run](prompts/validate-before-run.md) |
| Start, resume, or stop safely | [Control a run](prompts/control-a-run.md) |
| Diagnose a failure | [Diagnose a failed run](prompts/diagnose-failed-run.md) |
| Recover a stalled realtime pipeline | [Recover realtime](prompts/recover-realtime-pipeline.md) |
| Explain a capacity blocker | [Explain capacity](prompts/explain-capacity.md) |
| Report usage for a date range | [Report usage](prompts/report-usage.md) |

## What The Agent Will Ask You To Do

- Complete connection credentials only in a short-lived JoinLayer browser
  setup page. Credentials never belong in chat.
- Confirm ambiguous source/target tables, keys, write behavior, or surprising
  preview output.
- Review material target effects before execution.
- Open the direct JoinLayer approval link only when the workspace's governance
  policy requires a human approval.

Creating or changing a draft is not execution. Validation is not preview,
preview is not approval, and approval is not a completed run. The agent must
verify durable state before reporting success.
