# Inspect A Workspace Safely

```text
Use $joinlayer-pipelines. Inspect the authenticated JoinLayer workspace.
Report the workspace identity, agent identity, granted scopes, capacity,
connections, and pipelines. Do not create, change, start, or stop anything.
Recommend one next action based only on returned MCP state.
```

The agent should begin with `get_workspace_overview`, which obtains the four
required read-only scopes in one consent and returns the complete inspection in
one MCP result.
