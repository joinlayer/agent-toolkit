# OpenAI Plugin Submission Evidence

This document is the review packet for the JoinLayer universal plugin. It does
not contain reviewer credentials, OAuth tokens, customer identifiers, or
private deployment details. Reviewer access is provided only through the
OpenAI submission portal.

## Listing

- Name: **JoinLayer**
- Developer: **JoinLayer**
- Category: **Developer Tools**
- Website: `https://joinlayer.app`
- Support: `https://docs.joinlayer.app/agent-integrations`
- Privacy: `https://docs.joinlayer.app/privacy`
- Terms: `https://docs.joinlayer.app/terms`
- Production MCP URL: `https://mcp.joinlayer.app/mcp`
- Authentication: browser OAuth Authorization Code with PKCE; no client secret
  and no manually copied bearer token

Suggested listing description:

> Build, validate, operate, and diagnose secure JoinLayer data pipelines. Use
> browser OAuth to select a workspace and grant only the actions the agent
> needs. Connection credentials stay in short-lived JoinLayer browser setup
> sessions and never enter chat.

## Security And Authorization Contract

The production resource publishes protected-resource metadata and binds tokens
to the exact MCP resource audience. The authorization server publishes PKCE
`S256`, Client ID Metadata Document support, rate-limited Dynamic Client
Registration compatibility, supported scopes, and revocation. Consent shows
the client, callback, workspace, and exact action set. Every private action
rechecks workspace membership, role, scope, OAuth client, and delegated agent
identity. Tool arguments cannot select another tenant.

All MCP tools declare explicit `readOnlyHint`, `destructiveHint`,
`idempotentHint`, and `openWorldHint` values. The exact map is regression-tested
against the complete published tool set. Additive draft/setup/approval actions
are distinguished from destructive updates/cancellations and from execution
actions that can affect customer-controlled systems.

## Positive Review Cases

1. **Read-only workspace inspection.** Connect with `workspace:read`,
   `usage:read`, `connections:read`, and `pipelines:read`. Ask the plugin to
   report the authenticated workspace, capacity, connections, and pipelines
   without changes. Expected: tenant-attributed results and no mutations.
2. **Stored connection verification.** With `connections:test`, ask it to test
   one reviewer-provided connection and then discover its namespaces/tables.
   Expected: actionable connectivity result followed by structured discovery;
   no credential appears in arguments or output.
3. **Safe pipeline draft.** With `pipelines:write`, ask it to create an
   idempotent draft between reviewer fixtures, then fetch the durable draft.
   Expected: one draft, stable ID, no execution.
4. **Validation and preview.** With validation scope, validate the reviewer
   draft and preview sample output. Expected: structured blockers/warnings and
   bounded preview evidence; no target write.
5. **Approval-aware execution boundary.** Ask it to prepare a run for a
   workspace that requires approval. Expected: a direct JoinLayer approval
   instruction and no run until an administrator approves the exact action.
   After approval, one idempotent start may be reviewed through Activity.

## Negative Review Cases

1. **Credential request in chat.** Ask the plugin to accept a database password
   or connection string. Expected: refusal to collect the secret and creation
   of a short-lived JoinLayer browser setup instead.
2. **Insufficient scope.** Connect read-only and ask it to edit or start a
   pipeline. Expected: structured `insufficient_scope`/step-up guidance and no
   state change.
3. **Cross-workspace identifier.** Supply a resource ID from a workspace not in
   the reviewer grant. Expected: tenant-safe not-found/authorization failure
   without existence, metadata, or content disclosure.

## Submission Preconditions

Before submission, the maintainer verifies business/developer identity in the
OpenAI Platform, supplies reviewer credentials through the portal, completes
any `/.well-known/openai-apps-challenge` domain challenge, selects actual
country availability, uploads the reviewed logo, and records release notes.
The public tag and gateway digest must match the private production release
contract. None of those external values belongs in this repository.

OpenAI references: [package a plugin](https://developers.openai.com/plugins/build/plugins),
[submit a plugin](https://developers.openai.com/plugins/deploy/submission), and
[MCP server review requirements](https://developers.openai.com/plugins/deploy/app-review).
