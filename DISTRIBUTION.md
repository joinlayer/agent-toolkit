# Distribution Runbook

JoinLayer has one canonical public source repository and one hosted production
MCP resource. Catalog entries point to that resource; they do not deploy an
independent gateway. The production build continues to come from the private
platform repository and must match the tagged public `mcp-gateway/` snapshot
byte for byte.

## Distribution Surfaces

| Surface | Published artifact | External release action |
|---|---|---|
| OpenAI / Codex / ChatGPT | Universal plugin: hosted MCP plus bundled skill | Submit through the OpenAI Platform plugin portal after business identity, domain verification, and reviewer E2E |
| Codex repository marketplace | `.agents/plugins/marketplace.json` and `plugins/joinlayer/` | Users add `joinlayer/agent-toolkit`, then install `joinlayer@joinlayer` |
| Claude Code | `.claude-plugin/marketplace.json` and plugin manifest | Users add the GitHub marketplace and install the user-scoped plugin |
| skills.sh | `skills/joinlayer-pipelines/` | Public GitHub discovery begins after the tagged repository is installable through the skills CLI |
| Official MCP Registry | `server.json` with the hosted Streamable HTTP URL | Authenticate the `app.joinlayer` namespace and publish with `mcp-publisher` |
| mcpservers.org | Public repository and hosted MCP URL | Submit the standard free listing only after the canonical release is public |

## Current Publication Status

- The official MCP Registry lists active server `app.joinlayer/mcp` version `0.1.2` with the production Streamable HTTP endpoint.
- skills.sh discovers and installs `joinlayer-pipelines` from this repository.
- The repository marketplaces install `joinlayer@joinlayer` for Codex and Claude Code; the repository package version is `0.1.7`.
- mcpservers.org accepted the free listing submission and is reviewing it.
- OpenAI marketplace publication is not yet complete; reviewer E2E and final submission remain.
- Anthropic's central community marketplace submission remains an authenticated Console action; the repository marketplace is already usable without it.

## Release Boundary

1. The private and public `mcp-gateway/` digests match.
2. Every public file passes the allowlist, secret, content, manifest, and MCP
   contract checks. Reviewed binary assets are pinned by SHA-256.
3. GitHub secret scanning and push protection are enabled before the first
   push.
4. The initial public commit and tag are explicitly authorized. The exact
   repository, commit, tag, and gateway digest are then pinned in the private
   release contract.
5. Production OAuth/discovery and clean client E2E pass against that release.
6. Catalog submissions use the same listing, policies, logos, support links,
   and production MCP URL. Reviewer credentials are entered only in each
   catalog's private submission channel.

The MCP Registry name remains `app.joinlayer/mcp`, which preserves the
JoinLayer-owned reverse-domain namespace. Publishing therefore uses the
registry's domain-ownership authentication rather than changing the identity
to a GitHub-derived namespace.

References: [OpenAI plugin submission](https://developers.openai.com/plugins/deploy/submission),
[MCP Registry quickstart](https://modelcontextprotocol.org/registry/quickstart),
[MCP remote servers](https://modelcontextprotocol.io/registry/remote-servers),
and [skills.sh documentation](https://www.skills.sh/docs).
