# Supported Clients

Compatibility is proven against a clean client session and the hosted
production endpoint before a toolkit release is announced. Static packaging
validation is recorded separately from runtime OAuth compatibility.

| Client | Local evidence | Required release runtime | Release status |
|---|---|---|---|
| Codex CLI | Plugin validator and CLI 0.148.0 packaging/configuration checks pass | Streamable HTTP, DCR or CIMD, browser OAuth, incremental scopes | Production OAuth, exact four-scope read-only calls, logout, revocation, and re-login passed on CLI 0.148.0 |
| ChatGPT/Codex public plugin | Static package, marketplace, skill, and explicit MCP tool-annotation validation | Current universal plugin submission, hosted MCP mapping, browser OAuth | Production resource is ready; OpenAI portal registration, domain verification, reviewer E2E, and approval remain |
| Claude Code | Marketplace and plugin manifests pass local validation | User-scoped HTTP MCP, DCR or CIMD, browser OAuth, token refresh | Production OAuth, workspace tool call, and clear-auth revocation passed on the installed 2.1.x client through `/mcp` |
| MCP protocol client | Locked gateway contracts run on Python 3.12 | MCP Streamable HTTP; protected-resource discovery, OAuth audience, structured tools and errors | Local contract and hosted discovery checks are release requirements |

The Claude production check used the interactive `/mcp` authentication path;
command availability varies by client release. Release notes record exact
clients used for E2E. A client must never require a manually copied bearer
token or custom Authorization header. No placeholder OpenAI app mapping is
published: the real hosted MCP connection is registered only through OpenAI's
submission portal.
