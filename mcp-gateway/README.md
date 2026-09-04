# JoinLayer MCP gateway

The gateway exposes JoinLayer pipeline operations over stateless Streamable HTTP at `/mcp`. It is an OAuth protected resource; the existing Go control-plane API is the authorization server.

## Authentication contract

Clients connect with OAuth Authorization Code and PKCE `S256`. They discover:

- `/.well-known/oauth-protected-resource/mcp` on the gateway (the canonical RFC 9728 path for the `/mcp` resource);
- `/.well-known/oauth-authorization-server` on `MCP_OAUTH_ISSUER`;
- the browser authorization, token, and revocation endpoints from that metadata.

Client registration follows the current MCP contract: pre-registration when configured and Client ID Metadata Documents (CIMD) as the preferred mechanism for clients without a prior relationship. The authorization server also exposes the specification's Dynamic Client Registration compatibility path for deployed clients such as Codex CLI that have not adopted CIMD yet. DCR accepts only rate-limited public clients using strict redirect URIs, Authorization Code, PKCE `S256`, and `token_endpoint_auth_method=none`; it never issues a client secret. MCP access tokens are opaque, short-lived, and audience-bound to `${MCP_PUBLIC_URL}/mcp`. The gateway sends them only in an introspection JSON body over the private API network, then uses the returned one-minute `jli_` token for downstream API calls. The public token is never passed through to the API.

`MCP_GATEWAY_TOKEN` or `MCP_GATEWAY_TOKEN_FILE` is a private, at-least-32-character service credential shared only by the gateway and API. It is not an MCP client credential. Production boot fails if it is missing.

`MCP_REQUEST_STATE_KEYS` or `MCP_REQUEST_STATE_KEYS_FILE` is the AES-GCM key ring used by MCP SDK v2 to protect short-lived multi-round request state. Every gateway replica uses the same ring; the first key mints and the remaining keys accept in-flight state during rotation. Each entry must contain at least 32 bytes and production boot fails without it.

Legacy `jla_` agent tokens and normal JoinLayer user-session tokens are not accepted.

## Network boundary

- `MCP_PUBLIC_URL` is the canonical HTTPS origin used for resource metadata, audience validation, Host, and Origin policy.
- `MCP_OAUTH_ISSUER` is the canonical JoinLayer web origin that serves authorization-server metadata and browser consent.
- `MCP_ALLOWED_HOSTS` and `MCP_ALLOWED_ORIGINS` may explicitly extend transport allowlists.
- `/metrics` is internal-only and requires the metrics credential; the public Caddy route returns `404`.
- `/healthz` is liveness-only. `/readyz` verifies API readiness.
- Request bodies, upstream payloads, and serialized MCP responses are independently bounded.
- MCP `2026-07-28` requests are stateless and may use any gateway replica; sealed request state is audience- and OAuth-principal-bound.
- Authentication attempts are source-rate-limited; authorized requests use an OAuth-client rate bucket.
- Missing or invalid credentials return an RFC 6750 challenge with `resource_metadata`. Missing tool scopes return the exact scope through either the OpenAI MCP tool-result challenge used by ChatGPT/Codex or the transport-level `403 insufficient_scope` retained for other MCP clients.
- Broad first-session inspection uses `get_workspace_overview`, whose per-tool OAuth metadata requests `workspace:read`, `usage:read`, `connections:read`, and `pipelines:read` together. This avoids a cascade of browser step-ups while the granular tools keep their narrow individual scope contracts.

The generic compose file binds the gateway to loopback. Internet-facing deployments terminate TLS at the provided Caddy route or an equivalent trusted reverse proxy.
