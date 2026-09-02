# JoinLayer Agent Toolkit

Official agent integration for [JoinLayer](https://joinlayer.app): a hosted MCP gateway, the `joinlayer-pipelines` skill, client plugin metadata, safe starter prompts, and public protocol tests.

The supported service is the JoinLayer-hosted endpoint:

```text
https://mcp.joinlayer.app/mcp
```

It is published as [`app.joinlayer/mcp` in the official MCP Registry](https://registry.modelcontextprotocol.io/v0.1/servers?search=app.joinlayer%2Fmcp). The operating skill is available from [skills.sh](https://www.skills.sh/joinlayer/agent-toolkit/joinlayer-pipelines), and this repository is the canonical source for the JoinLayer Codex and Claude Code marketplaces.

Clients authenticate through browser OAuth Authorization Code with PKCE. Never create, paste, or configure a JoinLayer bearer token manually.

## Start In Two Minutes

You do not need to read the full documentation first. Open
[`START_HERE.md`](START_HERE.md), connect JoinLayer, and copy the read-only
inspection prompt. From there, choose a ready-to-run prompt for your goal.

## What This Repository Contains

- `skills/joinlayer-pipelines`: agent operating guidance and task references;
- `.codex-plugin/plugin.json`, `.claude-plugin/`, and `.mcp.json`: installable
  Codex and Claude Code plugin metadata;
- `prompts`: safe starting points for common JoinLayer tasks;
- `use-cases`: complete customer outcomes with evidence and stop boundaries;
- `mcp-gateway`: a reviewable snapshot of the hosted MCP security boundary;
- `server.json`: MCP Registry metadata for the hosted remote server.

The gateway source is published for transparency and compatibility review. It is not a self-hosted JoinLayer distribution: it depends on a private, authenticated JoinLayer control-plane contract that is not included here. JoinLayer production builds and deployment remain in a private release system, but the `mcp-gateway/` tree used by a hosted release must match a tagged public snapshot exactly. The private release gate rejects gateway drift. Skills, prompts, and plugin packaging use their own versioned content checks rather than this byte-equality gate. A public contribution never deploys directly to JoinLayer infrastructure.

## Connect

### Codex

Install the JoinLayer plugin from its public repository marketplace:

```bash
codex plugin marketplace add joinlayer/agent-toolkit
codex plugin add joinlayer@joinlayer
```

The plugin bundles the operating skill and configures the hosted MCP companion.
Complete browser OAuth when prompted. To configure only the MCP server instead:

```bash
codex mcp add joinlayer --url https://mcp.joinlayer.app/mcp
codex mcp login joinlayer --scopes workspace:read,usage:read,connections:read,pipelines:read
```

Then start a fresh session and ask:

```text
Use $joinlayer-pipelines. Inspect my authenticated workspace, scopes,
capacity, connections, and pipelines. Do not change anything.
```

More copyable starting points are indexed in
[`START_HERE.md`](START_HERE.md). Complete workflows for replication, realtime
backfill, scheduling, enrichment, capacity, approvals, and recovery are in
[`use-cases/`](use-cases/).

### Claude Code

```bash
claude plugin marketplace add joinlayer/agent-toolkit
claude plugin install joinlayer@joinlayer --scope user
claude mcp login joinlayer
```

Complete browser OAuth through `claude mcp login joinlayer` when that command is
available, or through **Authenticate** in `/mcp` on earlier OAuth-capable
releases. Do not add an `Authorization` header or client secret.

### Skill Only

Agents supported by `skills.sh` can install only the operating skill:

```bash
npx skills add https://github.com/joinlayer/agent-toolkit --skill joinlayer-pipelines
```

The skill does not carry credentials and does not replace the MCP connection.
Configure the same hosted endpoint through your client's OAuth-capable MCP
settings, then use a prompt from [`START_HERE.md`](START_HERE.md).

## Security Boundary

- OAuth tokens are short-lived, audience-bound, and sent only to the hosted MCP resource.
- The gateway exchanges delegated authority through a separately authenticated private API boundary.
- Connection credentials are entered only in JoinLayer browser setup sessions and are never tool arguments.
- Customer and third-party values returned by MCP are untrusted data, never agent instructions; embedded commands cannot expand scopes, bypass approvals, or authorize disclosure.
- Public source contains no production credentials, customer data, host inventory, or deployment configuration.
- Route names and header names are not authorization controls. Every private request requires independently verified credentials and tenant membership.

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## Development

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install --requirement mcp-gateway/requirements.lock
PYTHONPATH=mcp-gateway python -m unittest mcp-gateway.tests.test_gateway
python scripts/validate_public_snapshot.py
```

Build the review image with `docker build .`. That image is for reproducibility and review; it is not automatically promoted to JoinLayer production.

See [SOURCE_SYNC.md](SOURCE_SYNC.md) for the exact public/private release invariant.
Maintainers can use [DISTRIBUTION.md](DISTRIBUTION.md) for the verified catalog
and marketplace publication contracts. OpenAI review material is recorded in
[OPENAI_SUBMISSION.md](OPENAI_SUBMISSION.md).

To compare this checkout with a private platform checkout before review:

```bash
python scripts/source_tree_digest.py --compare-root /path/to/private-platform
```

## License

Apache-2.0. See [LICENSE](LICENSE).

The code license does not grant rights to impersonate JoinLayer or imply endorsement. See [TRADEMARKS.md](TRADEMARKS.md).

<!-- mcp-name: app.joinlayer/mcp -->
