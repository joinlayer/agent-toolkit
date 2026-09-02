# Secure Connection Setup

Use [`connect-a-database.md`](../prompts/connect-a-database.md) when a source,
target, or lookup connection does not exist yet.

The agent first discovers supported connector types and existing connections.
It collects only safe choices such as a display name, connector type, and
intended source/target role. JoinLayer then creates a short-lived browser setup
page where the user enters credentials directly. A chat, prompt, MCP argument,
pipeline name, or idempotency key must never contain a password, private key,
certificate, access token, service-account document, or full connection string.

After the browser flow, the agent polls the existing setup rather than creating
duplicates. Completion requires a durable connection ID, returned capabilities,
a successful stored connection test, and then successful progressive schema
discovery. The test proves that JoinLayer can authenticate and reach the stored
endpoint; discovery proves that the selected identity can inspect the expected
objects. A created setup session or a user saying “done” is not enough evidence.
