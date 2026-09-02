# Security Policy

## Reporting A Vulnerability

Do not open a public issue for a suspected vulnerability. Email `hello@joinlayer.app` with the affected component and version, reproduction conditions, and potential impact. Do not include access tokens, connection credentials, customer data, or destructive proof-of-concept payloads.

JoinLayer will acknowledge the report, investigate it privately, and coordinate disclosure when appropriate. Testing must use accounts and workspaces you are authorized to access. Do not test denial of service, social engineering, credential harvesting, tenant crossover, or data modification against the hosted service without prior written authorization.

## Published Boundary

This repository intentionally excludes the JoinLayer control plane, workers, connectors, deployment inventory, and production configuration. A route, header, identifier prefix, or protocol shape shown in source is never a credential or authorization mechanism. Production access still requires valid delegated OAuth authority, the private gateway trust boundary, current workspace membership, role and scope checks, and action policy enforcement.

Public gateway snapshots are identified by repository release. JoinLayer must never deploy gateway source that differs from the mapped tagged public snapshot. A content mismatch blocks the private release and must be resolved before deployment.
