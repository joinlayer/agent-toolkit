# Connect A Database Safely

```text
Use $joinlayer-pipelines. Help me add the database connection I describe.
Inspect the authenticated workspace, discover supported connector types and
existing connections, and avoid creating a duplicate. Collect only non-secret
setup choices in chat. Create a short-lived JoinLayer browser setup when ready,
give me the setup URL, and wait while I enter credentials there. Never ask me
to paste a password, key, certificate, token, or connection string. After I
complete setup, verify the durable connection, its returned capabilities, and
run the stored connection test before schema discovery. Report success only
after both checks pass. Do not claim that creating a setup proves connectivity.
```

Completion evidence: expected workspace, selected connector type and role,
completed browser setup, durable connection ID, returned capabilities, and a
successful stored connection test followed by a successful first
schema-discovery response. Credentials are never evidence and must not be
repeated.
