# Start Or Stop A Run Safely

```text
Use $joinlayer-pipelines. Prepare the exact start, resume, or stop operation I
request. Verify workspace identity, current pipeline revision, active and queued
runs, capacity, checkpoint mode, and the effective human approval policy.
Explain the operational and target-data impact before submission. If approval
is required, create one request for the exact operation, give me its approval
URL and next action, and wait for durable approval. Do not alter approved
options, create duplicate runs, or treat approval as execution. After the tool
call, verify durable run state and report the run ID and current runtime health.
```
