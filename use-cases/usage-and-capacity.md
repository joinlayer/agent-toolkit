# Usage And Capacity Reporting

Use [`report-usage.md`](../prompts/report-usage.md).

Current capacity and historical usage answer different questions. Capacity
uses the current billing period and controls whether new work can start. A
usage report covers the explicit requested date range and may cross billing
periods. The agent must state both returned boundaries and must not compare
their totals as if they described the same period.

Report each limiter independently, including used, remaining, and limit values.
Separate subscription entitlement from physical worker placement. When a
blocker exists, name the affected action and returned remediation; do not retry
execution or stop competing work without a separate user request.
