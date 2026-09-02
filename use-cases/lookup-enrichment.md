# Lookup Enrichment

Use [`enrich-pipeline.md`](../prompts/enrich-pipeline.md).

The agent discovers the lookup schema and asks the user to resolve join-key, duplicate-match, missing-match, and error behavior. It must preview both matched and unmatched examples where available. Credentials remain in the JoinLayer connection setup browser flow and never enter the prompt or tool arguments.

Completion requires a saved draft, successful validation, and preview evidence for the enriched output. It does not require or imply execution.
