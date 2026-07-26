# lcm_cross_project_search

> Search exactly one explicitly selected project in the authorized Lossless Context Management (LCM) catalog.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Catalog selection: `packages/coding-agent/src/lcm/project-catalog.ts`
- Store validation, handles, and rendering: `packages/coding-agent/src/lcm/operations.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/lcm-cross-project-search.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `project` | `string` | Yes | Exact known project ID or explicit absolute canonical project path. |
| `query` | `string` | Yes | Non-empty full-text query over that project's redacted derived data. |
| `limit` | `number` | No | Positive safe-integer page size; defaults to 8, capped at 20. |
| `offset` | `number` | No | Non-negative safe-integer offset; defaults to 0, capped at 1,000. |

## Outputs
Returns the resolved project ID/root followed by bounded matches with rank, excerpt, and project-qualified opaque summary/source handles. `details.projectId`, `matches`, `offset`, and optional `nextOffset` report scope and pagination.

## Scope and safety
- Resolves exactly one catalog entry and never unions or scans projects.
- Validates the catalog store path against the canonical per-project location before opening SQLite.
- Opens the redacted derived store with corruption recovery disabled and closes it after the query.
- Returns no raw JSONL, database handle, or large-file bytes.

## Errors
Rejects unknown/relative/ambiguous/unauthorized selectors and invalid pagination. Throws when the selected derived store is unavailable. Cancellation propagates.
