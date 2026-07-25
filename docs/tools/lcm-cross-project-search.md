# lcm_cross_project_search

> Search one explicitly selected project in the authorized Lossless Context Management (LCM) project catalog.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Catalog selection: `packages/coding-agent/src/lcm/project-catalog.ts`
- Store validation and rendering: `packages/coding-agent/src/lcm/operations.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/lcm-cross-project-search.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `project` | `string` | Yes | Exact known project ID or explicit absolute canonical project path. |
| `query` | `string` | Yes | Non-empty full-text query over that project's redacted derived data. |
| `limit` | `number` | No | Positive integer match limit. Defaults to 8 and is capped at 20. |

## Outputs
Returns the resolved project ID and shortened root path followed by bounded search matches. Each match includes its kind, rank, an excerpt of at most 1,600 characters, and project-qualified opaque citation tokens. `details.projectId` and `details.matches` identify the selected project and match count.

When no source matches, the result includes `No LCM matches found.` and is marked as useless.

## Scope and safety
- Resolves exactly one catalog entry; it never scans all projects or automatically includes another project.
- The selector must match an authorized catalog project by exact ID or canonical absolute path. Ambiguous basenames are not selectors.
- Before opening SQLite, the implementation verifies that the catalog store path equals the canonical per-project LCM store path.
- Opens the derived store with corruption recovery disabled and closes it after the search.
- Returns bounded redacted derived data, never a database handle or raw journal.

## Errors
- Rejects unknown, relative, ambiguous, or unauthorized project selectors.
- Throws when the catalog store path is invalid or the derived store is unavailable.
- Rejects non-positive, fractional, or unsafe-integer limits.
- Cancellation aborts through the tool call signal.
