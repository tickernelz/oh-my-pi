# lcm_search

> Search the active Lossless Context Management (LCM) session/branch with bounded pagination and optional summary scope.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Retrieval, handles, and rendering: `packages/coding-agent/src/lcm/operations.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/lcm-search.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `query` | `string` | Yes | Non-empty full-text query over redacted current-session LCM data. |
| `limit` | `number` | No | Positive safe-integer page size; defaults to 8 and is capped at 20. |
| `offset` | `number` | No | Non-negative safe-integer offset; defaults to 0 and is capped at 1,000. |
| `summary` | `string` | No | Opaque summary handle restricting matches to that summary's active source lineage. |

## Outputs
Each bounded match includes kind, rank, a redacted excerpt of at most 1,600 characters, and executable `lcm-handle:v1:` handles. Source matches expose source handles. Summary matches expose the stable summary handle and bounded source-lineage handles. A full page includes the next offset.

`details.matches`, `details.offset`, and optional `details.nextOffset` report pagination. No matches returns `No LCM matches found.` and marks the result useless.

## Scope and safety
- Searches only the active project/session/branch.
- A summary scope is revalidated against an active aligned placement in that same branch.
- Summary handles derive from canonical ordered inputs, not generated prose, and survive a rebuild that regenerates text.
- Results are redacted derived data. Authoritative messages remain in JSONL.
- Large-file bytes never enter search storage; only opaque IDs and bounded metadata/exploration summaries are indexed.

## Errors
- Rejects malformed handles and non-summary scope handles.
- Rejects negative/fractional/unsafe offsets and non-positive/fractional/unsafe limits; valid upper values are capped.
- Throws `LCM is unavailable for this session.` if the runtime disappears after discovery.
- Cancellation aborts through the tool-call signal.
