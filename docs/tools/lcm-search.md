# lcm_search

> Search the active session and branch's Lossless Context Management (LCM) derived index.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Retrieval and rendering: `packages/coding-agent/src/lcm/operations.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/lcm-search.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `query` | `string` | Yes | Non-empty full-text query over redacted current-session LCM data. |
| `limit` | `number` | No | Positive integer match limit. Defaults to 8 and is capped at 20. |

## Outputs
Returns bounded, redacted derived matches. Each match includes its kind, rank, an excerpt of at most 1,600 characters, and up to eight opaque `lcm-citation:v1:` tokens. Use a token with `lcm_describe` to inspect that source.

When no source matches, the tool returns `No LCM matches found.` and marks the result as useless. `details.matches` contains the number of returned matches.

Artifact URIs remain clickable only when the artifact exists in the active session. Other references become explicit unavailable markers.

## Scope and availability
- Available only when the session exposes an LCM runtime, normally when `context.engine` is `lossless`.
- Searches only the active LCM project, session, and branch.
- Does not search raw journals, arbitrary SQLite files, or other projects.
- Results are derived data; original messages remain in the authoritative OMP journal.

## Errors
- Throws `LCM is unavailable for this session.` if the runtime disappears after tool discovery.
- Rejects non-positive, fractional, or unsafe-integer limits.
- Cancellation aborts through the tool call signal.
