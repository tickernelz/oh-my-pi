# lcm_recall

> Answer a question from bounded source slices selected from the active Lossless Context Management (LCM) session/branch.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Retrieval pipeline and handles: `packages/coding-agent/src/lcm/operations.ts`
- Runtime completion seam: `packages/coding-agent/src/session/session-lcm.ts`
- Model-facing prompts: `packages/coding-agent/src/prompts/tools/lcm-recall.md` and `packages/coding-agent/src/prompts/lcm/recall*.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `query` | `string` | Yes | Non-empty question over explicitly selected current-session LCM sources. |

## Flow and bounds
1. Caps the query at 2,048 characters and searches at most six current-session hits.
2. Selects at most eight unique source handles and resolves them in the active scope.
3. Builds at most 12,000 source characters, capped at 2,400 per slice.
4. Runs an isolated, toolless `@smol` one-shot with no inherited conversation, journal/store access, or child session.
5. Returns at most 1,200 model tokens / 12,000 display characters plus executable source handles.

Unavailable descriptions use the bounded search excerpt and are labeled degraded. No selected source returns a useless result. Artifact URIs remain visible only when valid in the active session.

## Scope and safety
- Reads only active project/session/branch derived records.
- The isolated completion receives selected redacted slices, never authoritative JSONL, arbitrary files, or a SQLite handle.
- Large-file raw bytes are never placed in the recall prompt; only reference metadata/exploration summaries may appear.

## Errors
- Throws `LCM recall query is required` if normalization leaves an empty query.
- Runtime, completion, and cancellation failures propagate.
