# lcm_recall

> Answer a question from bounded source slices selected from the active Lossless Context Management (LCM) session and branch.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Retrieval pipeline: `packages/coding-agent/src/lcm/operations.ts`
- Runtime completion seam: `packages/coding-agent/src/session/session-lcm.ts`
- Model-facing prompts: `packages/coding-agent/src/prompts/tools/lcm-recall.md` and `packages/coding-agent/src/prompts/lcm/recall*.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `query` | `string` | Yes | Non-empty question to answer from explicitly selected current-session LCM sources. |

## Flow
1. Normalizes and caps the query at 2,048 characters.
2. Searches for at most six current-session hits.
3. Selects at most eight unique citations and resolves each source description.
4. Builds at most 12,000 source characters, capped at 2,400 characters per slice.
5. Runs an isolated, toolless `@smol` one-shot with no inherited conversation, journal access, store access, or child session.
6. Returns the bounded answer plus an opaque citation legend.

## Outputs
The answer is limited to 1,200 model output tokens and 12,000 displayed characters. `details.citations` reports the number of selected sources.

If a source description cannot be resolved, the bounded search excerpt is used and labeled as degraded. If no cited source matches, the tool returns `No cited LCM sources matched this recall query.` and marks the result as useless.

Artifact URIs in source text or the answer remain visible only when valid in the active session; otherwise they become unavailable markers.

## Scope and availability
- Available only when the session exposes an LCM runtime, normally when `context.engine` is `lossless`.
- Reads only the active project, session, and branch's derived LCM data.
- The isolated completion receives selected redacted slices, not the authoritative journal or SQLite handle.

## Errors
- Throws `LCM recall query is required` if normalization leaves an empty query.
- Throws `LCM is unavailable for this session.` if the runtime disappears after tool discovery.
- Search, description, completion, and cancellation failures propagate to the caller.
