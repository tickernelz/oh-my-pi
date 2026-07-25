# lcm_expand

> Expand one Lossless Context Management (LCM) summary inside an explicitly LCM-enabled child/task session.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Scoped traversal and rendering: `packages/coding-agent/src/lcm/operations.ts`
- Journal resolver: `packages/coding-agent/src/session/session-lcm.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/lcm-expand.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `handle` | `string` | Yes | Opaque `lcm-handle:v1:` summary handle returned by `lcm_search`, `lcm_describe`, or projected LCM context. |
| `depth` | `number` | No | Traversal depth. Positive safe integer, capped at 4; defaults to 1. |
| `offset` | `number` | No | Zero-based item offset for deterministic pagination, capped at 1,000; defaults to 0. |
| `limit` | `number` | No | Maximum returned items, capped at 50; defaults to 20. |
| `max_tokens` | `number` | No | Approximate output-token ceiling, 1,024–8,000; defaults to 4,000. |

## Outputs
Returns a bounded, deterministic page of child summaries or source entries. Summary text comes from the redacted derived DAG. Source slices are resolved again from the active authoritative session journal and redacted at retrieval time; SQLite source text is never used as an expansion authority. Missing journal entries and unavailable referenced files are labeled unavailable rather than reconstructed from derived state.

The result includes the root summary handle, page offset, returned/total item counts, and a next offset when another page exists. Output is bounded by traversal depth, item count, and token budget.

## Scope and availability
- Constructed only for child/task sessions with an explicitly forwarded LCM runtime capability.
- Never constructed for the top-level agent, even when explicitly named in its tool list.
- Every handle is rechecked against the child session's active project, session, and branch.
- Does not open arbitrary projects, journals, SQLite files, artifacts, or filesystem paths.
- Large files remain reference-only. Their path/hash/type/size/token metadata and bounded exploration summary may be shown, but file bytes are never copied into LCM.

## Errors
- Rejects malformed or non-summary handles.
- Rejects non-positive/fractional bounds and token ceilings below 1,024; upper bounds are capped.
- Returns an explicit unavailable result when the summary no longer belongs to the active branch.
- Cancellation aborts through the tool call signal.
