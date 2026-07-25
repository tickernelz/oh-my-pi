# lcm_describe

> Describe one opaque Lossless Context Management (LCM) source, summary, or file handle in the active session/branch.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Handle codec and rendering: `packages/coding-agent/src/lcm/operations.ts`
- Scoped storage/runtime resolution: `packages/lcm-context/src/context.ts` and `packages/coding-agent/src/session/session-lcm.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/lcm-describe.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `handle` | `string` | Yes | Non-empty opaque `lcm-handle:v1:` source, summary, or file handle. |

## Outputs
- **Source:** source handle, kind, timestamp, position, bounded redacted journal-derived text, file handles, and at most 16 artifact references.
- **Summary:** stable handle, leaf/condensed kind, level, tokens, source/child counts, scoped parent handles, propagated file handles, and bounded redacted summary text.
- **File:** opaque file handle, redacted path, hash, type, byte/token size, availability, source handles, and bounded exploration summary. Raw file bytes are never returned from SQLite.

Artifact URIs are exposed only when they resolve in the current session. Missing artifacts/files and stale journal sources are labeled unavailable rather than reconstructed.

## Scope and availability
- Every handle embeds project/session/branch scope and is rechecked against the active branch.
- A token is not an authorization bypass; changed, inactive, cross-branch, cross-session, or cross-project identities return unavailable.
- Summary and file provenance is derived through active source lineage.
- SQLite remains redacted and rebuildable; JSONL/artifacts/filesystem remain authoritative.

## Errors
- Rejects malformed, overlong, incorrectly encoded, or structurally invalid handle tokens.
- Throws `LCM is unavailable for this session.` if the runtime disappears after discovery.
- Cancellation aborts through the tool-call signal.
