# lcm_describe

> Resolve one opaque citation from `lcm_search` within the active Lossless Context Management (LCM) session and branch.

## Source
- Tool: `packages/coding-agent/src/tools/lcm.ts`
- Citation decoding and rendering: `packages/coding-agent/src/lcm/operations.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/lcm-describe.md`

## Inputs

| Field | Type | Required | Description |
|---|---|---:|---|
| `citation` | `string` | Yes | Non-empty opaque `lcm-citation:v1:` token returned by `lcm_search`. |

## Outputs
A successful result contains the normalized citation, source kind, ISO timestamp, source position, and at most 8,000 characters of redacted derived text. `details.available` is `true`.

If the cited source is absent, changed, or outside the active session and branch, the tool returns `LCM citation is unavailable or is outside the current session/branch scope.`, marks the result as useless, and sets `details.available` to `false`.

At most 16 artifact references are shown. A reference is exposed as `artifact://<id>` only when that artifact exists in the active session; otherwise the output contains an opaque unavailable marker.

## Scope and availability
- Available only when the session exposes an LCM runtime, normally when `context.engine` is `lossless`.
- Citation fields are validated, but the token is not an authorization bypass: the runtime rechecks active project, session, branch, source identity, content hash, and position.
- Returns derived redacted text, never a direct journal or database handle.

## Errors
- Rejects malformed, overlong, incorrectly encoded, or structurally invalid citation tokens as `Invalid LCM citation token`.
- Throws `LCM is unavailable for this session.` if the runtime disappears after tool discovery.
- Cancellation aborts through the tool call signal.
