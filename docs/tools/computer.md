# computer

> Capture and control one real host window through native OS APIs. Pass a numeric window id for isolated, focus-preserving operation or `desktop` for the selected-display composite and its original global input behavior. This is not the `browser` tool and exposes no DOM or ARIA surface.

User setup, safety guidance, platform permissions, and verified limitations: [Window-scoped computer use](../computer-use.md).

## Source

- Entry: `packages/coding-agent/src/tools/computer.ts`
- Model-facing prompt: `packages/coding-agent/src/prompts/tools/computer.md`
- Safety prompt: `packages/coding-agent/src/prompts/system/computer-safety.md`
- Tool registration/gate: `packages/coding-agent/src/tools/index.ts`
- Approval wrapper: `packages/coding-agent/src/extensibility/extensions/wrapper.ts`
- Renderer: `packages/coding-agent/src/tools/computer-renderer.ts`
- Supervisor/protocol: `packages/coding-agent/src/tools/computer/{supervisor,protocol,worker,worker-entry}.ts`
- Native implementation: `crates/pi-natives/src/desktop.rs`
- Native loader: `packages/natives/native/loader-state.js`

## Availability and declaration

- `computer.enabled` gates registration and defaults to `false`. `/computer` toggles it for the current session without persisting settings.
- Enabled tool load mode: `essential`.
- Concurrency: `exclusive`.
- The tool is always a JSON-schema function, including for models with provider-native Computer Use capability. Provider-native computer declarations cannot represent its optional host-window selector.
- `/computer status` therefore reports `function` exposure for every active model.

Unlike `browser`, `computer` operates native host windows. It can act in IDEs, terminals, native applications, browser windows, and system dialogs, but has no structured application or DOM inspection.

## Settings

| Setting | Type | Default | Contract |
|---|---|---:|---|
| `computer.enabled` | boolean | `false` | Register tool. |
| `computer.backend` | `auto \| native` | `auto` | Both prohibit non-native fallback. |
| `computer.display` | string | `all` | `all` or numeric native monitor ID. |
| `computer.maxWidth` | number | `1920` | Maximum composite PNG width. Image transports that cannot preserve original detail, including GitHub Copilot Responses and xAI OAuth, cap the effective width at `1280`; Claude-family models use the same cap as a compatibility fallback. |
| `computer.maxHeight` | number | `1200` | Maximum composite PNG height. Those coordinate-safe transports cap the effective height at `896`; other models retain the configured limit. |

The controller snapshots these settings into one `DesktopSessionOptions`. Crossing the coordinate-safe sizing boundary during a model switch recreates the controller, resnapshots the options, and invalidates the prior coordinate frame; the next pointer action requires a fresh screenshot.

## Inputs

Public schema:

```ts
{
  window?: "desktop" | `${number}`,
  actions?: Array<{
    type: "click" | "double_click" | "drag" | "keypress" | "move" | "screenshot" | "scroll" | "type" | "wait",
    x?: int32 >= 0, y?: int32 >= 0,          // preceding screenshot pixels
    button?: "left" | "right" | "wheel" | "back" | "forward",
    path?: Array<{ x, y }>,
    keys?: string[],
    scroll_x?: int32, scroll_y?: int32,
    text?: string
  }>
}
```

Omitting both `window` and `actions` lists targets without capturing a display. `desktop` selects the configured display composite. A decimal id selects one entry from the latest list and normalizes to `1..=4294967295`. Actions require a window; omitted or empty `actions` captures the selected target without input.

### Action shapes

| Type | Shape |
|---|---|
| `click` | `{ type, button: "left" \| "right" \| "wheel" \| "back" \| "forward", x, y, keys? }` |
| `double_click` | `{ type, x, y, keys?: string[] \| null }` |
| `drag` | `{ type, path: Array<{x,y}>, keys? }`; minimum two points |
| `keypress` | `{ type, keys: string[] }`; non-empty array and entries |
| `move` | `{ type, x, y, keys? }` |
| `screenshot` | `{ type }` |
| `scroll` | `{ type, x, y, scroll_x, scroll_y, keys? }` |
| `type` | `{ type, text: string }` |
| `wait` | `{ type }`; fixed two-second sleep |

Validation rejects missing, unexpected, and action-inapplicable fields before input at both JS and native boundaries. Coordinates, drag points, and scroll deltas must fit signed 32-bit integers; coordinates must also be non-negative. Mouse `keys` accept unique modifiers only. Keypress strings are case-insensitive, accept aliases and `+`-separated chords, and fall back to one Unicode character. `wheel` is the middle-button spelling; `middle` is invalid.

Nonzero scroll delta `d` becomes `sign(d) × max(1, floor((abs(d)+50)/100))` native steps.

## Approval

`computerApproval(args)` returns:

- `read` when every action is `screenshot` or `wait`, including an omitted/empty action batch;
- `exec` for any input action or malformed action payload.

Approval prompts include the selected window, render up to 12 ordered action summaries, truncate each line to 240 characters, and cap combined details at 2,000 characters.

The system safety prompt independently treats all UI as untrusted and requires point-of-risk confirmation for consequential actions.

## Outputs

A successful list-only call returns:

- `content[0]`: text listing `desktop` plus current numeric window ids, app/title, geometry, and focus;
- `details.windows`: current `DesktopWindow[]`;
- backend, display-server, permission, and capability metadata; no image or dimensions.

A successful targeted call returns that text as `content[0]`, a fresh base64 PNG as `content[1]`, target dimensions and id, display metadata, capabilities, and executed action names.

The renderer shows the selected target, up to three windows and three displays when collapsed, and the bounded remainder counts. Window/app/title strings and all other native metadata are sanitized before TUI rendering.

The function result uses each provider's ordinary text/image tool-result path. OMP does not upload captures to provider Files or emit native `computer_call_output` metadata.

## Flow

1. Tool registration checks `computer.enabled`.
2. `ComputerTool` constructs a lazy `ComputerSupervisor` and exposes the window-aware function schema.
3. The model omits `window` and `actions` to enumerate targets without a capture.
4. The supervisor serializes the request, lazily starts one Bun worker, and calls native `DesktopSession.listWindows()`.
5. The tool returns the target list as text with structured metadata and no image.
6. The model chooses `desktop` or a numeric id; the approval wrapper classifies its action batch.
7. `ComputerTool.execute()` normalizes the target, validates actions, and passes both to the supervisor.
8. Coordinate input is rejected until the worker has returned a screenshot of that exact target.
9. Native code validates and executes actions in order, defers `screenshot` markers, and captures one fresh target PNG.
10. The worker transfers the PNG and preserves target/frame state for the next call.
11. The tool returns the refreshed window list followed by the image and structured details.

## Capture and coordinate mapping

Window enumeration is topmost-first, deduplicated, capped at 48, and filters minimized, tiny, and completely unlabeled entries. Each `DesktopWindow` reports `id`, `title`, `app`, global logical `x/y/width/height`, and `focused`.

For `desktop`, native capture enumerates selected monitors, sorts by logical `y/x/id`, coalesces mirrored rectangles, rejects invalid/overlapping layouts, and builds the bounded composite. Each `DesktopDisplay` maps global logical geometry to `pixelX/pixelY/pixelWidth/pixelHeight` in the PNG. Input maps screenshot pixels back through that display layout and retains the original global pointer/focus behavior.

For a numeric id, capture returns only that window. The frame contains one synthetic display mapping the PNG origin to the window's global rectangle. Before coordinate input, native code finds the id again: movement rebases against its current position; closure or size change clears the frame with `DESKTOP_LAYOUT_CHANGED`.

Window actions bypass global desktop input:

- macOS posts process-targeted NSEvent/CGEvent input;
- Win32 posts mouse/key/character messages to the HWND;
- X11 sends events directly to the selected client.

These paths do not activate the application or move the real pointer. Delivery can still be rejected by an application or protected OS surface.

Every coordinate action in a batch maps through the same frame returned by the prior successful call. A `screenshot` marker creates no intermediate result. Switching targets requires a capture-only call before coordinates.

## Platform variants

| Target | Desktop | Numeric window |
|---|---|---|
| `darwin-x64`, `darwin-arm64` | Bounded `screencapture`, Quartz/global native input | `screencapture -l`, process-targeted events |
| `linux-x64`, `linux-arm64` (glibc/musl) | X11 root `GetImage`, XTest | X11 window `GetImage`, direct client events |
| `win32-x64` | xcap displays, virtual-desktop `SendInput` | xcap window, direct Win32 messages |
| Other targets | Native loader rejection | Native loader rejection |

macOS performs non-prompting Screen Recording preflight; Accessibility is required for input. Linux speaks X11 directly and requires `DISPLAY`, RandR, and XTEST. Pure Wayland windows are unavailable. Rootless XWayland has no capturable desktop root; numeric targets can include only XWayland clients. Windows enables DPI awareness before capture.

## Worker and session lifecycle

`ComputerSupervisor` has a 10-second start timeout and 1.5-second close timeout, serializes calls after success or rejection, terminates the worker on abort, and supports owner-scoped bulk close.

`ComputerWorkerCore` serializes inbound messages and tracks the last target whose screenshot reached the caller. Every execute message carries both `window` and `actions`.

Native `DesktopSession` runs a named `omp-desktop-session` thread behind a FIFO channel. Every batch has a 60-second deadline checked before each action and final capture. Close waits up to two seconds, is idempotent, and does not let the destructor block indefinitely.

## Side effects

- Captures the requested real host window into model/provider context. `desktop` captures every selected visible display.
- Delivers real keyboard and pointer events to the selected application. Numeric targets preserve foreground focus and the real pointer; `desktop` uses global input.
- Keeps a native worker and desktop session alive across calls.
- May expose secrets or notifications visible in the selected target or desktop composite.
- Does not launch a browser, upload to provider Files, persist screenshots, or spawn arbitrary helpers beyond its Bun/native workers and the bounded macOS capture service.

## Errors

Stable native codes:

- `DESKTOP_INVALID_OPTIONS`
- `DESKTOP_INVALID_ACTION`
- `DESKTOP_BACKEND_UNAVAILABLE`
- `DESKTOP_PERMISSION_DENIED`
- `DESKTOP_CAPTURE_FAILED`
- `DESKTOP_INPUT_FAILED`
- `DESKTOP_LAYOUT_CHANGED`
- `DESKTOP_COORDINATE_OUT_OF_BOUNDS`
- `DESKTOP_DEADLINE_EXCEEDED`
- `DESKTOP_SESSION_CLOSED`
- `DESKTOP_WORKER_FAILED`

Tool and worker errors also include:

- `Computer call requires a window target`
- `Computer actions require a window target`
- `Computer window must be "desktop" or a numeric id from the preceding result`
- `Computer call requires an array of actions`
- `Computer call contains an invalid action`
- `Coordinate computer actions require a screenshot of window ...`
- `Computer session is closed`
- `Timed out starting native computer worker`

Platform remedies are listed in [Window-scoped computer use: Troubleshooting](../computer-use.md#troubleshooting).

## Limits and proof boundary

- No non-native backend, browser fallback, DOM, or accessibility-tree control.
- Numeric window ids are ephemeral and listings are capped at 48.
- Background event delivery is application-dependent.
- No pure Wayland capture; rootless XWayland cannot provide `desktop`, and native Wayland windows cannot be numeric targets.
- Linux desktop-coordinate input rejects negative global origins and positions above 32767. Window-local input avoids that global pointer path.
- Windows and X11 window modules were compile-checked, not exercised on live hosts.
- A live macOS addon smoke returned 36 windows through capture-free `listWindows()`, then captured id `49` at `500×442`, preserved frontmost pid `800`, and did not warp the real pointer during a targeted move.
