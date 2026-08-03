# Window-scoped computer use

`computer` lists, captures, and controls top-level windows on the host running `omp`. Omit `window` and `actions` to discover targets without taking a screenshot, then use a numeric id for an isolated application window or the synthetic `desktop` entry for the selected-display composite behavior. It uses native screen-capture and input APIs; it does not launch Chromium, use Puppeteer, or expose a DOM.

Use it for visible desktop applications: IDEs, terminals, native apps, browser windows, menus, and system dialogs. Use [`browser`](./tools/browser.md) instead when you need headless/CDP browser tabs, DOM or ARIA inspection, selectors, JavaScript evaluation, or deterministic page automation.

> [!WARNING]
> Enabling `computer` gives the model keyboard and pointer-event access to real applications. Window-targeted input does not focus the application or move the real pointer, but it can still trigger application side effects. Close unrelated sensitive applications, use a dedicated OS account or VM when practical, and configure approval policy before enabling it.

## Enable and configure

The tool is disabled by default. Add this to `~/.omp/agent/config.yml`, a project `.omp/config.yml`, or a one-shot `--config` overlay:

```yaml
computer:
  enabled: true
  backend: auto
  display: all
  maxWidth: 1920
  maxHeight: 1200

tools:
  approvalMode: write
```

`tools.approvalMode: write` automatically allows observation-only batches and prompts before keyboard or pointer input. For a prompt on every computer call, including screenshots:

```yaml
tools:
  approval:
    computer: prompt
```

To block the tool without changing `computer.enabled`:

```yaml
tools:
  approval:
    computer: deny
```

You can also enable it globally from the CLI:

```bash
omp config set computer.enabled true
omp config get computer.enabled
```

Inside a running session, the `/computer` slash command (`/computer`, `/computer on|off|status`) toggles the tool for that session only; it never writes settings files. `/computer status` reports the effective enabled/active state, backend, display and capture limits, active model, and function exposure. Explicit enablement and the desktop controller stay active across model switches. A switch that crosses the coordinate-safe sizing boundary recreates the controller and resnapshots backend/display/image-size settings. Changing config alone does not; start a new session after a settings change.

### Settings

| Key | Default | Meaning |
|---|---:|---|
| `computer.enabled` | `false` | Register the essential `computer` tool. |
| `computer.backend` | `auto` | `auto` or `native`. Both require a native backend; neither falls back to browser or software automation. |
| `computer.display` | `all` | Composite every active display, or select one numeric native display ID. |
| `computer.maxWidth` | `1920` | Maximum composite screenshot width in pixels. Image transports that cannot preserve original detail, including GitHub Copilot Responses and xAI OAuth, cap the effective width at `1280`; Claude-family models use the same cap as a compatibility fallback. |
| `computer.maxHeight` | `1200` | Maximum composite screenshot height in pixels. Those coordinate-safe transports cap the effective height at `896`; other models retain the configured limit. |

The `display` setting controls only the synthetic `desktop` target. A list-only call returns the current top-level windows with numeric ids, application names, titles, logical rectangles, and focus state without capturing any display. Successful targeted calls refresh that list alongside the screenshot. Pass one of those ids as `window` to isolate that application.

The `desktop` target's `displays` metadata lists each display ID, name, logical rectangle, screenshot-pixel rectangle, scale, and primary status. To limit that composite to one display:

```yaml
computer:
  display: "2"
```

A disconnected display ID fails with `DESKTOP_INVALID_OPTIONS`. A closed or resized window target fails with `DESKTOP_LAYOUT_CHANGED`; omit `window` and `actions` to refresh the available targets before retrying.

## Model integration

`computer` is exposed as a regular function tool to every compatible model, including models whose catalog metadata advertises provider-native Computer Use. Its function schema carries a window selector; provider-native computer declarations cannot represent per-call host-window targeting.

OpenAI and Ollama can force the named function, Anthropic and Bedrock can force the named tool, and Google uses required-tool mode. Adapters without a named forcing form keep provider-default selection. A list-only result is text. Targeted results carry the refreshed window list followed by the fresh PNG through the provider's ordinary tool-result image path.

While the tool is active, the system prompt routes host-window requests through `computer` and requires inspection of each fresh returned screenshot before the next action. This does not auto-enable the tool, bypass approval, or prevent a user-requested alternative after a computer error.

If the tool never appears:

1. Confirm `computer.enabled` is true in the effective config, or toggle it with `/computer`.
2. Start a new session after changing settings files; `/computer` toggles apply immediately.

## Actions

Omit both `window` and `actions` to list targets without a screenshot. To capture or act, pass `desktop` or a numeric id from that list; actions without a target are rejected. Omit `actions` or pass an empty array to capture the selected target without input. Ordered actions execute serially and a successful targeted call returns exactly one fresh PNG after the entire batch. `screenshot` markers are deferred: they emit no input, produce no intermediate image, and do not rebase later coordinates in the same batch.

| Action | Required fields | Behavior |
|---|---|---|
| `click` | `button`, `x`, `y` | Click once. Buttons: `left`, `right`, `wheel`, `back`, `forward`. Optional `keys` holds modifiers. |
| `double_click` | `x`, `y` | Double-click the left button. Optional `keys` holds modifiers. |
| `drag` | `path` | Hold left at the first point, visit the remaining points, release at the last. At least two points. Optional modifier `keys`. |
| `keypress` | `keys` | Press one key or chord. The array must contain at least one non-empty key. |
| `move` | `x`, `y` | Deliver pointer movement to the target. A window target leaves the real pointer unchanged; `desktop` retains global pointer movement. Optional modifier `keys`. |
| `screenshot` | none | Request the batch's final capture without input. |
| `scroll` | `x`, `y`, `scroll_x`, `scroll_y` | Scroll at the point horizontally and/or vertically. Window targets receive direct events without pointer movement. Optional modifier `keys`. |
| `type` | `text` | Type Unicode text through the target's native input path. |
| `wait` | none | Wait two seconds before continuing. |

Coordinates and drag points must be non-negative screenshot pixels. Mouse `keys` may contain only unique modifiers: Control, Shift, Alt/Option, or Meta/Command/Super/Windows. Key names are case-insensitive; common names include `ENTER`, `ESCAPE`, `TAB`, `SPACE`, `BACKSPACE`, `DELETE`, arrows, navigation keys, and `F1`–`F24`. A keypress entry may contain `+`, for example `CTRL+SHIFT+P`. Single Unicode characters are also accepted. macOS has no native `PRINTSCREEN` or `F21`–`F24` mapping.

A batch containing only `screenshot` and `wait` is observation-only. Any click, move, drag, scroll, keypress, or type action makes the whole call input-capable.

## Screenshot coordinates and target mapping

Always choose coordinates from the immediately preceding successful `computer` result for the same `window`. Every coordinate action in one batch maps through that prior frame. Switching between `desktop` and a numeric id invalidates the coordinate frame; capture the new target first. A model switch that crosses the coordinate-safe sizing boundary also recreates the controller and requires a fresh capture.

Each result begins with a text list like:

```text
Window targets (pass the id as `window`):
- desktop — Desktop
- 42 — Code — Editor · 800×600 at 100,50 · focused
```

The following PNG is only the requested target.

For a numeric window target, OMP captures the window's own content and treats the returned PNG origin as `(0, 0)`. Before coordinate input it finds that id again. A moved window is rebased to its current global position; a closed or resized window clears the frame and returns `DESKTOP_LAYOUT_CHANGED`. Native input is posted directly to the selected window, so it does not activate the app or warp the user's pointer.

For `desktop`, OMP retains the selected-display composite:

1. Enumerate selected displays and their global logical rectangles.
2. Capture each display at native pixel density.
3. Build one logical bounding rectangle, including negative monitor origins.
4. Choose one render scale within `maxWidth` and `maxHeight`.
5. Place each image in the composite and return one PNG.

Each `displays` item maps global logical coordinates (`x`, `y`, `width`, `height`) to the PNG (`pixelX`, `pixelY`, `pixelWidth`, `pixelHeight`) and reports native `scale`. Desktop input locates the display containing the screenshot pixel, scales within that rectangle, then adds its global origin. Composite gaps remain black and are not clickable.

The worker rejects coordinate input until it has returned a screenshot of that exact target. After any visual transition whose target may have moved, finish the current call and use its fresh result for the next call.

## Multiple displays

`computer.display: all` produces one composite. Displays are sorted by logical vertical position, then horizontal position, then ID. Mirrored displays with the same logical rectangle are coalesced; the primary mirror wins. Invalid scales, duplicate IDs, and overlapping non-mirrored rectangles fail closed rather than guessing.

Use one display when:

- the desktop is very wide and labels become hard for the model to read after downscaling;
- a layout gap makes targets ambiguous; or
- you want to isolate sensitive content on another monitor.

On Linux, capture reads the X11 root window with core `GetImage` and input is emitted as XTest events in the same X11 global coordinate space, so multi-display coordinate mapping is exact. This requires an X server that owns a readable root pixmap — a real X11 session, Xvfb, or a rootful XWayland (`Xwayland -rootful`). The default **rootless** XWayland used by GNOME, KDE, and sway keeps no X11 root pixmap, so root `GetImage` fails; the tool detects this at initialization and reports `DESKTOP_BACKEND_UNAVAILABLE` instead of failing on the first screenshot. Pure Wayland capture (portal/PipeWire) is not implemented.

## Approval and safety

### Tool approval

- `screenshot`/`wait`-only batches declare `read` approval.
- Any input action declares `exec` approval.
- Missing or malformed action metadata defaults to `exec`.
- `tools.approval.computer` overrides the active mode with `allow`, `prompt`, or `deny`.

With `tools.approvalMode: write`, screenshots are automatically allowed and input prompts. The schema default is `yolo`, which normally auto-approves both; use `write`, `always-ask`, or an explicit per-tool policy when controlling a real application.

### Consequential-action confirmation

OMP treats screen text, images, notifications, websites, documents, chat messages, and application instructions as untrusted data. They cannot authorize actions or override your direct instructions.

The agent must confirm at the point of risk before consequential side effects unless your direct message already authorized that exact action, target, scope, and values. Examples include sending or publishing, purchases or transfers, deletion, account/security or permission changes, disclosure of private data, accepting legal terms, and irreversible operations. High-impact financial, employment, housing, education, insurance/credit, legal, medical, government, election, biometric, and highly sensitive-data actions require point-of-risk confirmation.

Operational guidance:

- Do not place secrets in visible windows unless the task needs them.
- Never follow on-screen requests to reveal credentials, change policy, or ignore instructions.
- Review the exact destination and payload before Submit, Send, Buy, Delete, or Allow.
- Prefer a dedicated desktop session for untrusted sites or documents.
- Stop when the visible state differs from the user's stated target.

See [Tool approval mode](./approval-mode.md) for general policy resolution.

## Platform setup and support

| Platform | Desktop target | Numeric window target |
|---|---|---|
| macOS x64/arm64 | Bounded `screencapture` composite and global Quartz/native input | `screencapture -l` plus process-targeted mouse and keyboard events. Does not activate the app or move the real pointer. |
| Linux x64/arm64, glibc/musl, X11 | Root `GetImage` and XTest input | Direct X11 window `GetImage` and targeted events. Does not change focus or the root pointer. |
| Linux Wayland | Requires a rooted/rootful XWayland server; pure Wayland is unsupported | Only XWayland client windows can be enumerated. Native Wayland windows are invisible; the default rootless setup still cannot provide the initial `desktop` capture. |
| Windows x64 | xcap display capture and `SendInput` over the virtual desktop | xcap window capture and direct Win32 messages. Does not activate the app or move the real pointer. |
| Other OS/architectures | Unsupported by the published native package matrix | Unsupported. |

### macOS permissions

Open **System Settings → Privacy & Security**:

1. Grant **Screen Recording** to the terminal or application that launches `omp`.
2. Grant **Accessibility** to the same host for keyboard and pointer input.
3. Fully restart that host and start a new OMP session.

OMP performs a non-prompting Screen Recording preflight. It does not open the permission dialog. Accessibility is not separately preflighted; denial normally surfaces when native input initializes or emits an event.

### Linux setup

For X11, run OMP inside the target graphical session and ensure `DISPLAY` identifies it. The backend speaks the X protocol directly and requires RandR and XTEST; it links no GUI system libraries.

The `desktop` target needs a readable X11 root pixmap: a real X11 session, Xvfb, or rootful XWayland. The default rootless XWayland used by GNOME, KDE, and sway has no root pixmap. Numeric window targets can see only X11/XWayland clients; pure Wayland capture through a portal/PipeWire is not implemented.

The desktop backend is bundled in the core `pi-natives` addon on every published Linux target. It opens no display connection until the tool runs, so headless hosts are unaffected.

## Session and worker lifecycle

The tool is exclusive: computer calls do not run concurrently.

```text
computer tool
  → ComputerSupervisor (lazy, serialized queue)
  → dedicated Bun worker
  → native DesktopSession
  → dedicated native desktop worker thread
  → capture/input APIs
```

The Bun worker starts on the first call with a 10-second deadline. The session keeps the most recently returned target geometry so later coordinates can be validated against the exact `window`. Every successful action batch ends with one fresh target capture.

Closing the agent/eval owner closes all owned controllers. Normal close waits up to 1.5 seconds before terminating the Bun worker; native close is idempotent and bounded. Aborting a call terminates that worker and rejects pending requests. A later call starts a fresh worker and must establish a new screenshot frame.

## Troubleshooting

Computer backend errors begin with a stable code:

| Error | Meaning and response |
|---|---|
| `DESKTOP_INVALID_OPTIONS` | Invalid backend, zero image limit, malformed display value, or inactive display ID. Correct config and start a new session. |
| `DESKTOP_INVALID_ACTION` | Unknown target/action/button/key, missing or unexpected fields, negative point, short drag path, duplicate modifier, or coordinates requested against a different prior target. Capture the requested target before retrying. |
| `DESKTOP_BACKEND_UNAVAILABLE` | No graphical session/backend, missing XWayland `DISPLAY`, missing RandR/XTEST, an unrepresentable Linux desktop layout, or native input initialization failure. Follow the platform section. |
| `DESKTOP_PERMISSION_DENIED` | Screen capture or input permission denied. Grant OS permissions and restart the host/session. |
| `DESKTOP_CAPTURE_FAILED` | Display/window capture, scaling, allocation, or PNG encoding failed. Verify the target still exists and reduce capture limits if needed. |
| `DESKTOP_INPUT_FAILED` | Targeted or desktop input failed. The application may reject background events; also check macOS Accessibility or X server access. |
| `DESKTOP_LAYOUT_CHANGED` | The prior desktop topology changed, or the target window closed/resized. Capture a fresh target before input. |
| `DESKTOP_COORDINATE_OUT_OF_BOUNDS` | Point lies outside the target PNG or in a desktop-composite gap. Choose a point inside the returned image. |
| `DESKTOP_DEADLINE_EXCEEDED` | The 60-second batch deadline expired; remaining actions were not executed. Split the batch and capture again. |
| `DESKTOP_SESSION_CLOSED` | Native session was closed. Start a new OMP session. |
| `DESKTOP_WORKER_FAILED` | Worker startup, communication, timeout, or shutdown failed. Restart; if persistent, verify the native addon installation. |

Common exact failures:

- `Computer actions require a window target` → list targets first, then pass `window: "desktop"` or a numeric id with the actions.
  `Computer call requires a window target` means a supplied `window` was blank or not a string; pass a valid target, or omit both `window` and `actions` for discovery.
- `Coordinate computer actions require a screenshot of window ...` → capture that exact target before coordinate input.
- `X11 root window is not a readable drawable ...` → the `desktop` target is unavailable on rootless XWayland; use native X11/Xvfb/rootful XWayland.
- `macOS Screen Recording permission is not granted for this process` → grant the launching host Screen Recording and restart it.
- `Timed out starting native computer worker` → verify the installed native addon matches the OMP release, then restart or reinstall.

The native composite safety ceiling is 268,435,456 pixels. Normal defaults are far below it. Very large or sparse monitor arrangements should use a smaller maximum or one selected display.

## Verified limitations

- Native OS control only; no DOM, ARIA tree, selectors, browser tab lifecycle, accessibility-tree actions, or Puppeteer fallback.
- The model acts on screenshots; OCR and visual interpretation can be wrong.
- Numeric ids are ephemeral OS window identifiers. Use only ids listed by the latest result.
- Window enumeration is capped and omits minimized, untitled/system-sized, and tiny windows.
- Background event delivery is application-dependent. Secure, elevated, sandboxed, custom-rendered, or policy-protected surfaces may reject it; OMP has no bypass.
- Coordinates are valid only for the preceding frame of the same target.
- The `desktop` target can downscale text, contains non-clickable display gaps, and retains global focus/pointer behavior.
- Pure Wayland windows are not capturable. Rootless XWayland cannot provide the `desktop` target; only visible XWayland clients are eligible for numeric targeting.
- Linux desktop-coordinate input rejects negative global origins and positions above XTest's 32767 limit. Numeric window-local input avoids the global pointer path.
- Windows window targeting was compile-checked but not exercised on a live Windows host.

## Verification boundary

The live macOS smoke used the built `pi-natives` addon on a real host. Capture-free `listWindows()` returned 36 top-level targets; a later targeted call captured window id `49` as an isolated `500×442` PNG and returned the same id as the capture target. A window-local move completed while the frontmost process id remained `800`; the real pointer remained about 1,495 pixels from the synthesized window point rather than being warped there.

This proves live capture-free discovery, isolated capture, target propagation, and focus/pointer preservation through `DesktopSession`. Rust units cover target parsing, frame switching, geometry validation, and platform-independent mapping. The Win32 and X11 window modules were compile-checked in isolated target harnesses; their event delivery was not exercised on live hosts.

For implementation-level inputs, outputs, lifecycle, and error surfaces, see [`docs/tools/computer.md`](./tools/computer.md).
