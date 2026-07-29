Search the current Lossless Context Management (LCM) project/session/branch index with bounded pagination and an optional summary-handle scope. Returns only redacted derived matches plus executable opaque summary/source handles. Use `lcm_describe` to inspect a handle or delegate a summary handle to a child with `lcm_expand`.

Source matches are grouped under the summary node that currently covers them, so each match carries the region of history it belongs to; matches not yet summarized appear under the fresh tail.

`mode` selects the matcher:

- `text` (default) is SQLite FTS5. The query is reduced to word tokens joined by AND, so `|`, `.*`, `^`, and other operators carry no special meaning — `foo|bar` finds documents containing both `foo` and `bar`.
- `regex` applies a linear-time Rust regex to the redacted derived text, so alternation, anchors, and ordering behave as written. It examines at most 20,000 branch documents in stable insertion order and reports matches in that order rather than by relevance.
