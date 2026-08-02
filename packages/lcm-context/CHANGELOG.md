# Changelog

## [Unreleased]

### Added

- Added the standalone SQLite-backed Lossless Context Management engine with normalized source reconciliation, immutable summary lineage, leased completion jobs, branch-isolated projection, explicit project-scoped retrieval with active citations, diagnostics, retention, quarantine, and rebuild support.
- Added token-fenced job release, current-branch-prioritized claims and readiness counts, durable retry inspection, and contention-safe store opening so bounded summary workers can drain one SQLite-backed DAG safely in parallel.
- Added a per-attempt provider usage ledger (schema v7 `summary_attempts`) with start/finish fencing, so every dispatched summary request records its billed tokens and cost under exactly one terminal outcome and a superseded or re-leased job can never resurrect an obsolete retry.
- Added a derived branch-local span index (schema v8 `branch_summary_spans`) with exact fan-in condensation, append-only leaf scheduling, orphan repair, and a `branch-summary-spans` doctor check, replacing project-wide lineage scanning during projection and scheduling.
- Added optional process-local `LcmStatus.performance` counters for projection calls, wall/CPU time, lineage rows read, and scheduler branch passes.
- Added `SearchRequest.mode: "regex"`, a bounded linear-time alternative to FTS token conjunction. The package keeps zero runtime dependencies, so the host injects the matcher through `LcmContextOptions.regexEngine`; without one, regex mode throws instead of silently returning nothing. A scan streams candidates in insertion order, stops at the first full page, and examines at most 20,000 authorized branch documents.
- Added `SearchHit.position` and `SearchHit.coveringSummaryHandle`, resolved from the current revision's `branch_summary_spans` so a source match reports the summary node that presently covers it. The lowest-level containing span wins, which is the most specific cover.
- Added `ProjectedHistoricalItem.files`, the de-duplicated file metadata of every source a projected summary compacted, so callers can keep file awareness in the active context. It is filled by one query per projection rather than one per item, preserving the branch-span projection latency.
- Added schema v9 session attribution on `summary_attempts` plus `priorSummarySpendUsd(sessionId, before)`, so a resumed session can restore its own LCM spend from the ledger instead of restarting the total at zero. Pre-v9 rows stay `NULL` and are never billed to a session.
- Added schema v10 durable summary retry policies with epoch/token/nonce-fenced leases, attempt caps, wakeable backoff, relevant-branch availability, due/all retry control, and migration that revokes legacy leases atomically.
- Added an ordered active-source fingerprint to every projection so callers can prove exact coverage without a second store read.

### Changed

- LCM schema v6 removes redundant terminal-job input/lineage payloads while preserving safe requeue reconstruction, and retention-aware GC now prunes only complete quarantine artifacts older than 30 days while preserving active lineage and the latest or pending recovery unit.
- Projection, job claiming, retry delay, failure listing, and retention now derive placement from current-revision branch spans instead of realigning flattened `summary_lineage`. On a 20-branch/10,000-source store this cut projection wall p95 from 55.434 ms to 1.385 ms, process-CPU p95 from 65.189 ms to 3.689 ms, lineage rows read per pass from 195,600 to 0, and scheduler branch passes from 11,620 to 20.
- A projection now becomes ready on a complete leaf cover instead of waiting for the whole condensation tree, so background condensation no longer gates foreground readiness. `summary_lineage` and `summary_children` remain immutable provenance and continue to authorize retrieval.
- Existing v6/v7 stores upgrade lazily: migrations create empty attempt and span tables, and the first authoritative reconcile derives current spans. Until then projection is unready and native behavior owns the request.
- A file's `explorationSummary` no longer participates in the content-addressed source key. It is derived descriptive metadata, so including it meant that improving a file description minted new source keys and discarded every cached summary that referenced the file. Reconciling an otherwise identical entry now keeps its source key and refreshes `file_records.exploration_summary` in place, including when branch placement is unchanged.
- A summary job's output budget now scales with its input instead of a flat 2,048-token cap: `min(input - 1, 4096, max(callerCap, ceil(input / 2)))`. A summary model condenses already-summarized inputs by roughly 2x, so the flat cap demanded 2.4x on a large parent and 8.7x on an indivisible oversized leaf and was routinely missed.
- Completion acceptance now rejects only on a single cap, `min(input - 1, 4096, ceil(budget * 1.3))`, rather than on the leased budget exactly. The budget is a request, not an invariant — some provider wires strip output-cap fields entirely — so summaries that overshot the ask while still compressing their input were being discarded and escalated, spending two extra calls to reach a worse result. Monotonic shrink and the 4,096-token node ceiling remain hard invariants, so the DAG still converges and cover cost stays bounded.
- The terminal deterministic stage now keeps the aggressive budget instead of collapsing to 512 tokens. Truncation always compresses, so the old floor discarded most of a node's content without protecting any invariant, and it did the most damage at condensation levels where inputs are densest.
- Projection now returns the complete coarsest available frontier regardless of the render budget, keeps the newest atomic source unit mandatory, and treats fresh-tail source/token limits as targets for the complete raw tail.
- Retry availability is classified from one SQLite snapshot, and retry counts/backoff survive non-compressing escalation and same-epoch obsolete reactivation; only accepted completion or an authorized model/rebuild epoch resets them.

### Fixed

- `recoverCorrupt` now runs `PRAGMA quick_check(1)` before schema mutation and serializes quarantine through a sibling lock database, so latent B-tree damage is rebuilt once instead of repeatedly failing reconciliation or racing concurrent openers.
- Physical corruption recovery now requires exclusive sibling ownership while every live file-backed context retains a shared guard, preventing main/WAL/SHM quarantine beneath another running process.
- SQLite contention detection now honors explicit cause-chain codes before codeless lock-message fallbacks, so disk-full, permission, and ordinary I/O failures are never retried or quarantined as lock contention.
- Corruption recovery now verifies the database and coordinates with other live contexts before quarantining the main database, WAL, and SHM files.
- Lock contention is retried separately from disk-full, permission, and ordinary I/O failures, which are returned to the caller unchanged.
- Branch-scoped full-text search now filters to the requested lineage before pagination, including maximum-offset queries.
- Corruption quarantine now moves WAL/SHM before the main database, durably recovers interrupted moves, and avoids mode-changing writes for established recovery guards.
