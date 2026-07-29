# Changelog

## [Unreleased]

### Added

- Added the standalone SQLite-backed Lossless Context Management engine with normalized source reconciliation, immutable summary lineage, leased completion jobs, branch-isolated projection, explicit project-scoped retrieval with active citations, diagnostics, retention, quarantine, and rebuild support.
- Added token-fenced job release, current-branch-prioritized claims and readiness counts, durable retry inspection, and contention-safe store opening so bounded summary workers can drain one SQLite-backed DAG safely in parallel.
- Added a per-attempt provider usage ledger (schema v7 `summary_attempts`) with start/finish fencing, so every dispatched summary request records its billed tokens and cost under exactly one terminal outcome and a superseded or re-leased job can never resurrect an obsolete retry.
- Added a derived branch-local span index (schema v8 `branch_summary_spans`) with exact fan-in condensation, append-only leaf scheduling, orphan repair, and a `branch-summary-spans` doctor check, replacing project-wide lineage scanning during projection and scheduling.
- Added optional process-local `LcmStatus.performance` counters for projection calls, wall/CPU time, lineage rows read, and scheduler branch passes.

### Changed

- LCM schema v6 removes redundant terminal-job input/lineage payloads while preserving safe requeue reconstruction, and retention-aware GC now prunes only complete quarantine artifacts older than 30 days while preserving active lineage and the latest or pending recovery unit.
- Projection, job claiming, retry delay, failure listing, and retention now derive placement from current-revision branch spans instead of realigning flattened `summary_lineage`. On a 20-branch/10,000-source store this cut projection wall p95 from 55.434 ms to 1.385 ms, process-CPU p95 from 65.189 ms to 3.689 ms, lineage rows read per pass from 195,600 to 0, and scheduler branch passes from 11,620 to 20.
- A projection now becomes ready on a complete leaf cover instead of waiting for the whole condensation tree, so background condensation no longer gates foreground readiness. `summary_lineage` and `summary_children` remain immutable provenance and continue to authorize retrieval.
- Existing v6/v7 stores upgrade lazily: migrations create empty attempt and span tables, and the first authoritative reconcile derives current spans. Until then projection is unready and native behavior owns the request.

### Fixed

- `recoverCorrupt` now runs `PRAGMA quick_check(1)` before schema mutation and serializes quarantine through a sibling lock database, so latent B-tree damage is rebuilt once instead of repeatedly failing reconciliation or racing concurrent openers.
- Physical corruption recovery now requires exclusive sibling ownership while every live file-backed context retains a shared guard, preventing main/WAL/SHM quarantine beneath another running process.
- SQLite contention detection now honors explicit cause-chain codes before codeless lock-message fallbacks, so disk-full, permission, and ordinary I/O failures are never retried or quarantined as lock contention.
- Corruption recovery now verifies the database and coordinates with other live contexts before quarantining the main database, WAL, and SHM files.
- Lock contention is retried separately from disk-full, permission, and ordinary I/O failures, which are returned to the caller unchanged.
- Branch-scoped full-text search now filters to the requested lineage before pagination, including maximum-offset queries.
- Corruption quarantine now moves WAL/SHM before the main database, durably recovers interrupted moves, and avoids mode-changing writes for established recovery guards.
