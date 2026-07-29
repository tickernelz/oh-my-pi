# Changelog

## [Unreleased]

### Added

- Added the standalone SQLite-backed Lossless Context Management engine with normalized source reconciliation, immutable summary lineage, leased completion jobs, branch-isolated projection, explicit project-scoped retrieval with active citations, diagnostics, retention, quarantine, and rebuild support.
- Added token-fenced job release, current-branch-prioritized claims and readiness counts, durable retry inspection, and contention-safe store opening so bounded summary workers can drain one SQLite-backed DAG safely in parallel.

### Changed

- LCM schema v6 removes redundant terminal-job input/lineage payloads while preserving safe requeue reconstruction, and retention-aware GC now prunes only complete quarantine artifacts older than 30 days while preserving active lineage and the latest or pending recovery unit.

### Fixed

- `recoverCorrupt` now runs `PRAGMA quick_check(1)` before schema mutation and serializes quarantine through a sibling lock database, so latent B-tree damage is rebuilt once instead of repeatedly failing reconciliation or racing concurrent openers.
- Physical corruption recovery now requires exclusive sibling ownership while every live file-backed context retains a shared guard, preventing main/WAL/SHM quarantine beneath another running process.
- SQLite contention detection now honors explicit cause-chain codes before codeless lock-message fallbacks, so disk-full, permission, and ordinary I/O failures are never retried or quarantined as lock contention.
- Corruption recovery now verifies the database and coordinates with other live contexts before quarantining the main database, WAL, and SHM files.
- Lock contention is retried separately from disk-full, permission, and ordinary I/O failures, which are returned to the caller unchanged.
- Branch-scoped full-text search now filters to the requested lineage before pagination, including maximum-offset queries.
- Corruption quarantine now moves WAL/SHM before the main database, durably recovers interrupted moves, and avoids mode-changing writes for established recovery guards.
