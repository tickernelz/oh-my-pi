# Changelog

## [Unreleased]

### Added

- Added the standalone SQLite-backed Lossless Context Management engine with normalized source reconciliation, immutable summary lineage, leased completion jobs, branch-isolated projection, explicit project-scoped retrieval with active citations, diagnostics, retention, quarantine, and rebuild support.
- Added token-fenced job release, current-branch-prioritized claims and readiness counts, durable retry inspection, and contention-safe store opening so bounded summary workers can drain one SQLite-backed DAG safely in parallel.
