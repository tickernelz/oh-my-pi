# Lossless Context Management

Lossless Context Management (LCM) is an opt-in context engine that derives bounded, citation-bearing model context from the authoritative OMP session journal. It keeps its own redacted SQLite projection and summary DAG without rewriting or taking ownership of session JSONL.

LCM is disabled by default. Native OMP compaction remains the rollback and fail-open path.

## Runtime model

For automatic context maintenance, routing is exclusive:

1. LCM reconciles durable session entries and checks whether a complete projection fits the active model budget.
2. A ready projection owns the provider request and native compaction is skipped.
3. If projection is unavailable, unfit, backed off, or misses the 30-second hard wait, OMP continues with the configured native compaction strategy.

LCM does not compose its projection with `context-full`, `handoff`, `shake`, or `snapcompact`. Those remain native fallback strategies. Manual `/compact ...` commands also remain separate.

Below the soft threshold, LCM reconciles and drains summaries in the background while the request keeps native history. At the hard threshold, OMP waits at most 30 seconds for a ready projection before failing open.

## Enable and configure

Use `/settings` → **Context**:

- **Context Engine**: `Lossless`
- **Lossless Summary Model**: defaults dynamically to `@smol`
- **Concurrent Summaries**: `1` through `4`

Equivalent project configuration in `.omp/config.yml`:

```yaml
context:
  engine: lossless
  lossless:
    summaryModel: "@smol"
    maxConcurrentSummaries: 1
```

`context.engine` applies when a session starts or settings are reloaded. Engine reload is a real lifecycle transition: enabling creates and binds the runtime; disabling aborts and drains workers before closing its store. `context.lossless.*` changes apply live to an enabled runtime.

Concurrency is per `SessionLcm`, not a global provider limit:

- `1` preserves serial behavior and is the default.
- `2` is the recommended measured canary.
- `3` and `4` trade more provider traffic for faster backlog drain.

`providers.maxInFlightRequests` remains the aggregate cross-session/process provider cap.

## Cold warming and fallback

Existing or rebuilt projects may create hundreds of leaf summaries plus dependent condensed levels. A healthy queue can therefore remain `WARMING` longer than the 30-second foreground deadline. During this catch-up period, `lcmFallback: deadline` is expected: native compaction protects foreground responsiveness while background work continues.

Check progress with:

```text
/lcm status
```

Relevant fields include:

- active/configured workers;
- project-wide pending, running, failed, completed, and obsolete jobs;
- preferred/fallback backoff deadlines;
- current-branch revision and relevant pending jobs;
- latest fitted projection and resolved summary model.

A large pending count that steadily decreases with no failed jobs is warming, not a provider timeout. Diagnose persistent failures with:

```text
/lcm doctor
```

## Data, privacy, and cost

The session JSONL remains authoritative. LCM SQLite data is:

- derived and rebuildable;
- stored under the OMP data root, outside the repository;
- redacted through the session secret obfuscator before persistence;
- scoped by project, session, and branch;
- never a replacement for journal history.

LCM sends summary jobs to `context.lossless.summaryModel`. This creates additional provider requests, token usage, and cost. Retrieval/recall completions are isolated from the primary conversation and do not write provider output into session history.

Opaque LCM handles carry scoped identity, not arbitrary filesystem authority. File contents remain reference-only unless explicitly captured within the bounded derived record.

## Recovery and rebuild

SQLite uses WAL, lease tokens, conditional completion, and child-before-parent DAG scheduling. `/lcm doctor` checks schema, SQLite integrity, foreign keys, branch sequences, full-text search, document ownership, and quarantine state.

On a classified corruption error, startup recovery:

1. verifies corruption before schema mutation;
2. obtains exclusive recovery ownership;
3. closes temporary target handles;
4. quarantines main, WAL, and SHM as one database unit;
5. creates a fresh store and records recovery provenance;
6. reconciles the authoritative journal again.

Every live file-backed context holds shared recovery ownership, so physical quarantine cannot rename the database beneath another running OMP process.

Use an explicit rebuild when derived state is healthy but must be regenerated:

```text
/lcm rebuild current --yes
/lcm rebuild project --yes
```

Do not remove only `context.sqlite-wal` or `context.sqlite-shm`; they belong to the same database as `context.sqlite`.

## Disable and roll back

Set **Context Engine** back to `Native`, then reload session settings. The active LCM runtime stops claiming work, drains fenced finalizers, and closes its derived store. Session history needs no conversion because LCM never rewrites it.

The retained derived store is inert while Native is selected and can be reused if Lossless is enabled again. To discard it permanently, first stop every OMP process that may own the project store, then remove the complete project-specific derived-store directory. The next Lossless start rebuilds from JSONL.

## Extensions and alternate clients

LCM projection runs before primary extension context transforms. Ownership is pinned when the final primary provider request starts. An overflow from an already projected request proceeds to promotion/native compaction instead of retrying the same extension-expanded payload; a native request may use one projection that became ready while it was in flight.

Projection markers are live UI evidence only. They are not persisted into JSONL, print transcripts, RPC replay, exports, or shared snapshots. The transient `historicalContext` message is rejected by journal and replication boundaries before provider lowering.

LCM retrieval capability is forwarded only to explicitly scoped child/task sessions. Cross-project search requires a catalog-authorized project and never unions projects or opens arbitrary stores.

## Key implementation files

- `packages/lcm-context/src/context.ts`
- `packages/lcm-context/src/schema.ts`
- `packages/coding-agent/src/session/session-lcm.ts`
- `packages/coding-agent/src/session/agent-session.ts`
- `packages/coding-agent/src/lcm/operations.ts`
- `packages/coding-agent/src/lcm/slash-command.ts`
- `packages/coding-agent/src/tools/lcm.ts`
