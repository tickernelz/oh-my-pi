# LCM performance implementation notes

Machine-specific measurements for the credential-free LCM benchmarks. These are
**engineering records, not product guarantees** — absolute timings depend on the
host, and only the relative invariants each benchmark asserts are portable.

Generated benchmark output (`*.json` baselines) stays out of version control;
capture it under `/tmp` and record the summary here.

## Environment

| | |
|---|---|
|Host|Linux 6.18.35.2-microsoft-standard-WSL2 (x64)|
|CPU|12th Gen Intel(R) Core(TM) i5-12600K|
|Repository|`can1357/oh-my-pi` lossless-context feature branch|

## `lcm-backlog` — summary-pool throughput canary

Command (repository root):

```text
LCM_BACKLOG_SAMPLES=5 bun run packages/coding-agent/bench/lcm-backlog.bench.ts
```

Workload: 385 sources, 25 ms synthetic completion latency, 5 samples per width,
fresh temporary SQLite store per sample.

### Pre-change baseline (before span index and provider-handle serialization)

|Width|Peak/limit|Median|MAD|Jobs|Projection|
|---|---|---|---|---|---|
|1|1/1|725.394 ms|1.453 ms|21 completed (16 leaf), 0 pending/leased/failed/obsolete|ready, 0 pending, 0 uncovered|
|2|2/2|405.351 ms|1.607 ms|21 completed (16 leaf), 0 pending/leased/failed/obsolete|ready, 0 pending, 0 uncovered|

Width-1/width-2 speedup: **1.790x**. Exit status 0; the canary fails unless the
width-2 median lands at least 25% below width 1.

### Post-change result

|Width|Peak/limit|Median|MAD|Jobs|Projection|
|---|---|---|---|---|---|
|1|1/1|517.348 ms|4.221 ms|16 completed (16 leaf), 4 pending background|ready, 0 pending, 0 uncovered|
|2|2/2|275.604 ms|0.868 ms|16 completed (16 leaf), 3 pending + 1 leased background|ready, 0 pending, 0 uncovered|

Width-1 median fell **28.7%** and width-2 **32.0%**; speedup rose to **1.877x**.

Foreground readiness now needs 16 provider calls instead of 21 because a projection
is satisfied by a complete leaf cover and no longer waits for the condensation tree.
The remaining level-1 parents are legitimate background work still in flight when
`project()` returns, so the canary asserts foreground readiness, concurrency, unique
completions, exact attempt accounting, and deterministic leaf counts — not
whole-tree completion. Note `lcm.close()` aborts before draining
(`beginDispose()` cancels `#summaryAbortController`), so post-close job totals are
reported as metrics rather than asserted.

## `lcm-scale` — projection CPU and total-cost reconciliation

Run it directly; no saved baseline is needed:

```text
LCM_SCALE_SOURCES=10000 LCM_SCALE_BRANCHES=20 LCM_SCALE_SAMPLES=5 \
  bun run packages/coding-agent/bench/lcm-scale.bench.ts
```

Set `LCM_SCALE_REPORT_OUT=<path>` to also write the full JSON report.

The pre-change numbers live in the `PRE_CHANGE_REFERENCE` constant in the benchmark
source, so the regression gate is versioned and reviewable and survives a fresh
checkout. An earlier design compared against an external baseline JSON under `/tmp`;
that was replaced because the artifact could not be committed (generated output) and
could not be regenerated either — baseline mode records whatever the current tree
does, so recapturing it would silently turn the gate into a self-comparison.

Thresholds always enforced:

- projection wall-clock and process-CPU p95 below 50 ms
- completion-to-next-claim p95 below 25 ms (intentional retry backoff excluded)
- multi-branch/single-branch projection-latency ratio at most 1.5
- zero `summary_lineage` rows read during projection
- scheduler branch passes at most `2 × branches` (one pass per branch reconcile)
- no duplicate/gap/cross-branch coverage failures, no duplicate completion input hash
- provider-visible historical bytes strictly below the legacy source-ID serialization,
  recomputed from the same projection in the same run
- exactly-accounted dispatched attempts, no orphaned `in_flight` rows, retry probe
  records one billed `provider_error` plus one `completed` attempt

Additionally, when the knobs exactly match `PRE_CHANGE_REFERENCE.knobs`
(`on_reference_workload=1`), the run must beat the recorded pre-change wall p95, CPU
p95, and scheduler passes by at least `REQUIRED_SPEEDUP` (5×) and improve the latency
ratio. `REQUIRED_SPEEDUP` is a chosen policy floor, not a measurement: observed
post-change runs land 18–39×. A differing workload sets `on_reference_workload=0` and
reports without asserting, so the P3 tuning matrix cannot trip a gate calibrated for
10,000 sources across 20 branches.

### Plan gates that were measured false and relaxed with the user's approval

Two acceptance criteria in the plan could not be met by this workload. Both were
restored verbatim, measured, and shown to fail; the user accepted the finding on
2026-07-29, so they are now reported as metrics rather than asserted:

|Plan gate|Required|Measured|Reported as|
|---|---|---|---|
|historical-byte reduction vs saved baseline|≥80%|**19.1%**|`lcm_baseline_byte_reduction`|
|lossless total tokens vs context-full|below|**2.33× above**|`ratio_lossless_over_context_full`|
|lossless total tokens vs Snapcompact|below|**8.54× above**|`ratio_lossless_over_snapcompact`|

The cause is structural, not a defect: LCM keeps a lossless hierarchical cover of all
10,000 sources (34,721 primary tokens) while native context-full keeps one lossy
summary plus `keepRecentTokens` (2,188). The faithful projection is necessarily the
larger payload, so `break_even_turns_vs_context_full` is `never`. Meeting these gates
would require shortening the handle encoding or changing chunk/fan-in defaults, both
of which the plan explicitly rules out for this iteration.

What replaced them: historical bytes must fall strictly below the legacy source-ID
serialization, recomputed from the same projection in the same run.

### Pre-change baseline (re-keyed to fingerprint `9030ec97…01e7b`)

Captured before the span index and provider-handle serialization landed, with
`SOURCES=10000 BRANCHES=20 SAMPLES=5` (283 s wall, exit 0). Timing stores use
`SOURCES / BRANCHES` = 500 sources per branch and the measured active branch is
byte-identical in both, so the latency ratio isolates project-wide scan cost.
The cost lane is a separate single 10,000-source branch.

|Metric|Baseline|
|---|---|
|projection wall p95, 1 branch|5.204 ms|
|projection wall p95, 20 branches|55.434 ms|
|projection CPU p95, 20 branches|65.189 ms|
|projection latency ratio|**10.652×**|
|`summary_lineage` rows read per pass|**195,600**|
|scheduler branch passes|**11,620**|
|provider-visible historical bytes|170,057|
|LCM primary / maintenance tokens|42,829 / 813,682 (557 requests)|
|context-full primary / maintenance|2,188 / 346,375 (2 requests)|
|Snapcompact primary / frame tokens|9,577 / 85,408 (17 frames)|

The first three bolded rows are the defect this work removes: projection cost
scales with total project rows, not with the branch being projected.

### Post-change result (same command, same fingerprint)

|Metric|Pre|Post|
|---|---|---|
|total benchmark wall|283.35 s|**20.12 s**|
|projection wall p95, 20 branches|55.434 ms|**1.385 ms**|
|projection CPU p95, 20 branches|65.189 ms|**3.689 ms**|
|projection latency ratio|10.652×|**0.670×**|
|`summary_lineage` rows read per pass|195,600|**0**|
|scheduler branch passes|11,620|**20**|
|provider-visible historical bytes|170,057|137,625 (22.1% under the legacy form)|
|LCM maintenance requests|557|418|

Every normal-mode threshold passed (exit 0). The retry probe recorded exactly one
billed `provider_error` and one `completed` attempt with zero orphaned `in_flight`
rows, and the 1.87 ms retry pass contained no real sleeping.

### Baseline identity migration

The pre-change baseline was captured while `journalFingerprint()` still hashed
per-run minted entry ids, which made every comparison a false mismatch. The fix
hashes ordinal + type + normalized content instead. Rather than lose the only
pre-change record, its `fingerprint` field was re-keyed to the current scheme after
asserting identical `knobs` and generator version; **its metrics were not
recaptured**. This is sound because every fingerprint input — workload, knobs,
model, compaction settings, serializer version, runtime — is independent of the
optimization.

### Why the byte saving is scale-dependent

One ~300-byte handle replaces one source-ID list, so the saving tracks how many
sources each selected summary covers: a 200-source branch fell 85% (few summaries,
many sources each) while the 10,000-source branch fell 22.1% once per-item handles
dominate. The shipped gate is a strict reduction versus the legacy serialization,
recomputed from the same projection so it needs no cross-run baseline.

### Tuning matrix (evidence only — no default was changed)

`LCM_SCALE_LEAF_TOKENS`, `LCM_SCALE_FAN_IN`, and `LCM_SCALE_LEAF_SOURCES` drive the
cost lane as well as the timing lane. The first matrix attempt was inert: the cost
lane ran on `SessionLcm`'s own defaults, and `LEAF_SOURCES` was hardcoded at 24, so
the token cap never bound on short sources. Both are fixed; the knobs now move real
numbers (10,000 sources, 20 branches, 3 samples):

|leafTokens|fanIn|leafSources|maintenance requests|maintenance input|primary tokens|historical bytes|
|---|---|---|---|---|---|---|
|4,000|4|24 (production)|418|775,677|34,721|137,625|
|16,000|4|96|106|715,369|8,825|34,041|
|16,000|12|96|106|715,534|8,161|31,385|

Raising the leaf source cap 24 → 96 cuts maintenance requests ~4×, primary tokens
~4×, and historical bytes ~4×; fan-in barely matters by comparison.

**Do not read this as a recommendation.** The fixture returns a one-token summary no
matter how large the leaf, so a 96-source leaf looks free when a real provider would
need a proportionally longer summary. The matrix measures the *structural* effect
(fewer summaries, fewer handles in the projection) and overstates the token win.
Production defaults stay at 24 sources / 4,000 tokens / fan-in 4, as the plan
requires, until there is a real quality/cost corpus.
