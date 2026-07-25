import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { type ContextScope, type LcmContext, openLcmContext, type SourceEntry, type SourceSnapshot } from "../src";
import { initializeLcmSchema, summaryHandleForInput } from "../src/schema";

const MAIN: ContextScope = { projectId: "project", sessionId: "session", branchId: "main" };

function contentAddress(parts: readonly string[]): string {
	const hasher = new Bun.CryptoHasher("sha256");
	for (const part of parts) {
		hasher.update(`${Buffer.byteLength(part, "utf8")}:`);
		hasher.update(part);
	}
	return hasher.digest("hex");
}

function legacyFilelessSourceKey(source: SourceEntry): string {
	return contentAddress([
		"lcm-source-v1",
		source.projectId,
		source.contentHash,
		String(source.timestamp),
		source.kind,
		source.redactedText,
		JSON.stringify([...new Set(source.artifactRefs)].sort()),
	]);
}

function entry(
	scope: ContextScope,
	entryId: string,
	redactedText: string,
	parentId: string | null = null,
	atomicGroupId?: string,
): SourceEntry {
	return {
		...scope,
		entryId,
		parentId,
		timestamp: 1_800_000_000_000 + Number(entryId.replace(/\D/g, "") || 0),
		kind: "message",
		atomicGroupId,
		redactedText,
		contentHash: new Bun.CryptoHasher("sha256").update(`journal:${entryId}:${redactedText}`).digest("hex"),
		artifactRefs: [`artifact://${entryId}`],
	};
}

function snapshot(scope: ContextScope, entries: readonly SourceEntry[]): SourceSnapshot {
	return { scope, entries };
}

async function completeEveryJob(context: LcmContext): Promise<void> {
	for (let round = 0; round < 20; round++) {
		const result = await context.runSummaryJobs(
			{ workerId: "summarizer", leaseMs: 60_000, limit: 100, retryDelayMs: 0, maxOutputTokens: 100 },
			async job => ({
				redactedText: `s${job.level}`,
				tokenCount: 1,
			}),
		);
		if (result.claimed === 0) return;
	}
	throw new Error("summary hierarchy did not settle");
}

describe("LCM context contracts", () => {
	let tempDir = "";
	let dbPath = "";
	let context: LcmContext;
	let now = 1_900_000_000_000;

	beforeEach(async () => {
		tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "lcm-context-"));
		dbPath = path.join(tempDir, "context.db");
		context = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => now,
		});
	});

	afterEach(async () => {
		context?.close();
		if (tempDir) await fs.rm(tempDir, { recursive: true, force: true });
	});

	test("reconciliation is atomic and idempotent when a replacement snapshot is rejected", () => {
		const sources = [entry(MAIN, "e1", "first"), entry(MAIN, "e2", "second", "e1")];
		const first = context.reconcile(snapshot(MAIN, sources));
		const second = context.reconcile(snapshot(MAIN, sources));
		expect(first).toMatchObject({ changed: true, revision: 1, activeSources: 2 });
		expect(second).toMatchObject({ changed: false, revision: 1, activeSources: 2 });
		expect(context.status()).toMatchObject({ schemaVersion: 5, journalMode: "wal" });

		const duplicate = [sources[0]!, { ...sources[1]!, entryId: "e1" }];
		expect(() => context.reconcile(snapshot(MAIN, duplicate))).toThrow("duplicate source entry id");
		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 10, maxTokens: 100 } });
		expect(projection.freshTailSourceIds).toEqual(["e1", "e2"]);
		expect(projection.revision).toBe(1);
	});

	test("v4 migration enforces stable handles on later updates", () => {
		const migrationPath = path.join(tempDir, "v4.db");
		const db = new Database(migrationPath);
		try {
			db.run(
				"CREATE TABLE summaries (summary_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, level INTEGER NOT NULL)",
			);
			db.run(
				"CREATE TABLE summary_children (summary_id TEXT NOT NULL, ordinal INTEGER NOT NULL, child_summary_id TEXT NOT NULL)",
			);
			db.run(
				"CREATE TABLE summary_lineage (summary_id TEXT NOT NULL, ordinal INTEGER NOT NULL, source_key TEXT NOT NULL)",
			);
			db.run("INSERT INTO summaries (summary_id, project_id, level) VALUES ('sum-v4', 'project', 0)");
			db.run("INSERT INTO summary_lineage (summary_id, ordinal, source_key) VALUES ('sum-v4', 0, 'source-v4')");
			db.run("PRAGMA user_version = 4");

			initializeLcmSchema(db, 1_000);
			expect(db.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(5);
			expect(
				db.query<{ stable_handle: string }, []>("SELECT stable_handle FROM summaries").get()?.stable_handle,
			).toStartWith("summary_");
			expect(() => db.run("UPDATE summaries SET stable_handle = NULL")).toThrow("summary stable_handle is required");
		} finally {
			db.close();
		}
	});

	test("v5 reconciliation preserves fileless v4 source identity and migrated summary alignment", async () => {
		const migrationPath = path.join(tempDir, "v4-fileless.db");
		const source = entry(
			MAIN,
			"e1",
			"legacy fileless source with enough content to keep its migrated summary useful",
		);
		const sourceKey = legacyFilelessSourceKey(source);
		const summaryHandle = summaryHandleForInput(MAIN.projectId, 0, [{ kind: "source", id: sourceKey }]);
		const inputHash = contentAddress(["lcm-summary-input-v1", MAIN.projectId, "0", "source", sourceKey]);
		const db = new Database(migrationPath);
		try {
			initializeLcmSchema(db, 1_000);
			db.run("DROP TRIGGER summaries_stable_handle_required");
			db.run("DROP TRIGGER summaries_stable_handle_update_required");
			db.run("DROP INDEX summaries_stable_handle");
			db.run("DROP TABLE source_files");
			db.run("DROP TABLE file_records");
			db.run("ALTER TABLE summaries DROP COLUMN stable_handle");
			db.run("PRAGMA user_version = 4");

			const branch = db.run(
				"INSERT INTO branches (project_id, session_id, branch_id, reconciled_at) VALUES (?, ?, ?, ?)",
				[MAIN.projectId, MAIN.sessionId, MAIN.branchId, now],
			);
			db.run(
				`INSERT INTO source_contents
					(source_key, project_id, content_hash, timestamp_ms, kind, redacted_text, artifact_refs, token_count, created_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				[
					sourceKey,
					MAIN.projectId,
					source.contentHash,
					source.timestamp,
					source.kind,
					source.redactedText,
					JSON.stringify(source.artifactRefs),
					Math.ceil(Buffer.byteLength(source.redactedText, "utf8") / 4),
					now,
				],
			);
			db.run(
				`INSERT INTO branch_sources
					(branch_row_id, entry_id, parent_entry_id, position, source_key, active, created_at)
				 VALUES (?, ?, NULL, 0, ?, 1, ?)`,
				[Number(branch.lastInsertRowid), source.entryId, sourceKey, now],
			);
			db.run(
				`INSERT INTO summaries
					(summary_id, project_id, input_hash, level, redacted_text, token_count, created_at)
				 VALUES ('sum-v4-fileless', ?, ?, 0, 'migrated summary', 1, ?)`,
				[MAIN.projectId, inputHash, now],
			);
			db.run("INSERT INTO summary_lineage (summary_id, ordinal, source_key) VALUES ('sum-v4-fileless', 0, ?)", [
				sourceKey,
			]);
		} finally {
			db.close();
		}

		const migrated = await openLcmContext({ dbPath: migrationPath, now: () => now });
		try {
			expect(migrated.reconcile(snapshot(MAIN, [source]))).toMatchObject({
				changed: false,
				revision: 0,
				insertedSources: 0,
				tombstonedSources: 0,
				queuedJobs: 0,
				reusedSummaries: 1,
			});
			expect(migrated.describeSummary({ ...MAIN, summaryHandle })).toMatchObject({
				summaryHandle,
				sourceCount: 1,
				files: [],
			});
			expect(migrated.expandSummary({ ...MAIN, summaryHandle })?.items).toMatchObject([
				{ kind: "source", citation: { sourceId: source.entryId, sourceKey, position: 0 } },
			]);
		} finally {
			migrated.close();
		}
	});

	test("a completion callback failure leaves its job retryable without corrupting committed sources", async () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "durable source")]));
		const crashed = await context.runSummaryJobs(
			{ workerId: "crashing-worker", leaseMs: 1_000, limit: 1, retryDelayMs: 50, maxOutputTokens: 100 },
			async () => {
				throw new Error("simulated process exit");
			},
		);
		expect(crashed).toEqual({ claimed: 1, completed: 0, failed: 1, stale: 0, escalated: 0 });
		expect(context.status().jobs.failed).toBe(1);
		now += 50;
		const retried = context.claimSummaryJobs({
			workerId: "replacement-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(retried).toHaveLength(1);
		expect(retried[0]?.inputs[0]?.redactedText).toBe("durable source");
	});

	test("an abandoned lease is reclaimed after closing and reopening the store", async () => {
		context.reconcile(
			snapshot(MAIN, [entry(MAIN, "e1", "durable source survives an abandoned completion worker restart")]),
		);
		const [abandoned] = context.claimSummaryJobs({
			workerId: "departed-worker",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 5,
		});
		expect(abandoned).toBeDefined();
		context.close();
		now += 101;
		context = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => now,
		});
		const [reclaimed] = context.claimSummaryJobs({
			workerId: "replacement-worker",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 5,
		});
		expect(reclaimed?.jobId).toBe(abandoned?.jobId);
		expect(reclaimed?.leaseToken).not.toBe(abandoned?.leaseToken);
		expect(reclaimed?.inputs[0]?.redactedText).toBe("durable source survives an abandoned completion worker restart");
		expect(
			context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } })
				.freshTailSourceIds,
		).toEqual(["e1"]);
	});

	test("projection covers each source at most once and preserves branch order", async () => {
		const sources = [
			entry(MAIN, "e1", "one"),
			entry(MAIN, "e2", "two", "e1"),
			entry(MAIN, "e3", "three", "e2"),
			entry(MAIN, "e4", "four", "e3"),
			entry(MAIN, "e5", "five", "e4"),
		];
		context.reconcile(snapshot(MAIN, sources));
		await completeEveryJob(context);

		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 10 } });
		const covered = [...projection.historical.flatMap(item => item.sourceIds), ...projection.freshTailSourceIds];
		const historicalIds = projection.historical.flatMap(item => item.sourceIds);
		const tailIds = new Set(projection.freshTailSourceIds);
		expect(historicalIds.filter(sourceId => tailIds.has(sourceId))).toEqual([]);
		expect(projection.ready).toBe(true);
		expect(covered).toEqual(["e1", "e2", "e3", "e4", "e5"]);
		expect(new Set(covered).size).toBe(covered.length);
		expect(projection.estimatedTokens).toBeLessThanOrEqual(100);
	});

	test("summary expansion cites each repeated source-key occurrence in branch order", async () => {
		const first = entry(MAIN, "e1", "identical source content long enough to produce one useful leaf summary");
		const second = { ...first, entryId: "e2", parentId: first.entryId };
		context.reconcile(snapshot(MAIN, [first, second]));
		await completeEveryJob(context);

		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 0, maxTokens: 0 } });
		const summaryHandle = projection.historical[0]?.summaryHandle;
		if (!summaryHandle) throw new Error("expected a completed leaf summary");
		const expansion = context.expandSummary({ ...MAIN, summaryHandle });
		const citations = expansion?.items.flatMap(item => (item.kind === "source" ? [item.citation] : []));
		expect(citations?.map(citation => [citation.sourceId, citation.position])).toEqual([
			["e1", 0],
			["e2", 1],
		]);
		expect(citations?.map(citation => citation.sourceKey)).toEqual([
			legacyFilelessSourceKey(first),
			legacyFilelessSourceKey(first),
		]);
	});

	test("fresh-tail cut includes an assistant and every parallel tool result as one atomic closure", async () => {
		const sources = [
			entry(MAIN, "e1", "older context that can be summarized before the active tool turn"),
			{ ...entry(MAIN, "e2", "assistant issued two parallel tool calls", "e1", "tool-turn"), kind: "assistant" },
			{ ...entry(MAIN, "e3", "first parallel tool result", "e2", "tool-turn"), kind: "tool_result" },
			{ ...entry(MAIN, "e4", "second parallel tool result", "e3", "tool-turn"), kind: "tool_result" },
			entry(MAIN, "e5", "newest user follow-up", "e4"),
		];
		context.reconcile(snapshot(MAIN, sources));
		await completeEveryJob(context);

		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 2, maxTokens: 100 } });
		const historicalIds = projection.historical.flatMap(item => item.sourceIds);
		expect(projection.freshTailSourceIds).toEqual(["e2", "e3", "e4", "e5"]);
		expect(historicalIds).toEqual(["e1"]);
		expect([...historicalIds, ...projection.freshTailSourceIds]).toEqual(sources.map(source => source.entryId));
		expect(historicalIds.filter(sourceId => projection.freshTailSourceIds.includes(sourceId))).toEqual([]);
	});

	test("branches stay isolated while identical source prefixes reuse summaries", async () => {
		const mainSources = [
			entry(MAIN, "e1", "shared one"),
			entry(MAIN, "e2", "shared two", "e1"),
			entry(MAIN, "e3", "main three", "e2"),
			entry(MAIN, "e4", "main four", "e3"),
		];
		context.reconcile(snapshot(MAIN, mainSources));
		await completeEveryJob(context);

		const fork = { ...MAIN, branchId: "fork" };
		const forkSources = [
			entry(fork, "e1", "shared one"),
			entry(fork, "e2", "shared two", "e1"),
			entry(fork, "f3", "fork three", "e2"),
			entry(fork, "f4", "fork four", "f3"),
		];
		const reconciled = context.reconcile(snapshot(fork, forkSources));
		expect(reconciled.reusedSummaries).toBeGreaterThan(0);
		await completeEveryJob(context);

		const mainProjection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 0, maxTokens: 0 } });
		const forkProjection = context.project({ ...fork, tokenBudget: 100, freshTail: { maxSources: 0, maxTokens: 0 } });
		expect(mainProjection.historical.flatMap(item => item.sourceIds)).toEqual(["e1", "e2", "e3", "e4"]);
		expect(forkProjection.historical.flatMap(item => item.sourceIds)).toEqual(["e1", "e2", "f3", "f4"]);
	});

	test("shared summaries never cross a different branch's atomic boundaries", async () => {
		const mainSources = [
			entry(MAIN, "e1", "first shared source has enough text for its own summary"),
			entry(MAIN, "e2", "second shared source has enough text for summary reuse", "e1"),
			entry(MAIN, "e3", "third shared source closes the alternate branch group", "e2"),
		];
		context.reconcile(snapshot(MAIN, mainSources));
		await completeEveryJob(context);

		const fork = { ...MAIN, branchId: "grouped-fork" };
		context.reconcile(
			snapshot(fork, [
				entry(fork, "e1", mainSources[0]!.redactedText),
				entry(fork, "e2", mainSources[1]!.redactedText, "e1", "fork-tool-turn"),
				entry(fork, "e3", mainSources[2]!.redactedText, "e2", "fork-tool-turn"),
			]),
		);
		const projection = context.project({ ...fork, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } });
		expect(projection.freshTailSourceIds).toEqual(["e2", "e3"]);
		expect(projection.historical.flatMap(item => item.sourceIds)).toEqual([]);
		expect(projection.uncoveredSourceIds).toEqual(["e1"]);
	});

	test("leases are exclusive, reclaimable, and reject an expired owner's result", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "lease input")]));
		const first = context.claimSummaryJobs({ workerId: "worker-a", leaseMs: 100, limit: 1, maxOutputTokens: 100 });
		expect(first).toHaveLength(1);
		expect(context.claimSummaryJobs({ workerId: "worker-b", leaseMs: 100, limit: 1, maxOutputTokens: 100 })).toEqual(
			[],
		);

		now += 101;
		const replacement = context.claimSummaryJobs({
			workerId: "worker-b",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(replacement).toHaveLength(1);
		expect(replacement[0]?.leaseToken).not.toBe(first[0]?.leaseToken);
		expect(context.completeSummaryJob(first[0]!.jobId, first[0]!.leaseToken, { redactedText: "late" })).toEqual({
			accepted: false,
			reason: "lease_lost",
		});
		expect(
			context.completeSummaryJob(replacement[0]!.jobId, replacement[0]!.leaseToken, { redactedText: "accepted" }),
		).toMatchObject({ accepted: true });
	});

	test("completion rejects forged token counts and non-compressing output, then remains retryable", () => {
		const sourceText =
			"this source is intentionally long so returning it unchanged cannot be mistaken for compression";
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", sourceText)]));
		const [claim] = context.claimSummaryJobs({
			workerId: "worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 2,
		});
		expect(claim?.inputTokenCount).toBeGreaterThan(claim?.outputTokenBudget ?? 0);
		expect(claim?.outputTokenBudget).toBe(2);
		expect(
			context.completeSummaryJob(claim!.jobId, claim!.leaseToken, { redactedText: sourceText, tokenCount: 1 }),
		).toEqual({
			accepted: false,
			reason: "escalated",
			stage: "aggressive",
		});
		expect(context.status().jobs.pending).toBe(1);

		const [retry] = context.claimSummaryJobs({
			workerId: "replacement",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 2,
		});
		expect(
			context.completeSummaryJob(retry!.jobId, retry!.leaseToken, { redactedText: "ok", tokenCount: 1 }),
		).toMatchObject({
			accepted: true,
		});
	});

	test("completion rejects lineage that became misaligned with a branch atomic group", () => {
		const original = [
			entry(MAIN, "e1", "first source has enough content for compression"),
			entry(MAIN, "e2", "assistant starts a tool turn here", "e1"),
			entry(MAIN, "e3", "tool result closes that turn", "e2"),
		];
		context.reconcile(snapshot(MAIN, original));
		const claim = context
			.claimSummaryJobs({
				workerId: "worker",
				leaseMs: 1_000,
				limit: 10,
				maxOutputTokens: 2,
			})
			.find(job => job.sourceCount === 2);
		expect(claim).toBeDefined();
		context.reconcile(
			snapshot(MAIN, [
				original[0]!,
				{ ...original[1]!, atomicGroupId: "tool-turn" },
				{ ...original[2]!, atomicGroupId: "tool-turn" },
			]),
		);
		expect(context.completeSummaryJob(claim!.jobId, claim!.leaseToken, { redactedText: "ok" })).toEqual({
			accepted: false,
			reason: "stale",
		});
	});

	test("completion rejects a still-leased result whose source lineage disappeared", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "old lineage")]));
		const [claim] = context.claimSummaryJobs({ workerId: "worker", leaseMs: 1_000, limit: 1, maxOutputTokens: 100 });
		expect(claim).toBeDefined();
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e2", "replacement lineage")]));
		expect(context.completeSummaryJob(claim!.jobId, claim!.leaseToken, { redactedText: "stale summary" })).toEqual({
			accepted: false,
			reason: "stale",
		});
	});

	test("FTS searches only redacted source and summary text and returns resolvable citations", async () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "public alpha marker [redacted]")]));
		expect(context.search({ ...MAIN, query: "never-store-this-exact-value" })).toEqual([]);
		const sourceHits = context.search({ ...MAIN, query: "alpha + (marker)" });
		expect(sourceHits[0]).toMatchObject({ kind: "source", redactedText: "public alpha marker [redacted]" });
		expect(context.describe(sourceHits[0]!.citations[0]!)).toMatchObject({
			sourceId: "e1",
			redactedText: "public alpha marker [redacted]",
			artifactRefs: ["artifact://e1"],
		});

		await completeEveryJob(context);
		const summaryHit = context.search({ ...MAIN, query: "s0" }).find(hit => hit.kind === "summary");
		expect(summaryHit?.citations.map(citation => citation.sourceId)).toEqual(["e1"]);
	});

	test("project search is explicitly isolated and cites only active placements", async () => {
		const fork = { ...MAIN, branchId: "fork" };
		const other = { projectId: "other-project", sessionId: "session", branchId: "main" };
		const authorizedText = "catalogneedle authorized project source with enough content to summarize";
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", authorizedText)]));
		context.reconcile(snapshot(fork, [entry(fork, "e1", authorizedText)]));
		context.reconcile(
			snapshot(other, [entry(other, "e1", "catalogneedle foreign project source with enough content to summarize")]),
		);

		const shared = context.searchProject({ projectId: MAIN.projectId, query: "catalogneedle" });
		const sharedSource = shared.find(hit => hit.kind === "source");
		expect(sharedSource?.citations.map(citation => citation.branchId).sort()).toEqual(["fork", "main"]);
		expect(sharedSource?.citations.every(citation => citation.projectId === MAIN.projectId)).toBe(true);
		expect(sharedSource?.redactedText).toBe(authorizedText);

		context.reconcile(snapshot(fork, []));
		const activeOnly = context.searchProject({ projectId: MAIN.projectId, query: "catalogneedle" });
		expect(activeOnly.find(hit => hit.kind === "source")?.citations.map(citation => citation.branchId)).toEqual([
			"main",
		]);
		expect(context.searchProject({ projectId: "missing-project", query: "catalogneedle" })).toEqual([]);

		const foreign = context.searchProject({ projectId: other.projectId, query: "catalogneedle" });
		expect(
			foreign
				.find(hit => hit.kind === "source")
				?.citations.every(citation => citation.projectId === other.projectId),
		).toBe(true);
		await completeEveryJob(context);
		const summary = context
			.searchProject({ projectId: MAIN.projectId, query: "s0" })
			.find(hit => hit.kind === "summary");
		expect(summary?.citations.map(citation => [citation.projectId, citation.branchId])).toEqual([
			[MAIN.projectId, "main"],
		]);
		context.reconcile(snapshot(MAIN, []));
		expect(context.searchProject({ projectId: MAIN.projectId, query: "catalogneedle" })).toEqual([]);
		expect(context.searchProject({ projectId: MAIN.projectId, query: "s0" })).toEqual([]);
	});

	test("quarantine blocks projection until rebuild replaces every derived index", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "obsolete searchable term")]));
		expect(context.search({ ...MAIN, query: "obsolete" })).toHaveLength(1);
		context.quarantine("integrity check failed");
		expect(context.status()).toMatchObject({ quarantined: true, quarantineReason: "integrity check failed" });
		expect(() => context.project({ ...MAIN, tokenBudget: 10, freshTail: { maxSources: 1, maxTokens: 10 } })).toThrow(
			"quarantined",
		);

		const rebuilt = context.rebuild([snapshot(MAIN, [entry(MAIN, "e2", "replacement searchable term")])]);
		expect(rebuilt).toMatchObject({ branches: 1, activeSources: 1 });
		expect(context.status().quarantined).toBe(false);
		expect(context.search({ ...MAIN, query: "obsolete" })).toEqual([]);
		expect(context.search({ ...MAIN, query: "replacement" })[0]?.citations[0]?.sourceId).toBe("e2");
		expect(context.doctor().ok).toBe(true);
	});

	test("tombstones remain until the configured retention horizon and are then purged", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "temporary")]));
		context.reconcile(snapshot(MAIN, []));
		expect(context.status().tombstones).toBe(1);
		expect(context.purge().tombstones).toBe(0);
		now += 101;
		expect(context.purge().tombstones).toBe(1);
		expect(context.status().tombstones).toBe(0);
	});
});
