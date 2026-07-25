import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { type ContextScope, type LcmContext, openLcmContext, type SourceEntry, type SourceSnapshot } from "../src";

const MAIN: ContextScope = { projectId: "project", sessionId: "session", branchId: "main" };

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
		expect(context.status()).toMatchObject({ schemaVersion: 3, journalMode: "wal" });

		const duplicate = [sources[0]!, { ...sources[1]!, entryId: "e1" }];
		expect(() => context.reconcile(snapshot(MAIN, duplicate))).toThrow("duplicate source entry id");
		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 10, maxTokens: 100 } });
		expect(projection.freshTailSourceIds).toEqual(["e1", "e2"]);
		expect(projection.revision).toBe(1);
	});

	test("a completion callback failure leaves its job retryable without corrupting committed sources", async () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "durable source")]));
		const crashed = await context.runSummaryJobs(
			{ workerId: "crashing-worker", leaseMs: 1_000, limit: 1, retryDelayMs: 50, maxOutputTokens: 100 },
			async () => {
				throw new Error("simulated process exit");
			},
		);
		expect(crashed).toEqual({ claimed: 1, completed: 0, failed: 1, stale: 0 });
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
			reason: "not_compressed",
		});
		expect(context.status().jobs.failed).toBe(1);

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
