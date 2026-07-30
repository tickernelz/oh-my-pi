import { describe, expect, it, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type {
	Citation,
	ClaimSummaryJobsOptions,
	CompleteSummaryJobResult,
	ContextProjection,
	ContextScope,
	FileDescription,
	FileReference,
	LcmContext,
	LcmStatus,
	ProjectionRequest,
	ProjectSearchRequest,
	ReconcileOptions,
	ReconcileResult,
	SearchHit,
	SearchRequest,
	SourceDescription,
	SourceSnapshot,
	SummaryAttemptOutcome,
	SummaryAttemptProvenance,
	SummaryCompletion,
	SummaryDescription,
	SummaryExpansion,
	SummaryExpansionRequest,
	SummaryFailureAttemptOutcome,
	SummaryJob,
	SummaryLocalAttemptOutcome,
	SummaryProviderAttempt,
	SummaryProviderAttemptStart,
	SummaryProviderUsage,
	SummaryReference,
} from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import * as AIError from "@oh-my-pi/pi-ai/error";
import { decodeLcmHandle, encodeLcmHandle } from "@oh-my-pi/pi-coding-agent/lcm/operations";
import { convertToLlm, createHistoricalContextMessage } from "@oh-my-pi/pi-coding-agent/session/messages";
import {
	estimateLcmProjectionMessageTokens,
	LcmCompletionError,
	type LcmCompletionRequest,
	type LcmCompletionResult,
	normalizeLcmBranch,
	normalizeLcmHardProjectionWaitMs,
	SessionLcm,
	type SessionLcmOptions,
} from "@oh-my-pi/pi-coding-agent/session/session-lcm";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { createAssistantMessage } from "./helpers/agent-session-setup";

class FakeLcmContext implements LcmContext {
	snapshots: SourceSnapshot[] = [];
	closed = false;
	failedError: string | undefined;
	queuedJobs = 0;
	relevantPendingJobs: number | undefined;
	reconcileAttempts = 0;
	reconcileErrors: unknown[] = [];
	reconcileOptions: Array<ReconcileOptions | undefined> = [];
	nextDelayMs: number | null = null;
	deferredClaims = 0;
	claimErrors: unknown[] = [];
	releaseErrors: unknown[] = [];
	jobs: SummaryJob[] = [];
	leaseSequence = 0;
	readonly leasedJobs = new Map<string, SummaryJob>();
	readonly failureRecords = new Map<
		string,
		{ jobId: string; availableAt: number; queueClass: SummaryJob["queueClass"] }
	>();
	readonly claimCalls: ClaimSummaryJobsOptions[] = [];
	readonly delayCalls: Array<{ preferredScope: ContextScope | undefined; allowFallback: boolean }> = [];
	readonly releaseCalls: Array<{ jobId: string; leaseToken: string; accepted: boolean }> = [];
	readonly failureCalls: Array<{
		jobId: string;
		leaseToken: string;
		redactedError: string;
		retryDelayMs: number;
		provenance: SummaryAttemptProvenance | undefined;
	}> = [];
	readonly attemptRows = new Map<
		string,
		{
			jobId: string;
			outcome: SummaryAttemptOutcome | "in_flight";
			provenance: SummaryAttemptProvenance;
			usage: SummaryProviderUsage | undefined;
		}
	>();
	rejectAttemptStart = false;
	readonly completedJobIds = new Set<string>();
	readonly summaryCompleted = Promise.withResolvers<void>();
	readonly summaryFailed = Promise.withResolvers<void>();
	readonly delayRequested = Promise.withResolvers<void>();
	readonly failureRecordsRequested = Promise.withResolvers<void>();
	lastCompletion: SummaryCompletion | undefined;
	job: SummaryJob | undefined;
	now = 1_000_000;
	priorSpend = 0;
	readonly priorSpendCalls: Array<{ sessionId: string; before: number }> = [];
	maxLeased = 0;
	projectImpl: (request: ProjectionRequest, snapshot: SourceSnapshot) => ContextProjection = (_request, snapshot) => ({
		revision: 1,
		ready: false,
		historical: [],
		freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
		uncoveredSourceIds: [],
		sourceTokens: snapshot.entries.length,
		selectedLevelCounts: {},
		coveredSourceCount: 0,
		freshSourceCount: snapshot.entries.length,
		estimatedTokens: 0,
		pendingJobs: this.relevantPendingJobs ?? this.queuedJobs,
	});

	[Symbol.dispose](): void {
		this.close();
	}

	queueJobs(...jobs: SummaryJob[]): void {
		this.jobs.push(...jobs);
		this.queuedJobs += jobs.length;
		this.relevantPendingJobs ??= 0;
		this.relevantPendingJobs += jobs.filter(job => job.queueClass === "preferred").length;
	}

	seedFailure(jobId: string, availableAt: number, queueClass: SummaryJob["queueClass"]): void {
		this.failureRecords.set(jobId, { jobId, availableAt, queueClass });
	}

	reconcile(snapshot: SourceSnapshot, options?: ReconcileOptions): ReconcileResult {
		this.reconcileAttempts++;
		this.reconcileOptions.push(options);
		const error = this.reconcileErrors.shift();
		if (error !== undefined) throw error;
		this.snapshots.push(snapshot);
		return {
			changed: true,
			revision: this.snapshots.length,
			activeSources: snapshot.entries.length,
			insertedSources: snapshot.entries.length,
			tombstonedSources: 0,
			queuedJobs: this.queuedJobs,
			reusedSummaries: 0,
		};
	}

	project(request: ProjectionRequest): ContextProjection {
		return this.projectImpl(request, this.snapshots.at(-1)!);
	}

	claimSummaryJobs(options: ClaimSummaryJobsOptions): SummaryJob[] {
		this.claimCalls.push(options);
		const error = this.claimErrors.shift();
		if (error !== undefined) throw error;
		if (this.deferredClaims > 0) {
			this.deferredClaims--;
			return [];
		}
		if (this.job) {
			this.jobs.unshift(this.job);
			this.job = undefined;
		}
		let index = this.jobs.findIndex(
			job =>
				job.queueClass === "preferred" && (this.failureRecords.get(job.jobId)?.availableAt ?? this.now) <= this.now,
		);
		if (index < 0 && options.allowFallback !== false) {
			index = this.jobs.findIndex(
				job =>
					job.queueClass === "fallback" &&
					(this.failureRecords.get(job.jobId)?.availableAt ?? this.now) <= this.now,
			);
		}
		if (index < 0) return [];
		const [queuedJob] = this.jobs.splice(index, 1);
		if (!queuedJob) return [];
		const job = {
			...queuedJob,
			leaseToken: `lease-${queuedJob.jobId}-${++this.leaseSequence}`,
			leaseExpiresAt: this.now + options.leaseMs,
		};
		this.failureRecords.delete(job.jobId);
		this.leasedJobs.set(job.jobId, job);
		this.maxLeased = Math.max(this.maxLeased, this.leasedJobs.size);
		return [job];
	}

	nextSummaryJobDelayMs(preferredScope?: ContextScope, allowFallback = true): number | null {
		this.delayCalls.push({ preferredScope, allowFallback });
		this.delayRequested.resolve();
		if (this.nextDelayMs !== null) return this.nextDelayMs;
		const delays: number[] = [];
		for (const job of [...(this.job ? [this.job] : []), ...this.jobs]) {
			if (job.queueClass === "fallback" && !allowFallback) continue;
			delays.push(Math.max(0, (this.failureRecords.get(job.jobId)?.availableAt ?? this.now) - this.now));
		}
		for (const failure of this.failureRecords.values()) {
			if (failure.queueClass === "preferred" || allowFallback) {
				delays.push(Math.max(0, failure.availableAt - this.now));
			}
		}
		return delays.length === 0 ? null : Math.min(...delays);
	}

	priorSummarySpendUsd(sessionId: string, before: number): number {
		this.priorSpendCalls.push({ sessionId, before });
		return this.priorSpend;
	}

	summaryJobFailures() {
		this.failureRecordsRequested.resolve();
		return [...this.failureRecords.values()];
	}

	extendSummaryJob(jobId: string, leaseToken: string): boolean {
		return this.leasedJobs.get(jobId)?.leaseToken === leaseToken;
	}

	releaseSummaryJob(jobId: string, leaseToken: string): boolean {
		const error = this.releaseErrors.shift();
		if (error !== undefined) throw error;
		const leased = this.leasedJobs.get(jobId);
		const accepted = leased?.leaseToken === leaseToken;
		this.releaseCalls.push({ jobId, leaseToken, accepted });
		if (!accepted || !leased) return false;
		this.leasedJobs.delete(jobId);
		this.jobs.unshift(leased);
		return true;
	}

	beginSummaryAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttemptStart,
		provenance: SummaryAttemptProvenance,
	): boolean {
		const leased = this.leasedJobs.get(jobId);
		if (leased?.leaseToken !== leaseToken || this.rejectAttemptStart) return false;
		if (this.attemptRows.has(attempt.attemptId)) return false;
		this.attemptRows.set(attempt.attemptId, { jobId, outcome: "in_flight", provenance, usage: undefined });
		return true;
	}

	settleSummaryAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttempt,
		requestedOutcome: SummaryLocalAttemptOutcome,
	): SummaryAttemptOutcome | null {
		return this.#settleAttempt(jobId, leaseToken, attempt, requestedOutcome);
	}

	#settleAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttempt,
		requestedOutcome: SummaryAttemptOutcome,
	): SummaryAttemptOutcome | null {
		const row = this.attemptRows.get(attempt.attemptId);
		if (!row || row.jobId !== jobId || row.outcome !== "in_flight") return null;
		row.outcome = this.leasedJobs.get(jobId)?.leaseToken === leaseToken ? requestedOutcome : "lease_lost";
		row.usage = attempt.usage;
		return row.outcome;
	}

	completeSummaryJob(jobId: string, leaseToken: string, completion: SummaryCompletion): CompleteSummaryJobResult {
		const leased = this.leasedJobs.get(jobId);
		if (leased?.leaseToken !== leaseToken) return { accepted: false, reason: "lease_lost" };
		if (completion.attempt) this.#settleAttempt(jobId, leaseToken, completion.attempt, "completed");
		this.leasedJobs.delete(jobId);
		this.failureRecords.delete(jobId);
		this.completedJobIds.add(jobId);
		this.lastCompletion = completion;
		this.queuedJobs = Math.max(0, this.queuedJobs - 1);
		if (this.relevantPendingJobs !== undefined && leased.queueClass === "preferred") {
			this.relevantPendingJobs = Math.max(0, this.relevantPendingJobs - 1);
		}
		this.nextDelayMs = null;
		this.summaryCompleted.resolve();
		return { accepted: true, summaryId: `summary-${jobId}` };
	}

	failSummaryJob(
		jobId: string,
		leaseToken: string,
		redactedError: string,
		retryDelayMs: number,
		provenance?: SummaryAttemptProvenance,
		failedAttempt?: { attempt: SummaryProviderAttempt; outcome: SummaryFailureAttemptOutcome },
	): boolean {
		const leased = this.leasedJobs.get(jobId);
		if (leased?.leaseToken !== leaseToken) return false;
		if (
			failedAttempt &&
			this.#settleAttempt(jobId, leaseToken, failedAttempt.attempt, failedAttempt.outcome) !== failedAttempt.outcome
		) {
			return false;
		}
		this.leasedJobs.delete(jobId);
		this.jobs.push(leased);
		this.failedError = redactedError;
		this.failureCalls.push({ jobId, leaseToken, redactedError, retryDelayMs, provenance });
		this.seedFailure(jobId, this.now + retryDelayMs, leased.queueClass);
		this.summaryFailed.resolve();
		return true;
	}

	search(_request: SearchRequest): SearchHit[] {
		return [];
	}

	searchProject(_request: ProjectSearchRequest): SearchHit[] {
		return [];
	}

	describe(_citation: Citation): SourceDescription | null {
		return null;
	}

	describeSummary(_reference: SummaryReference): SummaryDescription | null {
		return null;
	}

	describeFile(_reference: FileReference): FileDescription | null {
		return null;
	}

	expandSummary(_request: SummaryExpansionRequest): SummaryExpansion | null {
		return null;
	}

	status(): LcmStatus {
		const pending = [...(this.job ? [this.job] : []), ...this.jobs].filter(
			job => !this.failureRecords.has(job.jobId),
		).length;
		return {
			schemaVersion: 6,
			journalMode: "wal",
			quarantined: false,
			branches: 1,
			activeSources: this.snapshots.at(-1)?.entries.length ?? 0,
			tombstones: 0,
			leafSummaries: this.completedJobIds.size,
			condensedSummaries: 0,
			jobs: {
				pending,
				leased: this.leasedJobs.size,
				failed: this.failureRecords.size,
				completed: this.completedJobIds.size,
				obsolete: 0,
			},
			storage: { databaseBytes: 1_024, walBytes: 256, quarantineBytes: 128 },
			latestRecovery: null,
		};
	}

	doctor() {
		return { ok: true, checks: [] };
	}

	quarantine(): void {}

	rebuild(snapshots: readonly SourceSnapshot[]) {
		this.snapshots = [...snapshots];
		return {
			branches: snapshots.length,
			activeSources: snapshots.reduce((total, snapshot) => total + snapshot.entries.length, 0),
			queuedJobs: this.queuedJobs,
		};
	}

	purge() {
		const jobs = this.queuedJobs;
		this.jobs = [];
		this.job = undefined;
		this.leasedJobs.clear();
		this.failureRecords.clear();
		this.queuedJobs = 0;
		this.relevantPendingJobs = 0;
		return { tombstones: 0, jobs, summaries: 0, sourceContents: 0, files: 0, quarantineFiles: 0, quarantineBytes: 0 };
	}

	close(): void {
		this.closed = true;
	}
}

function syntheticUsage(): SummaryProviderUsage {
	return {
		input: 120,
		output: 8,
		cacheRead: 40,
		cacheWrite: 16,
		totalTokens: 184,
		cost: { input: 0.0012, output: 0.0004, cacheRead: 0.0001, cacheWrite: 0.0002, total: 0.0019 },
	};
}

function createHarness(
	manager: SessionManager,
	context = new FakeLcmContext(),
	projectId?: string,
	registerProject?: SessionLcmOptions["registerProject"],
	projectionLimits = () => ({
		sourceTokens: 101,
		prewarmThresholdTokens: 40,
		hardThresholdTokens: 100,
		tokenBudget: 100_000,
		freshTail: { maxSources: 32, maxTokens: 20_000 },
	}),
	hardWaitMs = 20,
	projectRoot?: string,
	maxConcurrentSummaries = 1,
	projectionFits: (messages: AgentMessage[]) => boolean = () => true,
	peerPollMs?: number,
	hardProjectionWaitMs?: number,
) {
	const complete = vi.fn(async (_request: LcmCompletionRequest) => "redacted summary");
	let attemptOrdinal = 0;
	const attemptStarts: SummaryProviderAttemptStart[] = [];
	const completeWithAttempt = async (request: LcmCompletionRequest): Promise<LcmCompletionResult> => {
		const start: SummaryProviderAttemptStart = {
			attemptId: `attempt-${++attemptOrdinal}`,
			startedAt: context.now,
			provider: "test-provider",
			model: "test-model",
		};
		request.onResolvedModel?.("test-provider/test-model");
		if (request.onAttemptStart && !(await request.onAttemptStart(start))) {
			throw new LcmCompletionError("LCM completion was superseded before dispatch", {
				provider: "test-provider",
				category: "aborted",
			});
		}
		attemptStarts.push(start);
		const attempt: SummaryProviderAttempt = { ...start, completedAt: context.now, usage: syntheticUsage() };
		try {
			return { text: await complete(request), attempt };
		} catch (error) {
			if (!(error instanceof LcmCompletionError)) throw error;
			throw new LcmCompletionError(error.message, {
				...(error.provider === undefined ? {} : { provider: error.provider }),
				...(error.retryAfterMs === undefined ? {} : { retryAfterMs: error.retryAfterMs }),
				category: error.category,
				attempt,
			});
		}
	};
	const openContext = vi.fn(async () => context as LcmContext);
	const lcm = new SessionLcm(
		{
			sessionManager: manager,
			obfuscator: {
				hasSecrets: () => true,
				obfuscate: text => text.replaceAll("raw-secret", "#SECRET"),
			},
			projectionLimits,
			projectionFits,
			complete: completeWithAttempt,
		},
		{
			summaryModel: "@smol",
			maxConcurrentSummaries,
			hardProjectionWaitMs,
			registerProject,
			dependencies: {
				openContext,
				resolveProject: async cwd => {
					const rootPath = projectRoot ?? cwd;
					return {
						projectId: projectId ?? `project:${Bun.hash(cwd)}`,
						rootPath,
						storePath: `${rootPath}/context.sqlite`,
					};
				},
				hardWaitMs,
				peerPollMs,
				now: () => context.now,
			},
		},
	);
	return { lcm, context, complete, openContext };
}

function appendUser(manager: SessionManager, text: string, timestamp: number): AgentMessage {
	const message: AgentMessage = { role: "user", content: [{ type: "text", text }], timestamp };
	manager.appendMessage(message);
	return message;
}

function readyHistoricalProjection(snapshot: SourceSnapshot, summaryHandle: string): ContextProjection {
	const old = snapshot.entries[1]!;
	const fresh = snapshot.entries.at(-1)!;
	return {
		revision: 1,
		ready: true,
		historical: [
			{
				kind: "summary",
				summaryId: "summary-fail-open",
				summaryHandle,
				level: 0,
				redactedText: "older facts",
				tokenCount: 3,
				sourceIds: [old.entryId],
				citations: [],
				files: [],
			},
		],
		freshTailSourceIds: [fresh.entryId],
		uncoveredSourceIds: [],
		sourceTokens: snapshot.entries.length,
		selectedLevelCounts: { 0: 1 },
		coveredSourceCount: 1,
		freshSourceCount: 1,
		estimatedTokens: 10,
		pendingJobs: 0,
	};
}

function summaryJob(
	jobId: string,
	overrides: Partial<Pick<SummaryJob, "queueClass" | "transportRetryCount">> = {},
): SummaryJob {
	return {
		jobId,
		leaseToken: `lease-${jobId}`,
		leaseExpiresAt: Date.now() + 60_000,
		queueClass: overrides.queueClass ?? "preferred",
		kind: "leaf",
		level: 0,
		inputs: [{ kind: "source", id: `source-${jobId}`, redactedText: "safe historical facts", tokenCount: 8 }],
		sourceCount: 1,
		inputTokenCount: 8,
		outputTokenBudget: 4,
		stage: "normal",
		strategy: "preserve_details",
		transportRetryCount: overrides.transportRetryCount ?? 0,
	};
}

function softProjectionLimits() {
	return {
		sourceTokens: 90,
		prewarmThresholdTokens: 40,
		hardThresholdTokens: 100,
		tokenBudget: 80,
		freshTail: { maxSources: 8, maxTokens: 40 },
	};
}

async function settleUntil(predicate: () => boolean, detail: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Promise.resolve();
	}
	throw new Error(`Scheduler did not settle: ${detail}`);
}

async function flushScheduler(): Promise<void> {
	for (let attempt = 0; attempt < 20; attempt++) await Promise.resolve();
}

describe("SessionLcm", () => {
	it("charges serialized historical context against projection fit budgets", () => {
		const historical = createHistoricalContextMessage({
			redactedCitedContent: "historical payload ".repeat(300),
			timestamp: 1,
		});
		expect(estimateLcmProjectionMessageTokens(historical)).toBeGreaterThan(1_000);
	});

	it("normalizes fractional provider retry hints and caps malformed long delays", () => {
		expect(new LcmCompletionError("fractional", { retryAfterMs: 34_074.224 }).retryAfterMs).toBe(34_075);
		expect(new LcmCompletionError("oversized", { retryAfterMs: Number.MAX_VALUE }).retryAfterMs).toBe(
			24 * 60 * 60_000,
		);
		expect(new LcmCompletionError("invalid", { retryAfterMs: Number.NaN }).retryAfterMs).toBeUndefined();
	});

	it("returns the exact native input below prewarm without opening the derived store", async () => {
		const manager = SessionManager.inMemory("/below-prewarm");
		appendUser(manager, "small", 1);
		const { lcm, context, complete, openContext } = createHarness(manager, undefined, undefined, undefined, () => ({
			sourceTokens: 39,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		const input = manager.buildSessionContext().messages;
		const result = await lcm.project(input);
		expect(result.messages).toBe(input);
		expect(result.owned).toBe(false);
		expect(openContext).not.toHaveBeenCalled();
		expect(context.snapshots).toEqual([]);
		expect(complete).not.toHaveBeenCalled();
		await lcm.close();
	});

	it("returns soft-threshold foreground work before a held summary completes", async () => {
		const manager = SessionManager.inMemory("/soft-background");
		appendUser(manager, "growing history", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("soft"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		const completionGate = Promise.withResolvers<void>();
		const completionStarted = Promise.withResolvers<void>();
		complete.mockImplementation(async () => {
			completionStarted.resolve();
			await completionGate.promise;
			return "tiny";
		});
		const input = manager.buildSessionContext().messages;
		const result = await lcm.project(input);
		expect(result.messages).toBe(input);
		expect(result.owned).toBe(false);
		await completionStarted.promise;
		expect(context.completedJobIds.size).toBe(0);
		expect(context.reconcileOptions[0]?.summarize).toEqual({
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		});
		completionGate.resolve();
		await context.summaryCompleted.promise;
		await lcm.close();
	});

	it("starts summary work between prewarm and soft", async () => {
		const manager = SessionManager.inMemory("/prewarm-arming");
		appendUser(manager, "growing history", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("prewarm"));
		const { lcm, complete, openContext } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 60,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		const input = manager.buildSessionContext().messages;
		const result = await lcm.project(input);
		expect(result.messages).toBe(input);
		expect(result.owned).toBe(false);
		expect(openContext).toHaveBeenCalled();
		await context.summaryCompleted.promise;
		expect(complete).toHaveBeenCalled();
		expect(context.reconcileOptions[0]?.summarize).toEqual({
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		});
		await lcm.close();
	});

	it("keeps width one strictly serial and claims one durable job at a time", async () => {
		const manager = SessionManager.inMemory("/serial-pool");
		appendUser(manager, "serial backlog", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("serial-1"), summaryJob("serial-2"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		const gates = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
		const starts: number[] = [];
		let active = 0;
		let peak = 0;
		complete.mockImplementation(async () => {
			const index = starts.length;
			starts.push(index);
			active++;
			peak = Math.max(peak, active);
			await gates[index]!.promise;
			active--;
			return `summary-${index}`;
		});

		await lcm.project(manager.buildSessionContext().messages);
		await settleUntil(() => starts.length === 1, "first serial worker");
		await flushScheduler();
		expect(starts).toHaveLength(1);
		expect(context.maxLeased).toBe(1);
		gates[0]!.resolve();
		await settleUntil(() => starts.length === 2, "second serial worker");
		gates[1]!.resolve();
		await settleUntil(() => context.completedJobIds.size === 2, "serial backlog completion");
		expect(peak).toBe(1);
		expect(context.claimCalls.every(call => call.limit === 1)).toBe(true);
		await lcm.close();
	});

	it("runs width two in parallel without leasing or dispatching a third job", async () => {
		const manager = SessionManager.inMemory("/bounded-pool");
		appendUser(manager, "parallel backlog", 1);
		const context = new FakeLcmContext();
		context.queueJobs(
			summaryJob("parallel-1"),
			summaryJob("parallel-2"),
			summaryJob("parallel-3"),
			summaryJob("parallel-4"),
		);
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
			20,
			undefined,
			2,
		);
		const gates = Array.from({ length: 4 }, () => Promise.withResolvers<void>());
		const starts: number[] = [];
		let active = 0;
		let peak = 0;
		complete.mockImplementation(async () => {
			const index = starts.length;
			starts.push(index);
			active++;
			peak = Math.max(peak, active);
			await gates[index]!.promise;
			active--;
			return `summary-${index}`;
		});

		await lcm.project(manager.buildSessionContext().messages);
		await settleUntil(() => starts.length === 2, "two parallel workers");
		await flushScheduler();
		expect(starts).toHaveLength(2);
		expect(context.leasedJobs.size).toBe(2);
		gates[0]!.resolve();
		await settleUntil(() => starts.length === 3, "parallel refill after first settlement");
		gates[1]!.resolve();
		await settleUntil(() => starts.length === 4, "parallel refill after second settlement");
		gates[2]!.resolve();
		gates[3]!.resolve();
		await settleUntil(() => context.completedJobIds.size === 4, "parallel backlog completion");
		expect(peak).toBe(2);
		expect(context.maxLeased).toBe(2);
		expect(context.claimCalls.every(call => call.limit === 1)).toBe(true);
		await lcm.close();
	});

	it("applies live width increases and decreases only to future claims", async () => {
		const manager = SessionManager.inMemory("/live-resize");
		appendUser(manager, "resize backlog", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("resize-1"), summaryJob("resize-2"), summaryJob("resize-3"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		const gates = Array.from({ length: 3 }, () => Promise.withResolvers<void>());
		const signals: AbortSignal[] = [];
		complete.mockImplementation(async request => {
			const index = signals.length;
			signals.push(request.signal!);
			await gates[index]!.promise;
			return `summary-${index}`;
		});

		await lcm.project(manager.buildSessionContext().messages);
		await settleUntil(() => signals.length === 1, "initial width-one worker");
		lcm.configure({ summaryModel: "@smol", maxConcurrentSummaries: 2 });
		await settleUntil(() => signals.length === 2, "worker added after width increase");
		lcm.configure({ summaryModel: "@smol", maxConcurrentSummaries: 1 });
		expect(signals.map(signal => signal.aborted)).toEqual([false, false]);
		gates[0]!.resolve();
		await settleUntil(() => context.completedJobIds.has("resize-1"), "first resize worker settlement");
		await flushScheduler();
		expect(signals).toHaveLength(2);
		expect(signals[1]!.aborted).toBe(false);
		gates[1]!.resolve();
		await settleUntil(() => signals.length === 3, "refill after active count reaches reduced width");
		gates[2]!.resolve();
		await settleUntil(() => context.completedJobIds.size === 3, "resized backlog completion");
		expect(context.maxLeased).toBe(2);
		await lcm.close();
	});

	for (const lifecycle of ["rebind", "close"] as const) {
		it(`${lifecycle} aborts active summaries, awaits finalizers, and releases the captured lease`, async () => {
			const manager = SessionManager.inMemory(`/lifecycle-${lifecycle}`);
			appendUser(manager, "held summary", 1);
			const context = new FakeLcmContext();
			context.queueJobs(summaryJob(`lifecycle-${lifecycle}`));
			const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
			complete.mockImplementation(request => {
				const deferred = Promise.withResolvers<string>();
				const signal = request.signal!;
				const rejectAbort = () => deferred.reject(new AIError.AbortError("test lifecycle abort"));
				if (signal.aborted) rejectAbort();
				else signal.addEventListener("abort", rejectAbort, { once: true });
				return deferred.promise;
			});

			await lcm.project(manager.buildSessionContext().messages);
			await settleUntil(() => context.leasedJobs.size === 1, `${lifecycle} active lease`);
			const captured = [...context.leasedJobs.values()][0]!;
			if (lifecycle === "rebind") await lcm.rebind();
			else await lcm.close();
			expect(context.releaseCalls).toContainEqual({
				jobId: captured.jobId,
				leaseToken: captured.leaseToken,
				accepted: true,
			});
			expect(context.leasedJobs.size).toBe(0);
			expect(context.failureCalls).toHaveLength(0);
			if (lifecycle === "rebind") await lcm.close();
		});
	}

	for (const lifecycle of ["rebind", "close"] as const) {
		it(`${lifecycle} surfaces a durable release failure instead of closing the live store early`, async () => {
			const manager = SessionManager.inMemory(`/release-failure-${lifecycle}`);
			appendUser(manager, "held summary", 1);
			const context = new FakeLcmContext();
			context.queueJobs(summaryJob(`release-failure-${lifecycle}`));
			const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
			const deferred = Promise.withResolvers<string>();
			complete.mockImplementation(request => {
				const signal = request.signal!;
				const rejectAbort = () => deferred.reject(new AIError.AbortError("release failure abort"));
				if (signal.aborted) rejectAbort();
				else signal.addEventListener("abort", rejectAbort, { once: true });
				return deferred.promise;
			});

			await lcm.project(manager.buildSessionContext().messages);
			await settleUntil(() => context.leasedJobs.size === 1, "leased job before release failure");
			const releaseError = new Error("release transaction failed");
			context.releaseErrors.push(releaseError);
			const caught = await lcm[lifecycle]().catch(error => error);

			expect(caught).toBe(releaseError);
			expect(context.closed).toBe(lifecycle === "close");
			if (lifecycle === "rebind") await lcm.close();
		});
	}

	it("keeps the first store failure sticky through release finalization", async () => {
		const manager = SessionManager.inMemory("/sticky-store-failure");
		appendUser(manager, "completion failure", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("sticky-store-failure"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		const completionAttempted = Promise.withResolvers<void>();
		context.completeSummaryJob = () => {
			completionAttempted.resolve();
			throw undefined;
		};
		context.releaseErrors.push(new Error("later release finalization failed"));
		complete.mockResolvedValue("redacted summary");

		await lcm.project(manager.buildSessionContext().messages);
		await completionAttempted.promise;
		const lifecycle = await lcm.close().then(
			() => ({ rejected: false as const }),
			error => ({ rejected: true as const, error }),
		);

		expect(lifecycle).toEqual({ rejected: true, error: undefined });
		expect(context.leasedJobs.size).toBe(1);
		expect(context.releaseErrors).toHaveLength(0);
		expect(context.closed).toBe(true);
	});

	it("releases structural aborts without backoff and fences a later owner from the stale token", async () => {
		const manager = SessionManager.inMemory("/structural-abort");
		appendUser(manager, "abort me", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("structural-abort"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		complete.mockRejectedValueOnce(new AIError.AbortError("provider-local cancellation"));

		await lcm.project(manager.buildSessionContext().messages);
		await settleUntil(() => context.releaseCalls.length === 1, "abort release finalizer");
		const firstRelease = context.releaseCalls[0]!;
		expect(firstRelease).toMatchObject({ jobId: "structural-abort", accepted: true });
		expect(context.failureCalls).toHaveLength(0);
		expect(context.failureRecords.size).toBe(0);

		const held = Promise.withResolvers<string>();
		complete.mockImplementation(request => {
			const signal = request.signal!;
			const rejectAbort = () => held.reject(new AIError.AbortError("reclaimed job aborted"));
			if (signal.aborted) rejectAbort();
			else signal.addEventListener("abort", rejectAbort, { once: true });
			return held.promise;
		});
		await flushScheduler();
		const status = await lcm.status();
		await settleUntil(() => context.leasedJobs.size === 1, "reclaimed structural-abort job");
		const reclaimed = context.leasedJobs.get("structural-abort")!;
		expect(reclaimed.leaseToken).not.toBe(firstRelease.leaseToken);
		expect(context.releaseSummaryJob(reclaimed.jobId, firstRelease.leaseToken)).toBe(false);
		expect(context.leasedJobs.get(reclaimed.jobId)?.leaseToken).toBe(reclaimed.leaseToken);
		expect(status.runtime.summaryBackoff).toBeUndefined();
		expect(status.store?.jobs.failed).toBe(0);
		expect(status.runtime.phase).not.toBe("degraded");
		expect(status.runtime.lastFailureCategory).toBeUndefined();
		expect(lcm.takePendingFallbackCategory()).toBeUndefined();
		await lcm.rebind();
		expect(context.releaseCalls.at(-1)).toEqual({
			jobId: reclaimed.jobId,
			leaseToken: reclaimed.leaseToken,
			accepted: true,
		});
		await lcm.close();
	});

	it("wakes a delayed retry once without polling", async () => {
		const manager = SessionManager.inMemory("/delayed-retry");
		appendUser(manager, "retryable history", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("delayed"));
		context.deferredClaims = 1;
		context.nextDelayMs = 1;
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		await lcm.project(manager.buildSessionContext().messages);
		await context.delayRequested.promise;
		await context.summaryCompleted.promise;
		expect(complete).toHaveBeenCalledTimes(1);
		await lcm.close();
	});

	it("normalizes a malformed selector to @smol and captures reconfiguration per claimed job", async () => {
		const manager = SessionManager.inMemory("/selector-capture");
		appendUser(manager, "two jobs", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("first"), summaryJob("second"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		lcm.configure({ summaryModel: { malformed: true } as unknown as string });
		const first = Promise.withResolvers<void>();
		const firstCalled = Promise.withResolvers<void>();
		const secondCalled = Promise.withResolvers<void>();
		const selectors: Array<string | undefined> = [];
		complete.mockImplementation(async request => {
			selectors.push(request.modelSelector);
			if (selectors.length === 1) {
				firstCalled.resolve();
				await first.promise;
			}
			if (selectors.length === 2) secondCalled.resolve();
			return "tiny";
		});
		expect((await lcm.status()).runtime.summaryModelSelector).toBe("@smol");
		await lcm.project(manager.buildSessionContext().messages);
		await firstCalled.promise;
		lcm.configure({ summaryModel: "@next" });
		first.resolve();
		await secondCalled.promise;
		expect(selectors).toEqual(["@smol", "@next"]);
		await settleUntil(() => context.completedJobIds.size === 2, "selector-capture completions");
		await lcm.close();
	});

	it("hydrates durable preferred and fallback failures and prunes peer-completed records", async () => {
		const manager = SessionManager.inMemory("/durable-failure-hydration");
		appendUser(manager, "durable failures", 1);
		const context = new FakeLcmContext();
		context.seedFailure("preferred-failure", context.now + 10_000, "preferred");
		context.seedFailure("fallback-failure", context.now + 20_000, "fallback");
		const { lcm } = createHarness(manager, context);

		const hydrated = await lcm.status();
		expect(hydrated.runtime.phase).toBe("degraded");
		expect(hydrated.runtime.summaryBackoff).toEqual({
			preferred: context.now + 10_000,
			fallback: context.now + 20_000,
		});
		expect(hydrated.runtime.retryAt).toBe(context.now + 20_000);

		context.failureRecords.delete("preferred-failure");
		const preferredPruned = await lcm.status();
		expect(preferredPruned.runtime.summaryBackoff).toEqual({ fallback: context.now + 20_000 });
		context.failureRecords.delete("fallback-failure");
		const allPruned = await lcm.status();
		expect(allPruned.runtime.summaryBackoff).toBeUndefined();
		expect(allPruned.runtime.retryAt).toBeUndefined();
		await lcm.close();
	});

	it("lets a durable preferred failure gate both preferred and fallback claims", async () => {
		const manager = SessionManager.inMemory("/preferred-failure-gate");
		appendUser(manager, "gated backlog", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("blocked-preferred"), summaryJob("blocked-fallback", { queueClass: "fallback" }));
		context.seedFailure("blocked-preferred", context.now + 50_000, "preferred");
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
			20,
			undefined,
			2,
		);

		await lcm.project(manager.buildSessionContext().messages);
		await context.failureRecordsRequested.promise;
		await flushScheduler();
		expect(context.claimCalls).toHaveLength(0);
		expect(complete).not.toHaveBeenCalled();
		expect(context.leasedJobs.size).toBe(0);
		await lcm.close();
	});

	it("allows newly relevant preferred work to bypass fallback failure backoff", async () => {
		const manager = SessionManager.inMemory("/fallback-bypass");
		appendUser(manager, "initial history", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("delayed-fallback", { queueClass: "fallback" }));
		context.seedFailure("delayed-fallback", context.now + 50_000, "fallback");
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
			20,
			undefined,
			2,
		);

		await lcm.project(manager.buildSessionContext().messages);
		await context.delayRequested.promise;
		await flushScheduler();
		expect(complete).not.toHaveBeenCalled();

		context.queueJobs(summaryJob("new-preferred"));
		appendUser(manager, "new relevant history", 2);
		await lcm.project(manager.buildSessionContext().messages);
		await settleUntil(() => context.completedJobIds.has("new-preferred"), "preferred bypass completion");
		expect(context.completedJobIds.has("delayed-fallback")).toBe(false);
		expect(context.failureRecords.get("delayed-fallback")).toMatchObject({ queueClass: "fallback" });
		expect(complete).toHaveBeenCalledTimes(1);
		expect(complete.mock.calls[0]![0].prompt).toContain("source-new-preferred");
		await lcm.close();
	});

	it("admits preferred work within width while only fallback work is backed off", async () => {
		const manager = SessionManager.inMemory("/fallback-preferred-capacity");
		appendUser(manager, "fallback backlog", 1);
		const context = new FakeLcmContext();
		context.queueJobs(
			summaryJob("failed-fallback", { queueClass: "fallback" }),
			summaryJob("held-fallback", { queueClass: "fallback" }),
		);
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
			20,
			undefined,
			2,
		);
		const heldGate = Promise.withResolvers<void>();
		const heldStarted = Promise.withResolvers<void>();
		complete.mockImplementation(async request => {
			if (request.prompt.includes("source-failed-fallback")) {
				throw new LcmCompletionError("fallback provider failure", { provider: "test" });
			}
			if (request.prompt.includes("source-held-fallback")) {
				heldStarted.resolve();
				await heldGate.promise;
			}
			return "redacted summary";
		});

		await lcm.project(manager.buildSessionContext().messages);
		await Promise.all([context.summaryFailed.promise, heldStarted.promise]);
		await flushScheduler();
		const backedOff = await lcm.status();
		expect(backedOff.runtime.summaryBackoff).toEqual({ fallback: context.now + 2_000 });
		const preferredClaimStart = context.claimCalls.length;

		context.queueJobs(summaryJob("new-preferred-with-capacity"));
		appendUser(manager, "new preferred history", 2);
		await lcm.project(manager.buildSessionContext().messages);
		await settleUntil(
			() => context.completedJobIds.has("new-preferred-with-capacity"),
			"preferred admission during fallback backoff",
		);
		await flushScheduler();
		const status = await lcm.status();

		expect(context.claimCalls.slice(preferredClaimStart).some(call => call.allowFallback === false)).toBe(true);
		expect(context.maxLeased).toBe(2);
		expect(context.leasedJobs.has("held-fallback")).toBe(true);
		expect(status.runtime.phase).not.toBe("degraded");
		expect(status.runtime.summaryWorkers).toEqual({ active: 1, limit: 2 });
		expect(status.store?.jobs).toMatchObject({ leased: 1, failed: 1, completed: 1 });
		heldGate.resolve();
		await settleUntil(() => context.completedJobIds.has("held-fallback"), "held fallback completion");
		await lcm.close();
	});

	it("keeps one provider failure visible after its sibling succeeds", async () => {
		const manager = SessionManager.inMemory("/sibling-failure");
		appendUser(manager, "sibling jobs", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("failing-sibling"), summaryJob("successful-sibling"));
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
			20,
			undefined,
			2,
		);
		const successGate = Promise.withResolvers<void>();
		const successStarted = Promise.withResolvers<void>();
		complete.mockImplementation(async request => {
			if (request.prompt.includes("source-failing-sibling")) {
				throw new LcmCompletionError("bounded provider failure", { provider: "test" });
			}
			successStarted.resolve();
			await successGate.promise;
			return "successful sibling";
		});

		await lcm.project(manager.buildSessionContext().messages);
		await Promise.all([context.summaryFailed.promise, successStarted.promise]);
		successGate.resolve();
		await settleUntil(() => context.completedJobIds.has("successful-sibling"), "successful sibling settlement");
		const status = await lcm.status();
		expect(context.completedJobIds.has("failing-sibling")).toBe(false);
		expect(context.failureRecords.get("failing-sibling")).toEqual({
			jobId: "failing-sibling",
			availableAt: context.now + 2_000,
			queueClass: "preferred",
		});
		expect(status.runtime.summaryBackoff).toEqual({ preferred: context.now + 2_000 });
		expect(status.runtime.lastFailureCategory).toBe("provider");
		await lcm.close();
	});

	for (const unfitFirst of [true, false] as const) {
		it(`keeps preferred unfit degraded when its sibling settles ${unfitFirst ? "after" : "before"} it`, async () => {
			const manager = SessionManager.inMemory(`/preferred-unfit-${unfitFirst ? "first" : "last"}`);
			appendUser(manager, "preferred siblings", 1);
			const context = new FakeLcmContext();
			context.queueJobs(summaryJob("preferred-unfit"), summaryJob("preferred-sibling"));
			let atHard = false;
			const { lcm, complete } = createHarness(
				manager,
				context,
				undefined,
				undefined,
				() => ({ ...softProjectionLimits(), sourceTokens: atHard ? 101 : 90 }),
				20,
				undefined,
				2,
			);
			const unfitGate = Promise.withResolvers<void>();
			const siblingGate = Promise.withResolvers<void>();
			const unfitStarted = Promise.withResolvers<void>();
			const siblingStarted = Promise.withResolvers<void>();
			complete.mockImplementation(async request => {
				const unfit = request.prompt.includes("source-preferred-unfit");
				(unfit ? unfitStarted : siblingStarted).resolve();
				await (unfit ? unfitGate : siblingGate).promise;
				return unfit ? "unfit summary" : "sibling summary";
			});
			const completeSummaryJob = context.completeSummaryJob.bind(context);
			context.completeSummaryJob = (jobId, leaseToken, completion) => {
				if (jobId !== "preferred-unfit") return completeSummaryJob(jobId, leaseToken, completion);
				const leased = context.leasedJobs.get(jobId);
				if (leased?.leaseToken !== leaseToken) return { accepted: false, reason: "lease_lost" };
				context.leasedJobs.delete(jobId);
				context.queuedJobs = Math.max(0, context.queuedJobs - 1);
				if (context.relevantPendingJobs !== undefined) {
					context.relevantPendingJobs = Math.max(0, context.relevantPendingJobs - 1);
				}
				return { accepted: false, reason: "deterministic_failed" };
			};

			await lcm.project(manager.buildSessionContext().messages);
			await Promise.all([unfitStarted.promise, siblingStarted.promise]);
			const firstJob = unfitFirst ? "preferred-unfit" : "preferred-sibling";
			const secondJob = unfitFirst ? "preferred-sibling" : "preferred-unfit";
			(unfitFirst ? unfitGate : siblingGate).resolve();
			await settleUntil(() => !context.leasedJobs.has(firstJob), `${firstJob} settlement`);
			await flushScheduler();
			const afterFirst = await lcm.status();

			(unfitFirst ? siblingGate : unfitGate).resolve();
			await settleUntil(() => !context.leasedJobs.has(secondJob), `${secondJob} settlement`);
			await flushScheduler();
			const afterBoth = await lcm.status();

			if (unfitFirst) {
				expect(afterFirst.runtime.phase).toBe("degraded");
				expect(afterFirst.runtime.lastFailureCategory).toBe("unfit");
			}
			expect(afterBoth.runtime.phase).toBe("degraded");
			expect(afterBoth.runtime.lastFailureCategory).toBe("unfit");

			context.projectImpl = (_request, snapshot) => ({
				revision: 2,
				ready: true,
				historical: [],
				freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: {},
				coveredSourceCount: 0,
				freshSourceCount: snapshot.entries.length,
				estimatedTokens: snapshot.entries.length,
				pendingJobs: 0,
			});
			atHard = true;
			const fitted = await lcm.project(manager.buildSessionContext().messages);
			const active = await lcm.status();
			expect(fitted.owned).toBe(true);
			expect(active.runtime.phase).toBe("active");
			expect(active.runtime.lastFailureCategory).toBeUndefined();
			await lcm.close();
		});
	}

	it("uses the larger retry hint or capped exponential transport delay", async () => {
		const cases = [
			{ retry: 0, hint: 45_000, expected: 45_000 },
			{ retry: 3, hint: undefined, expected: 16_000 },
			{ retry: 9, hint: undefined, expected: 32_000 },
		] as const;
		for (const testCase of cases) {
			const manager = SessionManager.inMemory(`/retry-delay-${testCase.retry}`);
			appendUser(manager, "retry job", 1);
			const context = new FakeLcmContext();
			context.queueJobs(summaryJob(`retry-${testCase.retry}`, { transportRetryCount: testCase.retry }));
			const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
			complete.mockRejectedValue(
				new LcmCompletionError("retryable", {
					provider: "test",
					...(testCase.hint === undefined ? {} : { retryAfterMs: testCase.hint }),
				}),
			);

			await lcm.project(manager.buildSessionContext().messages);
			await context.summaryFailed.promise;
			expect(context.failureCalls[0]?.retryDelayMs).toBe(testCase.expected);
			expect(context.failureRecords.get(`retry-${testCase.retry}`)?.availableAt).toBe(
				context.now + testCase.expected,
			);
			await lcm.close();
		}
	});

	it("retries transient SQLite contention before accepting the projection", async () => {
		const manager = SessionManager.inMemory("/contention-retry");
		appendUser(manager, "contention", 1);
		const context = new FakeLcmContext();
		context.reconcileErrors.push(Object.assign(new Error("database is locked"), { code: "SQLITE_BUSY" }));
		context.projectImpl = (_request, snapshot) => ({
			revision: 1,
			ready: true,
			historical: [],
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context, undefined, undefined, undefined, 500);

		const result = await lcm.project(manager.buildSessionContext().messages);
		expect(result.owned).toBe(true);
		expect(context.reconcileAttempts).toBe(2);
		expect(lcm.takePendingFallbackCategory()).toBeUndefined();
		await lcm.close();
	});

	it("fails open without retrying non-contention errors and clears the fallback annotation on rebind", async () => {
		const manager = SessionManager.inMemory("/non-contention-fail-open");
		appendUser(manager, "store failure", 1);
		const context = new FakeLcmContext();
		const storeError = Object.assign(new Error("disk full"), { code: "SQLITE_FULL" });
		context.reconcileErrors.push(storeError);
		const { lcm, complete } = createHarness(manager, context);
		const input = manager.buildSessionContext().messages;

		const result = await lcm.project(input);
		expect(result.messages).toBe(input);
		expect(result.owned).toBe(false);
		expect(context.reconcileAttempts).toBe(1);
		expect(complete).not.toHaveBeenCalled();
		expect((await lcm.status()).runtime.lastFailureCategory).toBe("store");
		await lcm.rebind();
		expect(lcm.takePendingFallbackCategory()).toBeUndefined();
		await lcm.close();
	});

	it("reports a quarantined current store even when reconcile cannot inspect it", async () => {
		const manager = SessionManager.inMemory("/quarantined-status");
		appendUser(manager, "quarantined", 1);
		const context = new FakeLcmContext();
		context.reconcileErrors.push(new Error("LCM store is quarantined"));
		const baseStatus = context.status.bind(context);
		context.status = () =>
			({
				...baseStatus(),
				quarantined: true,
				latestRecovery: { occurredAt: 1_900_000_000_000, category: "corruption" },
				dbPath: "/private/context.sqlite",
				recoveredFrom: "/private/recovered.sqlite",
				quarantineReason: "integrity check failed /private/token=top-secret",
			}) as LcmStatus;
		const { lcm } = createHarness(manager, context);

		const status = await lcm.status();

		expect(status.runtime.phase).toBe("quarantined");
		expect(status.store).toMatchObject({
			quarantined: true,
			storage: { databaseBytes: 1_024, walBytes: 256, quarantineBytes: 128 },
			latestRecovery: { occurredAt: 1_900_000_000_000, category: "corruption" },
		});
		expect(status.store).not.toHaveProperty("dbPath");
		expect(status.store).not.toHaveProperty("recoveredFrom");
		expect(status.store).not.toHaveProperty("quarantineReason");
		await lcm.close();
	});

	it("reports active normalized scope health before any projection evaluation", async () => {
		const manager = SessionManager.inMemory("/active-scope-status");
		appendUser(manager, "current branch only", 1);
		const context = new FakeLcmContext();
		const baseStatus = context.status.bind(context);
		context.status = () => ({ ...baseStatus(), branches: 41, activeSources: 9_999 });
		const { lcm } = createHarness(manager, context);

		const status = await lcm.status();
		const snapshot = context.snapshots.at(-1)!;
		const sourceTokens = snapshot.entries.reduce(
			(total, entry) => total + Math.ceil(Buffer.byteLength(entry.redactedText, "utf8") / 4),
			0,
		);

		expect(status.store).toMatchObject({ branches: 41, activeSources: 9_999 });
		expect(status.runtime.currentBranch).toEqual({
			...snapshot.scope,
			revision: 1,
			activeSources: snapshot.entries.length,
			sourceTokens,
			projectionState: "unevaluated",
		});
		await lcm.close();
	});

	it("marks an evaluated candidate unfitted only after checking the current request", async () => {
		const manager = SessionManager.inMemory("/unfitted-status");
		appendUser(manager, "does not fit", 1);
		const { lcm } = createHarness(manager);

		const result = await lcm.project(manager.buildSessionContext().messages);
		const status = await lcm.status();

		expect(result.owned).toBe(false);
		expect(status.runtime.currentBranch).toMatchObject({
			projectionState: "unfitted",
			projection: { revision: 1 },
		});
		await lcm.close();
	});

	it("clears fitted candidates on revision changes and branch rebinds", async () => {
		const manager = SessionManager.inMemory("/projection-freshness");
		appendUser(manager, "branch root", 1);
		appendUser(manager, "original leaf", 2);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		let atHard = true;
		const { lcm } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: atHard ? 101 : 39,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 100_000,
			freshTail: { maxSources: 32, maxTokens: 20_000 },
		}));

		expect((await lcm.project(manager.buildSessionContext().messages)).owned).toBe(true);
		const fitted = (await lcm.status()).runtime.currentBranch!;
		expect(fitted.projectionState).toBe("fitted");
		expect(fitted.projection).toBeDefined();

		atHard = false;
		expect((await lcm.project(manager.buildSessionContext().messages)).owned).toBe(false);
		const superseded = (await lcm.status()).runtime.currentBranch!;
		expect(superseded.projectionState).toBe("unevaluated");
		expect(superseded.projection).toBeUndefined();
		expect(superseded.sourceTokens).toBeGreaterThan(39);
		expect((await lcm.status()).runtime.phase).toBe("warming");
		atHard = true;

		appendUser(manager, "same branch revision", 3);
		const revised = (await lcm.status()).runtime.currentBranch!;
		expect(revised.branchId).toBe(fitted.branchId);
		expect(revised.revision).toBeGreaterThan(fitted.revision);
		expect(revised.projectionState).toBe("unevaluated");
		expect(revised.projection).toBeUndefined();

		manager.branch(manager.getBranch()[0]!.id);
		const selected = (await lcm.status()).runtime.currentBranch!;
		expect(selected.activeSources).toBe(1);
		expect(selected.revision).toBeGreaterThan(revised.revision);
		expect(selected.projectionState).toBe("unevaluated");
		expect(selected.projection).toBeUndefined();
		appendUser(manager, "forked branch", 4);
		await lcm.rebind();
		const rebound = (await lcm.status()).runtime.currentBranch!;
		expect(rebound.sessionId).toBe(revised.sessionId);
		expect(rebound.branchId).not.toBe(revised.branchId);
		expect(rebound.projectionState).toBe("unevaluated");
		expect(rebound.projection).toBeUndefined();
		await lcm.close();
	});

	it("keeps a newer below-threshold request authoritative while an older hard attempt settles", async () => {
		const manager = SessionManager.inMemory("/projection-attempt-freshness");
		appendUser(manager, "overlapping request", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("superseded-hard"));
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: context.queuedJobs,
		});
		let requestCount = 0;
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: requestCount++ === 0 ? 101 : 79,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 100_000,
			freshTail: { maxSources: 32, maxTokens: 20_000 },
		}));
		const completionStarted = Promise.withResolvers<void>();
		const completionGate = Promise.withResolvers<void>();
		complete.mockImplementation(async () => {
			completionStarted.resolve();
			await completionGate.promise;
			return "redacted summary";
		});

		const older = lcm.project(manager.buildSessionContext().messages);
		await completionStarted.promise;
		expect((await lcm.project(manager.buildSessionContext().messages)).owned).toBe(false);
		completionGate.resolve();
		expect((await older).owned).toBe(false);
		await settleUntil(() => context.completedJobIds.has("superseded-hard"), "superseded hard completion");

		const status = await lcm.status();
		expect(status.runtime.currentBranch?.projectionState).toBe("unevaluated");
		expect(status.runtime.currentBranch?.projection).toBeUndefined();
		expect(status.runtime.phase).not.toBe("active");
		expect(status.runtime.lastFailureCategory).toBeUndefined();
		expect(lcm.takePendingFallbackCategory()).toBeUndefined();
		await lcm.close();
	});

	it("waits at hard and exposes diagnostics only after the pending projection fits", async () => {
		const manager = SessionManager.inMemory("/hard-fit");
		const first = appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.queuedJobs = 1;
		context.job = summaryJob("hard");
		context.projectImpl = (_request, snapshot) => {
			const old = snapshot.entries[1]!;
			const fresh = snapshot.entries.at(-1)!;
			return {
				revision: 2,
				ready: true,
				historical: [
					{
						kind: "summary",
						summaryId: "summary-hard",
						summaryHandle: "summary_handle_hard",
						level: 0,
						redactedText: "older facts",
						tokenCount: 2,
						sourceIds: [old.entryId],
						citations: [
							{
								...snapshot.scope,
								sourceId: old.entryId,
								sourceKey: "source-key-hard",
								contentHash: old.contentHash,
								position: 1,
							},
						],
						files: [],
					},
				],
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: 90,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: 1,
				freshSourceCount: 1,
				estimatedTokens: 12,
				pendingJobs: context.queuedJobs,
			};
		};
		const { lcm, complete } = createHarness(manager, context);
		const result = await lcm.project(manager.buildSessionContext().messages);
		expect(complete).toHaveBeenCalledTimes(1);
		expect(result.owned).toBe(true);
		expect(result.messages[0]).toBe(first);
		expect(result.projection).toMatchObject({
			revision: 2,
			sourceTokens: 90,
			selectedLevelCounts: { 0: 1 },
			pendingJobs: 0,
		});
		await lcm.rebind();
		const rebound = await lcm.status();
		expect(rebound.runtime.phase).toBe("idle");
		expect(rebound.runtime.currentBranch).toMatchObject({ projectionState: "unevaluated" });
		expect(rebound.runtime.currentBranch?.projection).toBeUndefined();
		await lcm.close();
	});

	it("lets a hard current-branch projection finish while an older fallback sibling is still running", async () => {
		const manager = SessionManager.inMemory("/hard-preferred-progress");
		const first = appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("older-fallback", { queueClass: "fallback" }));
		context.projectImpl = (_request, snapshot) => {
			const old = snapshot.entries[1]!;
			const fresh = snapshot.entries.at(-1)!;
			return {
				revision: 3,
				ready: true,
				historical: [
					{
						kind: "summary",
						summaryId: "summary-current",
						summaryHandle: "summary_handle_current",
						level: 0,
						redactedText: "current branch facts",
						tokenCount: 3,
						sourceIds: [old.entryId],
						citations: [
							{
								...snapshot.scope,
								sourceId: old.entryId,
								sourceKey: "source-key-current",
								contentHash: old.contentHash,
								position: 1,
							},
						],
						files: [],
					},
				],
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: 90,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: 1,
				freshSourceCount: 1,
				estimatedTokens: 14,
				pendingJobs: context.relevantPendingJobs ?? 0,
			};
		};
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, undefined, 200, undefined, 2);
		const fallbackGate = Promise.withResolvers<void>();
		complete.mockImplementation(async request => {
			if (request.prompt.includes("source-older-fallback")) await fallbackGate.promise;
			return "redacted summary";
		});

		await lcm.status();
		await settleUntil(() => context.leasedJobs.has("older-fallback"), "older fallback dispatch");
		context.queueJobs(summaryJob("new-preferred"));
		const result = await lcm.project(manager.buildSessionContext().messages);

		expect(result.owned).toBe(true);
		expect(result.messages[0]).toBe(first);
		expect(context.completedJobIds.has("new-preferred")).toBe(true);
		expect(context.leasedJobs.has("older-fallback")).toBe(true);
		fallbackGate.resolve();
		await settleUntil(() => context.completedJobIds.has("older-fallback"), "older fallback completion");
		await lcm.close();
	});

	it("uses the bounded deterministic fallback without a provider call", async () => {
		const manager = SessionManager.inMemory("/deterministic");
		appendUser(manager, "deterministic", 1);
		const context = new FakeLcmContext();
		const inputText = "fact ".repeat(2_000);
		context.queuedJobs = 1;
		context.job = {
			...summaryJob("deterministic"),
			inputs: [{ kind: "source", id: "source-deterministic", redactedText: inputText, tokenCount: 600 }],
			inputTokenCount: 600,
			outputTokenBudget: 512,
			stage: "deterministic",
			strategy: "deterministic_truncate",
		};
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 90,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		await lcm.project(manager.buildSessionContext().messages);
		await context.summaryCompleted.promise;
		expect(complete).not.toHaveBeenCalled();
		const output = context.lastCompletion?.redactedText ?? "";
		expect(Buffer.byteLength(output, "utf8")).toBeLessThanOrEqual(2_048);
		expect(Buffer.byteLength(output, "utf8")).toBeLessThan(Buffer.byteLength(inputText, "utf8"));
		await lcm.close();
	});

	it("scales the deterministic fallback to the leased budget instead of a fixed 512 tokens", async () => {
		const manager = SessionManager.inMemory("/deterministic-budget");
		appendUser(manager, "deterministic", 1);
		const context = new FakeLcmContext();
		const inputText = "x".repeat(19_388);
		context.queuedJobs = 1;
		context.job = {
			...summaryJob("deterministic-budget"),
			inputs: [{ kind: "source", id: "source-deterministic", redactedText: inputText, tokenCount: 4_847 }],
			inputTokenCount: 4_847,
			outputTokenBudget: 1_212,
			stage: "deterministic",
			strategy: "deterministic_truncate",
		};
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		await lcm.project(manager.buildSessionContext().messages);
		await context.summaryCompleted.promise;
		expect(complete).not.toHaveBeenCalled();
		const bytes = Buffer.byteLength(context.lastCompletion?.redactedText ?? "", "utf8");
		// The old fixed cap would have produced 512 * 4 bytes; the leased budget is 1212.
		expect(bytes).toBeGreaterThan(512 * 4);
		expect(bytes).toBeLessThanOrEqual(1_212 * 4);
		expect(bytes).toBeLessThan(Buffer.byteLength(inputText, "utf8"));
		await lcm.close();
	});

	it("tells a condensation job its inputs are already summaries and leaves leaf prompts alone", async () => {
		for (const level of [0, 1]) {
			const manager = SessionManager.inMemory(`/condense-${level}`);
			appendUser(manager, "condense", 1);
			const context = new FakeLcmContext();
			context.queuedJobs = 1;
			context.job = {
				...summaryJob(`condense-${level}`),
				kind: level > 0 ? "condensed" : "leaf",
				level,
				inputs: [
					{
						kind: level > 0 ? "summary" : "source",
						id: `input-${level}`,
						redactedText: "safe historical facts",
						tokenCount: 8,
					},
				],
			};
			const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
			await lcm.project(manager.buildSessionContext().messages);
			await context.summaryCompleted.promise;
			expect(complete).toHaveBeenCalledTimes(1);
			const prompt = complete.mock.calls[0]![0].prompt;
			if (level > 0) expect(prompt).toContain("The inputs below are already summaries");
			else expect(prompt).not.toContain("The inputs below are already summaries");
			await lcm.close();
		}
	});

	it("asks the provider for exactly the leased budget, whatever the transport does with it", async () => {
		for (const capHonoring of [true, false]) {
			const manager = SessionManager.inMemory(`/budget-request-${capHonoring}`);
			appendUser(manager, "budget request", 1);
			const context = new FakeLcmContext();
			context.queuedJobs = 1;
			context.job = {
				...summaryJob(`budget-${capHonoring}`),
				inputs: [{ kind: "source", id: "source-budget", redactedText: "safe historical facts", tokenCount: 4_847 }],
				inputTokenCount: 4_847,
				outputTokenBudget: 2_424,
			};
			const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
			// A cap-stripping wire returns more than it was asked for; the request is identical either way.
			complete.mockImplementation(async request =>
				"y".repeat((capHonoring ? request.maxOutputTokens : Math.ceil(request.maxOutputTokens * 1.28)) * 4),
			);
			await lcm.project(manager.buildSessionContext().messages);
			await context.summaryCompleted.promise;
			expect(complete).toHaveBeenCalledTimes(1);
			// Not the flat SUMMARY_MAX_OUTPUT_TOKENS: the request carries the job's leased budget.
			expect(complete.mock.calls[0]![0].maxOutputTokens).toBe(2_424);
			expect(complete.mock.calls[0]![0].prompt).toContain("at most 2424 tokens");
			await lcm.close();
		}
	});

	it("re-projects on the poll tick so a peer commit lands before the foreground deadline", async () => {
		const manager = SessionManager.inMemory("/peer-progress-poll");
		const first = appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		// One job exists but is permanently unclaimable here: a peer holds it, and the only
		// local wake is that peer's lease expiry, far past the foreground deadline. The local
		// worker pool emits a couple of settle signals and then goes quiet forever, so
		// reaching the eighth read is only possible by repeated poll ticks re-reading the store.
		context.queueJobs(summaryJob("peer-held"));
		context.deferredClaims = Number.MAX_SAFE_INTEGER;
		context.nextDelayMs = 600_000;
		const readsBeforePeerCommit = 8;
		let reads = 0;
		const notReady = context.projectImpl;
		context.projectImpl = (request, snapshot) => {
			// The peer commits partway through, emitting no local signal of any kind.
			if (++reads < readsBeforePeerCommit) return notReady(request, snapshot);
			const old = snapshot.entries[1]!;
			const fresh = snapshot.entries.at(-1)!;
			return {
				revision: 2,
				ready: true,
				historical: [
					{
						kind: "summary",
						summaryId: "summary-peer",
						summaryHandle: "summary_handle_peer",
						level: 0,
						redactedText: "facts committed by a peer process",
						tokenCount: 2,
						sourceIds: [old.entryId],
						citations: [
							{
								...snapshot.scope,
								sourceId: old.entryId,
								sourceKey: "source-key-peer",
								contentHash: old.contentHash,
								position: 1,
							},
						],
						files: [],
					},
				],
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: 90,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: 1,
				freshSourceCount: 1,
				estimatedTokens: 12,
				pendingJobs: 0,
			};
		};
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			500,
			undefined,
			undefined,
			undefined,
			5,
		);
		try {
			const result = await lcm.project(manager.buildSessionContext().messages);
			expect(result.owned).toBe(true);
			expect(result.messages[0]).toBe(first);
			expect(reads).toBeGreaterThanOrEqual(readsBeforePeerCommit);
			// The peer did the work; this session never dispatched a completion.
			expect(complete).not.toHaveBeenCalled();
		} finally {
			await lcm.close();
		}
	});

	it("normalizes the configured projection wait to the supported window", () => {
		expect(normalizeLcmHardProjectionWaitMs(undefined)).toBe(60_000);
		expect(normalizeLcmHardProjectionWaitMs(30_000)).toBe(30_000);
		expect(normalizeLcmHardProjectionWaitMs(15_000)).toBe(15_000);
		// Hand-written config is clamped to the supported window at both ends.
		expect(normalizeLcmHardProjectionWaitMs(1_000)).toBe(15_000);
		expect(normalizeLcmHardProjectionWaitMs(600_000)).toBe(60_000);
		// Anything that is not a finite integer falls back to the default.
		expect(normalizeLcmHardProjectionWaitMs(30_000.5)).toBe(60_000);
		expect(normalizeLcmHardProjectionWaitMs(Number.NaN)).toBe(60_000);
		expect(normalizeLcmHardProjectionWaitMs("30000")).toBe(60_000);
		expect(normalizeLcmHardProjectionWaitMs(null)).toBe(60_000);
	});

	it("seeds this session's prior LCM spend from the ledger at bind and stamps the session on attempts", async () => {
		const manager = SessionManager.inMemory("/prior-spend");
		appendUser(manager, "prior spend", 1);
		const context = new FakeLcmContext();
		context.priorSpend = 1.1;
		context.queueJobs(summaryJob("stamped"));
		const { lcm } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		try {
			await lcm.project(manager.buildSessionContext().messages);
			await context.summaryCompleted.promise;
			expect(lcm.priorSpendUsd()).toBe(1.1);
			// Scoped to this session and bounded by the process epoch, so live spend is not double counted.
			expect(context.priorSpendCalls[0]?.sessionId).toBe(manager.getSessionId());
			expect(context.priorSpendCalls[0]?.before).toBeGreaterThan(0);
			expect(context.attemptRows.get("attempt-1")?.provenance.sessionId).toBe(manager.getSessionId());
		} finally {
			await lcm.close();
		}
	});

	it("lets the injected wait seam outrank the configured projection wait", async () => {
		const manager = SessionManager.inMemory("/wait-precedence");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("never-ready"));
		context.deferredClaims = Number.MAX_SAFE_INTEGER;
		context.nextDelayMs = 600_000;
		// A 60 s configured wait must not override the 20 ms injected seam: if precedence
		// inverted, this projection would block for a full minute instead of failing open.
		// The poll stays at its production interval so the 20 ms deadline is what expires.
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			20,
			undefined,
			undefined,
			undefined,
			undefined,
			60_000,
		);
		try {
			const result = await lcm.project(manager.buildSessionContext().messages);
			expect(result.owned).toBe(false);
			expect(lcm.takePendingFallbackCategory()).toBe("deadline");
		} finally {
			await lcm.close();
		}
	});
	it("opens lazily, coexists with collab append, backfills, rebinds, disposes, and hides dbPath", async () => {
		const manager = SessionManager.inMemory("/worktree-a");
		const collabEntries: string[] = [];
		manager.onEntryAppended = entry => collabEntries.push(entry.id);
		appendUser(manager, "first", 1);
		const { lcm, context, openContext } = createHarness(manager);
		expect(openContext).not.toHaveBeenCalled();

		await lcm.project(manager.buildSessionContext().messages);
		expect(openContext).toHaveBeenCalledTimes(1);
		expect(context.snapshots.at(-1)?.entries).toHaveLength(1);
		expect(collabEntries).toHaveLength(1);
		expect(await lcm.status()).not.toHaveProperty("store.dbPath");

		await manager.newSession();
		appendUser(manager, "new branch", 2);
		await lcm.rebind();
		await lcm.status();
		expect(context.snapshots.at(-1)?.scope.sessionId).toBe(manager.getSessionId());
		const reconciles = context.snapshots.length;
		await lcm.close();
		appendUser(manager, "after close", 3);
		expect(context.snapshots).toHaveLength(reconciles);
		expect(context.closed).toBe(true);
	});

	it("registers each resolved project at lazy runtime initialization and treats catalog failure as nonfatal", async () => {
		const manager = SessionManager.inMemory("/catalog-project");
		appendUser(manager, "first", 1);
		const registerProject = vi.fn(async () => {});
		const registered = createHarness(manager, new FakeLcmContext(), "project:catalog", registerProject);
		await registered.lcm.project(manager.buildSessionContext().messages);
		expect(registerProject).toHaveBeenCalledWith(
			{
				projectId: "project:catalog",
				rootPath: "/catalog-project",
				storePath: "/catalog-project/context.sqlite",
			},
			expect.objectContaining({ sessionDir: expect.any(String) }),
		);
		expect(registered.openContext).toHaveBeenCalledTimes(1);
		await registered.lcm.close();

		const failingManager = SessionManager.inMemory("/catalog-failure");
		appendUser(failingManager, "still opens", 1);
		const failing = createHarness(failingManager, new FakeLcmContext(), undefined, async () => {
			throw new Error("catalog unavailable");
		});
		await failing.lcm.project(failingManager.buildSessionContext().messages);
		expect(failing.openContext).toHaveBeenCalledTimes(1);
		expect(failing.context.snapshots).toHaveLength(1);
		await failing.lcm.close();
	});

	it("redacts before reconcile and gives parallel and incomplete tool groups one atomic id", async () => {
		const manager = SessionManager.inMemory("/worktree-a");
		appendUser(manager, "raw-secret artifact://123?token=raw-secret", 1);
		const assistant = {
			...createAssistantMessage(""),
			timestamp: 2,
			stopReason: "toolUse" as const,
			content: [
				{ type: "toolCall" as const, id: "call-a", name: "read", arguments: { path: "raw-secret" } },
				{ type: "toolCall" as const, id: "call-b", name: "read", arguments: { path: "b" } },
				{ type: "toolCall" as const, id: "call-incomplete", name: "read", arguments: { path: "c" } },
			],
		};
		manager.appendMessage(assistant);
		manager.appendMessage({
			role: "toolResult",
			toolCallId: "call-a",
			toolName: "read",
			content: [{ type: "text", text: "a" }],
			isError: false,
			timestamp: 3,
		});
		manager.appendMessage({
			role: "toolResult",
			toolCallId: "call-b",
			toolName: "read",
			content: [{ type: "text", text: "b" }],
			isError: false,
			timestamp: 4,
		});
		const { lcm, context } = createHarness(manager);
		await lcm.project(manager.buildSessionContext({ keepDanglingToolCalls: true }).messages);
		const entries = context.snapshots.at(-1)!.entries;
		expect(entries.some(entry => entry.redactedText.includes("raw-secret"))).toBe(false);
		expect(JSON.stringify(entries)).not.toContain("raw-secret");
		expect(entries[0]?.artifactRefs).toEqual(["artifact://123"]);
		expect(entries.some(entry => entry.redactedText.includes("#SECRET"))).toBe(true);
		const toolEntries = entries.filter(entry => entry.atomicGroupId);
		expect(new Set(toolEntries.map(entry => entry.atomicGroupId))).toEqual(new Set([toolEntries[0]!.atomicGroupId]));
		expect(toolEntries).toHaveLength(3);
		await lcm.close();
	});

	it("keeps skipped file bytes out of SQLite sources and records only bounded identity metadata", async () => {
		const manager = SessionManager.inMemory("/worktree-files");
		const contentHash = new Bun.CryptoHasher("sha256").update("original file bytes").digest("hex");
		manager.appendMessage({
			role: "fileMention",
			files: [
				{
					path: "artifacts/raw-secret-large.bin",
					content: "raw-file-bytes-must-not-persist",
					byteSize: 8 * 1024 * 1024,
					contentHash,
					skippedReason: "tooLarge",
				},
			],
			timestamp: 1,
		});
		const { lcm, context } = createHarness(manager);
		await lcm.project(manager.buildSessionContext().messages);
		const source = context.snapshots.at(-1)?.entries[0];
		expect(source?.redactedText).not.toContain("raw-file-bytes-must-not-persist");
		expect(JSON.stringify(context.snapshots)).not.toContain("raw-secret");
		expect(source?.files).toHaveLength(1);
		expect(source?.files?.[0]).toMatchObject({
			contentHash,
			path: "artifacts/#SECRET-large.bin",
			fileType: "bin",
			byteSize: 8 * 1024 * 1024,
			explorationSummary: "Reference-only oversized file; bytes remain outside the LCM store.",
		});
		expect(source?.files?.[0]?.fileId).toMatch(/^file_[a-f0-9]{64}$/);
		await lcm.close();
	});

	it("checks active reference-only bytes from the session cwd without trusting redacted or out-of-project paths", async () => {
		const workspace = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-files-"));
		const projectRoot = path.join(workspace, "repo");
		const cwd = path.join(projectRoot, "subdir");
		const referencedPath = path.join(cwd, "raw-secret", "large.bin");
		const outsidePath = path.join(workspace, "outside.bin");
		const original = "original-bytes";
		const replacement = "replaced-bytes";
		const byteSize = Buffer.byteLength(original);
		expect(Buffer.byteLength(replacement)).toBe(byteSize);
		await fs.mkdir(path.dirname(referencedPath), { recursive: true });
		await Promise.all([Bun.write(referencedPath, replacement), Bun.write(outsidePath, original)]);

		const contentHash = new Bun.CryptoHasher("sha256").update(original).digest("hex");
		const outsideMention = path.relative(cwd, outsidePath);
		const manager = SessionManager.inMemory(cwd);
		manager.appendMessage({
			role: "fileMention",
			files: [
				{
					path: "raw-secret/large.bin",
					content: "(skipped auto-read: binary file)",
					byteSize,
					contentHash,
					skippedReason: "binary",
				},
				{
					path: outsideMention,
					content: "(skipped auto-read: binary file)",
					byteSize,
					contentHash,
					skippedReason: "binary",
				},
			],
			timestamp: 1,
		});
		const context = new FakeLcmContext();
		const { lcm } = createHarness(
			manager,
			context,
			"project:file-availability",
			undefined,
			undefined,
			20,
			projectRoot,
		);
		try {
			await lcm.project(manager.buildSessionContext().messages);
			const snapshot = context.snapshots.at(-1)!;
			const metadata = snapshot.entries.flatMap(entry => entry.files ?? []);
			const referenced = metadata.find(file => file.path === "#SECRET/large.bin");
			const outside = metadata.find(file => file.path === outsideMention);
			if (!referenced || !outside) throw new Error("Expected both reference-only file records");
			expect(JSON.stringify(snapshot)).not.toContain("raw-secret");
			expect(JSON.stringify(snapshot)).not.toContain(referencedPath);

			const scope = snapshot.scope;
			context.describeFile = reference => {
				const file = metadata.find(candidate => candidate.fileId === reference.fileId);
				return file ? { ...scope, ...file, sources: [] } : null;
			};
			const referencedHandle = {
				kind: "file" as const,
				reference: { ...scope, fileId: referenced.fileId },
			};
			const outsideHandle = { kind: "file" as const, reference: { ...scope, fileId: outside.fileId } };

			expect(await lcm.describe(referencedHandle)).toMatchObject({ kind: "file", value: { available: false } });
			await Bun.write(referencedPath, original);
			expect(await lcm.describe(referencedHandle)).toMatchObject({ kind: "file", value: { available: true } });
			expect(await lcm.describe(outsideHandle)).toMatchObject({ kind: "file", value: { available: false } });

			await manager.newSession();
			appendUser(manager, "replacement session", 2);
			await lcm.rebind();
			await lcm.status();
			const reboundScope = context.snapshots.at(-1)!.scope;
			const staleHandle = {
				kind: "file" as const,
				reference: { ...reboundScope, fileId: referenced.fileId },
			};
			expect(await lcm.describe(staleHandle)).toMatchObject({ kind: "file", value: { available: false } });
			lcm.beginDispose();
			expect(await lcm.describe(staleHandle)).toBeNull();
		} finally {
			await lcm.close();
			await fs.rm(workspace, { recursive: true, force: true });
		}
	});

	it("bridges scoped search, describe, and expansion while rejecting untracked file paths", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-bridge-"));
		const manager = SessionManager.inMemory(root);
		appendUser(manager, "retrievable history", 1);
		const context = new FakeLcmContext();
		const { lcm } = createHarness(manager, context);
		try {
			await lcm.project(manager.buildSessionContext().messages);
			const scope = context.snapshots.at(-1)!.scope;
			const citation: Citation = {
				...scope,
				sourceId: "source-1",
				sourceKey: "source-key-1",
				contentHash: "source-hash-1",
				position: 0,
			};
			const source: SourceDescription = {
				...citation,
				parentId: null,
				timestamp: 1,
				kind: "message:user",
				atomicGroupId: null,
				redactedText: "resolved source text",
				artifactRefs: [],
				files: [],
			};
			const summary: SummaryDescription = {
				...scope,
				summaryHandle: "summary-stable",
				kind: "leaf",
				level: 0,
				redactedText: "summary text",
				tokenCount: 2,
				sourceCount: 1,
				childCount: 0,
				parentHandles: [],
				files: [],
			};
			const file: FileDescription = {
				...scope,
				fileId: "file-stable",
				contentHash: new Bun.CryptoHasher("sha256").update("untracked-bytes").digest("hex"),
				path: "large.bin",
				fileType: "bin",
				byteSize: Buffer.byteLength("untracked-bytes"),
				tokenCount: 4,
				explorationSummary: "reference only",
				sources: [citation],
			};
			const hit: SearchHit = {
				kind: "summary",
				id: "generated-summary-id",
				summaryHandle: summary.summaryHandle,
				redactedText: summary.redactedText,
				rank: -1,
				citations: [citation],
			};
			const expansion: SummaryExpansion = {
				root: summary,
				items: [{ kind: "source", depth: 1, citation, tokenCount: 2, files: [] }],
				offset: 0,
				totalItems: 1,
				estimatedTokens: 2,
				truncated: false,
			};
			const search = vi.fn((_request: Parameters<LcmContext["search"]>[0]) => [hit]);
			context.search = search;
			context.describe = candidate => (candidate.sourceKey === citation.sourceKey ? source : null);
			context.describeSummary = reference => (reference.summaryHandle === summary.summaryHandle ? summary : null);
			context.describeFile = reference => (reference.fileId === file.fileId ? file : null);
			context.expandSummary = request => (request.summaryHandle === summary.summaryHandle ? expansion : null);

			expect(await lcm.search("needle", { limit: 3, offset: 2, summary })).toEqual([hit]);
			expect(search).toHaveBeenLastCalledWith({
				...scope,
				query: "needle",
				limit: 3,
				offset: 2,
				summaryHandle: "summary-stable",
			});
			expect(await lcm.search("needle", { summary: { ...summary, branchId: "other" } })).toEqual([]);
			expect(search).toHaveBeenCalledTimes(1);

			const fileHandle = { kind: "file" as const, reference: { ...scope, fileId: file.fileId } };
			expect(await lcm.describe(fileHandle)).toMatchObject({ kind: "file", value: { available: false } });
			expect(
				await lcm.describe({ kind: "file", reference: { ...fileHandle.reference, branchId: "other" } }),
			).toBeNull();

			const expanded = await lcm.expand({ reference: summary, depth: 1, offset: 0, limit: 20, maxTokens: 1_024 });
			expect(expanded?.items[0]).toMatchObject({
				kind: "source",
				available: true,
				redactedText: source.redactedText,
			});
			expect(
				await lcm.expand({
					reference: { ...summary, branchId: "other" },
					depth: 1,
					offset: 0,
					limit: 20,
					maxTokens: 1_024,
				}),
			).toBeNull();
		} finally {
			await lcm.close();
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	it("preserves a colliding same-millisecond live tail around transient history", async () => {
		const manager = SessionManager.inMemory("/worktree-a");
		const first = appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("settled"), timestamp: 2 });
		const active = appendUser(manager, "active", 3);
		const live: AgentMessage = { role: "user", content: [{ type: "text", text: "live" }], timestamp: 3 };
		const { lcm, context } = createHarness(manager);
		context.projectImpl = (_request, snapshot) => {
			const old = snapshot.entries[1]!;
			const fresh = snapshot.entries.at(-1)!;
			return {
				revision: 1,
				ready: true,
				historical: [
					{
						kind: "summary",
						summaryId: "summary-1",
						summaryHandle: "summary-handle-1",
						level: 0,
						redactedText: "older facts",
						tokenCount: 3,
						sourceIds: [old.entryId],
						citations: [
							{
								...snapshot.scope,
								sourceId: old.entryId,
								sourceKey: "key",
								contentHash: old.contentHash,
								position: 1,
							},
						],
						files: [],
					},
				],
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: 1,
				freshSourceCount: 1,
				estimatedTokens: 10,
				pendingJobs: 0,
			};
		};
		const input = [...manager.buildSessionContext().messages, live];
		const result = await lcm.project(input);
		const projected = result.messages;
		expect(projected[0]).toBe(first);
		expect(projected[1]?.role).toBe("historicalContext");
		expect(projected.filter(message => message.role === "historicalContext")).toHaveLength(1);
		expect(projected).toContain(active);
		expect(projected.at(-1)).toBe(live);
		const scope = context.snapshots.at(-1)!.scope;
		const lowered = convertToLlm(projected.filter(message => message.role === "historicalContext"));
		const providerText = lowered
			.map(message =>
				typeof message.content === "string"
					? message.content
					: message.content.map(block => (block.type === "text" ? block.text : "")).join("\n"),
			)
			.join("\n");
		const tokens = providerText.match(/lcm-handle:v1:[A-Za-z0-9_-]+/g) ?? [];
		expect(tokens).toHaveLength(1);
		expect(decodeLcmHandle(tokens[0]!)).toEqual({
			kind: "summary",
			reference: { ...scope, summaryHandle: "summary-handle-1" },
		});
		expect(providerText).toContain("older facts");
		expect(providerText).not.toContain("source:");
		expect(providerText).not.toContain("[Sources:");
		for (const entry of context.snapshots.at(-1)!.entries) {
			expect(providerText).not.toContain(`source:${entry.entryId}`);
		}
		expect(result.projection).toMatchObject({
			sourceTokens: context.snapshots.at(-1)!.entries.length,
			selectedLevelCounts: { 0: 1 },
			coveredSourceCount: 1,
			freshSourceCount: 1,
		});
		expect(manager.getBranch()).not.toContainEqual(
			expect.objectContaining({
				type: "message",
				message: expect.objectContaining({ role: "historicalContext" }),
			}),
		);
		await lcm.close();
	});

	it("annotates projected summaries with bounded file handles so compaction keeps file awareness", async () => {
		const manager = SessionManager.inMemory("/projected-files");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("settled"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const { lcm, context } = createHarness(manager);
		const files = Array.from({ length: 5 }, (_, index) => ({
			fileId: `file_${index}`,
			contentHash: `${index}`.repeat(64),
			path: `/repo/data-${index}.csv`,
			fileType: "csv",
			byteSize: 6_000_000,
			tokenCount: 1_500_000,
			explorationSummary: `Columns (1): c${index}`,
		}));
		context.projectImpl = (_request, snapshot) => {
			const old = snapshot.entries[1]!;
			const fresh = snapshot.entries.at(-1)!;
			return {
				revision: 1,
				ready: true,
				historical: [
					{
						kind: "summary",
						summaryId: "summary-files",
						summaryHandle: "summary-handle-files",
						level: 0,
						redactedText: "older facts about the datasets",
						tokenCount: 3,
						sourceIds: [old.entryId],
						citations: [
							{
								...snapshot.scope,
								sourceId: old.entryId,
								sourceKey: "key",
								contentHash: old.contentHash,
								position: 1,
							},
						],
						files,
					},
				],
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: 1,
				freshSourceCount: 1,
				estimatedTokens: 10,
				pendingJobs: 0,
			};
		};

		const result = await lcm.project(manager.buildSessionContext().messages);
		expect(result.owned).toBe(true);
		const scope = context.snapshots.at(-1)!.scope;
		const providerText = convertToLlm(result.messages.filter(message => message.role === "historicalContext"))
			.map(message =>
				typeof message.content === "string"
					? message.content
					: message.content.map(block => (block.type === "text" ? block.text : "")).join("\n"),
			)
			.join("\n");

		expect(providerText).toContain("[Files: ");
		// Three per summary keeps the active context bounded; the rest are counted, not dropped silently.
		expect(providerText).toContain("(+2 more)");
		expect(providerText).toContain("data-0.csv");
		expect(providerText).toContain("data-2.csv");
		expect(providerText).not.toContain("data-3.csv");

		const fileHandles = (providerText.match(/lcm-handle:v1:[A-Za-z0-9_-]+/g) ?? [])
			.map(token => decodeLcmHandle(token))
			.filter(handle => handle.kind === "file");
		expect(fileHandles).toEqual([
			{ kind: "file", reference: { ...scope, fileId: "file_0" } },
			{ kind: "file", reference: { ...scope, fileId: "file_1" } },
			{ kind: "file", reference: { ...scope, fileId: "file_2" } },
		]);
		await lcm.close();
	});

	it("indexes a large auto-read file as LCM metadata while keeping its head in the source text", () => {
		const manager = SessionManager.inMemory("/tracked-file-mention");
		appendUser(manager, "look at the dataset", 1);
		manager.appendMessage({
			role: "fileMention",
			files: [
				{
					path: "data/big.csv",
					content: "id,name,email\n1,ada,ada@example.com\n[truncated]",
					lineCount: 40_001,
					byteSize: 1_400_000,
					contentHash: "c".repeat(64),
					explorationSummary: "csv, 1.4MB; a truncated head was inlined.\nColumns (3): id, name, email",
				},
			],
			timestamp: 2,
		});

		const snapshot = normalizeLcmBranch(manager, "tracked-project", text => text);
		const mention = snapshot.entries.find(source => (source.files?.length ?? 0) > 0);
		expect(mention).toBeDefined();

		// No `skippedReason`, so the head must survive into the indexed source text.
		expect(mention?.redactedText).toContain("id,name,email");
		expect(mention?.redactedText).not.toContain("reference-only");

		// And the file is still registered, which is what lets a projected summary carry its handle.
		expect(mention?.files).toHaveLength(1);
		expect(mention?.files?.[0]).toMatchObject({
			contentHash: "c".repeat(64),
			path: "data/big.csv",
			fileType: "csv",
			byteSize: 1_400_000,
		});
		expect(mention?.files?.[0]?.explorationSummary).toContain("Columns (3): id, name, email");
		expect(mention?.files?.[0]?.fileId).toMatch(/^file_[a-f0-9]{64}$/);
	});

	it("fails open to the exact native input when handle-bearing history does not fit", async () => {
		const manager = SessionManager.inMemory("/unfitted-handle-history");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("settled"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		const projectionFits = vi.fn((_messages: AgentMessage[]) => false);
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			20,
			undefined,
			1,
			projectionFits,
		);
		context.projectImpl = (_request, snapshot) => readyHistoricalProjection(snapshot, "summary-handle");
		const input = manager.buildSessionContext().messages;

		const result = await lcm.project(input);

		expect(result.owned).toBe(false);
		expect(result.messages).toBe(input);
		expect(result.messages).toEqual(input);
		expect(result.messages.some(message => message.role === "historicalContext")).toBe(false);
		expect(projectionFits).toHaveBeenCalledTimes(1);
		const candidate = projectionFits.mock.calls[0]![0];
		expect(JSON.stringify(convertToLlm(candidate))).toContain("lcm-handle:v1:");
		await lcm.close();
	});

	it("fails open to the exact native input when a summary handle exceeds the token limit", async () => {
		const manager = SessionManager.inMemory("/oversized-summary-handle");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("settled"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const maxEncodedPayloadBytes = Math.floor((4_096 - "lcm-handle:v1:".length) * (3 / 4));
		const summaryHandle = "x".repeat(maxEncodedPayloadBytes + 256);
		expect(() =>
			encodeLcmHandle({
				kind: "summary",
				reference: { projectId: "project", sessionId: "session", branchId: "branch", summaryHandle },
			}),
		).toThrow("LCM handle token exceeds 4096 characters");
		const context = new FakeLcmContext();
		const projectionFits = vi.fn((_messages: AgentMessage[]) => true);
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			20,
			undefined,
			1,
			projectionFits,
		);
		context.projectImpl = (_request, snapshot) => readyHistoricalProjection(snapshot, summaryHandle);
		const input = manager.buildSessionContext().messages;

		const result = await lcm.project(input);

		expect(result.owned).toBe(false);
		expect(result.messages).toBe(input);
		expect(result.messages).toEqual(input);
		expect(result.messages.some(message => message.role === "historicalContext")).toBe(false);
		expect(JSON.stringify(result.messages)).not.toContain("[handle unavailable]");
		expect(projectionFits).not.toHaveBeenCalled();
		await lcm.close();
	});

	it("isolates sessions and worktree branches inside a shared project store", async () => {
		const managerA = SessionManager.inMemory("/repo/worktree-a");
		const managerB = SessionManager.inMemory("/repo/worktree-b");
		appendUser(managerA, "only-a", 1);
		appendUser(managerB, "only-b", 1);
		const a = createHarness(managerA, new FakeLcmContext(), "shared-project");
		const b = createHarness(managerB, new FakeLcmContext(), "shared-project");
		await a.lcm.project(managerA.buildSessionContext().messages);
		await b.lcm.project(managerB.buildSessionContext().messages);
		const scopeA = a.context.snapshots[0]!.scope;
		const scopeB = b.context.snapshots[0]!.scope;
		expect(scopeA.projectId).toBe(scopeB.projectId);
		expect(scopeA.sessionId).not.toBe(scopeB.sessionId);
		expect(a.context.snapshots[0]!.entries.map(entry => entry.redactedText).join("\n")).not.toContain("only-b");
		expect(b.context.snapshots[0]!.entries.map(entry => entry.redactedText).join("\n")).not.toContain("only-a");
		await Promise.all([a.lcm.close(), b.lcm.close()]);
	});

	it("fails open to the exact native input and records redacted summary failure without journal mutation", async () => {
		const manager = SessionManager.inMemory("/worktree-a");
		appendUser(manager, "first", 1);
		const context = new FakeLcmContext();
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, undefined, 5);
		context.queueJobs({
			...summaryJob("failure"),
			inputs: [{ kind: "source", id: "source-1", redactedText: "safe", tokenCount: 8 }],
		});
		complete.mockRejectedValueOnce(
			new LcmCompletionError("raw-secret summary failed", { provider: "test-provider" }),
		);
		const input = manager.buildSessionContext().messages;
		const output = await lcm.project(input);
		expect(output.messages).toBe(input);
		expect(output.owned).toBe(false);
		expect(context.failedError).toContain("#SECRET");
		expect(lcm.takePendingFallbackCategory()).toBe("provider");
		expect(context.failedError).not.toContain("raw-secret");
		expect(manager.getBranch()).toHaveLength(1);
		await lcm.close();
	});
});
