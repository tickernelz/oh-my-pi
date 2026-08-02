import { describe, expect, it, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type {
	Citation,
	ClaimSummaryJobsOptions,
	CompleteSummaryJobResult,
	ConfigureSummaryRetryPolicyOptions,
	ConfigureSummaryRetryPolicyResult,
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
	SummaryJobAvailability,
	SummaryJobLease,
	SummaryLocalAttemptOutcome,
	SummaryProviderAttempt,
	SummaryProviderAttemptStart,
	SummaryProviderUsage,
	SummaryReference,
	SummaryRetryMode,
	SummaryRetryPolicy,
} from "@oh-my-pi/lcm-context";
import { activeSourceFingerprint } from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import * as AIError from "@oh-my-pi/pi-ai/error";
import { decodeLcmHandle, encodeLcmHandle } from "@oh-my-pi/pi-coding-agent/lcm/operations";
import { convertToLlm, createHistoricalContextMessage } from "@oh-my-pi/pi-coding-agent/session/messages";
import {
	estimateLcmProjectionMessageTokens,
	estimateLcmProjectionMessageTokenUpperBound,
	LcmCompletionError,
	type LcmCompletionRequest,
	type LcmCompletionResult,
	MAX_LCM_PROJECTION_TOKEN_MEASUREMENTS,
	normalizeLcmBranch,
	SessionLcm,
	type SessionLcmDependencies,
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
	retryPolicy: SummaryRetryPolicy | undefined;
	retryPolicyToken = "policy-0";
	readonly retryPolicyCalls: Array<{
		projectId: string;
		retryKey: string;
		options: ConfigureSummaryRetryPolicyOptions | undefined;
	}> = [];
	readonly availabilityCalls: Array<{ request: ProjectionRequest; policy: SummaryRetryPolicy; limit: number }> = [];
	readonly retryCalls: Array<{
		request: ProjectionRequest;
		policy: SummaryRetryPolicy;
		limit: number;
		mode: SummaryRetryMode;
	}> = [];
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
	readonly delayCalls: Array<{
		policy: SummaryRetryPolicy;
		maxTransportRetries: number;
		preferredScope: ContextScope | undefined;
		allowFallback: boolean;
	}> = [];
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
	preResolvedModelOverride: string | undefined;
	resolvedModelOverride: string | undefined;
	beforeAttemptStart: ((request: LcmCompletionRequest) => void | Promise<void>) | undefined;
	readonly failureRecordsRequested = Promise.withResolvers<void>();
	lastCompletion: SummaryCompletion | undefined;
	job: SummaryJob | undefined;
	now = 1_000_000;
	priorSpend = 0;
	readonly priorSpendCalls: Array<{ sessionId: string; before: number }> = [];
	maxLeased = 0;
	projectionCalls = 0;
	projectImpl: (request: ProjectionRequest, snapshot: SourceSnapshot) => ContextProjection = (_request, snapshot) => ({
		revision: 1,
		ready: false,
		historical: [],
		activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
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
		this.projectionCalls++;
		return this.projectImpl(request, this.snapshots.at(-1)!);
	}
	configureSummaryRetryPolicy(
		projectId: string,
		retryKey: string,
		options?: ConfigureSummaryRetryPolicyOptions,
	): ConfigureSummaryRetryPolicyResult {
		this.retryPolicyCalls.push({ projectId, retryKey, options });
		const current = this.retryPolicy;
		if (!current) {
			const ready = { retryKey, retryEpoch: 1 };
			this.retryPolicy = ready;
			this.retryPolicyToken = "policy-1";
			return { kind: "ready", ...ready };
		}
		if (current.retryKey === retryKey) return { kind: "ready", ...current };
		const expected = options?.expected;
		if (
			options?.force !== true &&
			(expected?.retryKey !== current.retryKey || expected.retryEpoch !== current.retryEpoch)
		) {
			return { kind: "conflict", ...current };
		}
		const ready = { retryKey, retryEpoch: current.retryEpoch + 1 };
		this.retryPolicy = ready;
		this.retryPolicyToken = `policy-${ready.retryEpoch}`;
		for (const leased of this.leasedJobs.values()) this.jobs.unshift(leased);
		this.leasedJobs.clear();
		this.jobs = this.jobs.map(job => ({ ...job, retryKey, retryEpoch: ready.retryEpoch, transportRetryCount: 0 }));
		this.failureRecords.clear();
		return { kind: "ready", ...ready };
	}

	summaryJobAvailability(
		request: ProjectionRequest,
		policy: SummaryRetryPolicy,
		maxTransportRetries: number,
	): SummaryJobAvailability {
		this.availabilityCalls.push({ request, policy, limit: maxTransportRetries });
		const result: SummaryJobAvailability = {
			runnable: 0,
			leased: 0,
			backoff: 0,
			exhausted: 0,
			missing: 0,
			policyMismatch: 0,
		};
		if (
			!this.retryPolicy ||
			this.retryPolicy.retryKey !== policy.retryKey ||
			this.retryPolicy.retryEpoch !== policy.retryEpoch
		) {
			result.policyMismatch = Math.max(1, this.relevantPendingJobs ?? 0);
			return result;
		}
		let observed = 0;
		for (const job of [...this.jobs, ...this.leasedJobs.values()]) {
			if (job.queueClass !== "preferred") continue;
			observed++;
			if (job.retryEpoch !== policy.retryEpoch) {
				result.policyMismatch++;
				continue;
			}
			if (job.transportRetryCount >= maxTransportRetries) {
				result.exhausted++;
				continue;
			}
			if (this.leasedJobs.has(job.jobId) && job.leaseExpiresAt > this.now) {
				result.leased++;
				result.nextLeaseExpiryAt = Math.min(result.nextLeaseExpiryAt ?? job.leaseExpiresAt, job.leaseExpiresAt);
				continue;
			}
			const availableAt = this.failureRecords.get(job.jobId)?.availableAt ?? this.now;
			if (availableAt <= this.now) result.runnable++;
			else {
				result.backoff++;
				result.nextAvailableAt = Math.min(result.nextAvailableAt ?? availableAt, availableAt);
			}
		}
		result.missing = Math.max(0, (this.relevantPendingJobs ?? observed) - observed);
		return result;
	}

	retrySummaryJobs(
		request: ProjectionRequest,
		policy: SummaryRetryPolicy,
		maxTransportRetries: number,
		mode: SummaryRetryMode = "due",
	): SummaryJobAvailability {
		this.retryCalls.push({ request, policy, limit: maxTransportRetries, mode });
		if (mode === "all") {
			for (const [jobId, failure] of this.failureRecords) {
				const job = this.jobs.find(candidate => candidate.jobId === jobId);
				if (job && job.transportRetryCount < maxTransportRetries) {
					this.failureRecords.set(jobId, { ...failure, availableAt: this.now });
				}
			}
		}
		return this.summaryJobAvailability(request, policy, maxTransportRetries);
	}

	claimSummaryJobs(options: ClaimSummaryJobsOptions): SummaryJob[] {
		this.claimCalls.push(options);
		const error = this.claimErrors.shift();
		if (error !== undefined) throw error;
		if (this.retryPolicy?.retryKey !== options.retryKey || this.retryPolicy.retryEpoch !== options.retryEpoch) {
			return [];
		}
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
				job.queueClass === "preferred" &&
				job.retryEpoch === options.retryEpoch &&
				job.transportRetryCount < options.maxTransportRetries &&
				(this.failureRecords.get(job.jobId)?.availableAt ?? this.now) <= this.now,
		);
		if (index < 0 && options.allowFallback !== false) {
			index = this.jobs.findIndex(
				job =>
					job.queueClass === "fallback" &&
					job.retryEpoch === options.retryEpoch &&
					job.transportRetryCount < options.maxTransportRetries &&
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
			retryKey: options.retryKey,
			retryEpoch: options.retryEpoch,
			leasePolicyToken: this.retryPolicyToken,
			leaseMutationNonce: `nonce-${this.leaseSequence}`,
		};
		this.failureRecords.delete(job.jobId);
		this.leasedJobs.set(job.jobId, job);
		this.maxLeased = Math.max(this.maxLeased, this.leasedJobs.size);
		return [job];
	}

	nextSummaryJobDelayMs(
		policy: SummaryRetryPolicy,
		maxTransportRetries: number,
		preferredScope?: ContextScope,
		allowFallback = true,
	): number | null {
		this.delayCalls.push({ policy, maxTransportRetries, preferredScope, allowFallback });
		this.delayRequested.resolve();
		if (this.retryPolicy?.retryKey !== policy.retryKey || this.retryPolicy.retryEpoch !== policy.retryEpoch) {
			return null;
		}
		if (this.nextDelayMs !== null) return this.nextDelayMs;
		const delays: number[] = [];
		for (const job of [...(this.job ? [this.job] : []), ...this.jobs]) {
			if (job.queueClass === "fallback" && !allowFallback) continue;
			if (job.retryEpoch !== policy.retryEpoch || job.transportRetryCount >= maxTransportRetries) continue;
			delays.push(Math.max(0, (this.failureRecords.get(job.jobId)?.availableAt ?? this.now) - this.now));
		}
		return delays.length === 0 ? null : Math.min(...delays);
	}

	priorSummarySpendUsd(sessionId: string, before: number): number {
		this.priorSpendCalls.push({ sessionId, before });
		return this.priorSpend;
	}

	summaryJobFailures(policy: SummaryRetryPolicy, maxTransportRetries: number) {
		this.failureRecordsRequested.resolve();
		if (this.retryPolicy?.retryKey !== policy.retryKey || this.retryPolicy.retryEpoch !== policy.retryEpoch) {
			return [];
		}
		return [...this.failureRecords.values()].filter(failure => {
			const job = this.jobs.find(candidate => candidate.jobId === failure.jobId);
			return !job || (job.retryEpoch === policy.retryEpoch && job.transportRetryCount < maxTransportRetries);
		});
	}

	#leaseMatches(leased: SummaryJob | undefined, lease: SummaryJobLease): leased is SummaryJob {
		if (!leased) return false;
		return (
			leased?.leaseToken === lease.leaseToken &&
			leased.retryKey === lease.retryKey &&
			leased.retryEpoch === lease.retryEpoch &&
			leased.leasePolicyToken === lease.leasePolicyToken &&
			leased.leaseMutationNonce === lease.leaseMutationNonce
		);
	}

	extendSummaryJob(lease: SummaryJobLease): string | null {
		const leased = this.leasedJobs.get(lease.jobId);
		if (!this.#leaseMatches(leased, lease)) return null;
		const nonce = `nonce-${++this.leaseSequence}`;
		this.leasedJobs.set(lease.jobId, { ...leased, leaseMutationNonce: nonce });
		return nonce;
	}

	releaseSummaryJob(lease: SummaryJobLease): boolean {
		const error = this.releaseErrors.shift();
		if (error !== undefined) throw error;
		const leased = this.leasedJobs.get(lease.jobId);
		const accepted = this.#leaseMatches(leased, lease);
		this.releaseCalls.push({ jobId: lease.jobId, leaseToken: lease.leaseToken, accepted });
		if (!accepted || !leased) return false;
		this.leasedJobs.delete(lease.jobId);
		this.jobs.unshift(leased);
		return true;
	}

	beginSummaryAttempt(
		lease: SummaryJobLease,
		attempt: SummaryProviderAttemptStart,
		provenance: SummaryAttemptProvenance,
	): boolean {
		const leased = this.leasedJobs.get(lease.jobId);
		if (!this.#leaseMatches(leased, lease) || this.rejectAttemptStart) return false;
		if (this.attemptRows.has(attempt.attemptId)) return false;
		this.attemptRows.set(attempt.attemptId, {
			jobId: lease.jobId,
			outcome: "in_flight",
			provenance,
			usage: undefined,
		});
		return true;
	}

	settleSummaryAttempt(
		lease: SummaryJobLease,
		attempt: SummaryProviderAttempt | SummaryProviderAttemptStart,
		requestedOutcome: SummaryLocalAttemptOutcome,
	): SummaryAttemptOutcome | null {
		return this.#settleAttempt(lease, attempt, requestedOutcome);
	}

	#settleAttempt(
		lease: SummaryJobLease,
		attempt: SummaryProviderAttempt | SummaryProviderAttemptStart,
		requestedOutcome: SummaryAttemptOutcome,
	): SummaryAttemptOutcome | null {
		const row = this.attemptRows.get(attempt.attemptId);
		if (!row || row.jobId !== lease.jobId) return null;
		if (row.outcome !== "in_flight") {
			if (!("usage" in attempt) || row.usage) return null;
			row.usage = attempt.usage;
			return row.outcome;
		}
		row.outcome = this.#leaseMatches(this.leasedJobs.get(lease.jobId), lease) ? requestedOutcome : "lease_lost";
		row.usage = "usage" in attempt ? attempt.usage : undefined;
		return row.outcome;
	}

	completeSummaryJob(lease: SummaryJobLease, completion: SummaryCompletion): CompleteSummaryJobResult {
		const leased = this.leasedJobs.get(lease.jobId);
		if (!this.#leaseMatches(leased, lease)) return { accepted: false, reason: "lease_lost" };
		if (completion.attempt) this.#settleAttempt(lease, completion.attempt, "completed");
		this.leasedJobs.delete(lease.jobId);
		this.failureRecords.delete(lease.jobId);
		this.completedJobIds.add(lease.jobId);
		this.lastCompletion = completion;
		this.queuedJobs = Math.max(0, this.queuedJobs - 1);
		if (this.relevantPendingJobs !== undefined && leased.queueClass === "preferred") {
			this.relevantPendingJobs = Math.max(0, this.relevantPendingJobs - 1);
		}
		this.nextDelayMs = null;
		this.summaryCompleted.resolve();
		return { accepted: true, summaryId: `summary-${lease.jobId}` };
	}

	failSummaryJob(
		lease: SummaryJobLease,
		redactedError: string,
		retryDelayMs: number,
		provenance?: SummaryAttemptProvenance,
		failedAttempt?: {
			attempt: SummaryProviderAttempt | SummaryProviderAttemptStart;
			outcome: SummaryFailureAttemptOutcome;
		},
		countTransportRetry = true,
	): boolean {
		const leased = this.leasedJobs.get(lease.jobId);
		if (!this.#leaseMatches(leased, lease)) return false;
		if (
			failedAttempt &&
			this.#settleAttempt(lease, failedAttempt.attempt, failedAttempt.outcome) !== failedAttempt.outcome
		) {
			return false;
		}
		this.leasedJobs.delete(lease.jobId);
		this.jobs.push({
			...leased,
			transportRetryCount: leased.transportRetryCount + (countTransportRetry ? 1 : 0),
		});
		this.failedError = redactedError;
		this.failureCalls.push({
			jobId: lease.jobId,
			leaseToken: lease.leaseToken,
			redactedError,
			retryDelayMs,
			provenance,
		});
		this.seedFailure(lease.jobId, this.now + Math.max(1, retryDelayMs), leased.queueClass);
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
			schemaVersion: 10,
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
	schedulerTickMs = 20,
	projectRoot?: string,
	maxConcurrentSummaries = 1,
	projectionTokenMeasurements: (messages: AgentMessage[]) => { tokens: number; upperBound: number } = messages => {
		let tokens = 0;
		let upperBound = 0;
		for (const message of messages) {
			tokens += estimateLcmProjectionMessageTokens(message);
			upperBound += estimateLcmProjectionMessageTokenUpperBound(message);
		}
		return { tokens, upperBound };
	},
	peerPollMs?: number,
	providerAttemptTimeoutMs?: number,
	resolveProject?: SessionLcmDependencies["resolveProject"],
) {
	const complete = vi.fn(async (_request: LcmCompletionRequest) => "redacted summary");
	const resolveSummaryModel = vi.fn((selector?: string) => {
		const configured = selector ?? "@smol";
		return (
			context.preResolvedModelOverride ??
			context.resolvedModelOverride ??
			(configured.startsWith("@") ? "test-provider/test-model" : configured)
		);
	});
	let attemptOrdinal = 0;
	const attemptStarts: SummaryProviderAttemptStart[] = [];
	const completeWithAttempt = async (request: LcmCompletionRequest): Promise<LcmCompletionResult> => {
		const start: SummaryProviderAttemptStart = {
			attemptId: `attempt-${++attemptOrdinal}`,
			startedAt: context.now,
			provider: "test-provider",
			model: "test-model",
		};
		const resolvedModel = context.resolvedModelOverride ?? resolveSummaryModel(request.modelSelector);
		request.onResolvedModel?.(resolvedModel);
		await context.beforeAttemptStart?.(request);
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
			projectionTokenMeasurements,
			complete: completeWithAttempt,
			resolveSummaryModel,
		},
		{
			summaryModel: "@smol",
			maxConcurrentSummaries,
			registerProject,
			dependencies: {
				openContext,
				resolveProject:
					resolveProject ??
					(async cwd => {
						const rootPath = projectRoot ?? cwd;
						return {
							projectId: projectId ?? `project:${Bun.hash(cwd)}`,
							rootPath,
							storePath: `${rootPath}/context.sqlite`,
						};
					}),
				peerPollMs: peerPollMs ?? schedulerTickMs,
				providerAttemptTimeoutMs,
				now: () => context.now,
			},
		},
	);
	return { lcm, context, complete, openContext, resolveSummaryModel };
}

function appendUser(manager: SessionManager, text: string, timestamp: number): AgentMessage {
	const message: AgentMessage = { role: "user", content: [{ type: "text", text }], timestamp };
	manager.appendMessage(message);
	return message;
}

function readyHistoricalProjection(snapshot: SourceSnapshot, summaryHandle: string): ContextProjection {
	const historical = snapshot.entries.slice(0, -1);
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
				sourceIds: historical.map(entry => entry.entryId),
				citations: [],
				files: [],
			},
		],
		activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
		freshTailSourceIds: [fresh.entryId],
		uncoveredSourceIds: [],
		sourceTokens: snapshot.entries.length,
		selectedLevelCounts: { 0: 1 },
		coveredSourceCount: historical.length,
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
		retryKey: "@smol",
		retryEpoch: 1,
		leasePolicyToken: "policy-1",
		leaseMutationNonce: `nonce-${jobId}`,
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

async function waitForSummaryWorkerCount(lcm: SessionLcm, active: number): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if ((await lcm.status()).runtime.summaryWorkers.active === active) return;
		await Promise.resolve();
	}
	throw new Error(`Summary workers did not settle at ${active}`);
}

describe("SessionLcm", () => {
	it("charges serialized historical context against projection fit budgets", () => {
		const historical = createHistoricalContextMessage({
			redactedCitedContent: "historical payload ".repeat(300),
			timestamp: 1,
		});
		expect(estimateLcmProjectionMessageTokens(historical)).toBeGreaterThan(1_000);
	});

	it("uses provider-visible UTF-8 bytes as a conservative historical upper bound", () => {
		const historical = createHistoricalContextMessage({
			redactedCitedContent: 'first line\nquoted "世界" \\ payload\nlast line',
			timestamp: 1,
		});
		const providerBytes = convertToLlm([historical]).reduce((total, message) => {
			if (typeof message.content === "string") return total + Buffer.byteLength(message.content, "utf8");
			return (
				total +
				message.content.reduce(
					(sum, block) => sum + (block.type === "text" ? Buffer.byteLength(block.text, "utf8") : 0),
					0,
				)
			);
		}, 0);

		expect(estimateLcmProjectionMessageTokenUpperBound(historical)).toBe(providerBytes);
		expect(estimateLcmProjectionMessageTokenUpperBound(historical)).toBeGreaterThanOrEqual(
			estimateLcmProjectionMessageTokens(historical),
		);
	});

	it("preserves finite provider retry hints up to the safe numeric bound", () => {
		const fortyEightHours = 48 * 60 * 60_000;
		expect(new LcmCompletionError("fractional", { retryAfterMs: 34_074.224 }).retryAfterMs).toBe(34_075);
		expect(new LcmCompletionError("long quota reset", { retryAfterMs: fortyEightHours }).retryAfterMs).toBe(
			fortyEightHours,
		);
		expect(new LcmCompletionError("oversized", { retryAfterMs: Number.MAX_VALUE }).retryAfterMs).toBe(
			Number.MAX_SAFE_INTEGER,
		);
		expect(new LcmCompletionError("negative", { retryAfterMs: -1 }).retryAfterMs).toBeUndefined();
		expect(
			new LcmCompletionError("infinite", { retryAfterMs: Number.POSITIVE_INFINITY }).retryAfterMs,
		).toBeUndefined();
		expect(new LcmCompletionError("invalid", { retryAfterMs: Number.NaN }).retryAfterMs).toBeUndefined();
	});

	it("rejects ready projections with a gap, duplicate, reorder, stale source set, or mismatched fingerprint", async () => {
		const invalidProofs = [
			["gap", (ids: string[]) => ({ ids: [ids[0]!, ids[2]!], fingerprint: activeSourceFingerprint(ids) })],
			[
				"duplicate",
				(ids: string[]) => ({ ids: [ids[0]!, ids[1]!, ids[1]!], fingerprint: activeSourceFingerprint(ids) }),
			],
			[
				"reorder",
				(ids: string[]) => ({ ids: [ids[1]!, ids[0]!, ids[2]!], fingerprint: activeSourceFingerprint(ids) }),
			],
			[
				"fingerprint",
				(ids: string[]) => ({ ids, fingerprint: activeSourceFingerprint([ids[2]!, ids[1]!, ids[0]!]) }),
			],
			[
				"stale-source-set",
				(ids: string[]) => {
					const staleIds = ids.map((_id, index) => `stale-${index}`);
					return { ids: staleIds, fingerprint: activeSourceFingerprint(staleIds) };
				},
			],
		] as const;

		for (const [label, invalidProof] of invalidProofs) {
			const manager = SessionManager.inMemory(`/invalid-coverage-proof-${label}`);
			appendUser(manager, "first", 1);
			appendUser(manager, "second", 2);
			appendUser(manager, "third", 3);
			const context = new FakeLcmContext();
			context.projectImpl = (_request, snapshot) => {
				const activeIds = snapshot.entries.map(entry => entry.entryId);
				expect(activeIds).toHaveLength(3);
				const proof = invalidProof(activeIds);
				return {
					revision: 1,
					ready: true,
					activeSourceFingerprint: proof.fingerprint,
					historical: [],
					freshTailSourceIds: proof.ids,
					uncoveredSourceIds: [],
					sourceTokens: activeIds.length,
					selectedLevelCounts: {},
					coveredSourceCount: 0,
					freshSourceCount: proof.ids.length,
					estimatedTokens: activeIds.length,
					pendingJobs: 0,
				};
			};
			const { lcm } = createHarness(manager, context);
			try {
				const input = manager.buildSessionContext().messages;
				const result = await lcm.project(input);
				expect(result.messages).toBe(input);
				expect(result.owned).toBe(false);
				expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
				expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
					kind: "native_fallback",
					category: "unfit",
					reason: "assembly_invalid",
				});
			} finally {
				await lcm.close();
			}
		}
	});

	it("returns the exact native input below prewarm without resolving a project or opening the derived store", async () => {
		const manager = SessionManager.inMemory("/below-prewarm");
		appendUser(manager, "small", 1);
		const resolveProject = vi.fn(async (cwd: string) => ({
			projectId: "project:below-prewarm",
			rootPath: cwd,
			storePath: `${cwd}/context.sqlite`,
		}));
		const { lcm, context, complete, openContext } = createHarness(
			manager,
			undefined,
			undefined,
			undefined,
			() => ({
				sourceTokens: 39,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 80,
				freshTail: { maxSources: 8, maxTokens: 40 },
			}),
			20,
			undefined,
			1,
			undefined,
			undefined,
			undefined,
			resolveProject,
		);
		const input = manager.buildSessionContext().messages;
		const result = await lcm.project(input);
		expect(result.messages).toBe(input);
		expect(result.owned).toBe(false);
		expect(typeof result.routeKey?.scope?.projectId).toBe("string");
		expect(typeof result.routeKey?.scope?.branchId).toBe("string");
		expect(typeof result.routeKey?.scope?.inputAnchor).toBe("string");
		expect(result.routeKey?.scope).not.toHaveProperty("revision");
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		expect(resolveProject).not.toHaveBeenCalled();
		expect(openContext).not.toHaveBeenCalled();
		expect(context.snapshots).toEqual([]);
		const stale = await lcm.project(input);
		appendUser(manager, "mutated before dispatch", 2);
		expect(lcm.commitPrimaryRequestRoute(stale.routeKey)).toBe(false);
		expect(resolveProject).not.toHaveBeenCalled();
		expect(openContext).not.toHaveBeenCalled();
		expect(context.snapshots).toEqual([]);
		expect(complete).not.toHaveBeenCalled();
		expect((await lcm.status()).runtime).toMatchObject({
			health: "healthy",
			coverageReadiness: "idle",
			lastRequestRoute: { kind: "native_passthrough", reason: "below_prewarm" },
		});
		await lcm.close();
	});

	for (const [reason, passthroughTokens] of [
		["below_prewarm", 39],
		["below_hard", 90],
	] as const) {
		it(`fences a warmed ${reason} route by its matching derived revision`, async () => {
			const manager = SessionManager.inMemory(`/warmed-${reason}-revision-fence`);
			appendUser(manager, "covered request", 1);
			const context = new FakeLcmContext();
			let derivedRevision = 1;
			context.projectImpl = (_request, snapshot) => ({
				revision: derivedRevision,
				ready: true,
				historical: [],
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: {},
				coveredSourceCount: 0,
				freshSourceCount: snapshot.entries.length,
				estimatedTokens: snapshot.entries.length,
				pendingJobs: 0,
			});
			const reconcile = context.reconcile.bind(context);
			vi.spyOn(context, "reconcile").mockImplementation((snapshot, options) => ({
				...reconcile(snapshot, options),
				revision: derivedRevision,
			}));
			const rebuild = context.rebuild.bind(context);
			vi.spyOn(context, "rebuild").mockImplementation(snapshots => {
				const result = rebuild(snapshots);
				derivedRevision++;
				return result;
			});
			let sourceTokens = 101;
			const { lcm } = createHarness(manager, context, undefined, undefined, () => ({
				sourceTokens,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 80,
				freshTail: { maxSources: 8, maxTokens: 40 },
			}));
			try {
				const input = manager.buildSessionContext().messages;
				expect((await lcm.project(input)).owned).toBe(true);
				sourceTokens = passthroughTokens;
				const staged = await lcm.project(input);
				expect(staged.owned).toBe(false);
				expect(await lcm.rebuild()).not.toBeNull();
				expect((await lcm.status()).runtime.currentBranch?.revision).toBe(2);
				expect(lcm.commitPrimaryRequestRoute(staged.routeKey)).toBe(false);
				expect(staged.routeKey?.scope?.revision).toBe(1);
			} finally {
				await lcm.close();
			}
		});
	}

	it("rejects a warmed tokenBudget-zero below-hard route after rebuild", async () => {
		const manager = SessionManager.inMemory("/warmed-invalid-limit-revision-fence");
		appendUser(manager, "covered request", 1);
		const context = new FakeLcmContext();
		let derivedRevision = 1;
		context.projectImpl = (_request, snapshot) => ({
			revision: derivedRevision,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const reconcile = context.reconcile.bind(context);
		vi.spyOn(context, "reconcile").mockImplementation((snapshot, options) => ({
			...reconcile(snapshot, options),
			revision: derivedRevision,
		}));
		const rebuild = context.rebuild.bind(context);
		vi.spyOn(context, "rebuild").mockImplementation(snapshots => {
			const result = rebuild(snapshots);
			derivedRevision++;
			return result;
		});
		let sourceTokens = 101;
		let tokenBudget = 80;
		const { lcm } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		try {
			const input = manager.buildSessionContext().messages;
			expect((await lcm.project(input)).owned).toBe(true);
			sourceTokens = 90;
			tokenBudget = 0;
			const staged = await lcm.project(input);
			expect(staged.owned).toBe(false);
			expect(staged.routeKey).toBeDefined();
			expect(await lcm.rebuild()).not.toBeNull();
			expect((await lcm.status()).runtime.currentBranch?.revision).toBe(2);
			expect(lcm.commitPrimaryRequestRoute(staged.routeKey)).toBe(false);
			expect(staged.routeKey?.scope?.revision).toBe(1);
		} finally {
			await lcm.close();
		}
	});

	it("samples request pressure even when it stands down below prewarm", async () => {
		const manager = SessionManager.inMemory("/pressure-sample");
		appendUser(manager, "small", 1);
		const { lcm } = createHarness(manager, undefined, undefined, undefined, () => ({
			sourceTokens: 39,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		expect((await lcm.status()).runtime.pressure).toBeUndefined();
		await lcm.project(manager.buildSessionContext().messages);
		// Recorded despite the stand-down, and taken from the projection limits rather than
		// the branch's own source-token total.
		expect((await lcm.status()).runtime.pressure).toEqual({
			requestTokens: 39,
			armTokens: 39,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
		});
		await lcm.close();
		expect((await lcm.status()).runtime.pressure).toBeUndefined();
	});

	it("uses a maintenance pressure floor once without changing dispatched route history", async () => {
		const manager = SessionManager.inMemory("/maintenance-pressure-intent");
		appendUser(manager, "covered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const projectionLimits = vi.fn(() => ({
			sourceTokens: 60,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		const { lcm } = createHarness(manager, context, undefined, undefined, projectionLimits);
		const input = manager.buildSessionContext().messages;

		const decision = await lcm.ownsRequest(input, undefined, 150);
		expect(decision).toMatchObject({ kind: "owned", projection: { ready: true } });
		expect((await lcm.status()).runtime.lastRequestRoute).toBeUndefined();

		const projected = await lcm.project(input);
		expect(projected.owned).toBe(true);
		expect(lcm.commitPrimaryRequestRoute(projected.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.pressure).toMatchObject({ requestTokens: 150, armTokens: 150 });
		const committedRoute = (await lcm.status()).runtime.lastRequestRoute;
		expect(committedRoute).toMatchObject({ kind: "lossless" });

		expect(await lcm.ownsRequest(input, undefined, 150)).toMatchObject({ kind: "owned" });
		expect((await lcm.status()).runtime.lastRequestRoute).toEqual(committedRoute);
		expect((await lcm.project(input)).owned).toBe(true);
		expect((await lcm.project(input)).owned).toBe(false);
		expect((await lcm.status()).runtime.lastRequestRoute).toEqual(committedRoute);
		await lcm.close();
	});

	it("clears a maintenance pressure intent when primary messages mismatch", async () => {
		const manager = SessionManager.inMemory("/maintenance-pressure-mismatch");
		appendUser(manager, "covered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 60,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		const input = manager.buildSessionContext().messages;
		expect(await lcm.ownsRequest(input, undefined, 150)).toMatchObject({ kind: "owned" });

		const mismatched = [{ role: "developer", content: "different primary payload", timestamp: 1 } as AgentMessage];
		expect((await lcm.project(mismatched)).owned).toBe(false);
		expect((await lcm.project(input)).owned).toBe(false);
		await lcm.close();
	});

	it("rearms an owned maintenance intent for the post-drop retry payload", async () => {
		const manager = SessionManager.inMemory("/maintenance-pressure-rearm");
		appendUser(manager, "covered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 60,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		const retry = manager.buildSessionContext().messages;
		const truncated = [
			...retry,
			{ ...createAssistantMessage("truncated"), stopReason: "length" as const, timestamp: 2 },
		];
		expect(await lcm.ownsRequest(truncated, undefined, 150)).toMatchObject({ kind: "owned" });
		const rearm = lcm as SessionLcm & {
			rearmPrimaryIntent(messages: readonly AgentMessage[], requestTokensFloor: number): void;
		};
		expect(typeof rearm.rearmPrimaryIntent).toBe("function");
		if (typeof rearm.rearmPrimaryIntent === "function") rearm.rearmPrimaryIntent(retry, 150);

		expect((await lcm.project(retry)).owned).toBe(true);
		await lcm.close();
	});

	it("preserves an owned mid-run floor when steering and asides extend the provider request", async () => {
		const manager = SessionManager.inMemory("/maintenance-pressure-final-payload");
		appendUser(manager, "covered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 60,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		const retry = manager.buildSessionContext().messages;
		expect(await lcm.ownsRequest(retry, undefined, 150)).toMatchObject({ kind: "owned" });
		const finalPayload = [
			...retry,
			{ role: "developer", content: "queued steering", timestamp: 2 } as AgentMessage,
			{
				role: "custom",
				customType: "test-aside",
				content: "late aside",
				display: false,
				attribution: "agent",
				timestamp: 3,
			} as AgentMessage,
		];

		expect((await lcm.project(finalPayload)).owned).toBe(true);
		expect((await lcm.status()).runtime.pressure).toMatchObject({ requestTokens: 150, armTokens: 150 });
		await lcm.close();
	});

	it("returns a request-local maintenance fallback without staging route history", async () => {
		const manager = SessionManager.inMemory("/maintenance-local-fallback");
		appendUser(manager, "uncovered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: false,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: [],
			uncoveredSourceIds: snapshot.entries.map(entry => entry.entryId),
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: 0,
			estimatedTokens: 0,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context);

		expect(await lcm.ownsRequest(manager.buildSessionContext().messages, undefined, 150)).toEqual({
			kind: "native",
			fallback: { category: "unfit", reason: "coverage_gap" },
		});
		const runtime = (await lcm.status()).runtime;
		expect(runtime.lastRequestRoute).toBeUndefined();
		expect(runtime.lastTakeover).toBeUndefined();
		await lcm.close();
	});

	it("keeps coverage readiness independent and derives health from current durable failures", async () => {
		const manager = SessionManager.inMemory("/independent-runtime-status");
		appendUser(manager, "covered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: 1,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context);

		expect((await lcm.project(manager.buildSessionContext().messages)).owned).toBe(true);
		expect((await lcm.status()).runtime).toMatchObject({ health: "healthy", coverageReadiness: "ready" });

		context.seedFailure("preferred", context.now + 10_000, "preferred");
		const degraded = (await lcm.status()).runtime;
		expect(degraded).toMatchObject({ health: "degraded", coverageReadiness: "ready" });
		expect(degraded.lastFailure).toBeUndefined();

		context.now += 10_000;
		const due = (await lcm.status()).runtime;
		expect(due).toMatchObject({ health: "degraded", coverageReadiness: "ready" });
		expect(due.summaryBackoff).toBeUndefined();
		context.failureRecords.clear();
		const recovered = (await lcm.status()).runtime;
		expect(recovered).toMatchObject({ health: "healthy", coverageReadiness: "ready" });
		expect(recovered.lastFailure).toBeUndefined();
		await lcm.close();
	});

	it("clears provider degradation only after an accepted summary succeeds", async () => {
		const manager = SessionManager.inMemory("/provider-health-success");
		appendUser(manager, "retry job", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("health-recovery"));
		const pendingProjection = context.projectImpl;
		context.projectImpl = (request, snapshot) => ({
			...pendingProjection(request, snapshot),
			ready: context.completedJobIds.has("health-recovery"),
		});
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		complete
			.mockRejectedValueOnce(new LcmCompletionError("retry later", { provider: "test", retryAfterMs: 10_000 }))
			.mockResolvedValue("recovered summary");

		const projection = lcm.project(manager.buildSessionContext().messages);
		await context.summaryFailed.promise;
		expect((await lcm.status()).runtime.health).toBe("degraded");

		context.now += 10_000;
		await lcm.retrySummaries("all");
		await settleUntil(() => context.completedJobIds.has("health-recovery"), "successful health recovery");
		await projection;
		await flushScheduler();
		expect((await lcm.status()).runtime.health).toBe("healthy");
		await lcm.close();
	});

	it("reads terminal availability once per provider-failure settlement", async () => {
		const manager = SessionManager.inMemory("/terminal-availability-read");
		appendUser(manager, "retry job", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("terminal-availability", { transportRetryCount: 4 }));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		const failureGate = Promise.withResolvers<void>();
		complete.mockImplementation(async () => {
			await failureGate.promise;
			throw new LcmCompletionError("fifth failure", { provider: "test" });
		});

		try {
			await lcm.project(manager.buildSessionContext().messages);
			await settleUntil(() => complete.mock.calls.length === 1, "terminal provider attempt");
			const readsBeforeSettlement = context.availabilityCalls.length;
			failureGate.resolve();
			await settleUntil(
				() => context.availabilityCalls.length > readsBeforeSettlement,
				"terminal availability snapshot",
			);
			await flushScheduler();

			expect(context.availabilityCalls.length - readsBeforeSettlement).toBe(1);
		} finally {
			failureGate.resolve();
			await lcm.close();
		}
	});

	it("lets an accepted summary clear provider exhaustion without erasing diagnostics", async () => {
		const manager = SessionManager.inMemory("/provider-exhaustion-success");
		appendUser(manager, "retry job", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("exhausted", { transportRetryCount: 4 }));
		const { lcm, complete } = createHarness(manager, context);
		complete
			.mockRejectedValueOnce(new LcmCompletionError("fifth failure", { provider: "test" }))
			.mockResolvedValue("recovered summary");

		const exhaustedProjection = lcm.project(manager.buildSessionContext().messages);
		await context.summaryFailed.promise;
		const exhaustedResult = await exhaustedProjection;
		expect(lcm.commitPrimaryRequestRoute(exhaustedResult.routeKey)).toBe(true);
		expect((await lcm.status()).runtime).toMatchObject({
			health: "degraded",
			lastFailure: { category: "provider", reason: "provider_exhausted" },
		});

		context.jobs = [];
		context.failureRecords.clear();
		context.queuedJobs = 0;
		context.relevantPendingJobs = 0;
		context.queueJobs(summaryJob("recovered"));
		await lcm.retrySummaries("all");
		await settleUntil(() => context.completedJobIds.has("recovered"), "post-exhaustion summary success");
		await flushScheduler();
		expect((await lcm.status()).runtime).toMatchObject({
			health: "healthy",
			lastFailure: { category: "provider", reason: "provider_exhausted" },
		});
		await lcm.close();
	});

	for (const recovery of ["peer completion", "branch rewrite"] as const) {
		it(`restores health after ${recovery} removes current durable provider failures without erasing diagnostics`, async () => {
			const suffix = recovery === "peer completion" ? "peer" : "rewrite";
			const manager = SessionManager.inMemory(`/provider-health-${suffix}`);
			appendUser(manager, "initial request", 1);
			if (recovery === "branch rewrite") appendUser(manager, "old branch tail", 2);
			const context = new FakeLcmContext();
			const jobId = `exhausted-${suffix}`;
			context.queueJobs(summaryJob(jobId, { transportRetryCount: 4 }));
			const { lcm, complete } = createHarness(manager, context);
			complete.mockRejectedValueOnce(new LcmCompletionError("fifth failure", { provider: "test" }));

			try {
				const pending = lcm.project(manager.buildSessionContext().messages);
				await context.summaryFailed.promise;
				const result = await pending;
				expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
				const degraded = (await lcm.status()).runtime;
				expect(degraded).toMatchObject({
					health: "degraded",
					lastFailure: { category: "provider", reason: "provider_exhausted" },
				});

				context.jobs = [];
				context.failureRecords.clear();
				context.queuedJobs = 0;
				context.relevantPendingJobs = 0;
				context.projectImpl = (_request, snapshot) => readyHistoricalProjection(snapshot, `health-${suffix}`);
				if (recovery === "peer completion") {
					context.completedJobIds.add(jobId);
				} else {
					manager.branch(manager.getBranch()[0]!.id);
					appendUser(manager, "rewritten branch", 3);
					await lcm.rebind();
				}

				const recovered = (await lcm.status()).runtime;
				expect(recovered.health).toBe("healthy");
				expect(recovered.lastFailure).toEqual(degraded.lastFailure);
			} finally {
				await lcm.close();
			}
		});
	}

	it("does not treat a lifecycle-aborted retry as provider recovery", async () => {
		const manager = SessionManager.inMemory("/provider-health-abort");
		appendUser(manager, "retry job", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("health-abort"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		const retryStarted = Promise.withResolvers<void>();
		complete
			.mockRejectedValueOnce(new LcmCompletionError("retry later", { provider: "test", retryAfterMs: 10_000 }))
			.mockImplementation(
				request =>
					new Promise<string>((_resolve, reject) => {
						retryStarted.resolve();
						if (request.signal?.aborted) {
							reject(new LcmCompletionError("lifecycle abort", { provider: "test", category: "aborted" }));
						} else {
							request.signal?.addEventListener(
								"abort",
								() =>
									reject(new LcmCompletionError("lifecycle abort", { provider: "test", category: "aborted" })),
								{ once: true },
							);
						}
					}),
			);

		void lcm.project(manager.buildSessionContext().messages);
		await context.summaryFailed.promise;
		expect((await lcm.status()).runtime.health).toBe("degraded");
		context.now += 10_000;
		await lcm.retrySummaries("all");
		await retryStarted.promise;
		await lcm.rebind();

		expect((await lcm.status()).runtime.health).toBe("degraded");
		await lcm.close();
	});

	for (const [label, nonAccepted] of [
		["lease-lost", { accepted: false, reason: "lease_lost" }],
		["stale", { accepted: false, reason: "stale" }],
		["escalated", { accepted: false, reason: "escalated", stage: "aggressive" }],
		["deterministic-failed", { accepted: false, reason: "deterministic_failed" }],
	] as const) {
		it(`does not clear provider degradation for a ${label} completion`, async () => {
			const manager = SessionManager.inMemory(`/provider-health-${label}`);
			appendUser(manager, "retry job", 1);
			const context = new FakeLcmContext();
			const jobId = `health-${label}`;
			context.queueJobs(summaryJob(jobId));
			const completeSummaryJob = context.completeSummaryJob.bind(context);
			context.completeSummaryJob = (lease, completion) => {
				if (!context.leasedJobs.has(lease.jobId)) return completeSummaryJob(lease, completion);
				context.leasedJobs.delete(lease.jobId);
				context.queuedJobs = Math.max(0, context.queuedJobs - 1);
				context.relevantPendingJobs = Math.max(0, (context.relevantPendingJobs ?? 0) - 1);
				return nonAccepted;
			};
			const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
			const attemptStarted = Promise.withResolvers<void>();
			const releaseAttempt = Promise.withResolvers<void>();
			complete.mockImplementation(async () => {
				attemptStarted.resolve();
				await releaseAttempt.promise;
				return "non-accepted summary";
			});

			try {
				await lcm.project(manager.buildSessionContext().messages);
				await attemptStarted.promise;
				context.seedFailure("prior-provider-failure", context.now + 10_000, "preferred");
				expect((await lcm.status()).runtime.health).toBe("degraded");
				context.failureRecords.clear();
				releaseAttempt.resolve();
				await settleUntil(() => !context.leasedJobs.has(jobId), `${label} completion`);
				await flushScheduler();
				expect(context.completedJobIds.has(jobId)).toBe(false);
				expect((await lcm.status()).runtime.health).toBe("degraded");
			} finally {
				releaseAttempt.resolve();
				await lcm.close();
			}
		});
	}

	for (const reason of ["coverage_gap", "assembly_invalid", "fit_invariant"] as const) {
		const clearsOnReconcile = reason === "coverage_gap";
		it(`an unchanged complete reconcile ${clearsOnReconcile ? "clears" : "does not clear"} ${reason} while preserving lastFailure`, async () => {
			const manager = SessionManager.inMemory(`/unchanged-reconcile-${reason}`);
			if (reason === "assembly_invalid") {
				manager.appendMessage({ ...createAssistantMessage("first assistant"), timestamp: 1 });
				manager.appendMessage({ ...createAssistantMessage("latest assistant"), timestamp: 2 });
			} else {
				appendUser(manager, "first request", 1);
				appendUser(manager, "active request", 2);
			}
			const context = new FakeLcmContext();
			let coverageAvailable = reason !== "coverage_gap";
			context.projectImpl = (_request, snapshot) => {
				const ready = readyHistoricalProjection(snapshot, `unchanged-${reason}`);
				return coverageAvailable
					? ready
					: {
							...ready,
							ready: false,
							historical: [],
							freshTailSourceIds: [],
							uncoveredSourceIds: snapshot.entries.map(entry => entry.entryId),
							selectedLevelCounts: {},
							coveredSourceCount: 0,
							freshSourceCount: 0,
							estimatedTokens: 0,
						};
			};
			const measurements =
				reason === "fit_invariant" ? () => ({ tokens: 2, upperBound: 2 }) : () => ({ tokens: 1, upperBound: 1 });
			const { lcm } = createHarness(
				manager,
				context,
				undefined,
				undefined,
				undefined,
				20,
				undefined,
				1,
				measurements,
			);

			try {
				const result = await lcm.project(manager.buildSessionContext().messages);
				expect(result.owned).toBe(false);
				expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
				const failed = (await lcm.status()).runtime;
				expect(failed).toMatchObject({
					health: "degraded",
					lastFailure: { category: "unfit", reason },
				});

				coverageAvailable = true;
				context.reconcile = snapshot => ({
					changed: false,
					revision: 1,
					activeSources: snapshot.entries.length,
					insertedSources: 0,
					tombstonedSources: 0,
					queuedJobs: 0,
					reusedSummaries: 0,
				});
				manager.appendCustomEntry("lcm-health-probe", { reason });
				const reconciled = (await lcm.status()).runtime;
				expect(reconciled.health).toBe(clearsOnReconcile ? "healthy" : "degraded");
				expect(reconciled.lastFailure).toEqual(failed.lastFailure);
			} finally {
				await lcm.close();
			}
		});
	}

	it("commits only an exact current primary route and fences rebinds by full scope", async () => {
		const manager = SessionManager.inMemory("/primary-route-fencing");
		appendUser(manager, "covered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context);

		const first = await lcm.project(manager.buildSessionContext().messages);
		expect(first.routeKey).toBeDefined();
		expect((await lcm.status()).runtime.lastRequestRoute).toBeUndefined();
		expect(lcm.commitPrimaryRequestRoute(first.routeKey)).toBe(true);
		const committed = (await lcm.status()).runtime;
		expect(committed.lastRequestRoute).toMatchObject({ kind: "lossless", metrics: { revision: 1 } });
		expect(committed.lastTakeover).toMatchObject({ revision: 1 });

		appendUser(manager, "same-session revision", 2);
		const second = await lcm.project(manager.buildSessionContext().messages);
		expect(second.routeKey).toBeDefined();
		appendUser(manager, "mutated before dispatch", 3);
		expect(lcm.commitPrimaryRequestRoute(second.routeKey)).toBe(false);

		const third = await lcm.project(manager.buildSessionContext().messages);
		expect(third.routeKey).toBeDefined();
		manager.branch(manager.getBranch()[0]!.id);
		appendUser(manager, "same revision number on another branch", 4);
		await lcm.rebind();
		expect(lcm.commitPrimaryRequestRoute(third.routeKey)).toBe(false);
		const rebound = (await lcm.status()).runtime;
		expect(rebound.lastRequestRoute).toEqual(committed.lastRequestRoute);
		expect(rebound.lastTakeover).toEqual(committed.lastTakeover);

		await manager.newSession();
		appendUser(manager, "new session", 5);
		await lcm.rebind();
		const nextSession = (await lcm.status()).runtime;
		expect(nextSession.lastRequestRoute).toBeUndefined();
		expect(nextSession.lastTakeover).toBeUndefined();
		expect(nextSession.lastFailure).toBeUndefined();
		await lcm.close();
	});

	it("stages a first fallback against the exact reconciled scope", async () => {
		const manager = SessionManager.inMemory("/first-fallback-scope");
		appendUser(manager, "uncovered request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: 1,
			ready: false,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: [],
			uncoveredSourceIds: snapshot.entries.map(entry => entry.entryId),
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: 0,
			estimatedTokens: 0,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(manager, context);

		const result = await lcm.project(manager.buildSessionContext().messages);
		const branch = (await lcm.status()).runtime.currentBranch;
		expect(result.routeKey?.scope).toMatchObject({
			projectId: branch?.projectId,
			branchId: branch?.branchId,
			revision: branch?.revision,
		});
		expect(typeof result.routeKey?.scope?.inputAnchor).toBe("string");
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_fallback",
			category: "unfit",
			reason: "coverage_gap",
		});
		await lcm.close();
	});

	it("stages first-request irreducible and store fallbacks against the normalized input", async () => {
		const irreducibleManager = SessionManager.inMemory("/first-irreducible-scope");
		appendUser(irreducibleManager, "oversized request", 1);
		const irreducibleHarness = createHarness(irreducibleManager, undefined, undefined, undefined, () => ({
			sourceTokens: 101,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 0,
			freshTail: { maxSources: 32, maxTokens: 20_000 },
		}));
		const irreducible = await irreducibleHarness.lcm.project(irreducibleManager.buildSessionContext().messages);
		expect(typeof irreducible.routeKey?.scope?.projectId).toBe("string");
		expect(typeof irreducible.routeKey?.scope?.branchId).toBe("string");
		expect(typeof irreducible.routeKey?.scope?.inputAnchor).toBe("string");
		expect(irreducible.routeKey?.scope).not.toHaveProperty("revision");
		expect(irreducibleHarness.openContext).not.toHaveBeenCalled();
		expect(irreducibleHarness.lcm.commitPrimaryRequestRoute(irreducible.routeKey)).toBe(true);
		expect((await irreducibleHarness.lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_fallback",
			category: "unfit",
			reason: "irreducible_input",
		});
		await irreducibleHarness.lcm.close();

		const storeManager = SessionManager.inMemory("/first-store-scope");
		appendUser(storeManager, "store request", 1);
		const storeHarness = createHarness(storeManager, undefined, undefined, undefined, () => ({
			sourceTokens: 101,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		storeHarness.openContext.mockRejectedValueOnce(new Error("store unavailable"));
		const store = await storeHarness.lcm.project(storeManager.buildSessionContext().messages);
		expect(typeof store.routeKey?.scope?.projectId).toBe("string");
		expect(typeof store.routeKey?.scope?.branchId).toBe("string");
		expect(typeof store.routeKey?.scope?.inputAnchor).toBe("string");
		expect(store.routeKey?.scope).not.toHaveProperty("revision");
		expect(storeHarness.lcm.commitPrimaryRequestRoute(store.routeKey)).toBe(true);
		expect((await storeHarness.lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_fallback",
			category: "store",
		});
		await storeHarness.lcm.close();
	});

	it("rejects a route when the journal changes while project identity resolves", async () => {
		const manager = SessionManager.inMemory("/deferred-route-scope");
		appendUser(manager, "original request", 1);
		const started = Promise.withResolvers<void>();
		const resolved = Promise.withResolvers<{ projectId: string; rootPath: string; storePath: string }>();
		const { lcm, openContext } = createHarness(
			manager,
			undefined,
			undefined,
			undefined,
			() => ({
				sourceTokens: 41,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 80,
				freshTail: { maxSources: 8, maxTokens: 40 },
			}),
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			async () => {
				started.resolve();
				return resolved.promise;
			},
		);
		const pending = lcm.project(manager.buildSessionContext().messages);
		await started.promise;
		appendUser(manager, "appended during resolution", 2);
		resolved.resolve({ projectId: "deferred-project", rootPath: "/tmp", storePath: "/tmp/context.sqlite" });
		const result = await pending;
		expect(result.routeKey).toBeUndefined();
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(false);
		expect(openContext).not.toHaveBeenCalled();
		await lcm.close();
	});

	it("commits a scoped store fallback when project resolution fails", async () => {
		const manager = SessionManager.inMemory("/failed-route-scope");
		appendUser(manager, "store request", 1);
		const { lcm, openContext } = createHarness(
			manager,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			async () => {
				throw new Error("project unavailable");
			},
		);
		const result = await lcm.project(manager.buildSessionContext().messages);
		expect(typeof result.routeKey?.scope?.projectId).toBe("string");
		expect(typeof result.routeKey?.scope?.branchId).toBe("string");
		expect(typeof result.routeKey?.scope?.inputAnchor).toBe("string");
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_fallback",
			category: "store",
		});
		expect(openContext).not.toHaveBeenCalled();
		await lcm.close();
	});

	it("keeps a below-hard project resolution rejection as native passthrough", async () => {
		const manager = SessionManager.inMemory("/failed-below-hard-route-scope");
		appendUser(manager, "warming request", 1);
		const { lcm, openContext } = createHarness(
			manager,
			undefined,
			undefined,
			undefined,
			() => ({
				sourceTokens: 41,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 80,
				freshTail: { maxSources: 8, maxTokens: 40 },
			}),
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			async () => {
				throw new Error("project unavailable");
			},
		);
		const result = await lcm.project(manager.buildSessionContext().messages);
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		const runtime = (await lcm.status()).runtime;
		expect(runtime.lastRequestRoute).toMatchObject({ kind: "native_passthrough", reason: "below_hard" });
		expect(runtime.lastFailure).toBeUndefined();
		expect(openContext).not.toHaveBeenCalled();
		await lcm.close();
	});

	it("keeps a complete irreducible request ready and records its dispatched fallback", async () => {
		const manager = SessionManager.inMemory("/irreducible-route");
		appendUser(manager, "covered request", 1);

		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: 1,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		let irreducible = false;
		const { lcm } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 101,
			prewarmThresholdTokens: 40,
			hardThresholdTokens: 100,
			tokenBudget: irreducible ? 0 : 100_000,
			freshTail: { maxSources: 32, maxTokens: 20_000 },
		}));

		const owned = await lcm.project(manager.buildSessionContext().messages);
		expect(owned.routeKey).toBeDefined();
		expect(lcm.commitPrimaryRequestRoute(owned.routeKey)).toBe(true);
		irreducible = true;
		const fallback = await lcm.project(manager.buildSessionContext().messages);
		expect(fallback.routeKey).toBeDefined();
		expect(lcm.commitPrimaryRequestRoute(fallback.routeKey)).toBe(true);
		const status = (await lcm.status()).runtime;
		expect(status).toMatchObject({
			health: "healthy",
			coverageReadiness: "ready",
			lastRequestRoute: {
				kind: "native_fallback",
				category: "unfit",
				reason: "irreducible_input",
			},
			lastFailure: { category: "unfit", reason: "irreducible_input" },
		});
		expect(status.lastTakeover).toBeDefined();
		await lcm.close();
	});

	it("refuses a cue search whose expected revision no longer matches the branch", async () => {
		const manager = SessionManager.inMemory("/cue-staleness");
		appendUser(manager, "history", 1);
		const context = new FakeLcmContext();
		vi.spyOn(context, "search").mockReturnValue([
			{ kind: "summary", id: "s1", summaryHandle: "h1", redactedText: "text", rank: 1, citations: [] },
		]);
		const { lcm } = createHarness(manager, context);
		await lcm.project(manager.buildSessionContext().messages);
		const revision = (await lcm.status()).runtime.currentBranch?.revision;
		expect(revision).toBeDefined();
		expect(await lcm.searchProjected("text", 8, revision as number)).toHaveLength(1);
		// A reconcile between projection and cue lookup would describe a request that moved on.
		expect(await lcm.searchProjected("text", 8, (revision as number) + 1)).toEqual([]);
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
		expect(typeof result.routeKey?.scope?.projectId).toBe("string");
		expect(typeof result.routeKey?.scope?.branchId).toBe("string");
		expect(typeof result.routeKey?.scope?.inputAnchor).toBe("string");
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_passthrough",
			reason: "below_hard",
		});
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
		expect(context.releaseSummaryJob({ ...reclaimed, leaseToken: firstRelease.leaseToken })).toBe(false);
		expect(context.leasedJobs.get(reclaimed.jobId)?.leaseToken).toBe(reclaimed.leaseToken);
		expect(status.runtime.summaryBackoff).toBeUndefined();
		expect(status.store?.jobs.failed).toBe(0);
		expect(status.runtime.health).toBe("healthy");
		expect(status.runtime.lastFailure).toBeUndefined();
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

	it("aborts resolver-key drift before transport and retries under the rotated epoch", async () => {
		const manager = SessionManager.inMemory("/dispatch-key-drift");
		appendUser(manager, "drift", 1);
		const context = new FakeLcmContext();
		context.preResolvedModelOverride = "provider/claimed-model";
		context.resolvedModelOverride = "provider/actual-model";
		context.queueJobs(summaryJob("drift"));
		const { lcm, complete, resolveSummaryModel } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
		);
		resolveSummaryModel.mockImplementation(() =>
			context.claimCalls.length === 0 ? "provider/claimed-model" : "provider/actual-model",
		);
		try {
			await lcm.project(manager.buildSessionContext().messages);
			await settleUntil(() => context.completedJobIds.has("drift"), "rotated policy completion");
			expect(complete).toHaveBeenCalledTimes(1);
			expect(context.claimCalls).toHaveLength(2);
			expect(context.retryPolicy).toEqual({ retryKey: "provider/actual-model", retryEpoch: 2 });
			expect([...context.attemptRows.values()]).toHaveLength(1);
		} finally {
			await lcm.close();
		}
	});

	it("re-resolves the selector after provider setup before starting a durable attempt", async () => {
		const manager = SessionManager.inMemory("/api-key-model-drift");
		appendUser(manager, "drift during provider setup", 1);
		const context = new FakeLcmContext();
		context.preResolvedModelOverride = "provider/model-a";
		context.queueJobs(summaryJob("api-key-model-drift"));
		let switched = false;
		context.beforeAttemptStart = () => {
			if (switched) return;
			switched = true;
			context.preResolvedModelOverride = "provider/model-b";
		};
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);

		try {
			await lcm.project(manager.buildSessionContext().messages);
			await settleUntil(() => context.completedJobIds.has("api-key-model-drift"), "post-setup model retry");
			expect(complete).toHaveBeenCalledTimes(1);
			expect(context.claimCalls).toHaveLength(2);
			expect(context.retryPolicy).toEqual({ retryKey: "provider/model-b", retryEpoch: 2 });
			expect([...context.attemptRows.values()]).toHaveLength(1);
		} finally {
			await lcm.close();
		}
	});

	it("aborts old-epoch siblings and restarts the summary run after retry-policy rotation", async () => {
		const manager = SessionManager.inMemory("/multi-worker-policy-rotation");
		appendUser(manager, "rotate concurrent summaries", 1);
		const context = new FakeLcmContext();
		context.preResolvedModelOverride = "provider/model-a";
		context.queueJobs(summaryJob("rotate-a"), summaryJob("rotate-b"), summaryJob("rotate-c"));
		const oldAttemptsStarted = Promise.withResolvers<void>();
		let providerSetups = 0;
		context.beforeAttemptStart = async () => {
			providerSetups++;
			if (providerSetups !== 3) return;
			await oldAttemptsStarted.promise;
			context.preResolvedModelOverride = "provider/model-b";
		};
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
			20,
			undefined,
			3,
		);
		const oldEpochSignals: AbortSignal[] = [];
		let samePolicyNoopAbortedSibling = false;
		complete.mockImplementation(request => {
			if (oldEpochSignals.length >= 2) return Promise.resolve("rotated summary");
			const signal = request.signal!;
			oldEpochSignals.push(signal);
			if (oldEpochSignals.length === 2) {
				samePolicyNoopAbortedSibling = oldEpochSignals[0]!.aborted;
				oldAttemptsStarted.resolve();
			}
			return new Promise<string>((_resolve, reject) => {
				const rejectAbort = () =>
					reject(new LcmCompletionError("old retry epoch aborted", { provider: "test", category: "aborted" }));
				if (signal.aborted) rejectAbort();
				else signal.addEventListener("abort", rejectAbort, { once: true });
			});
		});

		try {
			await lcm.project(manager.buildSessionContext().messages);
			await settleUntil(() => context.completedJobIds.size === 3, "rotated summary run completion");
			expect(oldEpochSignals).toHaveLength(2);
			expect(samePolicyNoopAbortedSibling).toBe(false);
			expect(oldEpochSignals.every(signal => signal.aborted)).toBe(true);
			expect(complete).toHaveBeenCalledTimes(5);
			expect(context.failureCalls).toHaveLength(0);
			expect(context.retryPolicy).toEqual({ retryKey: "provider/model-b", retryEpoch: 2 });
		} finally {
			oldAttemptsStarted.resolve();
			await lcm.close();
		}
	});

	it("does not consume a durable retry before provider dispatch starts", async () => {
		const manager = SessionManager.inMemory("/predispatch-provider-failure");
		appendUser(manager, "provider setup fails", 1);
		const context = new FakeLcmContext();
		const jobId = "predispatch-provider-failure";
		context.queueJobs(summaryJob(jobId, { transportRetryCount: 4 }));
		context.beforeAttemptStart = () => {
			throw new LcmCompletionError("provider credential configuration unavailable", {
				provider: "test-provider",
				category: "provider_key_mismatch",
			});
		};
		const { lcm, complete } = createHarness(manager, context);

		try {
			const projection = lcm.project(manager.buildSessionContext().messages);
			await settleUntil(
				() => context.releaseCalls.some(release => release.jobId === jobId && release.accepted),
				"predispatch lease release",
			);
			const result = await projection;
			expect(complete).not.toHaveBeenCalled();
			expect(context.attemptRows.size).toBe(0);
			expect(context.failureCalls).toHaveLength(0);
			expect(context.jobs.find(job => job.jobId === jobId)?.transportRetryCount).toBe(4);
			expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
			expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
				kind: "native_fallback",
				category: "provider",
				reason: "provider_key_mismatch",
			});
		} finally {
			await lcm.close();
		}
	});

	it("adopts a persisted concrete key for @smol after reopen and owns the hard projection", async () => {
		const manager = SessionManager.inMemory("/selector-reopen");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const firstContext = new FakeLcmContext();
		firstContext.resolvedModelOverride = "provider/actual-model";
		firstContext.queueJobs(summaryJob("initial"));
		const first = createHarness(manager, firstContext, undefined, undefined, softProjectionLimits);
		try {
			await first.lcm.project(manager.buildSessionContext().messages);
			await settleUntil(() => firstContext.completedJobIds.has("initial"), "initial concrete-key completion");
			expect(firstContext.retryPolicyCalls[0]?.retryKey).toBe("provider/actual-model");
			expect(firstContext.retryPolicy).toEqual({ retryKey: "provider/actual-model", retryEpoch: 1 });
		} finally {
			await first.lcm.close();
		}

		const context = new FakeLcmContext();
		context.retryPolicy = { ...firstContext.retryPolicy! };
		context.retryPolicyToken = firstContext.retryPolicyToken;
		context.resolvedModelOverride = "provider/actual-model";
		context.queueJobs({
			...summaryJob("reopened"),
			retryKey: context.retryPolicy.retryKey,
			retryEpoch: context.retryPolicy.retryEpoch,
			leasePolicyToken: context.retryPolicyToken,
		});
		const pendingProjection = context.projectImpl;
		context.projectImpl = (request, snapshot) =>
			context.completedJobIds.has("reopened")
				? readyHistoricalProjection(snapshot, "summary_handle_reopened")
				: pendingProjection(request, snapshot);
		const reopened = createHarness(manager, context);
		reopened.lcm.configure({ summaryModel: "@smol" });
		try {
			const result = await reopened.lcm.project(manager.buildSessionContext().messages);
			expect(result.owned).toBe(true);
			expect(reopened.complete).toHaveBeenCalledTimes(1);
			expect(context.retryPolicy).toEqual({ retryKey: "provider/actual-model", retryEpoch: 1 });
			expect([...context.attemptRows.values()]).toHaveLength(1);
		} finally {
			await reopened.lcm.close();
		}
	});

	it("keeps a genuine policy mismatch degraded until an explicit model change", async () => {
		const manager = SessionManager.inMemory("/selector-policy-mismatch");
		appendUser(manager, "mismatch", 1);
		const context = new FakeLcmContext();
		context.retryPolicy = { retryKey: "provider/persisted-model", retryEpoch: 7 };
		context.retryPolicyToken = "policy-7";
		context.queueJobs({
			...summaryJob("mismatch"),
			retryKey: context.retryPolicy.retryKey,
			retryEpoch: context.retryPolicy.retryEpoch,
			leasePolicyToken: context.retryPolicyToken,
		});
		const { lcm, complete } = createHarness(manager, context);
		lcm.configure({ summaryModel: "provider/configured-model" });
		try {
			const result = await lcm.project(manager.buildSessionContext().messages);
			expect(result.owned).toBe(false);
			expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
			const status = (await lcm.status()).runtime;
			expect(status).toMatchObject({
				health: "degraded",
				lastFailure: { category: "provider", reason: "provider_key_mismatch" },
			});
			expect(status.summaryBackoff).toBeUndefined();
			expect(context.retryPolicy).toEqual({ retryKey: "provider/persisted-model", retryEpoch: 7 });
			expect(complete).not.toHaveBeenCalled();
			lcm.configure({ summaryModel: "provider/recovered-model" });
			expect((await lcm.status()).runtime.health).toBe("healthy");
		} finally {
			await lcm.close();
		}
	});

	it("clears a concurrent policy mismatch when the accepted worker outlives its coordinator", async () => {
		const manager = SessionManager.inMemory("/selector-policy-mismatch-success");
		appendUser(manager, "mismatch", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("mismatch-recovery"));
		const attemptStarted = Promise.withResolvers<void>();
		const releaseAttempt = Promise.withResolvers<void>();
		const { lcm, complete, resolveSummaryModel } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			softProjectionLimits,
		);
		complete.mockImplementation(async () => {
			attemptStarted.resolve();
			await releaseAttempt.promise;
			return "recovered summary";
		});

		try {
			await lcm.project(manager.buildSessionContext().messages);
			await attemptStarted.promise;
			const resolverCalls = resolveSummaryModel.mock.calls.length;
			resolveSummaryModel.mockImplementation(() => {
				throw new Error("resolver unavailable");
			});
			lcm.configure({ summaryModel: "@smol", maxConcurrentSummaries: 2 });
			await lcm.status();
			await flushScheduler();
			expect(resolveSummaryModel.mock.calls.length).toBeGreaterThan(resolverCalls);
			expect((await lcm.status()).runtime.health).toBe("degraded");

			resolveSummaryModel.mockReturnValue("test-provider/test-model");
			releaseAttempt.resolve();
			await settleUntil(() => context.completedJobIds.has("mismatch-recovery"), "policy-mismatch recovery");
			await flushScheduler();
			const runtime = (await lcm.status()).runtime;
			expect(runtime.summaryWorkers.active).toBe(0);
			expect(runtime.health).toBe("healthy");
		} finally {
			releaseAttempt.resolve();
			await lcm.close();
		}
	});

	it("retry all refreshes preferred and fallback cooldowns without advancing the clock", async () => {
		const manager = SessionManager.inMemory("/manual-retry-all-cooldowns");
		appendUser(manager, "retry backlog", 1);
		const context = new FakeLcmContext();
		context.queueJobs(
			summaryJob("preferred-retry-all"),
			summaryJob("fallback-retry-all", { queueClass: "fallback" }),
		);
		context.seedFailure("preferred-retry-all", context.now + 50_000, "preferred");
		context.seedFailure("fallback-retry-all", context.now + 60_000, "fallback");
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, softProjectionLimits);
		try {
			const hydrated = await lcm.status();
			expect(hydrated.runtime.summaryBackoff).toEqual({
				preferred: context.now + 50_000,
				fallback: context.now + 60_000,
			});
			const unchangedNow = context.now;
			const availability = await lcm.retrySummaries("all");
			expect(context.now).toBe(unchangedNow);
			expect(availability?.runnable).toBe(1);
			expect((await lcm.status()).runtime.summaryBackoff).toBeUndefined();
			await settleUntil(() => context.completedJobIds.size === 2, "manual retry-all completions");
			expect(complete).toHaveBeenCalledTimes(2);
			expect([...context.completedJobIds]).toEqual(["preferred-retry-all", "fallback-retry-all"]);
		} finally {
			await lcm.close();
		}
	});

	it("hydrates durable preferred and fallback failures and prunes peer-completed records", async () => {
		const manager = SessionManager.inMemory("/durable-failure-hydration");
		appendUser(manager, "durable failures", 1);
		const context = new FakeLcmContext();
		context.seedFailure("preferred-failure", context.now + 10_000, "preferred");
		context.seedFailure("fallback-failure", context.now + 20_000, "fallback");
		const { lcm } = createHarness(manager, context);

		const hydrated = await lcm.status();
		expect(hydrated.runtime.health).toBe("degraded");
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
		expect(status.runtime.health).toBe("degraded");
		expect(status.runtime.summaryWorkers).toEqual({ active: 1, limit: 2 });
		expect(status.store?.jobs).toMatchObject({ leased: 1, failed: 1, completed: 1 });
		heldGate.resolve();
		await settleUntil(() => context.completedJobIds.has("held-fallback"), "held fallback completion");
		await lcm.close();
	});

	for (const failureFirst of [true, false] as const) {
		it(`keeps a provider failure visible when its sibling settles ${failureFirst ? "after" : "before"} it`, async () => {
			const manager = SessionManager.inMemory(`/sibling-failure-${failureFirst ? "first" : "last"}`);
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
			const failureGate = Promise.withResolvers<void>();
			const successGate = Promise.withResolvers<void>();
			const failureStarted = Promise.withResolvers<void>();
			const successStarted = Promise.withResolvers<void>();
			complete.mockImplementation(async request => {
				const failing = request.prompt.includes("source-failing-sibling");
				(failing ? failureStarted : successStarted).resolve();
				await (failing ? failureGate : successGate).promise;
				if (failing) throw new LcmCompletionError("bounded provider failure", { provider: "test" });
				return "successful sibling";
			});

			await lcm.project(manager.buildSessionContext().messages);
			await Promise.all([failureStarted.promise, successStarted.promise]);
			(failureFirst ? failureGate : successGate).resolve();
			await settleUntil(
				() =>
					failureFirst
						? context.failureRecords.has("failing-sibling")
						: context.completedJobIds.has("successful-sibling"),
				"first sibling settlement",
			);
			(failureFirst ? successGate : failureGate).resolve();
			await settleUntil(
				() => context.failureRecords.has("failing-sibling") && context.completedJobIds.has("successful-sibling"),
				"both sibling settlements",
			);
			await flushScheduler();
			const status = await lcm.status();
			expect(context.completedJobIds.has("failing-sibling")).toBe(false);
			expect(context.failureRecords.get("failing-sibling")).toEqual({
				jobId: "failing-sibling",
				availableAt: context.now + 2_000,
				queueClass: "preferred",
			});
			expect(status.runtime.summaryBackoff).toEqual({ preferred: context.now + 2_000 });
			expect(status.runtime.health).toBe("degraded");
			await lcm.close();
		});
	}

	for (const terminalFirst of [true, false] as const) {
		it(`keeps required provider exhaustion degraded when the accepted sibling settles ${terminalFirst ? "after" : "before"} it`, async () => {
			const manager = SessionManager.inMemory(`/exhausted-sibling-${terminalFirst ? "first" : "last"}`);
			appendUser(manager, "exhausted sibling jobs", 1);
			const context = new FakeLcmContext();
			context.queueJobs(
				summaryJob("exhausted-sibling", { transportRetryCount: 4 }),
				summaryJob("accepted-sibling"),
				summaryJob("held-sibling"),
			);
			const { lcm, complete } = createHarness(
				manager,
				context,
				undefined,
				undefined,
				softProjectionLimits,
				20,
				undefined,
				3,
			);
			const failureGate = Promise.withResolvers<void>();
			const acceptedGate = Promise.withResolvers<void>();
			const heldGate = Promise.withResolvers<void>();
			const failureStarted = Promise.withResolvers<void>();
			const acceptedStarted = Promise.withResolvers<void>();
			const heldStarted = Promise.withResolvers<void>();
			complete.mockImplementation(async request => {
				const failing = request.prompt.includes("source-exhausted-sibling");
				const accepted = request.prompt.includes("source-accepted-sibling");
				(failing ? failureStarted : accepted ? acceptedStarted : heldStarted).resolve();
				await (failing ? failureGate : accepted ? acceptedGate : heldGate).promise;
				if (failing) throw new LcmCompletionError("fifth provider failure", { provider: "test" });
				return accepted ? "accepted sibling" : "held sibling";
			});

			try {
				await lcm.project(manager.buildSessionContext().messages);
				await Promise.all([failureStarted.promise, acceptedStarted.promise, heldStarted.promise]);
				(terminalFirst ? failureGate : acceptedGate).resolve();
				await settleUntil(
					() =>
						terminalFirst
							? context.failureRecords.has("exhausted-sibling")
							: context.completedJobIds.has("accepted-sibling"),
					"first exhausted-sibling settlement",
				);
				await waitForSummaryWorkerCount(lcm, 2);
				if (terminalFirst) expect((await lcm.status()).runtime.health).toBe("degraded");
				(terminalFirst ? acceptedGate : failureGate).resolve();
				await settleUntil(
					() =>
						context.completedJobIds.has("accepted-sibling") &&
						context.jobs.some(job => job.jobId === "exhausted-sibling" && job.transportRetryCount === 5),
					"mixed exhaustion settlement",
				);
				await waitForSummaryWorkerCount(lcm, 1);

				expect((await lcm.status()).runtime.health).toBe("degraded");
			} finally {
				failureGate.resolve();
				acceptedGate.resolve();
				heldGate.resolve();
				await lcm.close();
			}
		});
	}

	for (const terminalFirst of [true, false] as const) {
		it(`keeps a required policy mismatch degraded when the accepted sibling settles ${terminalFirst ? "after" : "before"} it`, async () => {
			const manager = SessionManager.inMemory(`/mismatched-sibling-${terminalFirst ? "first" : "last"}`);
			appendUser(manager, "mismatched sibling jobs", 1);
			const context = new FakeLcmContext();
			context.queueJobs(summaryJob("accepted-policy-sibling"), summaryJob("held-policy-sibling"), {
				...summaryJob("mismatched-policy-sibling"),
				retryEpoch: terminalFirst ? 2 : 1,
			});
			const completeSummaryJob = context.completeSummaryJob.bind(context);
			if (!terminalFirst) {
				context.completeSummaryJob = (lease, completion) => {
					const result = completeSummaryJob(lease, completion);
					if (result.accepted && lease.jobId === "accepted-policy-sibling") {
						context.jobs = context.jobs.map(job =>
							job.jobId === "mismatched-policy-sibling" ? { ...job, retryEpoch: 2 } : job,
						);
					}
					return result;
				};
			}
			const { lcm, complete } = createHarness(
				manager,
				context,
				undefined,
				undefined,
				softProjectionLimits,
				20,
				undefined,
				terminalFirst ? 3 : 2,
			);
			const acceptedGate = Promise.withResolvers<void>();
			const heldGate = Promise.withResolvers<void>();
			const acceptedStarted = Promise.withResolvers<void>();
			const heldStarted = Promise.withResolvers<void>();
			complete.mockImplementation(async request => {
				const accepted = request.prompt.includes("source-accepted-policy-sibling");
				const held = request.prompt.includes("source-held-policy-sibling");
				if (!accepted && !held) throw new Error("mismatched policy sibling must not dispatch");
				(accepted ? acceptedStarted : heldStarted).resolve();
				if (accepted && terminalFirst) await acceptedGate.promise;
				if (held) await heldGate.promise;
				return accepted ? "accepted policy sibling" : "held policy sibling";
			});

			try {
				await lcm.project(manager.buildSessionContext().messages);
				await Promise.all([acceptedStarted.promise, heldStarted.promise]);
				if (terminalFirst) {
					await settleUntil(() => context.availabilityCalls.length > 0, "policy mismatch before accepted sibling");
					await flushScheduler();
					expect((await lcm.status()).runtime.health).toBe("degraded");
					acceptedGate.resolve();
				}
				await settleUntil(
					() => context.completedJobIds.has("accepted-policy-sibling"),
					"accepted policy sibling settlement",
				);
				await waitForSummaryWorkerCount(lcm, 1);

				expect(context.completedJobIds.has("mismatched-policy-sibling")).toBe(false);
				expect((await lcm.status()).runtime.health).toBe("degraded");
			} finally {
				acceptedGate.resolve();
				heldGate.resolve();
				await lcm.close();
			}
		});
	}

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
			context.completeSummaryJob = (lease, completion) => {
				if (lease.jobId !== "preferred-unfit") return completeSummaryJob(lease, completion);
				const leased = context.leasedJobs.get(lease.jobId);
				if (leased?.leaseToken !== lease.leaseToken) return { accepted: false, reason: "lease_lost" };
				context.leasedJobs.delete(lease.jobId);
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
				expect(afterFirst.runtime.health).toBe("degraded");
				expect(afterFirst.runtime.lastFailure).toBeUndefined();
			}
			expect(afterBoth.runtime.health).toBe("degraded");
			expect(afterBoth.runtime.lastFailure).toBeUndefined();

			context.projectImpl = (_request, snapshot) => ({
				revision: 2,
				ready: true,
				historical: [],
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
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
			expect(active.runtime).toMatchObject({ health: "healthy", coverageReadiness: "ready" });
			expect(active.runtime.lastFailure).toBeUndefined();
			await lcm.close();
		});
	}

	it("uses the larger retry hint or capped exponential transport delay", async () => {
		const cases = [
			{ retry: 0, hint: 45_000, expected: 45_000 },
			{ retry: 3, hint: undefined, expected: 16_000 },
			{ retry: 4, hint: undefined, expected: 32_000 },
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
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
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
		await lcm.close();
	});

	it("fails open without retrying non-contention errors and preserves diagnostic status on rebind", async () => {
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
		const failedStatus = (await lcm.status()).runtime;
		expect(failedStatus.health).toBe("degraded");
		expect(failedStatus.lastFailure).toBeUndefined();
		await lcm.rebind();
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

		expect(status.runtime.health).toBe("quarantined");
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
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
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
		expect((await lcm.status()).runtime).toMatchObject({ health: "healthy", coverageReadiness: "ready" });
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
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
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
		expect(status.runtime.health).toBe("healthy");
		expect(status.runtime.lastFailure).toBeUndefined();
		await lcm.close();
	});

	it("aborts a maintenance decision superseded by a newer primary projection", async () => {
		const manager = SessionManager.inMemory("/maintenance-projection-attempt-freshness");
		appendUser(manager, "overlapping request", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("superseded-maintenance"));
		context.projectImpl = (_request, snapshot) => ({
			revision: context.snapshots.length,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
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
		const input = manager.buildSessionContext().messages;

		const staleMaintenance = lcm.ownsRequest(input, undefined, 150);
		await completionStarted.promise;
		expect((await lcm.project(input)).owned).toBe(false);
		completionGate.resolve();

		expect(await staleMaintenance).toEqual({ kind: "aborted" });
		expect((await lcm.project(input)).owned).toBe(false);
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
			const historical = snapshot.entries.slice(0, -1);
			const old = historical.at(-1)!;
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
						sourceIds: historical.map(entry => entry.entryId),
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
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: 90,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: historical.length,
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
		expect(rebound.runtime).toMatchObject({ health: "healthy", coverageReadiness: "idle" });
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
			const historical = snapshot.entries.slice(0, -1);
			const old = historical.at(-1)!;
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
						sourceIds: historical.map(entry => entry.entryId),
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
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: 90,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: historical.length,
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

	it("re-projects on poll ticks until a peer commit lands during the foreground wait", async () => {
		const manager = SessionManager.inMemory("/peer-progress-poll");
		const first = appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		// A peer holds the only job with a far-future lease expiry. The local worker pool
		// emits a couple of settle signals and then goes quiet, so reaching the eighth read
		// is only possible by repeated poll ticks re-reading the store.
		context.queueJobs(summaryJob("peer-held"));
		context.deferredClaims = Number.MAX_SAFE_INTEGER;
		context.nextDelayMs = 600_000;
		const readsBeforePeerCommit = 8;
		let reads = 0;
		const notReady = context.projectImpl;
		context.projectImpl = (request, snapshot) => {
			// The peer commits partway through, emitting no local signal of any kind.
			if (++reads < readsBeforePeerCommit) return notReady(request, snapshot);
			const historical = snapshot.entries.slice(0, -1);
			const old = historical.at(-1)!;
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
						sourceIds: historical.map(entry => entry.entryId),
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
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: 90,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: historical.length,
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

	it("refreshes a resolved retry policy and reaches ownership across summary waves", async () => {
		const manager = SessionManager.inMemory("/runnable-summary-waves");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.resolvedModelOverride = "provider/actual-model";
		let stalePolicyRead = false;
		const summaryJobAvailability = context.summaryJobAvailability.bind(context);
		context.summaryJobAvailability = (request, policy, limit) => {
			if (
				context.retryPolicy?.retryKey === "provider/actual-model" &&
				(policy.retryKey !== context.retryPolicy.retryKey || policy.retryEpoch !== context.retryPolicy.retryEpoch)
			) {
				stalePolicyRead = true;
			}
			return summaryJobAvailability(request, policy, limit);
		};
		context.queueJobs(summaryJob("wave-1"));
		const pendingProjection = context.projectImpl;
		context.projectImpl = (request, snapshot) =>
			context.completedJobIds.has("wave-2")
				? readyHistoricalProjection(snapshot, "summary_handle_wave_2")
				: pendingProjection(request, snapshot);
		const completeSummaryJob = context.completeSummaryJob.bind(context);
		context.completeSummaryJob = (lease, completion) => {
			const result = completeSummaryJob(lease, completion);
			if (result.accepted && lease.jobId === "wave-1") {
				context.queueJobs({
					...summaryJob("wave-2"),
					retryKey: context.retryPolicy!.retryKey,
					retryEpoch: context.retryPolicy!.retryEpoch,
				});
			}
			return result;
		};
		const { lcm, complete } = createHarness(manager, context);

		try {
			const result = await lcm.project(manager.buildSessionContext().messages);
			expect(stalePolicyRead).toBe(false);
			expect(result.owned).toBe(true);
			expect(complete).toHaveBeenCalledTimes(2);
			expect([...context.completedJobIds]).toEqual(["wave-1", "wave-2"]);
		} finally {
			await lcm.close();
		}
	});

	it("keeps a hard request pending while mixed availability still has runnable work", async () => {
		const manager = SessionManager.inMemory("/mixed-runnable-summary-waves");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("mixed-wave-1"));
		const pendingProjection = context.projectImpl;
		context.projectImpl = (request, snapshot) =>
			context.completedJobIds.has("mixed-wave-2")
				? readyHistoricalProjection(snapshot, "summary_handle_mixed_wave_2")
				: pendingProjection(request, snapshot);
		const completeSummaryJob = context.completeSummaryJob.bind(context);
		context.completeSummaryJob = (lease, completion) => {
			const result = completeSummaryJob(lease, completion);
			if (result.accepted && lease.jobId === "mixed-wave-1") {
				context.queueJobs(summaryJob("mixed-wave-2"));
				context.relevantPendingJobs = 2;
				context.deferredClaims = 1;
			}
			return result;
		};
		let mixedAvailability: SummaryJobAvailability | undefined;
		const summaryJobAvailability = context.summaryJobAvailability.bind(context);
		context.summaryJobAvailability = (request, policy, limit) => {
			const availability = summaryJobAvailability(request, policy, limit);
			if (availability.runnable > 0 && availability.missing > 0) mixedAvailability = { ...availability };
			return availability;
		};
		const { lcm, complete } = createHarness(manager, context);

		try {
			const result = await lcm.project(manager.buildSessionContext().messages);
			expect(mixedAvailability).toMatchObject({ runnable: 1, missing: 1 });
			expect(result.owned).toBe(true);
			expect(complete).toHaveBeenCalledTimes(2);
			expect([...context.completedJobIds]).toEqual(["mixed-wave-1", "mixed-wave-2"]);
		} finally {
			await lcm.close();
		}
	});

	it("waits without a wall deadline and returns promptly only when the caller aborts", async () => {
		vi.useFakeTimers();
		const manager = SessionManager.inMemory("/cancellation-only-foreground-wait");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("peer-held"));
		context.deferredClaims = Number.MAX_SAFE_INTEGER;
		context.nextDelayMs = 600_000;
		const controller = new AbortController();
		const { lcm } = createHarness(manager, context, undefined, undefined, undefined, 20, undefined, 1, undefined, 5);
		let settled = false;
		try {
			const projection = lcm.project(manager.buildSessionContext().messages, controller.signal).then(result => {
				settled = true;
				return result;
			});
			await settleUntil(() => context.projectionCalls > 0, "foreground projection began");
			context.now += 60_001;
			vi.advanceTimersByTime(60_001);
			await flushScheduler();
			await flushScheduler();
			await flushScheduler();
			expect(settled).toBe(false);
			vi.useRealTimers();
			controller.abort("caller cancelled");
			await flushScheduler();
			expect((await projection).owned).toBe(false);
			expect(settled).toBe(true);
		} finally {
			vi.useRealTimers();
			await lcm.close();
		}
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

	it("keeps a 48-hour Retry-After pending until caller cancellation without provider exhaustion", async () => {
		const manager = SessionManager.inMemory("/retryable-backoff-cancel");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("retryable"));
		const retryAfterMs = 48 * 60 * 60_000;
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, undefined, 5);
		complete.mockRejectedValue(new LcmCompletionError("retry later", { provider: "test", retryAfterMs }));
		const controller = new AbortController();
		let settled = false;
		try {
			const pending = lcm.project(manager.buildSessionContext().messages, controller.signal).then(result => {
				settled = true;
				return result;
			});
			await context.summaryFailed.promise;
			await flushScheduler();
			expect(settled).toBe(false);
			expect(context.failureCalls).toHaveLength(1);
			expect(context.failureRecords.get("retryable")?.availableAt).toBe(context.now + retryAfterMs);
			controller.abort("caller cancelled");
			expect((await pending).owned).toBe(false);
			const runtime = (await lcm.status()).runtime;
			expect(runtime.lastRequestRoute).toBeUndefined();
			expect(runtime.lastFailure).toBeUndefined();
		} finally {
			await lcm.close();
		}
	});

	it("cancels provider preparation without recording a transport failure", async () => {
		const manager = SessionManager.inMemory("/provider-preparation-cancel");
		appendUser(manager, "cancel", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("preparation-cancel"));
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			5,
			undefined,
			1,
			undefined,
			5,
			60_000,
		);
		const preparationStarted = Promise.withResolvers<void>();
		const unblockPreparation = Promise.withResolvers<void>();
		context.beforeAttemptStart = async () => {
			preparationStarted.resolve();
			await unblockPreparation.promise;
		};
		const controller = new AbortController();
		try {
			const pending = lcm.project(manager.buildSessionContext().messages, controller.signal);
			await preparationStarted.promise;
			controller.abort("caller cancelled");
			expect((await pending).owned).toBe(false);
			expect(context.failureCalls).toHaveLength(0);
			expect(context.attemptRows.size).toBe(0);
			expect(context.leasedJobs.get("preparation-cancel")?.transportRetryCount).toBe(0);
			unblockPreparation.resolve();
			await flushScheduler();
		} finally {
			await lcm.close();
		}
	});

	it("backs off complete timeouts and exhausts on the fifth without an attempt row", async () => {
		vi.useFakeTimers();
		const manager = SessionManager.inMemory("/provider-preparation-timeout");
		appendUser(manager, "timeout", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("preparation-timeout"));
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			5,
			undefined,
			1,
			undefined,
			5,
			25,
		);
		const preparationStarted = Array.from({ length: 5 }, () => Promise.withResolvers<void>());
		const releasePreparation = Array.from({ length: 5 }, () => Promise.withResolvers<void>());
		let preparationCalls = 0;
		context.beforeAttemptStart = async () => {
			const index = preparationCalls++;
			preparationStarted[index]!.resolve();
			await releasePreparation[index]!.promise;
		};
		try {
			const pending = lcm.project(manager.buildSessionContext().messages);
			for (let attempt = 0; attempt < 5; attempt++) {
				await preparationStarted[attempt]!.promise;
				context.now += 25;
				vi.advanceTimersByTime(25);
				await settleUntil(
					() => context.failureCalls.length === attempt + 1,
					`preparation timeout ${attempt + 1} settled`,
				);
				expect(context.failedError).toBe("Summary provider preparation timed out");
				expect(context.attemptRows.size).toBe(0);
				expect(context.jobs.find(job => job.jobId === "preparation-timeout")?.transportRetryCount).toBe(
					attempt + 1,
				);
				expect(complete).not.toHaveBeenCalled();
				if (attempt === 0) {
					const retryAt = context.failureRecords.get("preparation-timeout")?.availableAt;
					expect(retryAt).toBe(context.now + 2_000);
					const backedOff = (await lcm.status()).runtime;
					expect(backedOff.summaryBackoff).toEqual({ preferred: retryAt });
					expect(backedOff.lastFailure).toBeUndefined();
				}
				if (attempt === 4) break;
				await flushScheduler();
				const retryAt = context.failureRecords.get("preparation-timeout")!.availableAt;
				const delay = retryAt - context.now;
				context.now = retryAt;
				vi.advanceTimersByTime(delay);
			}

			const terminal = await pending;
			expect(terminal.owned).toBe(false);
			expect(lcm.commitPrimaryRequestRoute(terminal.routeKey)).toBe(true);
			const exhausted = (await lcm.status()).runtime;
			expect(exhausted.lastFailure).toMatchObject({ category: "provider", reason: "provider_exhausted" });
			expect(exhausted.summaryBackoff).toBeUndefined();
			expect(context.failureCalls).toHaveLength(5);
			expect(context.attemptRows.size).toBe(0);
		} finally {
			for (const gate of releasePreparation) gate.resolve();
			await flushScheduler();
			vi.useRealTimers();
			await lcm.close();
		}
	});

	it("keeps one absolute provider deadline and enriches late usage", async () => {
		vi.useFakeTimers();
		const manager = SessionManager.inMemory("/provider-attempt-timeout");
		appendUser(manager, "timeout", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("timeout"));
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			5,
			undefined,
			1,
			undefined,
			5,
			25,
		);
		const lateCompletion = Promise.withResolvers<string>();
		complete.mockImplementation(() => lateCompletion.promise);
		const attemptMayStart = Promise.withResolvers<void>();
		const attemptStartPending = Promise.withResolvers<void>();
		context.beforeAttemptStart = async request => {
			expect(request.providerSessionKey).toBe("timeout:1");
			attemptStartPending.resolve();
			await attemptMayStart.promise;
		};
		const controller = new AbortController();
		try {
			const pending = lcm.project(manager.buildSessionContext().messages, controller.signal);
			await attemptStartPending.promise;
			vi.advanceTimersByTime(24);
			await flushScheduler();
			expect(complete).not.toHaveBeenCalled();
			expect(context.failureCalls).toHaveLength(0);
			expect(context.attemptRows.size).toBe(0);
			attemptMayStart.resolve();
			await settleUntil(() => complete.mock.calls.length === 1, "provider attempt started");
			vi.advanceTimersByTime(1);
			await settleUntil(() => context.failureCalls.length === 1, "timed-out attempt settled");
			expect(context.failedError).toBe("Summary provider attempt timed out");
			expect(context.failureCalls[0]?.retryDelayMs).toBe(2_000);
			expect(context.attemptRows.get("attempt-1")?.outcome).toBe("transport_error");
			expect(context.attemptRows.get("attempt-1")?.usage).toBeUndefined();
			expect(context.retryPolicy).toEqual({ retryKey: "test-provider/test-model", retryEpoch: 1 });
			const projectId = context.snapshots.at(-1)!.scope.projectId;
			expect(
				context.configureSummaryRetryPolicy(projectId, "test-provider/replacement-model", {
					expected: { retryKey: "test-provider/test-model", retryEpoch: 1 },
				}),
			).toEqual({ kind: "ready", retryKey: "test-provider/replacement-model", retryEpoch: 2 });
			expect(context.leasedJobs.has("timeout")).toBe(false);
			lateCompletion.resolve("late summary");
			await settleUntil(() => context.attemptRows.get("attempt-1")?.usage !== undefined, "late usage enrichment");
			expect(context.attemptRows.get("attempt-1")?.outcome).toBe("transport_error");
			expect(context.attemptRows.get("attempt-1")?.usage?.totalTokens).toBe(184);
			expect(context.attemptRows.get("attempt-1")?.usage?.cost.total).toBe(0.0019);
			controller.abort("caller cancelled");
			await pending;
		} finally {
			vi.useRealTimers();
			await lcm.close();
		}
	});

	it("settles and later enriches an accepted attempt abandoned by lifecycle abort", async () => {
		const manager = SessionManager.inMemory("/provider-attempt-caller-abort");
		appendUser(manager, "abort after dispatch", 1);
		const context = new FakeLcmContext();
		context.queueJobs(summaryJob("caller-abort"));
		const { lcm, complete } = createHarness(manager, context);
		const dispatched = Promise.withResolvers<void>();
		const lateCompletion = Promise.withResolvers<string>();
		complete.mockImplementation(async () => {
			dispatched.resolve();
			return lateCompletion.promise;
		});
		const controller = new AbortController();
		try {
			const pending = lcm.project(manager.buildSessionContext().messages, controller.signal);
			await dispatched.promise;
			expect(context.attemptRows.get("attempt-1")?.outcome).toBe("in_flight");
			lcm.beginDispose();
			controller.abort("caller cancelled");
			expect((await pending).owned).toBe(false);
			await settleUntil(
				() => context.attemptRows.get("attempt-1")?.outcome !== "in_flight",
				"abort attempt settled",
			);
			expect(context.attemptRows.get("attempt-1")?.outcome).toBe("stale");
			expect(context.attemptRows.get("attempt-1")?.usage).toBeUndefined();
			expect(context.completedJobIds.has("caller-abort")).toBe(false);
			expect(context.lastCompletion).toBeUndefined();

			lateCompletion.resolve("late summary must not publish");
			await settleUntil(
				() => context.attemptRows.get("attempt-1")?.usage !== undefined,
				"late abort usage enrichment",
			);
			expect(context.attemptRows.get("attempt-1")?.outcome).toBe("stale");
			expect(context.completedJobIds.has("caller-abort")).toBe(false);
			expect(context.lastCompletion).toBeUndefined();
			expect(context.failureCalls).toHaveLength(0);
			expect(context.leasedJobs.has("caller-abort")).toBe(false);
		} finally {
			lateCompletion.resolve("late summary must not publish");
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
			const historical = snapshot.entries.slice(0, -1);
			const old = historical.at(-1)!;
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
						sourceIds: historical.map(entry => entry.entryId),
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
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: historical.length,
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
			coveredSourceCount: 2,
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

	it("bounds projected file handles without marking untruncated summary text as an excerpt", async () => {
		const manager = SessionManager.inMemory("/projected-files");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("settled"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		const measure = (messages: readonly AgentMessage[]) => {
			const historical = messages.find(message => message.role === "historicalContext");
			const citedContent = historical?.redactedCitedContent ?? "";
			const tokens = citedContent.includes("data-2.csv") || citedContent.includes("[Summary excerpt;") ? 101 : 100;
			const upperBound = messages.reduce(
				(total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message),
				0,
			);
			return { tokens, upperBound: Math.max(tokens, upperBound) };
		};
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 101,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 100,
				freshTail: { maxSources: 32, maxTokens: 20_000 },
			}),
			20,
			undefined,
			1,
			measure,
		);
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
			const historical = snapshot.entries.slice(0, -1);
			const old = historical.at(-1)!;
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
						sourceIds: historical.map(entry => entry.entryId),
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
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: { 0: 1 },
				coveredSourceCount: historical.length,
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

		expect(providerText).toContain("older facts about the datasets");
		expect(providerText).not.toContain("[Summary excerpt;");
		expect(providerText).toContain("[Files: ");
		expect(providerText).toContain("(+3 more)");
		expect(providerText).toContain("data-0.csv");
		expect(providerText).toContain("data-1.csv");
		expect(providerText).not.toContain("data-2.csv");

		const fileHandles = (providerText.match(/lcm-handle:v1:[A-Za-z0-9_-]+/g) ?? [])
			.map(token => decodeLcmHandle(token))
			.filter(handle => handle.kind === "file");
		expect(fileHandles).toEqual([
			{ kind: "file", reference: { ...scope, fileId: "file_0" } },
			{ kind: "file", reference: { ...scope, fileId: "file_1" } },
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

	it("owns a fresh-tail-only projection only when its real rendered input fits", async () => {
		const manager = SessionManager.inMemory("/fresh-tail-only-fit");
		appendUser(manager, "fresh request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: 1,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const measurements = vi.fn(() => ({ tokens: 90, upperBound: 90 }));
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 101,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 100,
				freshTail: { maxSources: 8, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			measurements,
		);
		const input = manager.buildSessionContext().messages;

		const projectionCallsBefore = context.projectionCalls;
		const result = await lcm.project(input);

		expect(result.owned).toBe(true);
		expect(result.messages).toBe(input);
		expect(measurements).toHaveBeenCalledTimes(1);
		expect(context.projectionCalls - projectionCallsBefore).toBe(2);
		await lcm.close();
	});

	it("rerenders and remeasures the same input without retaining a fitted payload", async () => {
		const manager = SessionManager.inMemory("/fresh-primary-render");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("older work"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		let summaryText = "first rendered metadata";
		context.projectImpl = (_request, snapshot) => {
			const projection = readyHistoricalProjection(snapshot, "fresh-render-handle");
			return {
				...projection,
				historical: [{ ...projection.historical[0]!, redactedText: summaryText }],
			};
		};
		let measuredTokens = 50;
		const measurements = vi.fn((messages: AgentMessage[]) => ({
			tokens: measuredTokens,
			upperBound: Math.max(
				measuredTokens,
				messages.reduce((total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message), 0),
			),
		}));
		const { lcm, complete } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 101,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 100,
				freshTail: { maxSources: 8, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			measurements,
		);
		const input = manager.buildSessionContext().messages;

		expect(await lcm.ownsRequest(input, undefined, 150)).toMatchObject({ kind: "owned" });
		context.reconcile = () => ({
			changed: false,
			revision: 1,
			activeSources: 3,
			insertedSources: 0,
			tombstonedSources: 0,
			queuedJobs: 0,
			reusedSummaries: 0,
		});
		measurements.mockClear();

		const first = await lcm.project(input);
		expect(first.projectionTokenMeasurements).toBe(1);
		expect(first.candidateTokens).toBe(50);
		expect(first.messages.find(message => message.role === "historicalContext")?.redactedCitedContent).toContain(
			"first rendered metadata",
		);

		measuredTokens = 60;
		const repeated = await lcm.project(input);
		expect(repeated.messages).not.toBe(first.messages);
		expect(repeated.projectionTokenMeasurements).toBe(1);
		expect(repeated.candidateTokens).toBe(60);

		summaryText = "updated rendered metadata";
		measuredTokens = 70;
		const updated = await lcm.project(input);
		const updatedText = updated.messages.find(message => message.role === "historicalContext")?.redactedCitedContent;
		expect(updatedText).toContain("updated rendered metadata");
		expect(updatedText).not.toContain("first rendered metadata");
		expect(updated.projectionTokenMeasurements).toBe(1);
		expect(updated.candidateTokens).toBe(70);
		expect(measurements).toHaveBeenCalledTimes(3);
		expect(complete).not.toHaveBeenCalled();
		await lcm.close();
	});

	it("classifies mandatory fresh input above the real budget as irreducible", async () => {
		const manager = SessionManager.inMemory("/fresh-tail-only-unfit");
		appendUser(manager, "fresh request", 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => ({
			revision: 1,
			ready: true,
			historical: [],
			activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
			freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
			uncoveredSourceIds: [],
			sourceTokens: snapshot.entries.length,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: snapshot.entries.length,
			estimatedTokens: snapshot.entries.length,
			pendingJobs: 0,
		});
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 101,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 100,
				freshTail: { maxSources: 8, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			() => ({ tokens: 101, upperBound: 101 }),
		);
		const input = manager.buildSessionContext().messages;
		const result = await lcm.project(input);

		expect(result.owned).toBe(false);
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_fallback",
			category: "unfit",
			reason: "irreducible_input",
		});
		await lcm.close();
	});

	it("fits fair bounded head-tail excerpts with bounded unique measurements", async () => {
		const manager = SessionManager.inMemory("/adaptive-fair-fit");
		for (const [index, text] of ["first", "second", "third", "active"].entries()) {
			appendUser(manager, text, index + 1);
		}
		let freshTailMaxSources = 1;
		const context = new FakeLcmContext();
		const labels = ["A", "B", "C"];
		context.projectImpl = (request, snapshot) => {
			const freshSourceCount = request.freshTail.maxSources;
			const historicalSources = snapshot.entries.slice(0, -freshSourceCount);
			const fresh = snapshot.entries.slice(-freshSourceCount);
			return {
				revision: 1,
				ready: true,
				historical: labels.slice(0, historicalSources.length).map((label, index) => ({
					kind: "summary" as const,
					summaryId: `summary-${label}`,
					summaryHandle: `handle-${label}`,
					level: 0,
					redactedText: `${label}-FIRST\n${label.repeat(600)}\n${label}-LAST`,
					tokenCount: 200,
					sourceIds: [historicalSources[index]!.entryId],
					citations: [],
					files: [],
				})),
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: fresh.map(entry => entry.entryId),
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: { 0: historicalSources.length },
				coveredSourceCount: historicalSources.length,
				freshSourceCount,
				estimatedTokens: 650,
				pendingJobs: 0,
			};
		};
		const excerptBytes = (messages: readonly AgentMessage[]): number[] => {
			const historical = messages.find(message => message.role === "historicalContext");
			if (!historical?.redactedCitedContent.includes("[Summary excerpt;")) return [];
			return historical.redactedCitedContent
				.split("[Summary excerpt; use the handle below to expand.]\n")
				.slice(1)
				.map(block => {
					const end = block.indexOf("\n[Summary:");
					return Buffer.byteLength(end < 0 ? "" : block.slice(0, end), "utf8");
				});
		};
		const measure = (messages: readonly AgentMessage[]) => {
			const upperBound = messages.reduce(
				(total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message),
				0,
			);
			const historical = messages.find(message => message.role === "historicalContext");
			if (!historical) return { tokens: 40, upperBound: Math.max(40, upperBound) };
			const excerpts = excerptBytes(messages);
			if (excerpts.length === 0) return { tokens: 1_000, upperBound: Math.max(1_000, upperBound) };
			const retained = excerpts.reduce((total, bytes) => total + bytes, 0);
			return {
				tokens: 40 + Math.ceil(retained / 4),
				upperBound: Math.max(40 + Math.ceil(retained / 4), upperBound),
			};
		};
		const measurements = vi.fn(measure);
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 201,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 200,
				freshTail: { maxSources: freshTailMaxSources, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			measurements,
		);

		const input = manager.buildSessionContext().messages;
		const projectionCallsBefore = context.projectionCalls;
		const result = await lcm.project(input);

		expect(result.owned).toBe(true);
		const historical = result.messages.find(message => message.role === "historicalContext");
		expect(historical?.redactedCitedContent.match(/\[Summary excerpt;/g)).toHaveLength(3);
		for (const label of labels) {
			expect(historical?.redactedCitedContent).toContain(`${label}-FIRST`);
			expect(historical?.redactedCitedContent).toContain(`${label}-LAST`);
		}
		const retained = excerptBytes(result.messages);
		expect(Math.max(...retained) - Math.min(...retained)).toBeLessThanOrEqual(1);
		const finalMeasurement = measure(result.messages);
		expect(finalMeasurement.tokens).toBeGreaterThanOrEqual(140);
		expect(finalMeasurement.tokens).toBeLessThanOrEqual(200);
		const initialMeasurementCount = measurements.mock.calls.length;
		expect(initialMeasurementCount).toBeLessThanOrEqual(MAX_LCM_PROJECTION_TOKEN_MEASUREMENTS);
		const rendered = measurements.mock.calls.map(([messages]) => JSON.stringify(messages));
		expect(new Set(rendered).size).toBe(rendered.length);
		expect(context.projectionCalls - projectionCallsBefore).toBe(2);
		expect(context.reconcileAttempts).toBe(1);
		lcm.recordPendingPrimaryProviderTokens(result.routeKey, finalMeasurement.tokens);
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "lossless",
			metrics: {
				messageTokenBudget: 200,
				candidateTokens: finalMeasurement.tokens,
				projectionTokenMeasurements: initialMeasurementCount,
			},
		});
		context.reconcile = () => ({
			changed: false,
			revision: 1,
			activeSources: 4,
			insertedSources: 0,
			tombstonedSources: 0,
			queuedJobs: 0,
			reusedSummaries: 0,
		});
		const measurementCountBeforeRepeated = measurements.mock.calls.length;
		const repeated = await lcm.project(input);
		expect(repeated.messages).not.toBe(result.messages);
		expect(repeated.projectionTokenMeasurements).toBe(initialMeasurementCount);
		expect(measurements).toHaveBeenCalledTimes(measurementCountBeforeRepeated + initialMeasurementCount);
		labels[2] = "D";
		const changedSummary = await lcm.project(input);
		expect(changedSummary.messages).not.toBe(result.messages);
		expect(
			changedSummary.messages.find(message => message.role === "historicalContext")?.redactedCitedContent,
		).toContain("D-FIRST");
		freshTailMaxSources = 2;
		const changedTail = await lcm.project(input);
		expect(changedTail.messages).not.toBe(result.messages);
		expect(
			changedTail.messages.find(message => message.role === "historicalContext")?.redactedCitedContent,
		).not.toContain("D-FIRST");
		expect(
			changedTail.messages.some(
				message =>
					message.role === "user" &&
					Array.isArray(message.content) &&
					message.content.some(block => block.type === "text" && block.text === "third"),
			),
		).toBe(true);
		lcm.recordPendingPrimaryProviderTokens(changedTail.routeKey, 201);
		expect(lcm.commitPrimaryRequestRoute(changedTail.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_fallback",
			category: "unfit",
			reason: "fit_invariant",
			metrics: { messageTokenBudget: 200, candidateTokens: 201 },
		});
		await lcm.close();
	});

	it("measures zero-text candidates before dropping conservatively expensive file handles", async () => {
		const manager = SessionManager.inMemory("/adaptive-file-fit");
		for (let index = 0; index < 5; index++) appendUser(manager, `message-${index}`, index + 1);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => {
			const historical = snapshot.entries.slice(0, -1);
			const fresh = snapshot.entries.at(-1)!;
			return {
				revision: 1,
				ready: true,
				historical: historical.map((source, summaryIndex) => ({
					kind: "summary" as const,
					summaryId: `summary-files-${summaryIndex}`,
					summaryHandle: `handle-files-${summaryIndex}`,
					level: 0,
					redactedText: `summary facts ${summaryIndex}`,
					tokenCount: 4,
					sourceIds: [source.entryId],
					citations: [],
					files: Array.from({ length: 3 }, (_, fileIndex) => {
						const ordinal = summaryIndex * 3 + fileIndex;
						return {
							fileId: `fit-file-${ordinal}`,
							contentHash: ordinal.toString(16).padStart(2, "0").repeat(32),
							path: `/repo/fit-${ordinal}.txt`,
							fileType: "txt",
							byteSize: 100,
							tokenCount: 25,
							explorationSummary: `file ${ordinal}`,
						};
					}),
				})),
				activeSourceFingerprint: activeSourceFingerprint(snapshot.entries.map(entry => entry.entryId)),
				freshTailSourceIds: [fresh.entryId],
				uncoveredSourceIds: [],
				sourceTokens: snapshot.entries.length,
				selectedLevelCounts: { 0: historical.length },
				coveredSourceCount: historical.length,
				freshSourceCount: 1,
				estimatedTokens: 1_000,
				pendingJobs: 0,
			};
		};
		const retainedSummaryTextBytes = (messages: readonly AgentMessage[]) => {
			const text = messages.find(message => message.role === "historicalContext")?.redactedCitedContent ?? "";
			const marker = "[Summary excerpt; use the handle below to expand.]\n";
			if (!text.includes(marker)) return -1;
			return text
				.split(marker)
				.slice(1)
				.reduce((total, block) => {
					const end = block.indexOf("\n[Summary:");
					return total + Buffer.byteLength(end < 0 ? "" : block.slice(0, end), "utf8");
				}, 0);
		};
		const measure = (messages: readonly AgentMessage[]) => {
			const upperBound = messages.reduce(
				(total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message),
				0,
			);
			if (!messages.some(message => message.role === "historicalContext")) {
				return { tokens: 40, upperBound: Math.max(40, upperBound) };
			}
			const retained = retainedSummaryTextBytes(messages);
			const tokens = retained === 0 ? 80 : 1_000;
			return { tokens, upperBound: Math.max(tokens, upperBound) };
		};
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 201,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 100,
				freshTail: { maxSources: 1, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			measure,
		);

		const result = await lcm.project(manager.buildSessionContext().messages);
		const citedContent =
			result.messages.find(message => message.role === "historicalContext")?.redactedCitedContent ?? "";
		const selectedFileIds = (citedContent.match(/lcm-handle:v1:[A-Za-z0-9_-]+/g) ?? []).flatMap(token => {
			const handle = decodeLcmHandle(token);
			return handle.kind === "file" ? [handle.reference.fileId] : [];
		});
		const finalMeasurement = measure(result.messages);

		expect(result.owned).toBe(true);
		expect(selectedFileIds).toEqual([
			"fit-file-0",
			"fit-file-1",
			"fit-file-2",
			"fit-file-3",
			"fit-file-4",
			"fit-file-5",
			"fit-file-6",
			"fit-file-7",
			"fit-file-8",
			"fit-file-9",
			"fit-file-10",
			"fit-file-11",
		]);
		expect(finalMeasurement.tokens).toBe(80);
		expect(finalMeasurement.tokens).toBeLessThanOrEqual(100);
		expect(finalMeasurement.upperBound).toBeGreaterThan(100);
		await lcm.close();
	});

	it("continues above a non-monotone estimator failure and selects a later fitting render", async () => {
		const manager = SessionManager.inMemory("/adaptive-non-monotone-fit");
		appendUser(manager, "first", 1);
		appendUser(manager, "older", 2);
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => {
			const projection = readyHistoricalProjection(snapshot, "non-monotone-handle");
			return {
				...projection,
				historical: [{ ...projection.historical[0]!, redactedText: `FIRST\n${"x".repeat(1_200)}\nLAST` }],
			};
		};
		const retainedBytes = (messages: readonly AgentMessage[]) => {
			const text = messages.find(message => message.role === "historicalContext")?.redactedCitedContent ?? "";
			if (!text.includes("[Summary excerpt;")) return -1;
			const start = text.indexOf("\n") + 1;
			const end = text.indexOf("\n[Summary:", start);
			return end < 0 ? 0 : Buffer.byteLength(text.slice(start, end), "utf8");
		};
		const observed: Array<{ retained: number; tokens: number }> = [];
		const measurements = vi.fn((messages: readonly AgentMessage[]) => {
			const upperBound = messages.reduce(
				(total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message),
				0,
			);
			if (!messages.some(message => message.role === "historicalContext")) {
				return { tokens: 40, upperBound: Math.max(40, upperBound) };
			}
			const retained = retainedBytes(messages);
			if (retained < 0) return { tokens: 1_000, upperBound: Math.max(1_000, upperBound) };
			const tokens = retained < 170 ? 40 + retained : retained < 500 ? 400 : retained < 800 ? 180 : 400;
			observed.push({ retained, tokens });
			return { tokens, upperBound: Math.max(tokens, upperBound) };
		});
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 201,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 200,
				freshTail: { maxSources: 8, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			measurements,
		);

		const result = await lcm.project(manager.buildSessionContext().messages);
		const selected = retainedBytes(result.messages);

		expect(result.owned).toBe(true);
		const richestMeasuredFit = Math.max(
			...observed.filter(sample => sample.tokens <= 200).map(sample => sample.retained),
		);
		expect(selected).toBe(richestMeasuredFit);
		expect(selected).toBeGreaterThan(606);
		expect(observed.some(sample => sample.retained < selected && sample.tokens > 200)).toBe(true);
		expect(measurements.mock.calls.length).toBeLessThanOrEqual(MAX_LCM_PROJECTION_TOKEN_MEASUREMENTS);
		await lcm.close();
	});

	it("uses eight evenly spaced refinements to find a non-monotone fitting island", async () => {
		const manager = SessionManager.inMemory("/adaptive-refinement-fit");
		appendUser(manager, "first", 1);
		appendUser(manager, "older", 2);
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => {
			const projection = readyHistoricalProjection(snapshot, "refinement-handle");
			return {
				...projection,
				historical: [{ ...projection.historical[0]!, redactedText: `FIRST\n${"x".repeat(1_189)}\nLAST` }],
			};
		};
		const retainedBytes = (messages: readonly AgentMessage[]) => {
			const text = messages.find(message => message.role === "historicalContext")?.redactedCitedContent ?? "";
			if (!text.includes("[Summary excerpt;")) return -1;
			const start = text.indexOf("\n") + 1;
			const end = text.indexOf("\n[Summary:", start);
			return end < 0 ? 0 : Buffer.byteLength(text.slice(start, end), "utf8");
		};
		const measure = (messages: readonly AgentMessage[]) => {
			const upperBound = messages.reduce(
				(total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message),
				0,
			);
			if (!messages.some(message => message.role === "historicalContext")) {
				return { tokens: 40, upperBound: Math.max(40, upperBound) };
			}
			const retained = retainedBytes(messages);
			const tokens = retained === 0 ? 80 : retained === 1_050 ? 180 : 400;
			return { tokens, upperBound: Math.max(tokens, upperBound) };
		};
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 201,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 200,
				freshTail: { maxSources: 8, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			measure,
		);

		const result = await lcm.project(manager.buildSessionContext().messages);
		const finalMeasurement = measure(result.messages);

		expect(result.owned).toBe(true);
		expect(retainedBytes(result.messages)).toBe(1_050);
		expect(finalMeasurement.tokens).toBe(180);
		expect(finalMeasurement.tokens).toBeLessThanOrEqual(200);
		await lcm.close();
	});

	it("uses minimum-representation fallback when required input fits but every handle marker does not", async () => {
		const manager = SessionManager.inMemory("/minimum-representation-fit");
		appendUser(manager, "first", 1);
		appendUser(manager, "older", 2);
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		context.projectImpl = (_request, snapshot) => readyHistoricalProjection(snapshot, "minimum-handle");
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			() => ({
				sourceTokens: 201,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: 100,
				freshTail: { maxSources: 8, maxTokens: 100 },
			}),
			20,
			undefined,
			1,
			messages => {
				const upperBound = messages.reduce(
					(total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message),
					0,
				);
				return messages.some(message => message.role === "historicalContext")
					? { tokens: 101, upperBound: Math.max(101, upperBound) }
					: { tokens: 60, upperBound: Math.max(60, upperBound) };
			},
		);
		const input = manager.buildSessionContext().messages;
		const result = await lcm.project(input);

		expect(result.messages).toBe(input);
		expect(lcm.commitPrimaryRequestRoute(result.routeKey)).toBe(true);
		expect((await lcm.status()).runtime.lastRequestRoute).toMatchObject({
			kind: "native_fallback",
			category: "unfit",
			reason: "minimum_representation",
		});
		await lcm.close();
	});

	it("fails open to the exact native input when handle-bearing history does not fit", async () => {
		const manager = SessionManager.inMemory("/unfitted-handle-history");
		appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("settled"), timestamp: 2 });
		appendUser(manager, "active", 3);
		const context = new FakeLcmContext();
		const projectionTokenMeasurements = vi.fn((messages: AgentMessage[]) =>
			messages.some(message => message.role === "historicalContext")
				? { tokens: Number.MAX_SAFE_INTEGER, upperBound: Number.MAX_SAFE_INTEGER }
				: { tokens: 1, upperBound: 1 },
		);
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			20,
			undefined,
			1,
			projectionTokenMeasurements,
		);
		context.projectImpl = (_request, snapshot) => readyHistoricalProjection(snapshot, "summary-handle");
		const input = manager.buildSessionContext().messages;

		const result = await lcm.project(input);

		expect(result.owned).toBe(false);
		expect(result.messages).toBe(input);
		expect(result.messages).toEqual(input);
		expect(result.messages.some(message => message.role === "historicalContext")).toBe(false);
		expect(projectionTokenMeasurements).toHaveBeenCalled();
		const candidate = projectionTokenMeasurements.mock.calls.find(([messages]) =>
			messages.some(message => message.role === "historicalContext"),
		)?.[0];
		expect(JSON.stringify(convertToLlm(candidate ?? []))).toContain("lcm-handle:v1:");
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
		const projectionTokenMeasurements = vi.fn((_messages: AgentMessage[]) => ({ tokens: 0, upperBound: 0 }));
		const { lcm } = createHarness(
			manager,
			context,
			undefined,
			undefined,
			undefined,
			20,
			undefined,
			1,
			projectionTokenMeasurements,
		);
		context.projectImpl = (_request, snapshot) => readyHistoricalProjection(snapshot, summaryHandle);
		const input = manager.buildSessionContext().messages;

		const result = await lcm.project(input);

		expect(result.owned).toBe(false);
		expect(result.messages).toBe(input);
		expect(result.messages).toEqual(input);
		expect(result.messages.some(message => message.role === "historicalContext")).toBe(false);
		expect(JSON.stringify(result.messages)).not.toContain("[handle unavailable]");
		expect(projectionTokenMeasurements).not.toHaveBeenCalled();
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

	it("keeps the fifth failure degraded as provider exhaustion until rebuild", async () => {
		const manager = SessionManager.inMemory("/worktree-a");
		appendUser(manager, "first", 1);
		const context = new FakeLcmContext();
		const pendingProjection = context.projectImpl;
		context.projectImpl = (request, snapshot) => ({
			...pendingProjection(request, snapshot),
			revision: context.snapshots.length,
		});
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, undefined, 5);
		context.queueJobs({
			...summaryJob("failure"),
			inputs: [{ kind: "source", id: "source-1", redactedText: "safe", tokenCount: 8 }],
		});
		complete.mockRejectedValue(new LcmCompletionError("raw-secret summary failed", { provider: "test-provider" }));
		try {
			const input = manager.buildSessionContext().messages;
			const pending = lcm.project(input);
			for (let attempt = 1; attempt <= 5; attempt++) {
				await settleUntil(() => context.failureCalls.length === attempt, `provider failure ${attempt}`);
				if (attempt < 5) {
					context.now = context.failureRecords.get("failure")!.availableAt;
					await lcm.retrySummaries("all");
				}
			}
			const output = await pending;
			expect(output.messages).toBe(input);
			expect(output.owned).toBe(false);
			expect(context.failedError).toContain("#SECRET");
			expect(context.failedError).not.toContain("raw-secret");
			expect(complete).toHaveBeenCalledTimes(5);
			expect(output).not.toHaveProperty("maintenanceFallback");
			expect(manager.getBranch()).toHaveLength(1);
			expect(lcm.commitPrimaryRequestRoute(output.routeKey)).toBe(true);
			const status = (await lcm.status()).runtime;
			expect(status).toMatchObject({
				health: "degraded",
				lastFailure: { category: "provider", reason: "provider_exhausted" },
			});
			expect(status.summaryBackoff).toBeUndefined();
			context.jobs = [];
			context.failureRecords.clear();
			context.queuedJobs = 0;
			context.relevantPendingJobs = 0;
			expect(await lcm.rebuild()).not.toBeNull();
			expect((await lcm.status()).runtime.health).toBe("healthy");
		} finally {
			await lcm.close();
		}
	});
});
