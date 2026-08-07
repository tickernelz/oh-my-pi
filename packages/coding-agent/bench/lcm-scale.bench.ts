/**
 * Credential-free scale benchmark for Lossless Context Management.
 *
 * Two questions, two fixtures:
 *
 * - Timing: how much does projecting ONE branch cost as sibling branches are
 *   added? The measured active branch is byte-identical in the 1-branch and
 *   N-branch stores, so a latency ratio above ~1 is project-wide scan cost and
 *   nothing else. Per-branch size is `LCM_SCALE_SOURCES / LCM_SCALE_BRANCHES`
 *   so the pre-change scheduler, which replays every branch on every summary
 *   completion, still finishes and a baseline can be captured at all.
 * - Cost: how many provider tokens does one `LCM_SCALE_SOURCES`-source branch
 *   cost end to end, versus native context-full compaction and Snapcompact?
 *   All three lanes are charged against ONE canonical journal.
 *
 * No lane performs provider I/O: context-full runs through an injected
 * `SummaryOptions.completeImpl`, and Snapcompact is local by construction.
 *
 * Run:
 *   LCM_SCALE_SOURCES=10000 LCM_SCALE_BRANCHES=20 LCM_SCALE_SAMPLES=5 \
 *     bun run packages/coding-agent/bench/lcm-scale.bench.ts
 *
 * Thresholds are absolute, so no saved baseline is required. Set
 * `LCM_SCALE_REPORT_OUT` to also write the full JSON report.
 */
import { Database } from "bun:sqlite";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type {
	ContextScope,
	SourceEntry,
	SummaryProviderAttempt,
	SummaryProviderAttemptStart,
	SummaryProviderUsage,
} from "@oh-my-pi/lcm-context";
import { openLcmContext } from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import {
	type CompactionPreparation,
	type CompactionSettings,
	compact,
	createCompactionSummaryMessage,
	effectiveReserveTokens,
	estimateTokens,
	prepareCompaction,
} from "@oh-my-pi/pi-agent-core/compaction";
import type { AssistantMessage, Message, Model, Usage } from "@oh-my-pi/pi-ai";
import { buildModel } from "@oh-my-pi/pi-catalog/build";
import * as snapcompact from "@oh-my-pi/snapcompact";
import { convertToLlm, createHistoricalContextMessage } from "../src/session/messages";
import { buildSessionContext } from "../src/session/session-context";
import type { CompactionEntry, SessionEntry } from "../src/session/session-entries";
import {
	estimateLcmProjectionMessageTokens,
	estimateLcmProjectionMessageTokenUpperBound,
	LcmCompletionError,
	type LcmCompletionRequest,
	type LcmCompletionResult,
	SessionLcm,
} from "../src/session/session-lcm";
import { SessionManager } from "../src/session/session-manager";

const GENERATOR_VERSION = "lcm-scale/v1";
const SERIALIZER_VERSION = "canonical-text/v1";
const IMAGE_MARKER = "[image:opaque]";

const COST_PER_MILLION = { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 } as const;
const MAINTENANCE_OUTPUT_TOKENS = 1;

function projectionTokenMeasurements(messages: readonly AgentMessage[]): { tokens: number; upperBound: number } {
	let tokens = 0;
	let upperBound = 0;
	for (const message of messages) {
		tokens += estimateLcmProjectionMessageTokens(message);
		upperBound += estimateLcmProjectionMessageTokenUpperBound(message);
	}
	return { tokens, upperBound };
}
const SOURCES = envInteger("LCM_SCALE_SOURCES", 2_000, 64);
const BRANCHES = envInteger("LCM_SCALE_BRANCHES", 4, 2);
const SAMPLES = envInteger("LCM_SCALE_SAMPLES", 3, 1);
const LEAF_TOKENS = envInteger("LCM_SCALE_LEAF_TOKENS", 4_000, 256);
const FAN_IN = envInteger("LCM_SCALE_FAN_IN", 4, 2);
// The source cap binds before the token cap on short sources.
const LEAF_SOURCES = envInteger("LCM_SCALE_LEAF_SOURCES", 24, 2);

// Self-contained gate: thresholds are absolute, so no external baseline artifact is
// needed and the benchmark is reproducible on a fresh checkout or in CI.
const REPORT_OUT = Bun.env.LCM_SCALE_REPORT_OUT;

const TIMING_SOURCES_PER_BRANCH = Math.max(LEAF_SOURCES * 2, Math.ceil(SOURCES / BRANCHES));

/**
 * Measured on the pre-change tree (project-wide lineage scan + per-completion branch
 * replay) at the knobs below. Recorded here instead of in an external baseline file so
 * the regression gate is versioned, reviewable, and reproducible on a fresh checkout.
 * See bench/lcm-performance-notes.md for the full capture.
 */
const PRE_CHANGE_REFERENCE = {
	knobs: { SOURCES: 10_000, BRANCHES: 20, SAMPLES: 5, LEAF_TOKENS: 4_000, FAN_IN: 4, LEAF_SOURCES: 24 },
	projectionWallP95Ms: 55.434,
	projectionCpuP95Ms: 65.189,
	projectionLatencyRatio: 10.652,
	projectionLineageRowsRead: 195_600,
	schedulerBranchPasses: 11_620,
	historicalBytes: 170_057,
} as const;
/** Policy, not a measurement: post-change runs land 18-39x, so 5x is a reviewable floor. */
const REQUIRED_SPEEDUP = 5;

const MODEL: Model = buildModel({
	id: "lcm-scale-fixture",
	name: "LCM Scale Fixture",
	api: "openai-completions",
	provider: "openai",
	baseUrl: "https://example.invalid/v1",
	reasoning: false,
	input: ["text", "image"],
	cost: {
		input: COST_PER_MILLION.input,
		output: COST_PER_MILLION.output,
		cacheRead: COST_PER_MILLION.cacheRead,
		cacheWrite: COST_PER_MILLION.cacheWrite,
	},
	contextWindow: 200_000,
	maxTokens: 32_768,
});

const COMPACTION_SETTINGS: CompactionSettings = {
	enabled: true,
	strategy: "context-full",
	keepRecentTokens: 2_000,
	remoteEnabled: false,
	remoteStreamingV2Enabled: false,
};

type ScaleEnv =
	| "LCM_SCALE_SOURCES"
	| "LCM_SCALE_BRANCHES"
	| "LCM_SCALE_SAMPLES"
	| "LCM_SCALE_LEAF_TOKENS"
	| "LCM_SCALE_FAN_IN"
	| "LCM_SCALE_LEAF_SOURCES";

function envInteger(name: ScaleEnv, fallback: number, minimum: number): number {
	const raw = Bun.env[name];
	if (raw === undefined) return fallback;
	if (!/^\d+$/.test(raw)) throw new Error(`${name} must be a base-10 integer`);
	const value = Number(raw);
	if (!Number.isSafeInteger(value) || value < minimum) {
		throw new Error(`${name} must be a safe integer >= ${minimum}`);
	}
	return value;
}

function canonicalTokens(text: string): number {
	return text.length === 0 ? 0 : Math.ceil(Buffer.byteLength(text, "utf8") / 4);
}

function percentile(values: readonly number[], fraction: number): number {
	if (values.length === 0) return 0;
	const sorted = [...values].sort((left, right) => left - right);
	const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(fraction * sorted.length) - 1));
	return sorted[index]!;
}

function usageFor(inputTokens: number, outputTokens: number): SummaryProviderUsage {
	const cost = {
		input: (inputTokens * COST_PER_MILLION.input) / 1e6,
		output: (outputTokens * COST_PER_MILLION.output) / 1e6,
		cacheRead: 0,
		cacheWrite: 0,
	};
	return {
		input: inputTokens,
		output: outputTokens,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: inputTokens + outputTokens,
		cost: { ...cost, total: cost.input + cost.output },
	};
}

function canonicalUsage(inputTokens: number, outputTokens: number): Usage {
	return usageFor(inputTokens, outputTokens) as Usage;
}

function normalizedMessageText(message: Message): string {
	const parts: string[] = [message.role];
	const content = message.content;
	if (typeof content === "string") return `${message.role}\n${content}`;
	for (const block of content) {
		switch (block.type) {
			case "text":
				parts.push(block.text);
				break;
			case "image":
				parts.push(`${IMAGE_MARKER}:${block.mimeType}`);
				break;
			case "thinking":
				parts.push(block.thinking);
				break;
			case "toolCall":
				parts.push(`${block.name}\n${JSON.stringify(block.arguments ?? null)}`);
				break;
			default:
				parts.push(block.type);
				break;
		}
	}
	return parts.join("\n");
}

function normalizedPayload(messages: readonly AgentMessage[]): { text: string; tokens: number; bytes: number } {
	const text = convertToLlm([...messages])
		.map(normalizedMessageText)
		.join("\n\n");
	return { text, tokens: canonicalTokens(text), bytes: Buffer.byteLength(text, "utf8") };
}

function branchScope(index: number): ContextScope {
	return { projectId: "lcm-scale", sessionId: "scale-session", branchId: `branch-${index}` };
}

function sourceText(branchIndex: number, index: number): string {
	return `Branch ${branchIndex} source ${index.toString().padStart(6, "0")}: alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon.`;
}

function syntheticSource(scope: ContextScope, branchIndex: number, index: number): SourceEntry {
	const redactedText = sourceText(branchIndex, index);
	return {
		...scope,
		entryId: `b${branchIndex}-e${index}`,
		parentId: index === 0 ? null : `b${branchIndex}-e${index - 1}`,
		timestamp: 1_700_000_000_000 + index,
		kind: "message",
		redactedText,
		contentHash: new Bun.CryptoHasher("sha256").update(redactedText).digest("hex"),
		artifactRefs: [],
	};
}

function createJournal(projectRoot: string): SessionManager {
	const manager = SessionManager.inMemory(projectRoot);
	for (let index = 0; index < SOURCES; index++) {
		const timestamp = 1_700_000_000_000 + index;
		if (index % 32 === 31) {
			manager.appendMessage({
				role: "user",
				content: [
					{ type: "text", text: `Attached screenshot ${index}.` },
					{ type: "image", data: "iVBORw0KGgo=", mimeType: "image/png" },
				],
				timestamp,
			});
			continue;
		}
		if (index % 8 === 7) {
			manager.appendMessage({
				role: "assistant",
				content: [{ type: "text", text: `Observation ${index}: ${sourceText(0, index)}` }],
				api: "openai-completions",
				provider: "openai",
				model: MODEL.id,
				usage: canonicalUsage(0, 0),
				stopReason: "stop",
				timestamp,
			});
			continue;
		}
		manager.appendMessage({
			role: "user",
			content: [{ type: "text", text: sourceText(0, index) }],
			timestamp,
		});
	}
	return manager;
}

/**
 * Ordinal + type + normalized content only. Entry ids are freshly minted per run,
 * so including them would make every baseline comparison a false mismatch.
 */
function journalFingerprint(entries: readonly SessionEntry[]): string {
	const hasher = new Bun.CryptoHasher("sha256");
	for (let index = 0; index < entries.length; index++) {
		const entry = entries[index]!;
		hasher.update(`${index}\u0000${entry.type}\u0000`);
		if (entry.type === "message") hasher.update(normalizedPayload([entry.message]).text);
		hasher.update("\u0001");
	}
	return hasher.digest("hex");
}

interface LaneCost {
	primaryTokens: number;
	primaryBytes: number;
	maintenanceRequests: number;
	maintenanceInputTokens: number;
	maintenanceCacheReadTokens: number;
	maintenanceOutputTokens: number;
	maintenanceCostUsd: number;
	deterministicFallbacks: number;
	frameTokens: number;
	totalTokens: number;
}

function laneTotal(lane: Omit<LaneCost, "totalTokens">): LaneCost {
	return {
		...lane,
		totalTokens: lane.primaryTokens + lane.frameTokens + lane.maintenanceInputTokens + lane.maintenanceOutputTokens,
	};
}

interface TimingResult {
	sourcesPerBranch: number;
	branches: number;
	projectionWallP95Ms: number;
	projectionCpuP95Ms: number;
	projectionLineageRowsRead: number;
	schedulerBranchPasses: number;
	completionToClaimP95Ms: number;
	duplicateCompletionHashes: number;
	coverageFailures: number;
}

async function measureTiming(storePath: string, branchCount: number): Promise<TimingResult> {
	const now = 1_800_000_000_000;
	const context = await openLcmContext({
		dbPath: storePath,
		leafChunk: { maxSources: LEAF_SOURCES, maxTokens: LEAF_TOKENS },
		condenseFanIn: FAN_IN,
		now: () => now,
	});
	try {
		const projectionRequest = (scope: ContextScope) => ({
			...scope,
			tokenBudget: 120_000,
			freshTail: { maxSources: 8, maxTokens: 4_000 },
		});
		for (let branchIndex = 0; branchIndex < branchCount; branchIndex++) {
			const scope = branchScope(branchIndex);
			const entries = Array.from({ length: TIMING_SOURCES_PER_BRANCH }, (_, index) =>
				syntheticSource(scope, branchIndex, index),
			);
			context.reconcile({ scope, entries }, { summarize: projectionRequest(scope) });
		}
		const retryPolicy = context.configureSummaryRetryPolicy("lcm-scale", "lcm-scale/lcm-scale-summary");
		if (retryPolicy.kind !== "ready") throw new Error("LCM scale retry policy initialization conflicted");

		const completionHashes = new Set<string>();
		let duplicateCompletionHashes = 0;
		const completionToClaimMs: number[] = [];
		let lastCompletionAt: number | undefined;
		for (;;) {
			const claimStarted = Bun.nanoseconds();
			const [job] = context.claimSummaryJobs({
				...retryPolicy,
				maxTransportRetries: 5,
				workerId: "scale",
				leaseMs: 600_000,
				limit: 1,
				maxOutputTokens: 2_048,
			});
			if (!job) break;
			if (lastCompletionAt !== undefined) completionToClaimMs.push((claimStarted - lastCompletionAt) / 1e6);
			const inputHash = new Bun.CryptoHasher("sha256")
				.update(job.inputs.map(input => `${input.kind}:${input.id}`).join("\n"))
				.digest("hex");
			if (completionHashes.has(inputHash)) duplicateCompletionHashes++;
			completionHashes.add(inputHash);
			context.completeSummaryJob(job, { redactedText: ".", tokenCount: 1 });
			lastCompletionAt = Bun.nanoseconds();
		}

		const measuredScope = branchScope(0);
		const before = context.status().performance;
		const wall: number[] = [];
		const cpu: number[] = [];
		let coverageFailures = 0;
		for (let sample = 0; sample < SAMPLES; sample++) {
			const startedWall = Bun.nanoseconds();
			const startedCpu = process.cpuUsage();
			const projection = context.project(projectionRequest(measuredScope));
			const usedCpu = process.cpuUsage(startedCpu);
			wall.push((Bun.nanoseconds() - startedWall) / 1e6);
			cpu.push((usedCpu.user + usedCpu.system) / 1_000);
			if (projection.uncoveredSourceIds.length > 0) coverageFailures++;
			const covered = new Set<string>();
			for (const item of projection.historical) {
				for (const sourceId of item.sourceIds) {
					if (covered.has(sourceId)) coverageFailures++;
					covered.add(sourceId);
				}
				for (const citation of item.citations) {
					if (citation.branchId !== measuredScope.branchId) coverageFailures++;
				}
			}
		}
		const after = context.status().performance;
		return {
			sourcesPerBranch: TIMING_SOURCES_PER_BRANCH,
			branches: branchCount,
			projectionWallP95Ms: percentile(wall, 0.95),
			projectionCpuP95Ms: percentile(cpu, 0.95),
			projectionLineageRowsRead: (after?.projectionLineageRowsRead ?? 0) - (before?.projectionLineageRowsRead ?? 0),
			schedulerBranchPasses: after?.schedulerBranchPasses ?? 0,
			completionToClaimP95Ms: percentile(completionToClaimMs, 0.95),
			duplicateCompletionHashes,
			coverageFailures,
		};
	} finally {
		context.close();
	}
}

interface LcmCostResult {
	lane: LaneCost;
	historicalBytes: number;
	legacyHistoricalBytes: number;
	owned: boolean;
	dispatchedAttempts: number;
	orphanedAttempts: number;
	nullUsageAttempts: number;
	attemptsByOutcome: Record<string, number>;
}

async function measureLcmCost(projectRoot: string, storePath: string, manager: SessionManager): Promise<LcmCostResult> {
	let dispatchedAttempts = 0;
	let attemptOrdinal = 0;
	let maintenanceRequests = 0;
	let maintenanceInputTokens = 0;

	const complete = async (request: LcmCompletionRequest): Promise<LcmCompletionResult> => {
		const start: SummaryProviderAttemptStart = {
			attemptId: `lcm-attempt-${++attemptOrdinal}`,
			startedAt: Date.now(),
			provider: "openai",
			model: MODEL.id,
		};
		request.onResolvedModel?.(`openai/${MODEL.id}`);
		if (request.onAttemptStart && !(await request.onAttemptStart(start))) {
			throw new LcmCompletionError("scale attempt superseded before dispatch", {
				provider: "openai",
				category: "aborted",
			});
		}
		dispatchedAttempts++;
		maintenanceRequests++;
		const inputTokens = canonicalTokens(request.systemPrompt) + canonicalTokens(request.prompt);
		maintenanceInputTokens += inputTokens;
		return {
			text: ".",
			attempt: {
				...start,
				completedAt: Math.max(start.startedAt, Date.now()),
				usage: usageFor(inputTokens, MAINTENANCE_OUTPUT_TOKENS),
			},
		};
	};

	const lcm = new SessionLcm(
		{
			sessionManager: manager,
			projectionLimits: () => ({
				sourceTokens: SOURCES * 64,
				prewarmThresholdTokens: 1,
				hardThresholdTokens: 1,
				tokenBudget: 120_000,
				freshTail: { maxSources: 8, maxTokens: 4_000 },
			}),
			projectionTokenMeasurements,
			complete,
			resolveSummaryModel: () => `openai/${MODEL.id}`,
		},
		{
			summaryModel: "@smol",
			maxConcurrentSummaries: 2,
			dependencies: {
				// Without this override the cost lane silently uses SessionLcm's own leaf/fan-in defaults.
				openContext: async options =>
					openLcmContext({
						...options,
						leafChunk: { maxSources: LEAF_SOURCES, maxTokens: LEAF_TOKENS },
						condenseFanIn: FAN_IN,
					}),
				resolveProject: async () => ({ projectId: "lcm-scale-cost", rootPath: projectRoot, storePath }),
			},
		},
	);

	let owned = false;
	let historicalBytes = 0;
	let legacyHistoricalBytes = 0;
	let primary = { tokens: 0, bytes: 0 };
	try {
		const projected = await lcm.project(manager.buildSessionContext().messages);
		owned = projected.owned;
		const payload = normalizedPayload(projected.messages);
		primary = { tokens: payload.tokens, bytes: payload.bytes };
		const historical = projected.messages.filter(message => message.role === "historicalContext");
		historicalBytes = historical.length === 0 ? 0 : normalizedPayload(historical).bytes;
		// Re-serialize the SAME projection the way the pre-change boundary did, so the
		// reduction is observable in one run instead of across two fixtures.
		const projection = projected.projection;
		if (projection && historical.length > 0) {
			const legacyText = projection.historical
				.map(item => {
					const sourceIds = [...new Set(item.citations.map(citation => citation.sourceId))];
					const suffix =
						sourceIds.length > 0 ? `\n[Sources: ${sourceIds.map(id => `source:${id}`).join(", ")}]` : "";
					return `${item.redactedText}${suffix}`;
				})
				.join("\n\n");
			legacyHistoricalBytes = normalizedPayload([
				createHistoricalContextMessage({
					redactedCitedContent: legacyText,
					timestamp: historical[0]!.timestamp,
				}),
			]).bytes;
		}
	} finally {
		await lcm.close();
	}

	const attemptsByOutcome: Record<string, number> = {};
	let orphanedAttempts = 0;
	let ledgerInputTokens = 0;
	let ledgerOutputTokens = 0;
	let ledgerCacheReadTokens = 0;
	let ledgerCostUsd = 0;
	let nullUsageAttempts = 0;
	const store = new Database(storePath, { readonly: true });
	try {
		for (const row of store
			.query<
				{
					outcome: string;
					count: number;
					input_tokens: number | null;
					output_tokens: number | null;
					cache_read_tokens: number | null;
					cost_total: number | null;
				},
				[]
			>(
				`SELECT outcome, COUNT(*) AS count,
					SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens,
					SUM(cache_read_tokens) AS cache_read_tokens, SUM(cost_total) AS cost_total
				 FROM summary_attempts GROUP BY outcome`,
			)
			.all()) {
			attemptsByOutcome[row.outcome] = row.count;
			if (row.outcome === "in_flight") orphanedAttempts += row.count;
			ledgerInputTokens += row.input_tokens ?? 0;
			ledgerOutputTokens += row.output_tokens ?? 0;
			ledgerCacheReadTokens += row.cache_read_tokens ?? 0;
			ledgerCostUsd += row.cost_total ?? 0;
		}
		nullUsageAttempts = store
			.query<{ count: number }, []>(
				"SELECT COUNT(*) AS count FROM summary_attempts WHERE outcome <> 'in_flight' AND total_tokens IS NULL",
			)
			.get()!.count;
	} finally {
		store.close();
	}

	if (ledgerInputTokens !== maintenanceInputTokens) {
		throw new Error(
			`LCM ledger input ${ledgerInputTokens} does not match ${maintenanceInputTokens} dispatched maintenance tokens`,
		);
	}

	return {
		lane: laneTotal({
			primaryTokens: primary.tokens,
			primaryBytes: primary.bytes,
			maintenanceRequests,
			maintenanceInputTokens: ledgerInputTokens,
			maintenanceCacheReadTokens: ledgerCacheReadTokens,
			maintenanceOutputTokens: ledgerOutputTokens,
			maintenanceCostUsd: ledgerCostUsd,
			deterministicFallbacks: 0,
			frameTokens: 0,
		}),
		historicalBytes,
		legacyHistoricalBytes,
		owned,
		dispatchedAttempts,
		orphanedAttempts,
		nullUsageAttempts,
		attemptsByOutcome,
	};
}

function compactionEntryFor(
	parentId: string,
	summary: string,
	shortSummary: string | undefined,
	firstKeptEntryId: string,
	tokensBefore: number,
	preserveData?: Record<string, unknown>,
): CompactionEntry {
	return {
		type: "compaction",
		id: "compaction-lane",
		parentId,
		timestamp: new Date(1_700_000_900_000).toISOString(),
		summary,
		...(shortSummary === undefined ? {} : { shortSummary }),
		firstKeptEntryId,
		tokensBefore,
		...(preserveData === undefined ? {} : { preserveData }),
	};
}

async function measureContextFull(entries: readonly SessionEntry[]): Promise<LaneCost> {
	const preparation = prepareCompaction([...entries] as never, COMPACTION_SETTINGS, MODEL);
	if (!preparation) throw new Error("context-full lane produced no compaction preparation");

	let maintenanceRequests = 0;
	let maintenanceInputTokens = 0;
	let maintenanceOutputTokens = 0;
	let maintenanceCostUsd = 0;
	const result = await compact(preparation, MODEL, "scale-fixture-key", undefined, undefined, {
		convertToLlm,
		completeImpl: async (requestModel, requestContext) => {
			const promptText = [
				...(requestContext.systemPrompt ?? []),
				...requestContext.messages.map(normalizedMessageText),
			].join("\n");
			const inputTokens = canonicalTokens(promptText);
			maintenanceRequests++;
			maintenanceInputTokens += inputTokens;
			maintenanceOutputTokens += MAINTENANCE_OUTPUT_TOKENS;
			const usage = canonicalUsage(inputTokens, MAINTENANCE_OUTPUT_TOKENS);
			maintenanceCostUsd += usage.cost.total;
			const message: AssistantMessage = {
				role: "assistant",
				content: [{ type: "text", text: "." }],
				api: requestModel.api,
				provider: requestModel.provider,
				model: requestModel.id,
				usage,
				stopReason: "stop",
				timestamp: 1_700_000_900_000,
			};
			return message;
		},
	});

	const entry = compactionEntryFor(
		entries.at(-1)!.id,
		result.summary,
		result.shortSummary,
		result.firstKeptEntryId,
		result.tokensBefore,
	);
	const payload = normalizedPayload(buildSessionContext([...entries, entry], entry.id).messages);
	return laneTotal({
		primaryTokens: payload.tokens,
		primaryBytes: payload.bytes,
		maintenanceRequests,
		maintenanceInputTokens,
		maintenanceCacheReadTokens: 0,
		maintenanceOutputTokens,
		maintenanceCostUsd,
		deterministicFallbacks: 0,
		frameTokens: 0,
	});
}

/** Mirrors the production frame cap so both native lanes are charged the same standing payload. */
function snapcompactMaxFrames(preparation: CompactionPreparation): number {
	const ctxWindow = MODEL.contextWindow ?? 0;
	if (ctxWindow <= 0) return Math.min(snapcompact.MAX_FRAMES_DEFAULT, snapcompact.maxFramesForDataBudget());
	const reserve = effectiveReserveTokens(ctxWindow, COMPACTION_SETTINGS);
	let baseTokens = 0;
	for (const message of preparation.recentMessages) baseTokens += estimateTokens(message);
	const totalBudget = ctxWindow - reserve;
	if (baseTokens >= totalBudget) return 0;
	const shape = snapcompact.resolveShape(MODEL, "auto");
	const capReserve = Math.ceil((2 * snapcompact.geometry(shape).capacity * 1.15) / 4) + 2_000;
	const frameBudget = totalBudget - baseTokens - capReserve;
	if (frameBudget < snapcompact.FRAME_TOKEN_ESTIMATE) return 1;
	return Math.min(
		Math.floor(frameBudget / snapcompact.FRAME_TOKEN_ESTIMATE),
		snapcompact.MAX_FRAMES_DEFAULT,
		snapcompact.maxFramesForDataBudget(),
	);
}

async function measureSnapcompact(entries: readonly SessionEntry[]): Promise<LaneCost & { frames: number }> {
	const preparation = prepareCompaction([...entries] as never, COMPACTION_SETTINGS, MODEL);
	if (!preparation) throw new Error("snapcompact lane produced no compaction preparation");
	const maxFrames = snapcompactMaxFrames(preparation);
	if (maxFrames < 1) throw new Error("snapcompact lane has no frame budget for this fixture");

	const result = await snapcompact.compact(preparation as never, { convertToLlm, model: MODEL, maxFrames });
	const archive = snapcompact.getPreservedArchive(result.preserveData);
	const frames = archive?.frames.length ?? 0;
	const framePayloadBytes = archive ? snapcompact.frameDataBytes(archive.frames) : 0;
	if (framePayloadBytes > snapcompact.FRAME_DATA_BYTES_BUDGET) {
		throw new Error(
			`snapcompact frame payload ${framePayloadBytes} exceeds the per-request budget ${snapcompact.FRAME_DATA_BYTES_BUDGET}`,
		);
	}

	const blocks = archive
		? snapcompact.historyBlocks(archive, { maxFrameDataBytes: snapcompact.FRAME_DATA_BYTES_BUDGET })
		: undefined;
	const summaryMessage = createCompactionSummaryMessage(
		result.summary,
		result.tokensBefore,
		new Date(1_700_000_900_000).toISOString(),
		result.shortSummary,
		undefined,
		undefined,
		blocks,
	);
	let projectedTokens = estimateTokens(summaryMessage);
	for (const message of preparation.recentMessages) projectedTokens += estimateTokens(message);
	const ctxWindow = MODEL.contextWindow ?? 0;
	const budget = ctxWindow - effectiveReserveTokens(ctxWindow, COMPACTION_SETTINGS);
	if (projectedTokens > budget) {
		throw new Error(`snapcompact projected ${projectedTokens} tokens above the window budget ${budget}`);
	}

	const entry = compactionEntryFor(
		entries.at(-1)!.id,
		result.summary,
		result.shortSummary,
		result.firstKeptEntryId,
		result.tokensBefore,
		result.preserveData,
	);
	const payload = normalizedPayload(buildSessionContext([...entries, entry], entry.id).messages);
	return {
		...laneTotal({
			primaryTokens: payload.tokens,
			primaryBytes: payload.bytes,
			maintenanceRequests: 0,
			maintenanceInputTokens: 0,
			maintenanceCacheReadTokens: 0,
			maintenanceOutputTokens: 0,
			maintenanceCostUsd: 0,
			deterministicFallbacks: 0,
			frameTokens: frames * snapcompact.FRAME_TOKEN_ESTIMATE,
		}),
		frames,
	};
}

interface RetryProbeResult {
	attempts: number;
	billedFailureUsageTokens: number;
	succeededAfterRetry: boolean;
	firstPassMs: number;
	retryPassMs: number;
	outcomes: Record<string, number>;
}

async function measureRetryProbe(projectRoot: string, storePath: string): Promise<RetryProbeResult> {
	const clock = { now: 1_850_000_000_000 };
	const manager = SessionManager.inMemory(projectRoot);
	for (let index = 0; index < 6; index++) {
		manager.appendMessage({
			role: "user",
			content: [{ type: "text", text: `Retry probe source ${index}: ${sourceText(9, index)}` }],
			timestamp: 1_700_000_000_000 + index,
		});
	}

	let call = 0;
	let billedFailureUsageTokens = 0;
	// Abort the foreground request only after schema-10 has durably settled the provider failure.
	const firstPass = new AbortController();
	const complete = async (request: LcmCompletionRequest): Promise<LcmCompletionResult> => {
		const attemptOrdinal = call + 1;
		const start: SummaryProviderAttemptStart = {
			attemptId: `retry-attempt-${attemptOrdinal}`,
			startedAt: clock.now,
			provider: "openai",
			model: MODEL.id,
		};
		request.onResolvedModel?.(`openai/${MODEL.id}`);
		if (request.onAttemptStart && !(await request.onAttemptStart(start))) {
			throw new LcmCompletionError("retry probe superseded before dispatch", {
				provider: "openai",
				category: "aborted",
			});
		}
		call = attemptOrdinal;
		const inputTokens = canonicalTokens(request.systemPrompt) + canonicalTokens(request.prompt);
		const attempt: SummaryProviderAttempt = {
			...start,
			completedAt: clock.now,
			usage: usageFor(inputTokens, MAINTENANCE_OUTPUT_TOKENS),
		};
		if (call === 1) {
			billedFailureUsageTokens = inputTokens + MAINTENANCE_OUTPUT_TOKENS;
			throw new LcmCompletionError("retry probe provider error", {
				provider: "openai",
				category: "provider_error",
				attempt,
			});
		}
		return { text: ".", attempt };
	};

	const lcm = new SessionLcm(
		{
			sessionManager: manager,
			projectionLimits: () => ({
				sourceTokens: 1_000,
				prewarmThresholdTokens: 1,
				hardThresholdTokens: 1,
				tokenBudget: 60_000,
				freshTail: { maxSources: 1, maxTokens: 2_000 },
			}),
			projectionTokenMeasurements,
			complete,
			resolveSummaryModel: () => `openai/${MODEL.id}`,
		},
		{
			summaryModel: "@smol",
			maxConcurrentSummaries: 1,
			dependencies: {
				openContext: async options => openLcmContext({ ...options, now: () => clock.now }),
				resolveProject: async () => ({ projectId: "lcm-scale-retry", rootPath: projectRoot, storePath }),
				now: () => clock.now,
			},
		},
	);

	let firstPassMs = 0;
	let retryPassMs = 0;
	let succeededAfterRetry = false;
	try {
		await lcm.status();
		const firstStarted = Bun.nanoseconds();
		const firstProjection = lcm.project(manager.buildSessionContext().messages, firstPass.signal);

		const failures = new Database(storePath, { readonly: true });
		let availableAt: number | undefined;
		try {
			for (let poll = 0; poll < 1_000; poll++) {
				availableAt = failures
					.query<{ available_at: number }, []>(
						"SELECT available_at FROM summary_jobs WHERE status = 'failed' ORDER BY available_at DESC LIMIT 1",
					)
					.get()?.available_at;
				if (availableAt !== undefined) break;
				await Promise.resolve();
			}
		} finally {
			failures.close();
		}
		firstPass.abort("retry probe observed the durable failure");
		await firstProjection;
		firstPassMs = (Bun.nanoseconds() - firstStarted) / 1e6;
		if (availableAt === undefined) {
			throw new Error("retry probe recorded no failed summary job, so no backoff could be advanced");
		}
		// Advance past the stored backoff and pump explicitly; the injected clock keeps
		// the intentional delay out of the measured retry latency.
		clock.now = availableAt + 1;
		await lcm.rebind();

		const retryStarted = Bun.nanoseconds();
		const retried = await lcm.project(manager.buildSessionContext().messages);
		retryPassMs = (Bun.nanoseconds() - retryStarted) / 1e6;
		succeededAfterRetry = retried.owned;
	} finally {
		await lcm.close();
	}

	const outcomes: Record<string, number> = {};
	let attempts = 0;
	const store = new Database(storePath, { readonly: true });
	try {
		for (const row of store
			.query<{ outcome: string; count: number }, []>(
				"SELECT outcome, COUNT(*) AS count FROM summary_attempts GROUP BY outcome",
			)
			.all()) {
			outcomes[row.outcome] = row.count;
			attempts += row.count;
		}
	} finally {
		store.close();
	}
	return { attempts, billedFailureUsageTokens, succeededAfterRetry, firstPassMs, retryPassMs, outcomes };
}

interface ScaleReport {
	version: string;
	fingerprint: string;
	knobs: Record<string, number>;
	timing: { singleBranch: TimingResult; multiBranch: TimingResult; latencyRatio: number };
	lcm: LcmCostResult;
	contextFull: LaneCost;
	snapcompact: LaneCost & { frames: number };
	retryProbe: RetryProbeResult;
}

function fingerprintOf(journalHash: string): string {
	return new Bun.CryptoHasher("sha256")
		.update(
			JSON.stringify({
				generator: GENERATOR_VERSION,
				serializer: SERIALIZER_VERSION,
				frameEstimate: snapcompact.FRAME_TOKEN_ESTIMATE,
				journal: journalHash,
				knobs: { SOURCES, BRANCHES, SAMPLES, LEAF_TOKENS, FAN_IN, TIMING_SOURCES_PER_BRANCH, LEAF_SOURCES },
				model: { id: MODEL.id, api: MODEL.api, contextWindow: MODEL.contextWindow, input: MODEL.input },
				compaction: COMPACTION_SETTINGS,
				cost: COST_PER_MILLION,
				runtime: Bun.version,
			}),
		)
		.digest("hex");
}

async function run(): Promise<ScaleReport> {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-scale-"));
	try {
		const projectRoot = path.join(root, "project");
		await fs.mkdir(projectRoot);
		const manager = createJournal(projectRoot);
		const entries = manager.getBranch();
		const journalHash = journalFingerprint(entries);

		const singleBranch = await measureTiming(path.join(root, "timing-single.sqlite"), 1);
		const multiBranch = await measureTiming(path.join(root, "timing-multi.sqlite"), BRANCHES);
		const lcm = await measureLcmCost(projectRoot, path.join(root, "cost.sqlite"), manager);

		const contextFullEntries = structuredClone(entries);
		const snapcompactEntries = structuredClone(entries);
		if (journalFingerprint(contextFullEntries) !== journalHash) throw new Error("context-full lane journal drifted");
		if (journalFingerprint(snapcompactEntries) !== journalHash) throw new Error("snapcompact lane journal drifted");

		const contextFull = await measureContextFull(contextFullEntries);
		const snapcompactLane = await measureSnapcompact(snapcompactEntries);
		const retryProbe = await measureRetryProbe(projectRoot, path.join(root, "retry.sqlite"));

		return {
			version: GENERATOR_VERSION,
			fingerprint: fingerprintOf(journalHash),
			knobs: { SOURCES, BRANCHES, SAMPLES, LEAF_TOKENS, FAN_IN, LEAF_SOURCES, TIMING_SOURCES_PER_BRANCH },
			timing: {
				singleBranch,
				multiBranch,
				latencyRatio:
					singleBranch.projectionWallP95Ms > 0
						? multiBranch.projectionWallP95Ms / singleBranch.projectionWallP95Ms
						: 1,
			},
			lcm,
			contextFull,
			snapcompact: snapcompactLane,
			retryProbe,
		};
	} finally {
		await fs.rm(root, { recursive: true, force: true });
	}
}

const report = await run();
const failures: string[] = [];
const requireInvariant = (condition: boolean, message: string): void => {
	if (!condition) failures.push(message);
};

// Reported for evidence: the same projection re-serialized the pre-change way.
const legacyByteReduction =
	report.lcm.legacyHistoricalBytes > 0 ? 1 - report.lcm.historicalBytes / report.lcm.legacyHistoricalBytes : 0;

requireInvariant(report.lcm.owned, "LCM projection did not own the request");
requireInvariant(
	report.lcm.orphanedAttempts === 0,
	`${report.lcm.orphanedAttempts} orphaned in-flight attempts remain`,
);
requireInvariant(
	report.lcm.dispatchedAttempts === report.lcm.lane.maintenanceRequests,
	`${report.lcm.dispatchedAttempts} dispatched attempts for ${report.lcm.lane.maintenanceRequests} maintenance requests`,
);
requireInvariant(report.timing.singleBranch.coverageFailures === 0, "single-branch projection has coverage failures");
requireInvariant(report.timing.multiBranch.coverageFailures === 0, "multi-branch projection has coverage failures");
requireInvariant(
	report.timing.singleBranch.duplicateCompletionHashes === 0 &&
		report.timing.multiBranch.duplicateCompletionHashes === 0,
	"a completion input hash was dispatched twice",
);
requireInvariant(
	report.retryProbe.attempts === 2,
	`retry probe recorded ${report.retryProbe.attempts} attempts, expected 2`,
);
requireInvariant(report.retryProbe.succeededAfterRetry, "retry probe never succeeded after the backoff advanced");
requireInvariant(
	(report.retryProbe.outcomes.provider_error ?? 0) === 1,
	"retry probe did not record exactly one billed provider_error attempt",
);
requireInvariant(report.retryProbe.billedFailureUsageTokens > 0, "retry probe failure carried no billed usage");

requireInvariant(
	report.timing.multiBranch.projectionWallP95Ms < 50,
	`multi-branch projection wall p95 ${report.timing.multiBranch.projectionWallP95Ms.toFixed(3)} ms is not below 50 ms`,
);
requireInvariant(
	report.timing.multiBranch.projectionCpuP95Ms < 50,
	`multi-branch projection CPU p95 ${report.timing.multiBranch.projectionCpuP95Ms.toFixed(3)} ms is not below 50 ms`,
);
requireInvariant(
	report.timing.multiBranch.completionToClaimP95Ms < 25,
	`completion-to-claim p95 ${report.timing.multiBranch.completionToClaimP95Ms.toFixed(3)} ms is not below 25 ms`,
);
requireInvariant(
	report.timing.latencyRatio <= 1.5,
	`multi/single projection latency ratio ${report.timing.latencyRatio.toFixed(3)} exceeds 1.5`,
);
requireInvariant(
	report.timing.multiBranch.projectionLineageRowsRead === 0,
	`projection read ${report.timing.multiBranch.projectionLineageRowsRead} lineage rows, expected 0`,
);
requireInvariant(
	report.timing.multiBranch.schedulerBranchPasses <= BRANCHES * 2,
	`scheduler branch passes ${report.timing.multiBranch.schedulerBranchPasses} exceed ${BRANCHES * 2} for ${BRANCHES} branches`,
);

// The 5x demand is calibrated to the reference workload only; the P3 tuning matrix
// deliberately moves these knobs, so a differing workload reports without asserting.
const onReferenceWorkload =
	SOURCES === PRE_CHANGE_REFERENCE.knobs.SOURCES &&
	BRANCHES === PRE_CHANGE_REFERENCE.knobs.BRANCHES &&
	SAMPLES === PRE_CHANGE_REFERENCE.knobs.SAMPLES &&
	LEAF_TOKENS === PRE_CHANGE_REFERENCE.knobs.LEAF_TOKENS &&
	FAN_IN === PRE_CHANGE_REFERENCE.knobs.FAN_IN &&
	LEAF_SOURCES === PRE_CHANGE_REFERENCE.knobs.LEAF_SOURCES;
if (onReferenceWorkload) {
	requireInvariant(
		report.timing.multiBranch.projectionWallP95Ms * REQUIRED_SPEEDUP <= PRE_CHANGE_REFERENCE.projectionWallP95Ms,
		`projection wall p95 ${report.timing.multiBranch.projectionWallP95Ms.toFixed(3)} ms is not ${REQUIRED_SPEEDUP}x better than the pre-change ${PRE_CHANGE_REFERENCE.projectionWallP95Ms} ms`,
	);
	requireInvariant(
		report.timing.multiBranch.projectionCpuP95Ms * REQUIRED_SPEEDUP <= PRE_CHANGE_REFERENCE.projectionCpuP95Ms,
		`projection CPU p95 ${report.timing.multiBranch.projectionCpuP95Ms.toFixed(3)} ms is not ${REQUIRED_SPEEDUP}x better than the pre-change ${PRE_CHANGE_REFERENCE.projectionCpuP95Ms} ms`,
	);
	requireInvariant(
		report.timing.multiBranch.schedulerBranchPasses * REQUIRED_SPEEDUP <= PRE_CHANGE_REFERENCE.schedulerBranchPasses,
		`scheduler branch passes ${report.timing.multiBranch.schedulerBranchPasses} is not ${REQUIRED_SPEEDUP}x better than the pre-change ${PRE_CHANGE_REFERENCE.schedulerBranchPasses}`,
	);
	requireInvariant(
		report.timing.latencyRatio < PRE_CHANGE_REFERENCE.projectionLatencyRatio,
		`projection latency ratio ${report.timing.latencyRatio.toFixed(3)} did not improve on the pre-change ${PRE_CHANGE_REFERENCE.projectionLatencyRatio}`,
	);
}
requireInvariant(
	report.lcm.legacyHistoricalBytes > 0 && report.lcm.historicalBytes < report.lcm.legacyHistoricalBytes,
	`provider-visible historical bytes did not fall below the legacy source-ID serialization (${report.lcm.historicalBytes} vs ${report.lcm.legacyHistoricalBytes})`,
);

console.log(`METRIC on_reference_workload=${onReferenceWorkload ? 1 : 0}`);

if (REPORT_OUT) {
	await Bun.write(REPORT_OUT, `${JSON.stringify(report, null, 2)}\n`);
	console.log(`\nReport written: ${REPORT_OUT}`);
}

console.log(
	`\nBenchmark: lcm-scale (sources=${SOURCES}, branches=${BRANCHES}, timing per-branch=${TIMING_SOURCES_PER_BRANCH}, samples=${SAMPLES}, leafTokens=${LEAF_TOKENS}, fanIn=${FAN_IN})\n`,
);
console.log(`METRIC fingerprint=${report.fingerprint}`);
console.log(`METRIC single_projection_wall_p95_ms=${report.timing.singleBranch.projectionWallP95Ms.toFixed(3)}`);
console.log(`METRIC multi_projection_wall_p95_ms=${report.timing.multiBranch.projectionWallP95Ms.toFixed(3)}`);
console.log(`METRIC multi_projection_cpu_p95_ms=${report.timing.multiBranch.projectionCpuP95Ms.toFixed(3)}`);
console.log(`METRIC projection_latency_ratio=${report.timing.latencyRatio.toFixed(3)}`);
console.log(`METRIC projection_lineage_rows_read=${report.timing.multiBranch.projectionLineageRowsRead}`);
console.log(`METRIC scheduler_branch_passes=${report.timing.multiBranch.schedulerBranchPasses}`);
console.log(`METRIC completion_to_claim_p95_ms=${report.timing.multiBranch.completionToClaimP95Ms.toFixed(3)}`);
console.log(`METRIC lcm_historical_bytes=${report.lcm.historicalBytes}`);
console.log(`METRIC lcm_legacy_historical_bytes=${report.lcm.legacyHistoricalBytes}`);
console.log(`METRIC lcm_legacy_byte_reduction=${(legacyByteReduction * 100).toFixed(1)}`);
console.log(`METRIC lcm_primary_tokens=${report.lcm.lane.primaryTokens}`);
console.log(`METRIC lcm_maintenance_requests=${report.lcm.lane.maintenanceRequests}`);
console.log(`METRIC lcm_maintenance_input_tokens=${report.lcm.lane.maintenanceInputTokens}`);
console.log(`METRIC lcm_maintenance_cache_read_tokens=${report.lcm.lane.maintenanceCacheReadTokens}`);
console.log(`METRIC lcm_maintenance_output_tokens=${report.lcm.lane.maintenanceOutputTokens}`);
console.log(`METRIC lcm_maintenance_cost_usd=${report.lcm.lane.maintenanceCostUsd.toFixed(6)}`);
console.log(`METRIC lcm_null_usage_attempts=${report.lcm.nullUsageAttempts}`);
console.log(`METRIC lcm_orphaned_attempts=${report.lcm.orphanedAttempts}`);
console.log(`METRIC lcm_total_tokens=${report.lcm.lane.totalTokens}`);
console.log(`METRIC context_full_primary_tokens=${report.contextFull.primaryTokens}`);
console.log(`METRIC context_full_maintenance_requests=${report.contextFull.maintenanceRequests}`);
console.log(`METRIC context_full_maintenance_input_tokens=${report.contextFull.maintenanceInputTokens}`);
console.log(`METRIC context_full_total_tokens=${report.contextFull.totalTokens}`);
console.log(`METRIC snapcompact_primary_tokens=${report.snapcompact.primaryTokens}`);
console.log(`METRIC snapcompact_frames=${report.snapcompact.frames}`);
console.log(`METRIC snapcompact_frame_tokens=${report.snapcompact.frameTokens}`);
console.log(`METRIC snapcompact_total_tokens=${report.snapcompact.totalTokens}`);
console.log(`METRIC retry_attempts=${report.retryProbe.attempts}`);
console.log(`METRIC retry_first_pass_ms=${report.retryProbe.firstPassMs.toFixed(3)}`);
console.log(`METRIC retry_pass_ms=${report.retryProbe.retryPassMs.toFixed(3)}`);
console.log(
	`METRIC ratio_lossless_over_context_full=${(report.lcm.lane.totalTokens / report.contextFull.totalTokens).toFixed(4)}`,
);
console.log(
	`METRIC ratio_lossless_over_snapcompact=${(report.lcm.lane.totalTokens / report.snapcompact.totalTokens).toFixed(4)}`,
);
console.log(
	`METRIC ratio_context_full_over_snapcompact=${(report.contextFull.totalTokens / report.snapcompact.totalTokens).toFixed(4)}`,
);
const breakEvenTurns = (nativePrimary: number): string => {
	const perTurnSaving = nativePrimary - report.lcm.lane.primaryTokens;
	if (perTurnSaving <= 0) return "never";
	const warmUp = report.lcm.lane.maintenanceInputTokens + report.lcm.lane.maintenanceOutputTokens;
	return String(Math.ceil(warmUp / perTurnSaving));
};
console.log(`METRIC break_even_turns_vs_context_full=${breakEvenTurns(report.contextFull.primaryTokens)}`);
console.log(
	`METRIC break_even_turns_vs_snapcompact=${breakEvenTurns(report.snapcompact.primaryTokens + report.snapcompact.frameTokens)}`,
);

if (failures.length > 0) throw new Error(`LCM scale benchmark failed:\n- ${failures.join("\n- ")}`);
