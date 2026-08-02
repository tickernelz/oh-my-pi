import * as path from "node:path";
import type {
	ContextProjection,
	ContextScope,
	DoctorReport,
	LcmContext,
	LcmFileMetadata,
	LcmRegexEngine,
	LcmStatus,
	PurgeResult,
	RebuildResult,
	SearchHit,
	SourceEntry,
	SourceSnapshot,
	SummaryFailureAttemptOutcome,
	SummaryJob,
	SummaryJobAvailability,
	SummaryJobLease,
	SummaryLocalAttemptOutcome,
	SummaryProviderAttempt,
	SummaryProviderAttemptStart,
	SummaryRetryPolicy,
} from "@oh-my-pi/lcm-context";
import { activeSourceFingerprint, isLcmSqliteContentionError, openLcmContext } from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import { estimateTokens } from "@oh-my-pi/pi-agent-core/compaction";
import * as AIError from "@oh-my-pi/pi-ai/error";
import { hasMatch, search as nativeSearch } from "@oh-my-pi/pi-natives";

import { logger, pathIsWithin, prompt, untilAborted } from "@oh-my-pi/pi-utils";
import type {
	LcmDescription,
	LcmExpandOptions,
	LcmHandle,
	LcmResolvedExpansion,
	LcmSearchOptions,
} from "../lcm/operations";
import { encodeLcmHandle } from "../lcm/operations";
import { type LcmProject, resolveLcmProject } from "../lcm/project-identity";
import lcmSummaryExcerptPrompt from "../prompts/lcm/summary-excerpt.md" with { type: "text" };
import lcmSummarySystemPrompt from "../prompts/lcm/summary-system.md" with { type: "text" };
import lcmSummaryUserPrompt from "../prompts/lcm/summary-user.md" with { type: "text" };
import type { SecretObfuscator } from "../secrets/obfuscator";
import { resolveReadPath } from "../tools/path-utils";
import { shortenPath } from "../tools/render-utils";
import { fileContentHash } from "../utils/file-content-hash";
import type { LcmFallbackCategory } from "./messages";
import {
	bashExecutionToText,
	convertToLlm,
	createBranchSummaryMessage,
	createCustomMessage,
	createHistoricalContextMessage,
	pythonExecutionToText,
} from "./messages";
import type { SessionContext } from "./session-context";
import type { SessionEntry } from "./session-entries";
import type { SessionManager } from "./session-manager";
import { sameMessageContent, sessionMessagePersistenceKey } from "./turn-persistence";

const SUMMARY_LEASE_MS = 10 * 60_000;
const SUMMARY_RETRY_DELAY_MS = 2_000;
const SUMMARY_MAX_OUTPUT_TOKENS = 2_048;
const SUMMARY_PROVIDER_RETRY_LIMIT = 5;
const SUMMARY_PROVIDER_ATTEMPT_TIMEOUT_MS = 5 * 60_000;
/** Mirrors `lcm-context`'s unexported `DEFAULT_LEAF_MAX_TOKENS`; the adapter must name its own default. */
const DEFAULT_LEAF_CHUNK_TOKENS = 4_000;
/** The project store is shared across processes and peer completions raise no local signal. */
const PEER_PROGRESS_POLL_MS = 1_000;
const SQLITE_CONTENTION_DELAYS_MS = [100, 200, 400] as const;
const MAX_TIMER_DELAY_MS = 2_147_483_647;

const ARTIFACT_REF_PATTERN = /(?:artifact:\/\/\d+|blob:sha256:[a-f0-9]{64})/g;
/** Caps on file handles spliced into the active context, per summary and per projection. */
const PROJECTED_FILES_PER_SUMMARY = 3;
const PROJECTED_FILES_TOTAL = 12;
const BOUNDED_SUMMARY_MARKER = lcmSummaryExcerptPrompt.trim();
const PROJECTION_GEOMETRIC_FRACTIONS = [1 / 8, 1 / 4, 1 / 2, 1] as const;
const PROJECTION_FIT_REFINEMENT_COUNT = 8;
export const MAX_LCM_PROJECTION_TOKEN_MEASUREMENTS =
	(PROJECTED_FILES_TOTAL + 1) * 2 + 1 + PROJECTION_GEOMETRIC_FRACTIONS.length + PROJECTION_FIT_REFINEMENT_COUNT;
export const MAX_LCM_PRIMARY_TOKEN_MEASUREMENTS = MAX_LCM_PROJECTION_TOKEN_MEASUREMENTS + 1;

/**
 * Rust-backed matcher handed to the derived store. Its engine is linear-time, so a
 * model-supplied pattern cannot stall a scan the way a backtracking `RegExp` could.
 */
const NATIVE_REGEX_ENGINE: LcmRegexEngine = {
	compile(pattern: string) {
		const probe = nativeSearch("", { pattern, maxCount: 1 });
		if (probe.error) throw new TypeError(`invalid regex: ${probe.error}`);
		return text => hasMatch(text, pattern, false, true);
	},
};

export type LcmRuntimeHealth = "disabled" | "uninitialized" | "healthy" | "degraded" | "quarantined";
export type LcmCoverageReadiness = "idle" | "warming" | "ready";
export type LcmProjectionFailureReason =
	| "coverage_gap"
	| "assembly_invalid"
	| "irreducible_input"
	| "minimum_representation"
	| "provider_key_mismatch"
	| "provider_exhausted"
	| "fit_invariant";

export type LcmProjectionFallback = {
	category: LcmFallbackCategory;
	reason?: LcmProjectionFailureReason;
};

export type LcmOwnershipDecision =
	| { kind: "owned"; projection: ContextProjection }
	| { kind: "native"; fallback?: LcmProjectionFallback }
	| { kind: "aborted" };
export type LcmCompletionErrorCategory =
	| "provider_error"
	| "transport_error"
	| "empty_output"
	| "provider_key_mismatch"
	| "aborted";

export class LcmCompletionError extends Error {
	readonly provider: string | undefined;
	readonly retryAfterMs: number | undefined;
	/** Safe measurement only: never a raw response, headers, prompt, or provider exception. */
	readonly attempt: SummaryProviderAttempt | undefined;
	readonly category: LcmCompletionErrorCategory;

	constructor(
		message: string,
		options: {
			provider?: string;
			retryAfterMs?: number;
			attempt?: SummaryProviderAttempt;
			category?: LcmCompletionErrorCategory;
			/** Original cancellation, rethrown verbatim by the public retrieval wrapper. */
			cause?: unknown;
		},
	) {
		super(message.slice(0, 2_048), ...(options.cause === undefined ? [] : [{ cause: options.cause }]));
		this.name = "LcmCompletionError";
		this.provider = options.provider?.slice(0, 128);
		const retryAfterMs = options.retryAfterMs;
		this.retryAfterMs =
			typeof retryAfterMs === "number" && Number.isFinite(retryAfterMs) && retryAfterMs >= 0
				? Math.min(Number.MAX_SAFE_INTEGER, Math.ceil(retryAfterMs))
				: undefined;
		this.attempt = options.attempt;
		this.category = options.category ?? "transport_error";
	}
}

export interface LcmCompletionResult {
	text: string;
	attempt: SummaryProviderAttempt;
}

export function normalizeLcmMaxConcurrentSummaries(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) && Number.isInteger(value)
		? Math.max(1, Math.min(4, value))
		: 1;
}

/** Leaf chunk sizes are clamped to measured presets: an arbitrary value has no benchmark behind it. */
export function normalizeLcmLeafChunkTokens(value: unknown): number {
	return value === 8_000 ? 8_000 : DEFAULT_LEAF_CHUNK_TOKENS;
}

export interface LcmProjectionAggregate {
	revision: number;
	sourceTokens: number;
	selectedLevelCounts: Readonly<Record<number, number>>;
	coveredSourceCount: number;
	uncoveredSourceCount: number;
	freshSourceCount: number;
	estimatedTokens: number;
	pendingJobs: number;
}

export type LcmProjectionState = "unevaluated" | "unfitted" | "fitted";

export interface LcmCurrentBranchStatus {
	projectId: string;
	sessionId: string;
	branchId: string;
	revision: number;
	activeSources: number;
	sourceTokens: number;
	projectionState: LcmProjectionState;
	projection?: LcmProjectionAggregate;
}

/** One arming-decision sample: the two token measures and the two thresholds they are judged against. */
export interface LcmProjectionPressure {
	/** Live request estimate. Shrinks on native compaction; decides takeover. */
	requestTokens: number;
	/** `max(requestTokens, branch total)`. Only grows; decides whether LCM engages at all. */
	armTokens: number;
	prewarmThresholdTokens: number;
	hardThresholdTokens: number;
}

export interface LcmRouteMetrics {
	observedAt: number;
	pressure?: LcmProjectionPressure;
	revision?: number;
	messageTokenBudget?: number;
	candidateTokens?: number;
	projectionTokenMeasurements?: number;
	projection?: LcmProjectionAggregate;
}

export type LcmPrimaryRequestRoute =
	| { kind: "lossless"; metrics: LcmRouteMetrics }
	| {
			kind: "native_passthrough";
			reason: "below_prewarm" | "below_hard" | "unavailable";
			metrics: LcmRouteMetrics;
	  }
	| {
			kind: "native_fallback";
			category: LcmFallbackCategory;
			reason?: LcmProjectionFailureReason;
			metrics: LcmRouteMetrics;
	  };

export interface LcmRuntimeStatus {
	health: LcmRuntimeHealth;
	coverageReadiness?: LcmCoverageReadiness;
	summaryWorkers: { active: number; limit: number };
	/** Live request pressure sampled at the most recent projection attempt. Absent before the first one. */
	pressure?: LcmProjectionPressure;
	summaryBackoff?: { preferred?: number; fallback?: number };
	summaryModelSelector?: string;
	resolvedSummaryModel?: string;
	currentBranch?: LcmCurrentBranchStatus;
	lastRequestRoute?: LcmPrimaryRequestRoute;
	lastTakeover?: LcmRouteMetrics;
	lastFailure?: {
		observedAt: number;
		category: LcmFallbackCategory;
		reason?: LcmProjectionFailureReason;
	};
	retryAt?: number;
}

export type LcmPublicStoreStatus = Pick<
	LcmStatus,
	| "schemaVersion"
	| "journalMode"
	| "quarantined"
	| "branches"
	| "activeSources"
	| "tombstones"
	| "leafSummaries"
	| "condensedSummaries"
	| "jobs"
	| "storage"
	| "latestRecovery"
>;

export interface LcmPublicStatus {
	runtime: LcmRuntimeStatus;
	store?: LcmPublicStoreStatus;
}

export interface LcmCompletionRequest {
	/** Stable opaque identity shared by transport attempts for one durable summary-job epoch. */
	providerSessionKey?: string;
	/** Stable durable job identity used to retire state from superseded retry epochs. */
	providerSessionFamilyKey?: string;
	/** Keep learned transport compatibility state only when this failure can durably retry. */
	retainProviderStateOnFailure?: boolean;
	systemPrompt: string;
	prompt: string;
	maxOutputTokens: number;
	oneshotKind: "lcm_summary" | "lcm_recall";
	modelSelector?: string;
	signal?: AbortSignal;
	/** Reports the concrete provider/model selected for this isolated request. */
	onResolvedModel?: (model: string) => void;
	/**
	 * Fences the dispatch: resolved once the provider/model is known and immediately
	 * before the request leaves. Returning false must dispatch nothing.
	 */
	onAttemptStart?: (attempt: SummaryProviderAttemptStart) => Promise<boolean>;
}

export interface LcmProjectionLimits {
	/** Total native request estimate, including stable non-message inputs. */
	sourceTokens: number;
	/** Only asynchronous gate: background summary work starts here. */
	prewarmThresholdTokens: number;
	hardThresholdTokens: number;
	tokenBudget: number;
	freshTail: {
		maxSources: number;
		maxTokens: number;
	};
}

export type SessionLcmJournal = Pick<
	SessionManager,
	| "buildSessionContext"
	| "getBranch"
	| "getChildren"
	| "getCwd"
	| "getLeafId"
	| "getSessionFile"
	| "getSessionDir"
	| "getSessionId"
	| "subscribeToDurableEntries"
>;

/** Narrow coding-agent capabilities consumed by the derived LCM lifecycle. */
export interface SessionLcmHost {
	sessionManager: SessionLcmJournal;
	obfuscator?: Pick<SecretObfuscator, "hasSecrets" | "obfuscate">;
	projectionLimits(messages: readonly AgentMessage[]): LcmProjectionLimits | undefined;
	projectionTokenMeasurements(messages: readonly AgentMessage[]): { tokens: number; upperBound: number };
	complete(request: LcmCompletionRequest): Promise<LcmCompletionResult>;
	resolveSummaryModel?(selector: string): string;
}

export interface SessionLcmDependencies {
	openContext?: typeof openLcmContext;
	resolveProject?: typeof resolveLcmProject;
	peerPollMs?: number;
	providerAttemptTimeoutMs?: number;
	now?: () => number;
	/** Observation-only hook for focused measurement-budget tests and benches. */
	onProjectionTokenMeasurement?: () => void;
}

export interface SessionLcmOptions {
	agentDir?: string;
	summaryModel?: string;
	maxConcurrentSummaries?: number;
	leafChunkTokens?: number;
	registerProject?: (project: LcmProject, journal: { sessionDir: string; sessionFile?: string }) => Promise<void>;
	dependencies?: SessionLcmDependencies;
}

interface LcmProjectBinding {
	projectId: string;
	rootPath: string;
	storePath: string;
}

interface ActiveFileReference {
	path: string;
	byteSize: number;
	contentHash: string;
}

interface ActiveFileReferenceCollector {
	cwd: string;
	projectRoot: string;
	references: Map<string, ActiveFileReference>;
}

interface NormalizedBranch {
	snapshot: SourceSnapshot;
	ordered: Array<{ source: SourceEntry; message: AgentMessage }>;
	firstUserSourceId: string | undefined;
	anchor: string;
	fileReferences: Map<string, ActiveFileReference>;
}

export interface LcmPrimaryRouteKey {
	readonly generation: number;
	readonly projectionAttempt: number;
	readonly sessionId: string;
	readonly scope?: Readonly<{
		projectId: string;
		branchId: string;
		inputAnchor: string;
		revision?: number;
	}>;
}

type LcmPrimaryRouteScope = NonNullable<LcmPrimaryRouteKey["scope"]>;

interface LcmPrimaryRouteScopeSource {
	generation: number;
	projectionAttempt: number;
	sessionId: string;
	cwd: string;
	branchId: string;
	inputAnchor: string;
	fallbackProjectId: string;
	project?: LcmProjectBinding;
}

type ProjectionFailure = { category: LcmFallbackCategory; reason?: LcmProjectionFailureReason };

interface PendingPrimaryRoute {
	key: LcmPrimaryRouteKey;
	route: LcmPrimaryRequestRoute;
}

interface PrimaryPressureIntent {
	generation: number;
	baseMessageFingerprint: string;
	baseMessageCount: number;
	requestTokensFloor: number;
}

export interface SessionLcmProjectResult {
	messages: AgentMessage[];
	owned: boolean;
	/** Present only when a complete, locally fitted projection owns the request. */
	projection?: ContextProjection;
	/** Opaque staged-route key consumed only by the immediately following primary dispatch. */
	routeKey?: LcmPrimaryRouteKey;
	/** Exact fitted message tokens already measured by SessionLcm. */
	candidateTokens?: number;
	/** LCM-controlled message budget used for this fitted render. */
	messageTokenBudget?: number;
	/** Real provider-representation measurements consumed by this projection attempt. */
	projectionTokenMeasurements?: number;
}

type ProjectAttempt = SessionLcmProjectResult & {
	maintenanceFallback?: LcmProjectionFallback;
	aborted?: true;
};

interface ProjectionCheck {
	result: ProjectAttempt;
	projection?: ContextProjection;
	terminal?: { category: LcmFallbackCategory; reason?: LcmProjectionFailureReason };
	candidateTokens?: number;
	measurementCount?: number;
}

type SummaryQueueClass = SummaryJob["queueClass"];

type SummaryWorkerOutcome =
	| {
			status: "completed" | "escalated" | "stale" | "lease_lost" | "unfit" | "aborted" | "provider_failed";
			jobId: string;
			queueClass: SummaryQueueClass;
	  }
	| { status: "store_failed"; jobId: string; queueClass: SummaryQueueClass; error: unknown };

interface ProjectionRequest {
	limits: LcmProjectionLimits;
	/** `max(live request, branch total)` as evaluated when LCM armed; see `#attemptProjection`. */
	armTokens: number;
}

function collectArtifactRefs(value: unknown, refs: Set<string>, depth = 0): void {
	if (depth > 12 || value == null) return;
	if (typeof value === "string") {
		if (!value.includes("artifact://") && !value.includes("blob:sha256:")) return;
		for (const match of value.matchAll(ARTIFACT_REF_PATTERN)) {
			const ref = match[0];
			if (ref) refs.add(ref);
		}
		return;
	}
	if (Array.isArray(value)) {
		for (const item of value) collectArtifactRefs(item, refs, depth + 1);
		return;
	}
	if (typeof value !== "object") return;
	for (const child of Object.values(value)) collectArtifactRefs(child, refs, depth + 1);
}

function stableValue(value: unknown, refs: Set<string>): unknown {
	if (value == null || typeof value === "boolean" || typeof value === "number") return value;
	if (typeof value === "string") {
		collectArtifactRefs(value, refs);
		return value;
	}
	if (typeof value === "bigint") return value.toString();
	if (Array.isArray(value)) return value.map(item => stableValue(item, refs));
	if (typeof value !== "object") return String(value);

	const output: Record<string, unknown> = {};
	for (const key of Object.keys(value as Record<string, unknown>).sort()) {
		const child = (value as Record<string, unknown>)[key];
		if (child === undefined) continue;
		output[key] = stableValue(child, refs);
	}
	return output;
}

function stableStringify(value: unknown): string {
	return JSON.stringify(value) ?? "null";
}

function primaryPressureFingerprint(messages: readonly AgentMessage[], messageCount = messages.length): string {
	const hash = new Bun.CryptoHasher("sha256");
	hash.update("omp:lcm-primary-pressure-intent:v1");
	for (let index = 0; index < messageCount; index++) {
		const serialized = stableStringify(stableValue(messages[index]!, new Set()));
		hash.update(`\0${Buffer.byteLength(serialized, "utf8")}\0`);
		hash.update(serialized);
	}
	return hash.digest("hex");
}

function sanitizedContent(content: unknown, refs: Set<string>): unknown[] {
	if (typeof content === "string") {
		collectArtifactRefs(content, refs);
		return [{ type: "text", text: content }];
	}
	if (!Array.isArray(content)) return [];

	const normalized: unknown[] = [];
	for (const rawBlock of content) {
		if (!rawBlock || typeof rawBlock !== "object") continue;
		const block = rawBlock as Record<string, unknown>;
		switch (block.type) {
			case "text":
				if (typeof block.text === "string") {
					collectArtifactRefs(block.text, refs);
					normalized.push({ type: "text", text: block.text });
				}
				break;
			case "thinking":
				if (typeof block.thinking === "string") {
					collectArtifactRefs(block.thinking, refs);
					normalized.push({ type: "thinking", thinking: block.thinking });
				}
				break;
			case "toolCall":
				normalized.push({
					type: "toolCall",
					id: typeof block.id === "string" ? block.id : "",
					name: typeof block.name === "string" ? block.name : "",
					arguments: stableValue(block.arguments, refs),
				});
				break;
			case "image": {
				const data = typeof block.data === "string" ? block.data : "";
				collectArtifactRefs(data, refs);
				normalized.push({
					type: "image",
					mimeType: typeof block.mimeType === "string" ? block.mimeType : "application/octet-stream",
					...(data.startsWith("blob:sha256:") ? { ref: data } : {}),
				});
				break;
			}
			// redactedThinking and unknown provider blocks are opaque wire state,
			// not LLM-visible source text. Their recognized artifact refs were
			// collected from the entry separately.
			default:
				break;
		}
	}
	return normalized;
}

function assistantToolCallIds(message: AgentMessage): string[] {
	if (message.role !== "assistant") return [];
	const ids: string[] = [];
	for (const block of message.content) {
		if (block.type === "toolCall") ids.push(block.id);
	}
	return ids;
}

function serializeMessage(message: AgentMessage, refs: Set<string>): string | undefined {
	switch (message.role) {
		case "user":
		case "developer":
			return stableStringify({
				role: message.role,
				attribution: message.attribution ?? (message.role === "user" ? "user" : "agent"),
				content: sanitizedContent(message.content, refs),
			});
		case "assistant":
			if (message.stopReason === "error" || message.stopReason === "aborted") return undefined;
			return stableStringify({ role: "assistant", content: sanitizedContent(message.content, refs) });
		case "toolResult":
			return stableStringify({
				role: "toolResult",
				toolCallId: message.toolCallId,
				toolName: message.toolName,
				isError: message.isError === true,
				content: sanitizedContent(message.content, refs),
			});
		case "bashExecution": {
			if (message.excludeFromContext) return undefined;
			const text = bashExecutionToText(message);
			collectArtifactRefs(text, refs);
			return stableStringify({ role: "user", kind: "bashExecution", text });
		}
		case "pythonExecution": {
			if (message.excludeFromContext) return undefined;
			const text = pythonExecutionToText(message);
			collectArtifactRefs(text, refs);
			return stableStringify({ role: "user", kind: "pythonExecution", text });
		}
		case "custom":
		case "hookMessage":
			return stableStringify({
				role: message.role,
				customType: message.customType,
				attribution: message.attribution ?? "agent",
				content: sanitizedContent(message.content, refs),
			});
		case "fileMention":
			return stableStringify({
				role: "fileMention",
				files: message.files.map(file => ({
					path: file.path,
					content: file.skippedReason
						? `[reference-only: ${file.skippedReason}; ${file.byteSize ?? 0} bytes]`
						: file.content,
					byteSize: file.byteSize,
					contentHash: file.contentHash,
					skippedReason: file.skippedReason,
					image: file.image ? { type: "image", mimeType: file.image.mimeType } : undefined,
				})),
			});
		case "branchSummary":
			return stableStringify({ role: "branchSummary", summary: message.summary });
		case "compactionSummary":
			return stableStringify({ role: "compactionSummary", summary: message.summary });
		case "historicalContext":
			return undefined;
		default:
			return undefined;
	}
}

/**
 * File metadata for one `fileMention`. Covers files whose bytes were skipped entirely and
 * large files that were auto-read but registered so their identity survives compaction.
 * Gated on `explorationSummary`, not on `contentHash`: skipped binaries hash too.
 */
function mentionFileMetadata(
	message: Extract<AgentMessage, { role: "fileMention" }>,
	projectId: string,
	redact: (text: string) => string,
	activeFiles?: ActiveFileReferenceCollector,
): LcmFileMetadata[] {
	const metadata: LcmFileMetadata[] = [];
	for (const file of message.files) {
		if (!file.skippedReason && file.explorationSummary === undefined) continue;
		const safePath = redact(file.path);
		const byteSize = Math.max(0, file.byteSize ?? 0);
		const contentHash =
			file.contentHash ??
			new Bun.CryptoHasher("sha256")
				.update(`legacy-reference\0${safePath}\0${byteSize}\0${file.skippedReason ?? "tracked"}`)
				.digest("hex");
		const fileId = `file_${new Bun.CryptoHasher("sha256")
			.update(`${projectId}\0${safePath}\0${contentHash}`)
			.digest("hex")}`;
		metadata.push({
			fileId,
			contentHash,
			path: safePath,
			fileType: file.image?.mimeType ?? (path.extname(safePath).slice(1).toLowerCase() || "binary"),
			byteSize,
			tokenCount: Math.ceil(byteSize / 4),
			// Redacted: the dispatcher embeds real file content (keys, headers, declarations).
			explorationSummary: redact(
				file.explorationSummary ??
					`Reference-only ${file.skippedReason === "tooLarge" ? "oversized" : "binary"} file; bytes remain outside the LCM store.`,
			),
		});
		if (activeFiles) {
			const originalPath = path.resolve(resolveReadPath(file.path, activeFiles.cwd));
			if (pathIsWithin(activeFiles.projectRoot, originalPath)) {
				activeFiles.references.set(fileId, { path: originalPath, byteSize, contentHash });
			}
		}
	}
	return metadata;
}

function entryMessage(entry: SessionEntry): AgentMessage | undefined {
	switch (entry.type) {
		case "message":
			return entry.message;
		case "custom_message":
			return createCustomMessage(
				entry.customType,
				entry.content,
				entry.display,
				entry.details,
				entry.timestamp,
				entry.attribution,
			);
		case "branch_summary":
			return entry.summary ? createBranchSummaryMessage(entry.summary, entry.fromId, entry.timestamp) : undefined;
		default:
			return undefined;
	}
}

function branchId(manager: SessionLcmJournal, entries: readonly SessionEntry[]): string {
	let id = entries[0]?.id ?? `empty:${manager.getSessionId()}`;
	for (const entry of entries) {
		if (entry.parentId && manager.getChildren(entry.parentId).length > 1) id = entry.id;
	}
	return id;
}

function branchAnchor(manager: SessionLcmJournal, entries: readonly SessionEntry[]): string {
	return [
		manager.getCwd(),
		manager.getSessionFile() ?? "",
		manager.getSessionId(),
		manager.getLeafId() ?? "",
		entries.length,
		entries.at(-1)?.id ?? "",
	].join("\0");
}

function fallbackRouteProjectId(cwd: string): string {
	const digest = new Bun.CryptoHasher("sha256").update(`omp-lcm-route-project:v1\0${path.resolve(cwd)}`).digest("hex");
	return `route-v1-${digest}`;
}

function messageIdentity(message: AgentMessage): string {
	const persisted = sessionMessagePersistenceKey(message);
	if (persisted) return persisted;
	const timestamp = "timestamp" in message ? message.timestamp : 0;
	switch (message.role) {
		case "custom":
		case "hookMessage":
			return `${message.role}:${timestamp}:${message.customType}`;
		case "branchSummary":
			return `${message.role}:${timestamp}:${message.fromId}`;
		case "compactionSummary":
		case "bashExecution":
		case "pythonExecution":
		case "historicalContext":
			return `${message.role}:${timestamp}`;
		default:
			return `${message.role}:${timestamp}`;
	}
}

/** Estimate the provider-visible cost of one message in an LCM projection. */
export function estimateLcmProjectionMessageTokens(message: AgentMessage): number {
	if (message.role === "developer") return estimateTokens({ ...message, role: "user" });
	if (message.role !== "historicalContext") return estimateTokens(message);
	const providerMessage = convertToLlm([message])[0];
	return providerMessage ? estimateTokens(providerMessage) : Number.POSITIVE_INFINITY;
}

export function estimateLcmProjectionMessageTokenUpperBound(message: AgentMessage): number {
	if (message.role !== "historicalContext") return estimateLcmProjectionMessageTokens(message);
	const providerMessages = convertToLlm([message]);
	if (providerMessages.length === 0) return Number.POSITIVE_INFINITY;
	let bytes = 0;
	for (const providerMessage of providerMessages) {
		if (typeof providerMessage.content === "string") {
			bytes += Buffer.byteLength(providerMessage.content, "utf8");
			continue;
		}
		for (const block of providerMessage.content) {
			if (block.type === "text") bytes += Buffer.byteLength(block.text, "utf8");
		}
	}
	return bytes;
}

function liveSuffix(messages: readonly AgentMessage[], persisted: SessionContext): AgentMessage[] {
	if (messages.length === 0) return [];
	let persistedIndex = persisted.messages.length - 1;
	for (let inputIndex = messages.length - 1; inputIndex >= 0; inputIndex--) {
		const inputMessage = messages[inputIndex]!;
		const key = messageIdentity(inputMessage);
		const requiresContentMatch = sessionMessagePersistenceKey(inputMessage) !== undefined;
		while (persistedIndex >= 0) {
			const persistedMessage = persisted.messages[persistedIndex]!;
			if (
				messageIdentity(persistedMessage) === key &&
				(!requiresContentMatch || sameMessageContent(persistedMessage, inputMessage))
			) {
				return messages.slice(inputIndex + 1);
			}
			persistedIndex--;
		}
	}
	return [...messages];
}

interface HistoricalRenderLimits {
	maxSummaryTextBytes?: number;
	maxFiles: number;
}

function historicalSummaryExcerpts(projection: ContextProjection, maxTextBytes: number): string[] {
	const sizes = projection.historical.map(item => Buffer.byteLength(item.redactedText, "utf8"));
	const totalBytes = sizes.reduce((total, size) => total + size, 0);
	let remaining = Math.max(0, Math.min(Math.floor(maxTextBytes), totalBytes));
	if (remaining === 0) return sizes.map(() => "");
	const ordered = sizes.map((bytes, index) => ({ bytes, index })).sort((left, right) => left.bytes - right.bytes);
	let level = 0;
	let active = ordered.length;
	let cursor = 0;
	while (cursor < ordered.length && active > 0) {
		const next = ordered[cursor]!.bytes;
		const cost = (next - level) * active;
		if (cost > remaining) break;
		remaining -= cost;
		level = next;
		while (cursor < ordered.length && ordered[cursor]!.bytes === next) {
			cursor++;
			active--;
		}
	}
	const share = active > 0 ? Math.floor(remaining / active) : 0;
	let remainder = active > 0 ? remaining % active : 0;
	const allocations = sizes.map(size => {
		if (size <= level) return size;
		const extra = remainder > 0 ? 1 : 0;
		if (remainder > 0) remainder--;
		return Math.min(size, level + share + extra);
	});
	return projection.historical.map((item, index) => utf8HeadTail(item.redactedText, allocations[index]!));
}

function historicalText(projection: ContextProjection, scope: ContextScope, limits?: HistoricalRenderLimits): string {
	const maxSummaryTextBytes = limits?.maxSummaryTextBytes;
	const bounded = maxSummaryTextBytes !== undefined;
	const excerpts = bounded ? historicalSummaryExcerpts(projection, maxSummaryTextBytes) : undefined;
	let fileBudget = Math.max(0, Math.min(PROJECTED_FILES_TOTAL, Math.floor(limits?.maxFiles ?? PROJECTED_FILES_TOTAL)));
	return projection.historical
		.map((item, index) => {
			const handle = encodeLcmHandle({
				kind: "summary",
				reference: { ...scope, summaryHandle: item.summaryHandle },
			});
			const excerpt = excerpts?.[index] ?? item.redactedText;
			const summary = bounded
				? `${BOUNDED_SUMMARY_MARKER}\n${excerpt ? `${excerpt}\n` : ""}[Summary: ${handle}]`
				: `${excerpt}\n[Summary: ${handle}]`;
			const allowed = Math.min(item.files.length, PROJECTED_FILES_PER_SUMMARY, fileBudget);
			if (allowed <= 0) return summary;
			fileBudget -= allowed;
			const rendered = item.files
				.slice(0, allowed)
				.map(
					file =>
						`${shortenPath(file.path)} ${encodeLcmHandle({
							kind: "file",
							reference: { ...scope, fileId: file.fileId },
						})}`,
				)
				.join(" | ");
			const omitted = item.files.length - allowed;
			return `${summary}\n[Files: ${rendered}${omitted > 0 ? ` (+${omitted} more)` : ""}]`;
		})
		.join("\n\n");
}

function errorLabel(error: unknown): string {
	return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function isStructuredAbortError(error: unknown): boolean {
	const seen = new Set<object>();
	let current = error;
	while (current && typeof current === "object" && !seen.has(current)) {
		seen.add(current);
		const errorId = "errorId" in current && typeof current.errorId === "number" ? current.errorId : undefined;
		if (AIError.is(errorId, AIError.Flag.Abort)) return true;
		current = "cause" in current ? current.cause : undefined;
	}
	return false;
}

function projectionAggregate(projection: ContextProjection): LcmProjectionAggregate {
	return {
		revision: projection.revision,
		sourceTokens: projection.sourceTokens,
		selectedLevelCounts: projection.selectedLevelCounts,
		coveredSourceCount: projection.coveredSourceCount,
		uncoveredSourceCount: projection.uncoveredSourceIds.length,
		freshSourceCount: projection.freshSourceCount,
		estimatedTokens: projection.estimatedTokens,
		pendingJobs: projection.pendingJobs,
	};
}

function hasExactProjectionCoverage(projection: ContextProjection, activeSources: readonly SourceEntry[]): boolean {
	if (!projection.ready || projection.uncoveredSourceIds.length > 0) return false;
	const seen = new Set<string>();
	let historicalCount = 0;
	let freshCount = 0;
	let duplicate = false;
	const frontierFingerprint = activeSourceFingerprint(
		(function* () {
			for (const item of projection.historical) {
				for (const sourceId of item.sourceIds) {
					historicalCount++;
					if (seen.has(sourceId)) duplicate = true;
					seen.add(sourceId);
					yield sourceId;
				}
			}
			for (const sourceId of projection.freshTailSourceIds) {
				freshCount++;
				if (seen.has(sourceId)) duplicate = true;
				seen.add(sourceId);
				yield sourceId;
			}
		})(),
	);
	const snapshotFingerprint = activeSourceFingerprint(
		(function* () {
			for (const source of activeSources) yield source.entryId;
		})(),
	);
	return (
		!duplicate &&
		historicalCount === projection.coveredSourceCount &&
		freshCount === projection.freshSourceCount &&
		historicalCount + freshCount === activeSources.length &&
		seen.size === activeSources.length &&
		frontierFingerprint === projection.activeSourceFingerprint &&
		projection.activeSourceFingerprint === snapshotFingerprint
	);
}

function normalizedSourceTokens(snapshot: SourceSnapshot): number {
	return snapshot.entries.reduce(
		(total, entry) => total + Math.ceil(Buffer.byteLength(entry.redactedText, "utf8") / 4),
		0,
	);
}

function summaryPromptHash(systemPrompt: string, userPrompt: string): string {
	return new Bun.CryptoHasher("sha256").update(systemPrompt).update("\0").update(userPrompt).digest("hex");
}

function utf8Prefix(text: string, maxBytes: number): string {
	if (maxBytes <= 0) return "";
	if (Buffer.byteLength(text, "utf8") <= maxBytes) return text;
	let low = 0;
	let high = text.length;
	while (low < high) {
		const middle = Math.ceil((low + high) / 2);
		if (Buffer.byteLength(text.slice(0, middle), "utf8") <= maxBytes) low = middle;
		else high = middle - 1;
	}
	if (low > 0 && /[\uD800-\uDBFF]/.test(text[low - 1]!)) low--;
	return text.slice(0, low);
}

function utf8Suffix(text: string, maxBytes: number): string {
	if (maxBytes <= 0) return "";
	if (Buffer.byteLength(text, "utf8") <= maxBytes) return text;
	let low = 0;
	let high = text.length;
	while (low < high) {
		const middle = Math.floor((low + high) / 2);
		if (Buffer.byteLength(text.slice(middle), "utf8") <= maxBytes) high = middle;
		else low = middle + 1;
	}
	const code = text.charCodeAt(low);
	if (code >= 0xdc00 && code <= 0xdfff) low++;
	return text.slice(low);
}

function utf8HeadTail(text: string, maxTextBytes: number): string {
	const maxBytes = Math.max(0, Math.floor(maxTextBytes));
	if (maxBytes === 0) return "";
	if (Buffer.byteLength(text, "utf8") <= maxBytes) return text;
	const separator = "\n…\n";
	const separatorBytes = Buffer.byteLength(separator, "utf8");
	if (maxBytes <= separatorBytes) {
		const head = utf8Prefix(text, Math.ceil(maxBytes / 2));
		return head + utf8Suffix(text, maxBytes - Buffer.byteLength(head, "utf8"));
	}
	const contentBytes = maxBytes - separatorBytes;
	const firstBreak = text.indexOf("\n");
	const lastBreak = text.lastIndexOf("\n");
	const firstLineBytes = Buffer.byteLength(firstBreak >= 0 ? text.slice(0, firstBreak) : text, "utf8");
	const lastLineBytes = Buffer.byteLength(lastBreak >= 0 ? text.slice(lastBreak + 1) : text, "utf8");
	let headBytes: number;
	let tailBytes: number;
	if (firstBreak >= 0 && lastBreak > firstBreak && firstLineBytes + lastLineBytes <= contentBytes) {
		const extra = contentBytes - firstLineBytes - lastLineBytes;
		headBytes = firstLineBytes + Math.ceil(extra / 2);
		tailBytes = lastLineBytes + Math.floor(extra / 2);
	} else {
		headBytes = Math.ceil(contentBytes / 2);
		tailBytes = Math.floor(contentBytes / 2);
	}
	return `${utf8Prefix(text, headBytes)}${separator}${utf8Suffix(text, tailBytes)}`;
}

function deterministicSummary(job: SummaryJob): string {
	const byteBudget = job.outputTokenBudget * 4;
	let remaining = byteBudget;
	const parts: string[] = [];
	for (const input of job.inputs) {
		if (remaining <= 0) break;
		if (parts.length > 0) {
			const separator = remaining >= 2 ? "\n\n" : "";
			parts.push(separator);
			remaining -= Buffer.byteLength(separator, "utf8");
		}
		const part = utf8Prefix(input.redactedText, remaining);
		parts.push(part);
		remaining -= Buffer.byteLength(part, "utf8");
	}
	const output = parts.join("");
	return output.trim().length > 0 ? output : ".";
}

function normalizeLcmBranchState(
	manager: SessionLcmJournal,
	projectId: string,
	redact: (text: string) => string,
	rootPath?: string,
): NormalizedBranch {
	const entries = manager.getBranch();
	const scope = {
		projectId,
		sessionId: manager.getSessionId(),
		branchId: branchId(manager, entries),
	};
	const ordered: NormalizedBranch["ordered"] = [];
	const sources: SourceEntry[] = [];
	const pendingToolGroups = new Map<string, string>();
	let firstUserSourceId: string | undefined;
	const fileReferences = new Map<string, ActiveFileReference>();
	const activeFiles = rootPath
		? { cwd: manager.getCwd(), projectRoot: path.resolve(rootPath), references: fileReferences }
		: undefined;

	for (const entry of entries) {
		const message = entryMessage(entry);
		if (!message) continue;

		let atomicGroupId: string | undefined;
		if (message.role === "assistant") {
			if (message.stopReason === "error" || message.stopReason === "aborted") continue;
			const toolCallIds = assistantToolCallIds(message);
			if (toolCallIds.length > 0) {
				atomicGroupId = `tool:${entry.id}`;
				for (const toolCallId of toolCallIds) pendingToolGroups.set(toolCallId, atomicGroupId);
			}
		} else if (message.role === "toolResult") {
			atomicGroupId = pendingToolGroups.get(message.toolCallId);
			if (!atomicGroupId) continue;
			pendingToolGroups.delete(message.toolCallId);
		}

		const artifactRefs = new Set<string>();
		collectArtifactRefs(entry, artifactRefs);
		const serialized = serializeMessage(message, artifactRefs);
		if (serialized === undefined) continue;
		const redactedText = redact(serialized);
		const contentHash = new Bun.CryptoHasher("sha256").update(redactedText).digest("hex");
		const files = message.role === "fileMention" ? mentionFileMetadata(message, projectId, redact, activeFiles) : [];
		const source: SourceEntry = {
			...scope,
			entryId: entry.id,
			parentId: entry.parentId,
			timestamp: new Date(entry.timestamp).getTime(),
			kind: entry.type === "message" ? `message:${message.role}` : entry.type,
			...(atomicGroupId ? { atomicGroupId } : {}),
			redactedText,
			contentHash,
			artifactRefs: [...artifactRefs].sort(),
			...(files.length > 0 ? { files } : {}),
		};
		sources.push(source);
		ordered.push({ source, message });
		if (!firstUserSourceId && message.role === "user") firstUserSourceId = source.entryId;
	}

	return {
		snapshot: { scope, entries: sources },
		ordered,
		firstUserSourceId,
		anchor: branchAnchor(manager, entries),
		fileReferences,
	};
}

export function normalizeLcmBranch(
	journal: SessionLcmJournal,
	projectId: string,
	redact: (text: string) => string,
): SourceSnapshot {
	return normalizeLcmBranchState(journal, projectId, redact).snapshot;
}

/**
 * Owns the coding-agent side of Lossless Context Management. The session journal
 * remains authoritative; this class only maintains and queries a rebuildable,
 * redacted per-project projection.
 */
export class SessionLcm {
	readonly #host: SessionLcmHost;
	readonly #agentDir: string | undefined;
	#summaryModel: string | undefined;
	#maxConcurrentSummaries: number;
	#leafChunkTokens: number;
	readonly #openContext: typeof openLcmContext;
	readonly #resolveProject: typeof resolveLcmProject;
	readonly #registerProject:
		| ((project: LcmProject, journal: { sessionDir: string; sessionFile?: string }) => Promise<void>)
		| undefined;
	readonly #peerPollMs: number;
	readonly #now: () => number;
	readonly #providerAttemptTimeoutMs: number;
	readonly #spendEpoch: number;
	#priorSpendUsd = 0;
	readonly #workerId = `omp-lcm:${Bun.randomUUIDv7()}`;

	#context: LcmContext | undefined;
	#project: LcmProjectBinding | undefined;
	#boundCwd: string | undefined;
	#activeBranch: NormalizedBranch | undefined;
	#dirty = true;
	#activeFileReferences = new Map<string, ActiveFileReference>();
	#generation = 0;
	#disposed = false;
	#operationTail: Promise<void> = Promise.resolve();
	#reconcileTask: Promise<boolean> | undefined;
	#pendingReconcileSummarize: false | Pick<LcmProjectionLimits, "tokenBudget" | "freshTail"> | undefined;
	#summaryTask: Promise<void> | undefined;
	#summaryAbortController: AbortController | undefined;
	#summaryRestartRequested = false;
	#deferReconcileUntilSummarySettles = false;
	#summaryRetryDeferred = false;
	readonly #activeSummaryJobs = new Map<string, Promise<SummaryWorkerOutcome>>();
	#summaryCapacitySignal = Promise.withResolvers<void>();
	#summaryProgressVersion = 0;
	#summaryProgressSignal = Promise.withResolvers<void>();
	#lastProjectionRequest: ProjectionRequest | undefined;
	/**
	 * Observation-only sample of the last arming decision. Separate from
	 * `#lastProjectionRequest` because that field also gates scheduling and phase selection,
	 * while this one must record a below-prewarm stand-down too.
	 */
	#lastPressure: LcmProjectionPressure | undefined;
	#projectionAttempt = 0;
	#summaryWakeTimer: NodeJS.Timeout | undefined;
	#summaryWakeAt: number | undefined;
	#summaryWakeTask: Promise<void> | undefined;
	#resolveSummaryWake: (() => void) | undefined;
	#closeTask: Promise<void> | undefined;
	#summaryRetryPolicy: SummaryRetryPolicy | undefined;
	#summaryPolicyMismatch = false;
	#unsubscribeDurableEntries: (() => void) | undefined;
	#registeredJournalKey: string | undefined;
	#runtimeHealth: LcmRuntimeHealth = "uninitialized";
	#coverageReadiness: LcmCoverageReadiness | undefined;
	#currentBranch: LcmCurrentBranchStatus | undefined;
	#healthFailure: ProjectionFailure | undefined;
	#providerFailureScope: ContextScope | undefined;
	#nextPrimaryPressureIntent: PrimaryPressureIntent | undefined;
	#pendingPrimaryRoute: PendingPrimaryRoute | undefined;
	#lastRequestRoute: LcmPrimaryRequestRoute | undefined;
	#lastTakeover: LcmRouteMetrics | undefined;
	#lastFailure: LcmRuntimeStatus["lastFailure"];
	#observedSessionId: string;
	#retryAt: number | undefined;
	#resolvedSummaryModel: string | undefined;
	readonly #preferredFailures = new Map<string, number>();
	readonly #fallbackFailures = new Map<string, number>();
	#preferredUnfit = false;

	constructor(host: SessionLcmHost, options: SessionLcmOptions) {
		this.#host = host;
		this.#agentDir = options.agentDir;
		this.#summaryModel =
			typeof options.summaryModel === "string" ? options.summaryModel.trim() || undefined : undefined;
		this.#maxConcurrentSummaries = normalizeLcmMaxConcurrentSummaries(options.maxConcurrentSummaries);
		this.#leafChunkTokens = normalizeLcmLeafChunkTokens(options.leafChunkTokens);
		this.#openContext = options.dependencies?.openContext ?? openLcmContext;
		this.#resolveProject = options.dependencies?.resolveProject ?? resolveLcmProject;
		this.#registerProject = options.registerProject;
		this.#providerAttemptTimeoutMs = Math.max(
			1,
			options.dependencies?.providerAttemptTimeoutMs ?? SUMMARY_PROVIDER_ATTEMPT_TIMEOUT_MS,
		);
		this.#peerPollMs = Math.max(1, options.dependencies?.peerPollMs ?? PEER_PROGRESS_POLL_MS);
		this.#now = options.dependencies?.now ?? Date.now;
		this.#spendEpoch = this.#now();
		this.#observedSessionId = host.sessionManager.getSessionId();
		this.#unsubscribeDurableEntries = host.sessionManager.subscribeToDurableEntries(() => {
			this.#dirty = true;
			if (this.#project && !this.#disposed) void this.#registerJournalHint(this.#project);
		});
	}

	get enabled(): boolean {
		return !this.#disposed;
	}

	configure(options: Pick<SessionLcmOptions, "summaryModel" | "maxConcurrentSummaries">): void {
		const nextModel = typeof options.summaryModel === "string" ? options.summaryModel.trim() || undefined : undefined;
		if (nextModel !== this.#summaryModel) {
			this.#resolvedSummaryModel = undefined;
			this.#summaryPolicyMismatch = false;
			this.#summaryRestartRequested = true;
			this.#summaryAbortController?.abort("LCM summary model changed");
			this.#clearProviderDegradation(true);
		}
		this.#summaryModel = nextModel;
		this.#maxConcurrentSummaries = normalizeLcmMaxConcurrentSummaries(options.maxConcurrentSummaries);
		// `leafChunkTokens` is deliberately absent: the store bakes `leafChunk` in at open time and
		// existing spans were built under the old chunking, so a live change would mix policies.
		this.#signalSummaryCapacity();
		if (this.#context) this.#startSummaryJobs();
	}

	/** Summary spend this session recorded before the current process, from the durable ledger. */
	priorSpendUsd(): number {
		return this.#priorSpendUsd;
	}

	commitPrimaryRequestRoute(key: LcmPrimaryRouteKey | undefined): boolean {
		const pending = this.#pendingPrimaryRoute;
		if (!key || !pending || pending.key !== key || !this.#isCurrentRouteKey(key)) return false;
		this.#pendingPrimaryRoute = undefined;
		this.#lastRequestRoute = pending.route;
		if (pending.route.kind === "lossless") this.#lastTakeover = pending.route.metrics;
		if (pending.route.kind === "native_fallback") {
			this.#lastFailure = {
				observedAt: pending.route.metrics.observedAt,
				category: pending.route.category,
				...(pending.route.reason ? { reason: pending.route.reason } : {}),
			};
		}
		return true;
	}

	recordPendingPrimaryProviderTokens(
		key: LcmPrimaryRouteKey | undefined,
		tokens: number,
		projectionTokenMeasurements?: number,
	): void {
		const pending = this.#pendingPrimaryRoute;
		if (
			!key ||
			!pending ||
			pending.key !== key ||
			pending.route.kind !== "lossless" ||
			!Number.isSafeInteger(tokens) ||
			tokens < 0 ||
			!this.#isCurrentRouteKey(key)
		) {
			return;
		}
		const metrics = {
			...pending.route.metrics,
			candidateTokens: tokens,
			...(projectionTokenMeasurements !== undefined &&
			Number.isSafeInteger(projectionTokenMeasurements) &&
			projectionTokenMeasurements >= 0
				? { projectionTokenMeasurements }
				: {}),
		};
		if (
			(metrics.messageTokenBudget !== undefined && tokens > metrics.messageTokenBudget) ||
			(metrics.projectionTokenMeasurements ?? 0) > MAX_LCM_PRIMARY_TOKEN_MEASUREMENTS
		) {
			const failure = { category: "unfit", reason: "fit_invariant" } as const;
			pending.route = { kind: "native_fallback", ...failure, metrics };
			this.#noteFailure(failure);
			return;
		}
		pending.route = { ...pending.route, metrics };
	}

	#runtimeStatus(): LcmRuntimeStatus {
		const preferred = this.#maxFailureDeadline(this.#preferredFailures);
		const fallback = this.#maxFailureDeadline(this.#fallbackFailures);
		return {
			health: this.#runtimeHealth,
			...(this.#coverageReadiness ? { coverageReadiness: this.#coverageReadiness } : {}),
			summaryWorkers: { active: this.#activeSummaryJobs.size, limit: this.#maxConcurrentSummaries },
			...(this.#lastPressure ? { pressure: this.#lastPressure } : {}),
			...(preferred === undefined && fallback === undefined
				? {}
				: {
						summaryBackoff: {
							...(preferred === undefined ? {} : { preferred }),
							...(fallback === undefined ? {} : { fallback }),
						},
					}),
			summaryModelSelector: this.#summaryModel ?? "@smol",
			...(this.#resolvedSummaryModel ? { resolvedSummaryModel: this.#resolvedSummaryModel } : {}),
			...(this.#currentBranch ? { currentBranch: this.#currentBranch } : {}),
			...(this.#lastRequestRoute ? { lastRequestRoute: this.#lastRequestRoute } : {}),
			...(this.#lastTakeover ? { lastTakeover: this.#lastTakeover } : {}),
			...(this.#lastFailure ? { lastFailure: this.#lastFailure } : {}),
			...(this.#retryAt === undefined ? {} : { retryAt: this.#retryAt }),
		};
	}

	#redact(text: string): string {
		return this.#host.obfuscator?.hasSecrets() ? this.#host.obfuscator.obfuscate(text) : text;
	}

	#clearActiveBranch(): void {
		this.#activeBranch = undefined;
		this.#currentBranch = undefined;
		this.#activeFileReferences.clear();
	}

	#syncSessionIdentity(): void {
		const sessionId = this.#host.sessionManager.getSessionId();
		if (sessionId === this.#observedSessionId) return;
		this.#observedSessionId = sessionId;
		this.#pendingPrimaryRoute = undefined;
		this.#lastRequestRoute = undefined;
		this.#lastTakeover = undefined;
		this.#lastFailure = undefined;
		this.#lastPressure = undefined;
		this.#nextPrimaryPressureIntent = undefined;
	}

	#routeMetrics(
		limits?: LcmProjectionLimits,
		projection?: ContextProjection,
		candidateTokens?: number,
		projectionTokenMeasurements?: number,
	): LcmRouteMetrics {
		return {
			observedAt: this.#now(),
			...(this.#lastPressure ? { pressure: this.#lastPressure } : {}),
			...(projection?.revision !== undefined
				? { revision: projection.revision }
				: this.#currentBranch
					? { revision: this.#currentBranch.revision }
					: {}),
			...(limits ? { messageTokenBudget: limits.tokenBudget } : {}),
			...(candidateTokens !== undefined ? { candidateTokens } : {}),
			...(projectionTokenMeasurements !== undefined ? { projectionTokenMeasurements } : {}),
			...(projection ? { projection: projectionAggregate(projection) } : {}),
		};
	}

	#capturePrimaryRouteScopeSource(attempt: number): LcmPrimaryRouteScopeSource | undefined {
		if (this.#disposed || attempt !== this.#projectionAttempt) return undefined;
		const manager = this.#host.sessionManager;
		const entries = manager.getBranch();
		const cwd = manager.getCwd();
		return {
			generation: this.#generation,
			projectionAttempt: attempt,
			sessionId: manager.getSessionId(),
			cwd,
			branchId: branchId(manager, entries),
			inputAnchor: branchAnchor(manager, entries),
			fallbackProjectId: fallbackRouteProjectId(cwd),
			...(this.#boundCwd === cwd && this.#project ? { project: this.#project } : {}),
		};
	}

	#primaryRouteScopeSourceIsCurrent(source: LcmPrimaryRouteScopeSource): boolean {
		return (
			source.generation === this.#generation &&
			source.projectionAttempt === this.#projectionAttempt &&
			source.sessionId === this.#observedSessionId &&
			source.cwd === this.#host.sessionManager.getCwd() &&
			this.#primaryRouteScopeIsCurrent({
				projectId: source.fallbackProjectId,
				branchId: source.branchId,
				inputAnchor: source.inputAnchor,
			})
		);
	}

	async #resolvePrimaryRouteScope(
		source: LcmPrimaryRouteScopeSource,
	): Promise<{ scope: LcmPrimaryRouteScope; error?: unknown }> {
		let project = source.project;
		try {
			project ??= await this.#resolveProject(source.cwd, this.#agentDir);
		} catch (error) {
			return {
				scope: {
					projectId: source.fallbackProjectId,
					branchId: source.branchId,
					inputAnchor: source.inputAnchor,
				},
				error,
			};
		}
		return {
			scope: {
				projectId: project.projectId,
				branchId: source.branchId,
				inputAnchor: source.inputAnchor,
			},
		};
	}

	#primaryRouteScopeIsCurrent(scope: LcmPrimaryRouteScope): boolean {
		const manager = this.#host.sessionManager;
		const entries = manager.getBranch();
		if (
			manager.getSessionId() !== this.#observedSessionId ||
			branchId(manager, entries) !== scope.branchId ||
			branchAnchor(manager, entries) !== scope.inputAnchor
		) {
			return false;
		}
		if (scope.revision === undefined) return true;
		const branch = this.#currentBranch;
		return (
			branch?.projectId === scope.projectId &&
			branch.sessionId === this.#observedSessionId &&
			branch.branchId === scope.branchId &&
			branch.revision === scope.revision
		);
	}

	#withPrimaryRouteRevision(
		scope: LcmPrimaryRouteScope | undefined,
		revision: number | undefined,
	): LcmPrimaryRouteScope | undefined {
		return scope && revision !== undefined ? { ...scope, revision } : scope;
	}

	#withMatchingActiveRevision(scope: LcmPrimaryRouteScope | undefined): LcmPrimaryRouteScope | undefined {
		const active = this.#activeBranch;
		const current = this.#currentBranch;
		if (!scope || !active || !current) return scope;
		const activeScope = active.snapshot.scope;
		if (
			active.anchor !== scope.inputAnchor ||
			activeScope.projectId !== scope.projectId ||
			activeScope.sessionId !== this.#observedSessionId ||
			activeScope.branchId !== scope.branchId ||
			current.projectId !== scope.projectId ||
			current.sessionId !== this.#observedSessionId ||
			current.branchId !== scope.branchId
		) {
			return scope;
		}
		return this.#withPrimaryRouteRevision(scope, current.revision);
	}

	#stagePrimaryRoute(
		route: LcmPrimaryRequestRoute,
		attempt: number,
		scope?: LcmPrimaryRouteScope,
	): LcmPrimaryRouteKey | undefined {
		if (this.#disposed || attempt !== this.#projectionAttempt) return undefined;
		const sessionId = this.#host.sessionManager.getSessionId();
		if (sessionId !== this.#observedSessionId) return undefined;
		if (!scope && !(route.kind === "native_passthrough" && route.reason === "unavailable")) return undefined;
		if (scope && !this.#primaryRouteScopeIsCurrent(scope)) return undefined;
		const key: LcmPrimaryRouteKey = {
			generation: this.#generation,
			projectionAttempt: attempt,
			sessionId,
			...(scope ? { scope } : {}),
		};
		this.#pendingPrimaryRoute = { key, route };
		return key;
	}

	#isCurrentRouteKey(key: LcmPrimaryRouteKey): boolean {
		if (
			this.#disposed ||
			key.generation !== this.#generation ||
			key.projectionAttempt !== this.#projectionAttempt ||
			key.sessionId !== this.#observedSessionId ||
			key.sessionId !== this.#host.sessionManager.getSessionId()
		) {
			return false;
		}
		if (!key.scope) return true;
		return this.#primaryRouteScopeIsCurrent(key.scope);
	}

	#activateBranch(normalized: NormalizedBranch, revision: number): void {
		const previous = this.#currentBranch;
		const sameRevision =
			this.#activeBranch?.anchor === normalized.anchor &&
			previous?.projectId === normalized.snapshot.scope.projectId &&
			previous.sessionId === normalized.snapshot.scope.sessionId &&
			previous.branchId === normalized.snapshot.scope.branchId &&
			previous.revision === revision;
		this.#activeBranch = normalized;
		this.#currentBranch = sameRevision
			? {
					...previous,
					activeSources: normalized.snapshot.entries.length,
					sourceTokens: normalizedSourceTokens(normalized.snapshot),
				}
			: {
					...normalized.snapshot.scope,
					revision,
					activeSources: normalized.snapshot.entries.length,
					sourceTokens: normalizedSourceTokens(normalized.snapshot),
					projectionState: "unevaluated",
				};
		if (!sameRevision) {
			this.#coverageReadiness = this.#lastProjectionRequest ? "warming" : "idle";
		}
		this.#activeFileReferences = normalized.fileReferences;
	}

	async #registerJournalHint(project: LcmProjectBinding): Promise<void> {
		if (!this.#registerProject || this.#disposed) return;
		const sessionDir = this.#host.sessionManager.getSessionDir();
		const sessionFile = this.#host.sessionManager.getSessionFile();
		const key = JSON.stringify([project.projectId, sessionDir, sessionFile ?? null]);
		if (key === this.#registeredJournalKey) return;
		try {
			await this.#registerProject(project, { sessionDir, ...(sessionFile ? { sessionFile } : {}) });
			if (!this.#disposed) this.#registeredJournalKey = key;
		} catch (error) {
			logger.warn("LCM project catalog registration failed; continuing without catalog update", {
				projectId: project.projectId,
				error: errorLabel(error),
			});
		}
	}

	#noteFailure(failure: ProjectionFailure, retryAt?: number): void {
		const nonDegrading =
			failure.category === "unfit" &&
			(failure.reason === "irreducible_input" || failure.reason === "minimum_representation");
		if (!nonDegrading) {
			this.#healthFailure = failure;
			this.#runtimeHealth = "degraded";
			if (failure.category === "provider") this.#providerFailureScope = this.#activeBranch?.snapshot.scope;
		}
		this.#retryAt = retryAt;
	}

	#clearProviderDegradation(clearBackoff = false): void {
		if (clearBackoff) {
			this.#preferredFailures.clear();
			this.#fallbackFailures.clear();
			this.#retryAt = undefined;
		}
		if (this.#healthFailure?.category === "provider") {
			this.#healthFailure = undefined;
			this.#providerFailureScope = undefined;
		}
		this.#summaryPolicyMismatch = false;
		this.#restoreHealth();
	}

	#hasPreferredHealthFailure(): boolean {
		return this.#preferredUnfit || this.#preferredFailures.size > 0;
	}

	#restoreHealth(): void {
		if (this.#runtimeHealth === "quarantined") return;
		this.#runtimeHealth =
			this.#healthFailure ||
			this.#preferredUnfit ||
			this.#preferredFailures.size > 0 ||
			this.#fallbackFailures.size > 0
				? "degraded"
				: this.#context
					? "healthy"
					: "uninitialized";
	}

	#failOpen(failure: ProjectionFailure, attempt: number): void {
		if (attempt !== this.#projectionAttempt) return;
		this.#noteFailure(failure, this.#retryAt);
	}

	#invalidateProjectionForRequest(): void {
		const currentBranch = this.#currentBranch;
		if (!currentBranch || currentBranch.projectionState === "unevaluated") return;
		this.#currentBranch = {
			projectId: currentBranch.projectId,
			sessionId: currentBranch.sessionId,
			branchId: currentBranch.branchId,
			revision: currentBranch.revision,
			activeSources: currentBranch.activeSources,
			sourceTokens: currentBranch.sourceTokens,
			projectionState: "unevaluated",
		};
	}

	#noteProjection(projection: ContextProjection, fitted: boolean, attempt: number): void {
		const currentBranch = this.#currentBranch;
		if (
			attempt !== this.#projectionAttempt ||
			!currentBranch ||
			currentBranch.projectId !== this.#activeBranch?.snapshot.scope.projectId ||
			currentBranch.sessionId !== this.#activeBranch.snapshot.scope.sessionId ||
			currentBranch.branchId !== this.#activeBranch.snapshot.scope.branchId ||
			currentBranch.revision !== projection.revision
		) {
			return;
		}
		this.#currentBranch = {
			...currentBranch,
			projectionState: fitted ? "fitted" : "unfitted",
			projection: projectionAggregate(projection),
		};
		const complete =
			projection.pendingJobs === 0 && hasExactProjectionCoverage(projection, this.#activeBranch.snapshot.entries);
		this.#coverageReadiness = complete ? "ready" : "warming";
		if (!complete) return;
		this.#preferredUnfit = false;
		if (this.#healthFailure?.category === "store" || this.#healthFailure?.category === "unfit") {
			this.#healthFailure = undefined;
		}
		if (this.#preferredFailures.size === 0 && this.#fallbackFailures.size === 0) this.#retryAt = undefined;
		this.#restoreHealth();
	}

	#signalSummaryCapacity(): void {
		const signal = this.#summaryCapacitySignal;
		this.#summaryCapacitySignal = Promise.withResolvers<void>();
		signal.resolve();
	}

	#signalSummaryProgress(): void {
		this.#summaryProgressVersion++;
		const signal = this.#summaryProgressSignal;
		this.#summaryProgressSignal = Promise.withResolvers<void>();
		signal.resolve();
	}

	#maxFailureDeadline(failures: ReadonlyMap<string, number>): number | undefined {
		let deadline: number | undefined;
		for (const value of failures.values()) deadline = deadline === undefined ? value : Math.max(deadline, value);
		return deadline;
	}

	#syncSummaryFailures(context: LcmContext): ProjectionFailure | undefined {
		const policy = this.#configureSummaryRetryPolicy(context);
		const failures = policy
			? context.summaryJobFailures(policy, SUMMARY_PROVIDER_RETRY_LIMIT, this.#activeBranch?.snapshot.scope)
			: [];
		this.#preferredFailures.clear();
		this.#fallbackFailures.clear();
		const now = this.#now();
		for (const failure of failures) {
			if (failure.availableAt <= now) continue;
			(failure.queueClass === "preferred" ? this.#preferredFailures : this.#fallbackFailures).set(
				failure.jobId,
				failure.availableAt,
			);
		}
		const preferred = this.#maxFailureDeadline(this.#preferredFailures);
		const fallback = this.#maxFailureDeadline(this.#fallbackFailures);
		this.#retryAt =
			preferred === undefined ? fallback : fallback === undefined ? preferred : Math.max(preferred, fallback);
		const terminalFailure = this.#currentProviderTerminalFailure(context, policy ?? this.#summaryRetryPolicy);
		const providerFailure = terminalFailure ?? (failures.length > 0 ? { category: "provider" as const } : undefined);
		if (providerFailure) {
			this.#healthFailure = providerFailure;
			this.#providerFailureScope = this.#activeBranch?.snapshot.scope;
			this.#runtimeHealth = "degraded";
		} else if (this.#healthFailure?.category === "provider") {
			const activeScope = this.#activeBranch?.snapshot.scope;
			const failedScope = this.#providerFailureScope;
			const scopeChanged =
				activeScope !== undefined &&
				failedScope !== undefined &&
				(activeScope.projectId !== failedScope.projectId ||
					activeScope.sessionId !== failedScope.sessionId ||
					activeScope.branchId !== failedScope.branchId);
			const activeSources = this.#activeBranch?.snapshot.entries;
			const projection = scopeChanged ? undefined : this.#projectForScheduling(context);
			const coverageRecovered =
				projection !== undefined &&
				activeSources !== undefined &&
				projection.pendingJobs === 0 &&
				hasExactProjectionCoverage(projection, activeSources);
			if (scopeChanged || coverageRecovered) {
				this.#healthFailure = undefined;
				this.#providerFailureScope = undefined;
			}
		}
		this.#restoreHealth();
		return terminalFailure;
	}

	#currentProviderTerminalFailure(
		context: LcmContext,
		policy: SummaryRetryPolicy | undefined,
	): ProjectionFailure | undefined {
		const scope = this.#activeBranch?.snapshot.scope;
		const request = this.#lastProjectionRequest;
		if (!scope || !request || !policy) return undefined;
		const availability = context.summaryJobAvailability(
			{
				...scope,
				tokenBudget: request.limits.tokenBudget,
				freshTail: request.limits.freshTail,
			},
			policy,
			SUMMARY_PROVIDER_RETRY_LIMIT,
		);
		const unresolved =
			availability.runnable +
			availability.leased +
			availability.backoff +
			availability.exhausted +
			availability.missing +
			availability.policyMismatch;
		if ((this.#summaryPolicyMismatch && unresolved > 0) || availability.policyMismatch > 0) {
			return { category: "provider", reason: "provider_key_mismatch" };
		}
		return availability.exhausted > 0 ? { category: "provider", reason: "provider_exhausted" } : undefined;
	}

	#clearSummaryWake(): void {
		clearTimeout(this.#summaryWakeTimer);
		this.#resolveSummaryWake?.();
		this.#summaryWakeTimer = undefined;
		this.#summaryWakeAt = undefined;
		this.#summaryWakeTask = undefined;
		this.#resolveSummaryWake = undefined;
	}

	#scheduleSummaryWake(context: LcmContext, generation: number, delayMs: number): void {
		const requestedDelay = Math.max(1, delayMs);
		const delay = Math.min(requestedDelay, MAX_TIMER_DELAY_MS);
		const wakeAt = this.#now() + requestedDelay;
		if (this.#summaryWakeAt !== undefined && this.#summaryWakeAt <= wakeAt) return;
		this.#clearSummaryWake();
		this.#summaryWakeAt = wakeAt;
		const wake = Promise.withResolvers<void>();
		this.#summaryWakeTask = wake.promise;
		this.#resolveSummaryWake = wake.resolve;
		this.#summaryWakeTimer = setTimeout(() => {
			this.#summaryWakeTimer = undefined;
			this.#summaryWakeAt = undefined;
			this.#summaryWakeTask = undefined;
			this.#resolveSummaryWake = undefined;
			wake.resolve();
			if (this.#disposed || generation !== this.#generation || context !== this.#context) return;
			this.#startSummaryJobs();
		}, delay);
	}

	#enqueue<T>(operation: () => Promise<T> | T): Promise<T> {
		const run = async (): Promise<T> => {
			for (let attempt = 0; ; attempt++) {
				try {
					return await operation();
				} catch (error) {
					const delay = SQLITE_CONTENTION_DELAYS_MS[attempt];
					if (!isLcmSqliteContentionError(error) || delay === undefined) throw error;
					await Bun.sleep(delay);
				}
			}
		};
		const result = this.#operationTail.then(run, run);
		this.#operationTail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	async #ensureOpen(
		deferredSummarize: false | Pick<LcmProjectionLimits, "tokenBudget" | "freshTail"> = false,
	): Promise<LcmContext | undefined> {
		if (this.#disposed) return undefined;
		this.#syncSessionIdentity();
		const cwd = this.#host.sessionManager.getCwd();
		if (this.#boundCwd !== undefined && this.#boundCwd !== cwd) {
			this.#nextPrimaryPressureIntent = undefined;
			this.#preferredUnfit = false;
		}
		if (this.#context && this.#boundCwd !== undefined && this.#boundCwd !== cwd && this.#summaryTask) {
			this.#summaryRestartRequested = false;
			this.#summaryAbortController?.abort("LCM store binding changed");
			this.#clearSummaryWake();
			this.#dirty = true;
			if (!this.#deferReconcileUntilSummarySettles) {
				this.#deferReconcileUntilSummarySettles = true;
				const drain = this.#summaryTask;
				const settled = (): void => {
					if (!this.#deferReconcileUntilSummarySettles) return;
					this.#deferReconcileUntilSummarySettles = false;
					if (!this.#disposed && this.#context && this.#boundCwd !== this.#host.sessionManager.getCwd()) {
						const summarize = this.#pendingReconcileSummarize ?? deferredSummarize;
						void this.#requestReconcile(false, summarize);
					}
				};
				void drain.then(settled, settled);
			}
			return undefined;
		}
		if (this.#boundCwd !== undefined && this.#boundCwd !== cwd) this.#clearActiveBranch();
		if (this.#context && this.#boundCwd === cwd) {
			if (this.#project) await this.#registerJournalHint(this.#project);
			return this.#context;
		}

		const generation = this.#generation;
		let project: LcmProjectBinding;
		try {
			project = await this.#resolveProject(cwd, this.#agentDir);
		} catch (error) {
			logger.warn("LCM project resolution failed; using native context", { error: errorLabel(error) });
			this.#noteFailure({ category: "store" });
			return undefined;
		}
		if (this.#disposed || generation !== this.#generation || cwd !== this.#host.sessionManager.getCwd())
			return undefined;
		await this.#registerJournalHint(project);
		if (this.#disposed || generation !== this.#generation || cwd !== this.#host.sessionManager.getCwd()) {
			return undefined;
		}

		const projectChanged = this.#project !== undefined && this.#project.projectId !== project.projectId;
		if (this.#context) {
			this.#clearSummaryWake();
			this.#context.close();
			this.#context = undefined;
			this.#clearActiveBranch();
		}
		if (projectChanged) {
			this.#nextPrimaryPressureIntent = undefined;
			this.#preferredUnfit = false;
			this.#preferredFailures.clear();
			this.#fallbackFailures.clear();
			this.#summaryRetryPolicy = undefined;
			this.#summaryPolicyMismatch = false;
			this.#retryAt = undefined;
			if (this.#healthFailure?.category === "provider") this.#healthFailure = undefined;
			this.#resolvedSummaryModel = undefined;
		}

		let context: LcmContext;
		try {
			context = await this.#openContext({
				dbPath: project.storePath,
				recoverCorrupt: true,
				regexEngine: NATIVE_REGEX_ENGINE,
				// 24 sources per 4,000 tokens is the ratio the tuning matrix measured; holding it
				// keeps a coarser chunk from also changing how many sources each leaf spans.
				leafChunk: {
					maxTokens: this.#leafChunkTokens,
					maxSources: (this.#leafChunkTokens / DEFAULT_LEAF_CHUNK_TOKENS) * 24,
				},
			});
		} catch (error) {
			logger.warn("LCM store open failed; using native context", { error: errorLabel(error) });
			this.#noteFailure({ category: "store" });
			return undefined;
		}
		if (this.#disposed || generation !== this.#generation || cwd !== this.#host.sessionManager.getCwd()) {
			context.close();
			return undefined;
		}
		this.#summaryRetryPolicy = undefined;
		this.#summaryPolicyMismatch = false;

		this.#context = context;
		try {
			this.#priorSpendUsd = context.priorSummarySpendUsd(this.#host.sessionManager.getSessionId(), this.#spendEpoch);
		} catch (error) {
			logger.debug("LCM prior spend unavailable", { error: errorLabel(error) });
		}
		this.#project = project;
		this.#boundCwd = cwd;
		if (this.#healthFailure?.category === "store") this.#healthFailure = undefined;
		this.#coverageReadiness ??= "idle";
		this.#restoreHealth();
		return context;
	}

	#normalizeActiveBranch(project: LcmProjectBinding): NormalizedBranch {
		return normalizeLcmBranchState(
			this.#host.sessionManager,
			project.projectId,
			text => this.#redact(text),
			project.rootPath,
		);
	}

	#projectForScheduling(context: LcmContext): ContextProjection | undefined {
		const request = this.#lastProjectionRequest;
		const scope = this.#activeBranch?.snapshot.scope;
		if (!request || !scope) return undefined;
		const projection = context.project({
			...scope,
			tokenBudget: request.limits.tokenBudget,
			freshTail: request.limits.freshTail,
		});
		return projection;
	}

	#configureSummaryRetryPolicy(
		context: LcmContext,
		force = false,
		resolvedRetryKey?: string,
	): SummaryRetryPolicy | undefined {
		const project = this.#project;
		if (!project) return undefined;
		if (this.#summaryPolicyMismatch && !force) return undefined;
		const selector = this.#summaryModel ?? "@smol";
		let retryKey = resolvedRetryKey;
		if (!retryKey && this.#host.resolveSummaryModel) {
			try {
				retryKey = this.#host.resolveSummaryModel(selector);
			} catch {
				this.#summaryPolicyMismatch = true;
				return undefined;
			}
		} else if (!retryKey) {
			retryKey = this.#resolvedSummaryModel ?? (!selector.startsWith("@") ? selector : undefined);
		}
		if (retryKey) this.#resolvedSummaryModel = retryKey;
		if (!retryKey) {
			this.#summaryPolicyMismatch = true;
			return undefined;
		}
		const current = this.#summaryRetryPolicy;
		const result = context.configureSummaryRetryPolicy(project.projectId, retryKey, {
			...(current ? { expected: current } : {}),
			...(force ? { force: true } : {}),
		});
		const accepted = { retryKey: result.retryKey, retryEpoch: result.retryEpoch };
		const rotated =
			current !== undefined &&
			(current.retryKey !== accepted.retryKey || current.retryEpoch !== accepted.retryEpoch);
		this.#summaryRetryPolicy = accepted;
		if (rotated && this.#summaryAbortController && !this.#summaryAbortController.signal.aborted) {
			this.#summaryRetryDeferred = false;
			this.#summaryRestartRequested = true;
			this.#summaryAbortController.abort("LCM summary retry policy changed");
			this.#signalSummaryCapacity();
		}
		if (result.kind === "conflict") {
			this.#summaryPolicyMismatch = result.retryKey !== retryKey;
			return this.#summaryPolicyMismatch ? undefined : accepted;
		}
		this.#summaryPolicyMismatch = false;
		return accepted;
	}

	async #drainReconcile(open: boolean): Promise<boolean> {
		let reconciled = false;
		while (this.#dirty && !this.#disposed) {
			if (!open && !this.#context) return reconciled;
			const summarize = this.#pendingReconcileSummarize ?? false;
			this.#pendingReconcileSummarize = undefined;
			this.#dirty = false;
			const context = await this.#ensureOpen(summarize);
			const project = this.#project;
			if (!context || !project) {
				if (
					this.#deferReconcileUntilSummarySettles &&
					summarize !== false &&
					(this.#pendingReconcileSummarize === undefined || this.#pendingReconcileSummarize === false)
				) {
					this.#pendingReconcileSummarize = summarize;
				}
				return false;
			}
			const generation = this.#generation;
			try {
				const normalized = this.#normalizeActiveBranch(project);
				if (
					generation !== this.#generation ||
					normalized.anchor !== branchAnchor(this.#host.sessionManager, this.#host.sessionManager.getBranch())
				) {
					this.#dirty = true;
					continue;
				}
				const reconcileResult = context.reconcile(normalized.snapshot, { summarize });
				this.#activateBranch(normalized, reconcileResult.revision);
				this.#syncSummaryFailures(context);
				reconciled = true;
				const scope = normalized.snapshot.scope;
				const projection =
					summarize === false
						? this.#projectForScheduling(context)
						: context.project({ ...scope, tokenBudget: summarize.tokenBudget, freshTail: summarize.freshTail });
				if (projection) {
					const complete =
						projection.pendingJobs === 0 && hasExactProjectionCoverage(projection, normalized.snapshot.entries);
					this.#coverageReadiness = complete ? "ready" : "warming";
					if (
						complete &&
						(this.#healthFailure?.category === "store" ||
							(this.#healthFailure?.category === "unfit" && this.#healthFailure.reason === "coverage_gap"))
					) {
						this.#healthFailure = undefined;
					}
					this.#restoreHealth();
				}
				const allowFallback = projection === undefined || projection.pendingJobs === 0;
				const preferredRetryAt = this.#maxFailureDeadline(this.#preferredFailures);
				const fallbackRetryAt = this.#maxFailureDeadline(this.#fallbackFailures);
				if (preferredRetryAt !== undefined && preferredRetryAt > this.#now()) {
					this.#scheduleSummaryWake(context, generation, preferredRetryAt - this.#now());
					continue;
				}
				const fallbackBlocked = allowFallback && fallbackRetryAt !== undefined && fallbackRetryAt > this.#now();
				const effectiveAllowFallback = allowFallback && !fallbackBlocked;
				const policy = this.#summaryPolicyMismatch ? undefined : this.#summaryRetryPolicy;
				const delayMs = policy
					? context.nextSummaryJobDelayMs(policy, SUMMARY_PROVIDER_RETRY_LIMIT, scope, effectiveAllowFallback)
					: null;
				if (!this.#hasPreferredHealthFailure() && (delayMs !== null || fallbackBlocked)) {
					this.#coverageReadiness = "warming";
				}
				if (delayMs === 0) this.#startSummaryJobs();
				else if (delayMs !== null) this.#scheduleSummaryWake(context, generation, delayMs);
				else if (fallbackBlocked && fallbackRetryAt !== undefined) {
					this.#scheduleSummaryWake(context, generation, fallbackRetryAt - this.#now());
				}
			} catch (error) {
				if (isLcmSqliteContentionError(error)) {
					this.#dirty = true;
					if (summarize !== false) this.#pendingReconcileSummarize = summarize;
					throw error;
				}
				logger.warn("LCM reconcile failed; using native context", { error: errorLabel(error) });
				this.#noteFailure({ category: "store" });
				return false;
			}
		}
		return reconciled;
	}

	#requestReconcile(
		open: boolean,
		summarize: false | Pick<LcmProjectionLimits, "tokenBudget" | "freshTail"> = false,
	): Promise<boolean> {
		this.#dirty = true;
		if (summarize !== false || this.#pendingReconcileSummarize === undefined) {
			this.#pendingReconcileSummarize = summarize;
		}
		if (!open && !this.#context) return Promise.resolve(false);
		if (this.#reconcileTask) return this.#reconcileTask;

		const task = this.#enqueue(() => this.#drainReconcile(open));
		this.#reconcileTask = task;
		const settled = (resumeDirty: boolean): void => {
			if (this.#reconcileTask !== task) return;
			this.#reconcileTask = undefined;
			const next = this.#pendingReconcileSummarize ?? summarize;
			if (
				resumeDirty &&
				!this.#deferReconcileUntilSummarySettles &&
				this.#dirty &&
				this.#context &&
				!this.#disposed
			) {
				void this.#requestReconcile(false, next);
			}
		};
		void task.then(
			() => settled(true),
			() => settled(false),
		);
		return task;
	}

	#startSummaryJobs(): void {
		if (this.#disposed || !this.#context) return;
		if (this.#summaryTask) {
			this.#summaryRestartRequested = true;
			this.#signalSummaryCapacity();
			return;
		}
		this.#summaryRestartRequested = false;
		this.#clearSummaryWake();
		const context = this.#context;
		const generation = this.#generation;
		const controller = new AbortController();
		this.#summaryAbortController = controller;
		const task = this.#runSummaryJobs(context, generation, controller.signal);
		this.#summaryTask = task;
		const clear = (): boolean => {
			if (this.#summaryTask !== task) return false;
			this.#summaryTask = undefined;
			if (this.#summaryAbortController === controller) this.#summaryAbortController = undefined;
			const restart = this.#summaryRestartRequested;
			this.#summaryRestartRequested = false;
			return restart;
		};
		void task.then(
			() => {
				const restart = clear();
				if (
					restart &&
					!this.#disposed &&
					generation === this.#generation &&
					context === this.#context &&
					!this.#deferReconcileUntilSummarySettles &&
					this.#boundCwd === this.#host.sessionManager.getCwd() &&
					!this.#summaryWakeTask
				) {
					this.#startSummaryJobs();
				}
			},
			error => {
				clear();
				if (!this.#disposed && generation === this.#generation && context === this.#context) {
					this.#noteFailure({ category: "store" });
					logger.warn("LCM summary worker failed", { error: errorLabel(error) });
				}
			},
		);
	}

	async #runSummaryJobs(context: LcmContext, generation: number, signal: AbortSignal): Promise<void> {
		const settled: SummaryWorkerOutcome[] = [];
		let abortObserved = false;
		let firstStoreError: unknown;
		let storeFailed = false;
		let preferredExhaustedWithoutProjection = false;
		let preferredWorkObserved = false;
		let terminalProviderFailureObserved = false;
		const recordStoreError = (error: unknown): void => {
			if (storeFailed) return;
			storeFailed = true;
			firstStoreError = error;
		};
		const isCurrent = (): boolean =>
			!signal.aborted && !this.#disposed && generation === this.#generation && context === this.#context;

		const startJob = (job: SummaryJob, summaryModel: string): void => {
			const task = this.#runSummaryJob(context, job, summaryModel, generation, signal).catch(
				(error): SummaryWorkerOutcome => ({
					status: "store_failed",
					jobId: job.jobId,
					queueClass: job.queueClass,
					error,
				}),
			);
			this.#activeSummaryJobs.set(job.jobId, task);
			void task.then(outcome => {
				settled.push(outcome);
				this.#signalSummaryProgress();
			});
		};

		while (isCurrent()) {
			if (settled.length > 0) {
				await Promise.resolve();
				const batch = settled.splice(0);
				const readyBeforeBatch = this.#coverageReadiness === "ready";
				let preferredUnfitObserved = false;
				let unfitObserved = false;
				let progressObserved = false;
				let completedObserved = false;
				for (const outcome of batch) {
					this.#activeSummaryJobs.delete(outcome.jobId);
					switch (outcome.status) {
						case "provider_failed":
							break;
						case "store_failed":
							recordStoreError(outcome.error);
							break;
						case "aborted":
							abortObserved = true;
							break;
						case "unfit":
							if (outcome.queueClass === "preferred") {
								preferredUnfitObserved = true;
								unfitObserved = true;
							} else if (!readyBeforeBatch) {
								unfitObserved = true;
							}
							break;
						case "completed":
							completedObserved = true;
							progressObserved = true;
							break;
						case "escalated":
							progressObserved = true;
							break;
					}
				}
				if (completedObserved) this.#clearProviderDegradation();
				if (preferredUnfitObserved) this.#preferredUnfit = true;
				if (unfitObserved) this.#noteFailure({ category: "unfit", reason: "coverage_gap" });
				else if (progressObserved) {
					if (this.#coverageReadiness !== "ready") this.#coverageReadiness = "warming";
					this.#restoreHealth();
				}
				let terminalProviderFailure: ProjectionFailure | undefined;
				try {
					await this.#enqueue(() => {
						terminalProviderFailure = this.#syncSummaryFailures(context);
					});
				} catch (error) {
					recordStoreError(error);
				}
				if (terminalProviderFailure) {
					terminalProviderFailureObserved = true;
					this.#noteFailure(terminalProviderFailure);
				}
				if (terminalProviderFailureObserved && this.#activeSummaryJobs.size === 0) {
					this.#summaryRestartRequested = false;
					return;
				}
			}

			if (storeFailed) this.#summaryAbortController?.abort("LCM summary store failure");
			if (storeFailed || abortObserved) {
				if (this.#activeSummaryJobs.size > 0) {
					await Promise.race(this.#activeSummaryJobs.values());
					continue;
				}
				if (storeFailed) throw firstStoreError;
				this.#summaryRetryDeferred = !this.#summaryRestartRequested;
				return;
			}

			let blockedWakeMs: number | null = null;
			while (
				isCurrent() &&
				!terminalProviderFailureObserved &&
				this.#activeSummaryJobs.size < this.#maxConcurrentSummaries
			) {
				let claimed: {
					job: SummaryJob | undefined;
					summaryModel: string;
					delayMs: number | null;
					preferredOnly: boolean;
					cancelled?: boolean;
					handoff?: boolean;
					failure?: ProjectionFailure;
				};
				try {
					claimed = await this.#enqueue(() => {
						if (
							!isCurrent() ||
							this.#activeSummaryJobs.size >= this.#maxConcurrentSummaries ||
							settled.length > 0
						) {
							return {
								job: undefined,
								summaryModel: "",
								delayMs: null,
								preferredOnly: false,
								cancelled: true,
							};
						}
						const scope = this.#activeBranch?.snapshot.scope;
						const projection = this.#projectForScheduling(context);
						if (projection?.pendingJobs && projection.pendingJobs > 0) preferredWorkObserved = true;
						if (projection?.pendingJobs === 0 && preferredWorkObserved) {
							this.#summaryRestartRequested = true;
							return {
								job: undefined,
								summaryModel: "",
								delayMs: null,
								preferredOnly: false,
								handoff: true,
							};
						}
						const allowFallback = projection
							? projection.pendingJobs === 0
							: scope === undefined || preferredExhaustedWithoutProjection;
						const preferredRetryAt = this.#maxFailureDeadline(this.#preferredFailures);
						if (preferredRetryAt !== undefined && preferredRetryAt > this.#now()) {
							return {
								job: undefined,
								summaryModel: "",
								delayMs: preferredRetryAt - this.#now(),
								preferredOnly: !allowFallback,
							};
						}
						const fallbackRetryAt = this.#maxFailureDeadline(this.#fallbackFailures);
						const fallbackBlocked =
							allowFallback && fallbackRetryAt !== undefined && fallbackRetryAt > this.#now();
						const effectiveAllowFallback = allowFallback && !fallbackBlocked;
						const policy = this.#configureSummaryRetryPolicy(context);
						if (!policy) {
							return {
								job: undefined,
								summaryModel: "",
								delayMs: null,
								preferredOnly: !allowFallback,
								failure: this.#summaryPolicyMismatch
									? { category: "provider", reason: "provider_key_mismatch" }
									: undefined,
							};
						}
						const job = context.claimSummaryJobs({
							...policy,
							maxTransportRetries: SUMMARY_PROVIDER_RETRY_LIMIT,
							workerId: this.#workerId,
							leaseMs: SUMMARY_LEASE_MS,
							limit: 1,
							maxOutputTokens: SUMMARY_MAX_OUTPUT_TOKENS,
							...(scope ? { preferredScope: scope } : {}),
							allowFallback: effectiveAllowFallback,
						})[0];
						const delayMs = job
							? null
							: (context.nextSummaryJobDelayMs(
									policy,
									SUMMARY_PROVIDER_RETRY_LIMIT,
									scope,
									effectiveAllowFallback,
								) ?? (fallbackBlocked && fallbackRetryAt !== undefined ? fallbackRetryAt - this.#now() : null));
						let failure: ProjectionFailure | undefined;
						const request = this.#lastProjectionRequest;
						if (!job && delayMs === null && projection?.pendingJobs && scope && request) {
							const availability = context.summaryJobAvailability(
								{
									...scope,
									tokenBudget: request.limits.tokenBudget,
									freshTail: request.limits.freshTail,
								},
								policy,
								SUMMARY_PROVIDER_RETRY_LIMIT,
							);
							if (availability.policyMismatch > 0) {
								failure = { category: "provider", reason: "provider_key_mismatch" };
							} else if (availability.exhausted > 0) {
								failure = { category: "provider", reason: "provider_exhausted" };
							}
						}
						return {
							job,
							summaryModel: this.#summaryModel ?? "@smol",
							delayMs,
							preferredOnly: !allowFallback,
							failure,
						};
					});
				} catch (error) {
					recordStoreError(error);
					this.#summaryAbortController?.abort("LCM summary store failure");
					break;
				}
				if (
					claimed.job &&
					(!isCurrent() || this.#activeSummaryJobs.size >= this.#maxConcurrentSummaries || settled.length > 0)
				) {
					try {
						await this.#enqueue(() => context.releaseSummaryJob(claimed.job!));
					} catch (error) {
						recordStoreError(error);
						this.#summaryAbortController?.abort("LCM summary store failure");
					}
					break;
				}
				if (!claimed.job) {
					if (claimed.failure) {
						this.#noteFailure(claimed.failure);
						this.#summaryRestartRequested = false;
						if (this.#activeSummaryJobs.size === 0) return;
						terminalProviderFailureObserved = true;
						break;
					}
					if (claimed.cancelled || claimed.handoff) break;
					if (claimed.preferredOnly && claimed.delayMs === null && this.#activeSummaryJobs.size === 0) {
						preferredExhaustedWithoutProjection = true;
						continue;
					}
					blockedWakeMs = claimed.delayMs;
					break;
				}
				if (claimed.job.queueClass === "preferred") preferredExhaustedWithoutProjection = false;
				startJob(claimed.job, claimed.summaryModel);
			}

			if (storeFailed) {
				this.#summaryAbortController?.abort("LCM summary store failure");
				await Promise.all(this.#activeSummaryJobs.values());
				this.#activeSummaryJobs.clear();
				throw firstStoreError;
			}
			if (this.#activeSummaryJobs.size === 0) {
				if (blockedWakeMs !== null) this.#scheduleSummaryWake(context, generation, blockedWakeMs);
				return;
			}
			const capacity = this.#summaryCapacitySignal.promise;
			await Promise.race([...this.#activeSummaryJobs.values(), capacity]);
		}

		const remaining = await Promise.all(this.#activeSummaryJobs.values());
		this.#activeSummaryJobs.clear();
		if (storeFailed) throw firstStoreError;
		const failed = remaining.find(
			(outcome): outcome is Extract<SummaryWorkerOutcome, { status: "store_failed" }> =>
				outcome.status === "store_failed",
		);
		if (failed) throw failed.error;
	}

	async #runSummaryJob(
		context: LcmContext,
		job: SummaryJob,
		summaryModel: string,
		generation: number,
		signal: AbortSignal,
	): Promise<SummaryWorkerOutcome> {
		const base = { jobId: job.jobId, queueClass: job.queueClass };
		let lease: SummaryJobLease = job;
		const jobController = new AbortController();
		const jobSignal = AbortSignal.any([signal, jobController.signal]);
		let leaseLost = false;
		let renewalTask: Promise<void> | undefined;
		let terminal = false;
		let firstStoreError: unknown;
		let storeFailed = false;
		const recordStoreError = (error: unknown): void => {
			if (storeFailed) return;
			storeFailed = true;
			firstStoreError = error;
		};
		let releaseLeaseTask: Promise<void> | undefined;
		const releaseLease = (): Promise<void> => {
			if (terminal) return Promise.resolve();
			releaseLeaseTask ??= this.#enqueue(() => context.releaseSummaryJob(lease)).then(
				() => undefined,
				error => {
					recordStoreError(error);
				},
			);
			return releaseLeaseTask;
		};
		const renewLease = setInterval(
			() => {
				if (renewalTask || jobSignal.aborted) return;
				renewalTask = this.#enqueue(() => {
					if (
						jobSignal.aborted ||
						this.#disposed ||
						generation !== this.#generation ||
						context !== this.#context
					) {
						return undefined;
					}
					return context.extendSummaryJob(lease, SUMMARY_LEASE_MS);
				})
					.then(nonce => {
						if (nonce === undefined) return;
						if (nonce === null) {
							leaseLost = true;
							jobController.abort("LCM summary lease lost");
						} else {
							lease = { ...lease, leaseMutationNonce: nonce };
						}
					})
					.catch(error => {
						recordStoreError(error);
						jobController.abort("LCM summary lease renewal failed");
					})
					.finally(() => {
						renewalTask = undefined;
					});
			},
			Math.floor(SUMMARY_LEASE_MS / 2),
		);

		let outcome: SummaryWorkerOutcome;
		try {
			outcome = await (async (): Promise<SummaryWorkerOutcome> => {
				let redactedText: string;
				let promptHash: string;
				let resolvedModel: string | undefined;
				let providerAttempt: SummaryProviderAttempt | undefined;
				const settleAttempt = async (
					attempt: SummaryProviderAttempt | SummaryProviderAttemptStart | undefined,
					requested: SummaryLocalAttemptOutcome,
				): Promise<void> => {
					if (!attempt) return;
					try {
						await this.#enqueue(() => context.settleSummaryAttempt(lease, attempt, requested));
					} catch (error) {
						recordStoreError(error);
					}
				};
				if (job.stage === "deterministic") {
					redactedText = deterministicSummary(job);
					promptHash = summaryPromptHash(
						"deterministic_truncate:v1",
						job.inputs.map(input => `${input.kind}:${input.id}`).join("\n"),
					);
				} else {
					const systemPrompt = prompt.render(lcmSummarySystemPrompt);
					const userPrompt = prompt.render(lcmSummaryUserPrompt, {
						aggressive: job.stage === "aggressive",
						condense: job.level >= 1,
						maxOutputTokens: job.outputTokenBudget,
						inputs: job.inputs.map(input => ({ kind: input.kind, id: input.id, text: input.redactedText })),
					});
					promptHash = summaryPromptHash(systemPrompt, userPrompt);
					const startProvenance = {
						promptHash,
						modelSelector: summaryModel,
						sessionId: this.#host.sessionManager.getSessionId(),
						strategy: job.strategy,
					};
					let completion: LcmCompletionResult;
					let providerAttemptStart: SummaryProviderAttemptStart | undefined;
					const providerTimeoutController = new AbortController();
					let providerTimedOut = false;
					let providerTimeoutMessage = "Summary provider preparation timed out";
					const providerDeadline = Promise.withResolvers<never>();
					const expireProvider = () => {
						providerTimedOut = true;
						providerTimeoutController.abort(new DOMException(providerTimeoutMessage, "TimeoutError"));
						providerDeadline.reject(
							new LcmCompletionError(providerTimeoutMessage, { category: "transport_error" }),
						);
					};
					const providerTimer = setTimeout(expireProvider, this.#providerAttemptTimeoutMs);
					const completionSignal = AbortSignal.any([jobSignal, providerTimeoutController.signal]);
					let retryPolicyRotated = false;
					let completionTask: Promise<LcmCompletionResult> | undefined;
					try {
						completionTask = this.#host.complete({
							systemPrompt,
							prompt: userPrompt,
							maxOutputTokens: job.outputTokenBudget,
							oneshotKind: "lcm_summary",
							modelSelector: summaryModel,
							providerSessionKey: `${job.jobId}:${lease.retryEpoch}`,
							providerSessionFamilyKey: job.jobId,
							retainProviderStateOnFailure: job.transportRetryCount + 1 < SUMMARY_PROVIDER_RETRY_LIMIT,
							signal: completionSignal,
							onResolvedModel: model => {
								resolvedModel = model;
								this.#resolvedSummaryModel = model;
							},
							onAttemptStart: async start => {
								try {
									return await this.#enqueue(() => {
										if (
											providerTimedOut ||
											jobSignal.aborted ||
											this.#disposed ||
											generation !== this.#generation ||
											context !== this.#context
										) {
											return false;
										}
										const policy = this.#configureSummaryRetryPolicy(context);
										if (
											!policy ||
											!resolvedModel ||
											policy.retryKey !== resolvedModel ||
											policy.retryKey !== lease.retryKey ||
											policy.retryEpoch !== lease.retryEpoch
										) {
											retryPolicyRotated = true;
											return false;
										}
										const accepted = context.beginSummaryAttempt(lease, start, startProvenance);
										if (accepted) {
											providerAttemptStart = start;
											providerTimeoutMessage = "Summary provider attempt timed out";
										}
										return accepted;
									});
								} catch (error) {
									recordStoreError(error);
									return false;
								}
							},
						});
						completion = await untilAborted(jobSignal, Promise.race([completionTask, providerDeadline.promise]));
						clearTimeout(providerTimer);
					} catch (error) {
						clearTimeout(providerTimer);
						const completionError = error instanceof LcmCompletionError ? error : undefined;
						const completedAttempt = completionError?.attempt;
						const acceptedAttempt = completedAttempt ?? providerAttemptStart;
						const attachLateAttemptSettlement = (requested: SummaryLocalAttemptOutcome): void => {
							if (!completionTask || !providerAttemptStart || completedAttempt) return;
							void completionTask.then(
								late =>
									this.#enqueue(() => context.settleSummaryAttempt(lease, late.attempt, requested)).catch(
										settleError =>
											logger.debug("LCM late abandoned-attempt settlement failed", {
												error: errorLabel(settleError),
											}),
									),
								lateError => {
									const lateAttempt = lateError instanceof LcmCompletionError ? lateError.attempt : undefined;
									if (!lateAttempt) return;
									void this.#enqueue(() => context.settleSummaryAttempt(lease, lateAttempt, requested)).catch(
										settleError =>
											logger.debug("LCM late abandoned-attempt settlement failed", {
												error: errorLabel(settleError),
											}),
									);
								},
							);
						};
						const settleAbandonedAttempt = async (requested: SummaryLocalAttemptOutcome): Promise<void> => {
							await renewalTask;
							await settleAttempt(acceptedAttempt, requested);
							await releaseLease();
							attachLateAttemptSettlement(requested);
						};
						if (storeFailed) {
							await settleAbandonedAttempt("aborted");
							return { status: "store_failed", ...base, error: firstStoreError };
						}
						if (leaseLost) {
							await settleAbandonedAttempt("lease_lost");
							return { status: "lease_lost", ...base };
						}
						if (generation !== this.#generation || context !== this.#context) {
							await settleAbandonedAttempt("stale");
							return { status: "aborted", ...base };
						}
						if (this.#disposed || jobSignal.aborted) {
							await settleAbandonedAttempt("aborted");
							return { status: "aborted", ...base };
						}
						if (completionError?.category === "aborted" && !providerTimedOut) {
							const requested = retryPolicyRotated ? "stale" : "aborted";
							await settleAbandonedAttempt(requested);
							return { status: retryPolicyRotated ? "stale" : "aborted", ...base };
						}
						if (!completionError && isStructuredAbortError(error)) {
							await settleAbandonedAttempt("aborted");
							return { status: "aborted", ...base };
						}
						if (!providerAttemptStart && !completedAttempt && !providerTimedOut) {
							this.#summaryPolicyMismatch = true;
							return { status: "stale", ...base };
						}
						const failureAt = this.#now();
						const retryDelayMs = Math.min(
							Number.MAX_SAFE_INTEGER - failureAt,
							Math.max(
								completionError?.retryAfterMs ?? 0,
								Math.min(300_000, SUMMARY_RETRY_DELAY_MS * 2 ** Math.min(job.transportRetryCount, 4)),
							),
						);
						const provenance = {
							promptHash,
							modelSelector: summaryModel,
							...(resolvedModel ? { resolvedModel } : {}),
							strategy: job.strategy,
						};
						const failureOutcome: SummaryFailureAttemptOutcome =
							providerTimedOut ||
							!completionError ||
							completionError.category === "aborted" ||
							completionError.category === "provider_key_mismatch"
								? "transport_error"
								: completionError.category;
						const failed = await this.#enqueue(() =>
							context.failSummaryJob(
								lease,
								providerTimedOut
									? providerTimeoutMessage
									: completionError
										? this.#redact(completionError.message)
										: "Summary completion failed",
								retryDelayMs,
								provenance,
								acceptedAttempt ? { attempt: acceptedAttempt, outcome: failureOutcome } : undefined,
							),
						);
						attachLateAttemptSettlement(leaseLost ? "lease_lost" : "aborted");
						if (!failed) return { status: "stale", ...base };
						terminal = true;
						return { status: "provider_failed", ...base };
					}
					const dispatched = completion.attempt;
					providerAttempt = dispatched;
					redactedText = this.#redact(completion.text).trim();
					if (!redactedText) {
						const retryDelayMs = Math.min(
							300_000,
							SUMMARY_RETRY_DELAY_MS * 2 ** Math.min(job.transportRetryCount, 4),
						);
						const failed = await this.#enqueue(() =>
							context.failSummaryJob(
								lease,
								"Summary completion returned no text",
								retryDelayMs,
								{
									promptHash,
									modelSelector: summaryModel,
									...(resolvedModel ? { resolvedModel } : {}),
									strategy: job.strategy,
								},
								{ attempt: dispatched, outcome: "empty_output" },
							),
						);
						if (!failed) return { status: "stale", ...base };
						terminal = true;
						return { status: "provider_failed", ...base };
					}
				}

				if (storeFailed) return { status: "store_failed", ...base, error: firstStoreError };
				if (leaseLost) {
					await settleAttempt(providerAttempt, "lease_lost");
					return { status: "lease_lost", ...base };
				}
				if (jobSignal.aborted || this.#disposed) {
					await settleAttempt(providerAttempt, "aborted");
					return { status: "aborted", ...base };
				}
				if (generation !== this.#generation || context !== this.#context) {
					await settleAttempt(providerAttempt, "stale");
					return { status: "aborted", ...base };
				}
				const provenance = {
					promptHash,
					...(job.stage === "deterministic" ? {} : { modelSelector: summaryModel }),
					...(resolvedModel ? { resolvedModel } : {}),
					strategy: job.strategy,
				};
				const result = await this.#enqueue(() =>
					context.completeSummaryJob(lease, {
						redactedText,
						provenance,
						...(providerAttempt ? { attempt: providerAttempt } : {}),
					}),
				);
				if (result.accepted) {
					terminal = true;
					return { status: "completed", ...base };
				}
				if (result.reason === "escalated") {
					terminal = true;
					return { status: "escalated", ...base };
				}
				if (result.reason === "deterministic_failed") {
					terminal = true;
					return { status: "unfit", ...base };
				}
				return { status: "stale", ...base };
			})();
		} catch (error) {
			recordStoreError(error);
			outcome = { status: "store_failed", ...base, error: firstStoreError };
		} finally {
			clearInterval(renewLease);
			await renewalTask;
			jobController.abort("LCM summary completion settled");
			if (storeFailed) outcome = { status: "store_failed", ...base, error: firstStoreError };
			else if (leaseLost && !terminal) outcome = { status: "lease_lost", ...base };
			await releaseLease();
			if (storeFailed) outcome = { status: "store_failed", ...base, error: firstStoreError };
		}
		return outcome;
	}

	#projectCurrent(messages: readonly AgentMessage[], limits: LcmProjectionLimits, attempt: number): ProjectionCheck {
		const native: ProjectAttempt = { messages: messages as AgentMessage[], owned: false };
		if (attempt !== this.#projectionAttempt) return { result: native };
		const context = this.#context;
		const branch = this.#activeBranch;
		if (!context || !branch || this.#dirty) return { result: native, terminal: { category: "store" } };
		let projection = context.project({
			...branch.snapshot.scope,
			tokenBudget: limits.tokenBudget,
			freshTail: limits.freshTail,
		});
		if (this.#currentBranch?.revision !== projection.revision) {
			const reconciled = context.reconcile(branch.snapshot, {
				summarize: { tokenBudget: limits.tokenBudget, freshTail: limits.freshTail },
			});
			this.#activateBranch(branch, reconciled.revision);
			this.#syncSummaryFailures(context);
			projection = context.project({
				...branch.snapshot.scope,
				tokenBudget: limits.tokenBudget,
				freshTail: limits.freshTail,
			});
		}
		if (this.#currentBranch?.revision !== projection.revision) {
			return { result: native, projection, terminal: { category: "store" } };
		}
		if (projection.pendingJobs > 0 && !this.#summaryRetryDeferred && !this.#summaryTask && !this.#summaryWakeTask) {
			this.#startSummaryJobs();
		}
		if (projection.pendingJobs > 0) {
			this.#noteProjection(projection, false, attempt);
			return { result: native, projection };
		}
		if (!projection.ready || projection.uncoveredSourceIds.length > 0) {
			this.#noteProjection(projection, false, attempt);
			return { result: native, projection, terminal: { category: "unfit", reason: "coverage_gap" } };
		}
		if (!hasExactProjectionCoverage(projection, branch.snapshot.entries)) {
			this.#noteProjection(projection, false, attempt);
			return { result: native, projection, terminal: { category: "unfit", reason: "assembly_invalid" } };
		}
		let measurementCount = 0;
		const fitted = (projectedMessages: AgentMessage[], candidateTokens: number): ProjectionCheck => {
			this.#noteProjection(projection, true, attempt);
			return {
				result: {
					messages: projectedMessages,
					owned: true,
					projection,
					candidateTokens,
					messageTokenBudget: limits.tokenBudget,
					projectionTokenMeasurements: measurementCount,
				},
				projection,
				candidateTokens,
				measurementCount,
			};
		};

		const validMeasurement = (measurement: { tokens: number; upperBound: number }): boolean =>
			Number.isSafeInteger(measurement.tokens) &&
			measurement.tokens >= 0 &&
			Number.isSafeInteger(measurement.upperBound) &&
			measurement.upperBound >= measurement.tokens;
		if (projection.historical.length === 0) {
			measurementCount++;
			const measurement = this.#host.projectionTokenMeasurements(messages);
			if (!validMeasurement(measurement)) {
				this.#noteProjection(projection, false, attempt);
				return { result: native, projection, terminal: { category: "unfit", reason: "fit_invariant" } };
			}
			if (measurement.tokens > limits.tokenBudget) {
				this.#noteProjection(projection, false, attempt);
				return {
					result: native,
					projection,
					terminal: { category: "unfit", reason: "irreducible_input" },
					candidateTokens: measurement.tokens,
				};
			}
			return fitted(messages as AgentMessage[], measurement.tokens);
		}

		const firstUserSourceId = branch.firstUserSourceId;
		if (!firstUserSourceId) {
			this.#noteProjection(projection, false, attempt);
			return { result: native, projection, terminal: { category: "unfit", reason: "assembly_invalid" } };
		}
		const wanted = new Set(projection.freshTailSourceIds);
		wanted.add(firstUserSourceId);
		const requiredMessages: AgentMessage[] = [];
		let firstUserIndex = -1;
		let foundFresh = 0;
		for (const item of branch.ordered) {
			if (!wanted.has(item.source.entryId)) continue;
			if (projection.freshTailSourceIds.includes(item.source.entryId)) foundFresh++;
			if (item.source.entryId === firstUserSourceId) firstUserIndex = requiredMessages.length;
			requiredMessages.push(item.message);
		}
		if (foundFresh !== projection.freshTailSourceIds.length || firstUserIndex < 0) {
			this.#noteProjection(projection, false, attempt);
			return { result: native, projection, terminal: { category: "unfit", reason: "assembly_invalid" } };
		}

		requiredMessages.push(...liveSuffix(messages, this.#host.sessionManager.buildSessionContext()));
		const activeFinalUserIndex = requiredMessages.findLastIndex(message => message.role === "user");
		if (activeFinalUserIndex <= firstUserIndex || requiredMessages[firstUserIndex + 1]?.role === "toolResult") {
			this.#noteProjection(projection, false, attempt);
			return { result: native, projection, terminal: { category: "unfit", reason: "assembly_invalid" } };
		}

		type Candidate = {
			messages: AgentMessage[];
			tokens: number;
			upperBound: number;
			retainedSummaryTextBytes: number;
			summaryTextCap: number;
		};
		const firstUser = requiredMessages[firstUserIndex]!;
		const measuredByRender = new Map<string, Candidate>();
		let invalidMeasurement = false;
		const measure = (
			candidateMessages: AgentMessage[],
			renderKey: string,
			retainedSummaryTextBytes: number,
			summaryTextCap: number,
		): Candidate | undefined => {
			const cached = measuredByRender.get(renderKey);
			if (cached) return cached;
			const measurement = this.#host.projectionTokenMeasurements(candidateMessages);
			measurementCount++;
			if (!validMeasurement(measurement)) {
				invalidMeasurement = true;
				return undefined;
			}
			const candidate = { ...measurement, messages: candidateMessages, retainedSummaryTextBytes, summaryTextCap };
			measuredByRender.set(renderKey, candidate);
			return candidate;
		};
		const render = (renderLimits?: HistoricalRenderLimits): Candidate | undefined => {
			const citedContent = historicalText(projection, branch.snapshot.scope, renderLimits);
			if (!citedContent) return undefined;
			const cached = measuredByRender.get(citedContent);
			if (cached) return cached;
			const cap = renderLimits?.maxSummaryTextBytes;
			const excerpts =
				cap === undefined
					? projection.historical.map(item => item.redactedText)
					: historicalSummaryExcerpts(projection, cap);
			const retainedSummaryTextBytes = excerpts.reduce(
				(total, excerpt) => total + Buffer.byteLength(excerpt, "utf8"),
				0,
			);
			const candidateMessages = requiredMessages.slice();
			candidateMessages.splice(
				firstUserIndex + 1,
				0,
				createHistoricalContextMessage({ redactedCitedContent: citedContent, timestamp: firstUser.timestamp }),
			);
			return measure(candidateMessages, citedContent, retainedSummaryTextBytes, cap ?? retainedSummaryTextBytes);
		};
		let fullRender: Candidate | undefined;
		try {
			const fullContent = historicalText(projection, branch.snapshot.scope);
			if (!fullContent) throw new Error("empty historical render");
			const fullHistorical = createHistoricalContextMessage({
				redactedCitedContent: fullContent,
				timestamp: firstUser.timestamp,
			});
			const fullMessages = requiredMessages.slice();
			fullMessages.splice(firstUserIndex + 1, 0, fullHistorical);
			const totalTextBytes = projection.historical.reduce(
				(total, item) => total + Buffer.byteLength(item.redactedText, "utf8"),
				0,
			);
			fullRender = measure(fullMessages, fullContent, totalTextBytes, totalTextBytes);
			if (!fullRender || invalidMeasurement) {
				this.#noteProjection(projection, false, attempt);
				return { result: native, projection, terminal: { category: "unfit", reason: "fit_invariant" } };
			}
			const fixedUpperBound = fullRender.upperBound - estimateLcmProjectionMessageTokenUpperBound(fullHistorical);
			if (!Number.isSafeInteger(fixedUpperBound) || fixedUpperBound < 0) {
				this.#noteProjection(projection, false, attempt);
				return { result: native, projection, terminal: { category: "unfit", reason: "fit_invariant" } };
			}
			const boundedHistoricalUpperBound = (maxSummaryTextBytes: number, maxFiles: number): number => {
				const citedContent = historicalText(projection, branch.snapshot.scope, { maxSummaryTextBytes, maxFiles });
				const historical = createHistoricalContextMessage({
					redactedCitedContent: citedContent,
					timestamp: firstUser.timestamp,
				});
				return fixedUpperBound + estimateLcmProjectionMessageTokenUpperBound(historical);
			};
			if (fullRender.tokens <= limits.tokenBudget) {
				return fitted(fullRender.messages, fullRender.tokens);
			}

			let maxFiles = PROJECTED_FILES_TOTAL;
			let zero: Candidate | undefined;
			let minimumUpperBoundFits = false;
			let minimumRepresentationTokens = 0;
			for (let candidateMaxFiles = PROJECTED_FILES_TOTAL; candidateMaxFiles >= 0; candidateMaxFiles--) {
				const fullText =
					candidateMaxFiles === PROJECTED_FILES_TOTAL ? fullRender : render({ maxFiles: candidateMaxFiles });
				if (!fullText || invalidMeasurement) {
					this.#noteProjection(projection, false, attempt);
					return { result: native, projection, terminal: { category: "unfit", reason: "fit_invariant" } };
				}
				if (fullText.tokens <= limits.tokenBudget) {
					return fitted(fullText.messages, fullText.tokens);
				}

				const upperBoundFits = boundedHistoricalUpperBound(0, candidateMaxFiles) <= limits.tokenBudget;
				const candidate = render({ maxSummaryTextBytes: 0, maxFiles: candidateMaxFiles });
				if (!candidate || invalidMeasurement) {
					this.#noteProjection(projection, false, attempt);
					return { result: native, projection, terminal: { category: "unfit", reason: "fit_invariant" } };
				}
				minimumRepresentationTokens = candidate.tokens;
				if (candidate.tokens <= limits.tokenBudget) {
					maxFiles = candidateMaxFiles;
					zero = candidate;
					minimumUpperBoundFits = upperBoundFits;
					break;
				}
				if (upperBoundFits) {
					this.#noteProjection(projection, false, attempt);
					return {
						result: native,
						projection,
						terminal: { category: "unfit", reason: "fit_invariant" },
						candidateTokens: candidate.tokens,
					};
				}
			}

			if (!zero) {
				const required = measure(requiredMessages, "", 0, 0);
				if (!required || invalidMeasurement) {
					this.#noteProjection(projection, false, attempt);
					return { result: native, projection, terminal: { category: "unfit", reason: "fit_invariant" } };
				}
				const irreducible = required.tokens > limits.tokenBudget;
				this.#noteProjection(projection, false, attempt);
				return {
					result: native,
					projection,
					terminal: {
						category: "unfit",
						reason: irreducible ? "irreducible_input" : "minimum_representation",
					},
					candidateTokens: irreducible ? required.tokens : minimumRepresentationTokens,
				};
			}

			let best = zero;
			const seenCaps = new Set<number>([0]);
			const evaluateCap = (rawCap: number): void => {
				const cap = Math.max(0, Math.min(totalTextBytes, Math.round(rawCap)));
				if (seenCaps.has(cap)) return;
				seenCaps.add(cap);
				const candidate = render({ maxSummaryTextBytes: cap, maxFiles });
				if (
					candidate &&
					candidate.tokens <= limits.tokenBudget &&
					candidate.retainedSummaryTextBytes > best.retainedSummaryTextBytes
				) {
					best = candidate;
				}
			};

			let conservativeSafeCap = 0;
			if (minimumUpperBoundFits) {
				let lower = 0;
				let upper = totalTextBytes;
				for (let step = 0; step < 53 && lower < upper; step++) {
					const midpoint = lower + Math.ceil((upper - lower) / 2);
					if (boundedHistoricalUpperBound(midpoint, maxFiles) <= limits.tokenBudget) lower = midpoint;
					else upper = midpoint - 1;
				}
				conservativeSafeCap = lower;
			}

			evaluateCap(conservativeSafeCap);
			const geometricSpan = totalTextBytes - conservativeSafeCap;
			for (const fraction of PROJECTION_GEOMETRIC_FRACTIONS) {
				evaluateCap(conservativeSafeCap + geometricSpan * fraction);
			}
			const refinementFloor = best.summaryTextCap;
			const refinementSpan = totalTextBytes - refinementFloor;
			for (let step = 1; step <= PROJECTION_FIT_REFINEMENT_COUNT; step++) {
				evaluateCap(refinementFloor + (refinementSpan * step) / PROJECTION_FIT_REFINEMENT_COUNT);
			}

			if (invalidMeasurement || best.tokens > limits.tokenBudget) {
				this.#noteProjection(projection, false, attempt);
				return { result: native, projection, terminal: { category: "unfit", reason: "fit_invariant" } };
			}
			return fitted(best.messages, best.tokens);
		} catch {
			this.#noteProjection(projection, false, attempt);
			return { result: native, projection, terminal: { category: "unfit", reason: "assembly_invalid" } };
		}
	}

	async #attemptProjection(
		messages: readonly AgentMessage[],
		signal?: AbortSignal,
		origin: "primary" | "maintenance" = "primary",
		requestTokensFloor?: number,
	): Promise<ProjectAttempt> {
		const native: ProjectAttempt = { messages: messages as AgentMessage[], owned: false };
		const aborted = (): ProjectAttempt => ({ ...native, aborted: true });
		if (this.#disposed || signal?.aborted) return aborted();
		this.#syncSessionIdentity();
		const attempt = ++this.#projectionAttempt;
		const isCurrent = (): boolean => attempt === this.#projectionAttempt && !this.#disposed;
		if (origin === "primary") this.#pendingPrimaryRoute = undefined;
		const finish = (
			result: ProjectAttempt,
			route: LcmPrimaryRequestRoute,
			scope?: LcmPrimaryRouteScope,
		): ProjectAttempt => {
			if (signal?.aborted || !isCurrent()) return origin === "maintenance" ? aborted() : result;
			if (origin === "maintenance") {
				return route.kind === "native_fallback"
					? {
							...result,
							maintenanceFallback: {
								category: route.category,
								...(route.reason ? { reason: route.reason } : {}),
							},
						}
					: result;
			}
			const routeKey = this.#stagePrimaryRoute(route, attempt, scope);
			return routeKey ? { ...result, routeKey } : result;
		};
		this.#lastProjectionRequest = undefined;
		this.#lastPressure = undefined;
		this.#invalidateProjectionForRequest();
		const limits = this.#host.projectionLimits(messages);
		if (!limits) {
			return finish(native, { kind: "native_passthrough", reason: "unavailable", metrics: this.#routeMetrics() });
		}
		const routeScopeSource = origin === "primary" ? this.#capturePrimaryRouteScopeSource(attempt) : undefined;
		const requestTokens = Math.max(limits.sourceTokens, requestTokensFloor ?? 0);
		const armTokens = Math.max(requestTokens, this.#currentBranch?.sourceTokens ?? 0);
		this.#lastPressure = {
			requestTokens,
			armTokens,
			prewarmThresholdTokens: limits.prewarmThresholdTokens,
			hardThresholdTokens: limits.hardThresholdTokens,
		};
		let routeScope: LcmPrimaryRouteScope | undefined = routeScopeSource
			? {
					projectId: routeScopeSource.project?.projectId ?? routeScopeSource.fallbackProjectId,
					branchId: routeScopeSource.branchId,
					inputAnchor: routeScopeSource.inputAnchor,
				}
			: undefined;
		if (armTokens < limits.prewarmThresholdTokens) {
			this.#coverageReadiness = "idle";
			return finish(
				native,
				{
					kind: "native_passthrough",
					reason: "below_prewarm",
					metrics: this.#routeMetrics(limits),
				},
				this.#withMatchingActiveRevision(routeScope),
			);
		}
		const atHard = requestTokens > limits.hardThresholdTokens;
		if (routeScopeSource) {
			const resolved = await this.#resolvePrimaryRouteScope(routeScopeSource);
			if (!isCurrent() || signal?.aborted || !this.#primaryRouteScopeSourceIsCurrent(routeScopeSource))
				return native;
			routeScope = resolved.scope;
			if (resolved.error !== undefined) {
				logger.warn("LCM project resolution failed; using native context", { error: errorLabel(resolved.error) });
				const failure = { category: "store" } as const;
				this.#failOpen(failure, attempt);
				return finish(
					native,
					atHard
						? { kind: "native_fallback", ...failure, metrics: this.#routeMetrics(limits) }
						: { kind: "native_passthrough", reason: "below_hard", metrics: this.#routeMetrics(limits) },
					routeScope,
				);
			}
		}
		if (limits.tokenBudget < 1 || limits.freshTail.maxSources < 1 || limits.freshTail.maxTokens < 1) {
			if (!atHard) {
				return finish(
					native,
					{
						kind: "native_passthrough",
						reason: "below_hard",
						metrics: this.#routeMetrics(limits),
					},
					this.#withMatchingActiveRevision(routeScope),
				);
			}
			const failure = { category: "unfit", reason: "irreducible_input" } as const;
			this.#failOpen(failure, attempt);
			return finish(
				native,
				{
					kind: "native_fallback",
					...failure,
					metrics: this.#routeMetrics(limits),
				},
				routeScope,
			);
		}

		this.#summaryRetryDeferred = false;
		this.#lastProjectionRequest = { limits, armTokens };
		if (this.#coverageReadiness !== "ready") this.#coverageReadiness = "warming";
		const reconcile = this.#requestReconcile(true, {
			tokenBudget: limits.tokenBudget,
			freshTail: limits.freshTail,
		});
		if (!atHard) {
			void reconcile.catch(error => {
				if (isCurrent()) logger.debug("LCM background reconcile failed", { error: errorLabel(error) });
			});
			// Preserve the pre-existing fire-and-forget head start after asynchronous scope capture.
			await Promise.resolve();
			if (!isCurrent() || signal?.aborted) return native;
			return finish(
				native,
				{
					kind: "native_passthrough",
					reason: "below_hard",
					metrics: this.#routeMetrics(limits),
				},
				this.#withMatchingActiveRevision(routeScope),
			);
		}

		try {
			const reconciled = await untilAborted(signal, reconcile);
			if (!isCurrent() || signal?.aborted) return native;
			if (!reconciled) {
				if (!isCurrent() || signal?.aborted) return native;
				const failure = this.#healthFailure ?? { category: "store" as const };
				this.#failOpen(failure, attempt);
				return finish(
					native,
					{
						kind: "native_fallback",
						...failure,
						metrics: this.#routeMetrics(limits),
					},
					routeScope,
				);
			}
			if (!isCurrent() || signal?.aborted) return native;

			while (!signal?.aborted && isCurrent()) {
				const progressVersion = this.#summaryProgressVersion;
				const settlement = this.#summaryProgressSignal.promise;
				const check = await this.#enqueue(() => this.#projectCurrent(messages, limits, attempt));
				if (!isCurrent()) return native;
				if (check.result.owned) {
					return finish(
						check.result,
						{
							kind: "lossless",
							metrics: this.#routeMetrics(
								limits,
								check.projection,
								check.candidateTokens,
								check.measurementCount,
							),
						},
						this.#withPrimaryRouteRevision(routeScope, check.projection?.revision),
					);
				}
				if (check.terminal) {
					this.#failOpen(check.terminal, attempt);
					return finish(
						check.result,
						{
							kind: "native_fallback",
							...check.terminal,
							metrics: this.#routeMetrics(limits, check.projection, check.candidateTokens),
						},
						this.#withPrimaryRouteRevision(routeScope, check.projection?.revision),
					);
				}
				if (progressVersion !== this.#summaryProgressVersion) continue;
				let failure: ProjectionFailure | undefined;
				let availability: SummaryJobAvailability | undefined;
				if (this.#summaryPolicyMismatch) {
					failure = { category: "provider", reason: "provider_key_mismatch" };
				} else if (check.projection && check.projection.pendingJobs > 0) {
					availability = await this.#enqueue(() => {
						const context = this.#context;
						if (!context) return undefined;
						const policy = this.#configureSummaryRetryPolicy(context);
						return policy
							? context.summaryJobAvailability(
									{
										...this.#activeBranch!.snapshot.scope,
										tokenBudget: limits.tokenBudget,
										freshTail: limits.freshTail,
									},
									policy,
									SUMMARY_PROVIDER_RETRY_LIMIT,
								)
							: undefined;
					});
					if (!availability || availability.policyMismatch > 0) {
						failure = { category: "provider", reason: "provider_key_mismatch" };
					} else if (availability.exhausted > 0) {
						failure = { category: "provider", reason: "provider_exhausted" };
					} else if (availability.runnable > 0) {
						this.#startSummaryJobs();
					} else if (availability.backoff > 0 && availability.nextAvailableAt !== undefined) {
						this.#scheduleSummaryWake(
							this.#context!,
							this.#generation,
							availability.nextAvailableAt - this.#now(),
						);
					} else if (availability.leased === 0) {
						failure = { category: "unfit", reason: "coverage_gap" };
					}
				} else {
					failure = { category: "unfit", reason: "coverage_gap" };
				}
				if (failure) {
					this.#failOpen(failure, attempt);
					return finish(
						native,
						{ kind: "native_fallback", ...failure, metrics: this.#routeMetrics(limits, check.projection) },
						this.#withPrimaryRouteRevision(routeScope, check.projection?.revision),
					);
				}
				const background = this.#summaryTask ?? this.#summaryWakeTask;
				const progress = Promise.race([
					...(background ? [background] : []),
					settlement,
					Bun.sleep(this.#peerPollMs),
				]);
				await untilAborted(signal, progress);
			}
			return native;
		} catch (error) {
			if (!isCurrent() || signal?.aborted) return native;
			logger.warn("LCM projection failed; using native context", { error: errorLabel(error) });
			const failure = { category: "store" } as const;
			this.#failOpen(failure, attempt);
			return finish(
				native,
				{
					kind: "native_fallback",
					...failure,
					metrics: this.#routeMetrics(limits),
				},
				routeScope,
			);
		}
	}

	/** Project the primary provider request, or return the input unchanged on any unsafe state. */
	async project(messages: AgentMessage[], signal?: AbortSignal): Promise<SessionLcmProjectResult> {
		const intent = this.#nextPrimaryPressureIntent;
		this.#nextPrimaryPressureIntent = undefined;
		const requestTokensFloor =
			intent?.generation === this.#generation &&
			messages.length >= intent.baseMessageCount &&
			intent.baseMessageFingerprint === primaryPressureFingerprint(messages, intent.baseMessageCount)
				? intent.requestTokensFloor
				: undefined;
		const attempt = await this.#attemptProjection(messages, signal, "primary", requestTokensFloor);
		return {
			messages: attempt.messages,
			owned: attempt.owned,
			...(attempt.projection ? { projection: attempt.projection } : {}),
			...(attempt.routeKey ? { routeKey: attempt.routeKey } : {}),
			...(attempt.candidateTokens !== undefined ? { candidateTokens: attempt.candidateTokens } : {}),
			...(attempt.messageTokenBudget !== undefined ? { messageTokenBudget: attempt.messageTokenBudget } : {}),
			...(attempt.projectionTokenMeasurements !== undefined
				? { projectionTokenMeasurements: attempt.projectionTokenMeasurements }
				: {}),
		};
	}

	rearmPrimaryIntent(messages: readonly AgentMessage[], requestTokensFloor?: number): void {
		const intent = this.#nextPrimaryPressureIntent;
		const floor = requestTokensFloor ?? intent?.requestTokensFloor;
		if (
			this.#disposed ||
			!intent ||
			intent.generation !== this.#generation ||
			typeof floor !== "number" ||
			!Number.isSafeInteger(floor) ||
			floor < 0
		) {
			return;
		}
		this.#nextPrimaryPressureIntent = {
			generation: this.#generation,
			baseMessageFingerprint: primaryPressureFingerprint(messages),
			baseMessageCount: messages.length,
			requestTokensFloor: floor,
		};
	}

	/** Decide whether Lossless owns one automatic maintenance request. */
	async ownsRequest(
		messages: readonly AgentMessage[],
		signal?: AbortSignal,
		requestTokensFloor?: number,
	): Promise<LcmOwnershipDecision> {
		this.#nextPrimaryPressureIntent = undefined;
		const generation = this.#generation;
		const projectionAttempt = this.#projectionAttempt + 1;
		const attempt = await this.#attemptProjection(messages, signal, "maintenance", requestTokensFloor);
		if (
			attempt.aborted ||
			signal?.aborted ||
			generation !== this.#generation ||
			projectionAttempt !== this.#projectionAttempt
		) {
			return { kind: "aborted" };
		}
		if (attempt.owned && attempt.projection) {
			this.#nextPrimaryPressureIntent = {
				generation,
				baseMessageFingerprint: primaryPressureFingerprint(messages),
				baseMessageCount: messages.length,
				requestTokensFloor: Math.max(0, requestTokensFloor ?? 0),
			};
			return { kind: "owned", projection: attempt.projection };
		}
		return {
			kind: "native",
			...(attempt.maintenanceFallback ? { fallback: attempt.maintenanceFallback } : {}),
		};
	}

	/** Mark branch/cwd transitions dirty without opening or reconciling below the automatic threshold. */
	async rebind(): Promise<void> {
		if (this.#disposed) return;
		this.#syncSessionIdentity();
		this.#generation++;
		this.#projectionAttempt++;
		this.#dirty = true;
		this.#summaryRestartRequested = false;
		this.#deferReconcileUntilSummarySettles = false;
		this.#summaryRetryDeferred = false;
		this.#nextPrimaryPressureIntent = undefined;
		this.#pendingPrimaryRoute = undefined;
		this.#preferredUnfit = false;
		this.#lastProjectionRequest = undefined;
		this.#lastPressure = undefined;
		this.#coverageReadiness = "idle";
		this.#clearActiveBranch();
		const drain = this.#summaryTask;
		this.#summaryAbortController?.abort("LCM session binding changed");
		this.#signalSummaryCapacity();
		this.#clearSummaryWake();
		try {
			await drain;
		} catch (error) {
			this.#noteFailure({ category: "store" });
			throw error;
		}
	}

	async status(): Promise<LcmPublicStatus> {
		const manager = this.#host.sessionManager;
		this.#syncSessionIdentity();
		const cwd = manager.getCwd();
		const activeAnchor = branchAnchor(manager, manager.getBranch());
		if (this.#dirty || !this.#context || this.#boundCwd !== cwd || this.#activeBranch?.anchor !== activeAnchor) {
			await this.#requestReconcile(true);
		} else {
			this.#startSummaryJobs();
		}
		return this.#enqueue(() => {
			const context = this.#context;
			if (!context || this.#boundCwd !== this.#host.sessionManager.getCwd()) {
				return { runtime: this.#runtimeStatus() };
			}
			this.#syncSummaryFailures(context);
			const contextStatus = context.status();
			const store: LcmPublicStoreStatus = {
				schemaVersion: contextStatus.schemaVersion,
				journalMode: contextStatus.journalMode,
				quarantined: contextStatus.quarantined,
				branches: contextStatus.branches,
				activeSources: contextStatus.activeSources,
				tombstones: contextStatus.tombstones,
				leafSummaries: contextStatus.leafSummaries,
				condensedSummaries: contextStatus.condensedSummaries,
				jobs: contextStatus.jobs,
				storage: contextStatus.storage,
				latestRecovery: contextStatus.latestRecovery,
			};
			if (store.quarantined) {
				this.#runtimeHealth = "quarantined";
				this.#coverageReadiness = undefined;
			} else if (this.#runtimeHealth === "quarantined") {
				this.#runtimeHealth = "healthy";
				this.#coverageReadiness ??= "idle";
				this.#restoreHealth();
			}
			return { runtime: this.#runtimeStatus(), store };
		});
	}

	async doctor(): Promise<DoctorReport | null> {
		if (!(await this.#requestReconcile(true))) return null;
		return this.#enqueue(() => this.#context?.doctor() ?? null);
	}

	async rebuild(): Promise<RebuildResult | null> {
		return this.#enqueue(async () => {
			const context = await this.#ensureOpen();
			const project = this.#project;
			if (!context || !project || this.#disposed) return null;
			try {
				const normalized = this.#normalizeActiveBranch(project);
				const result = context.rebuild([normalized.snapshot]);
				this.#summaryRetryPolicy = undefined;
				this.#clearProviderDegradation(true);
				const reconciled = context.reconcile(normalized.snapshot, { summarize: false });
				this.#activateBranch(normalized, reconciled.revision);
				this.#syncSummaryFailures(context);
				this.#dirty = false;
				if (result.queuedJobs > 0) this.#startSummaryJobs();
				return result;
			} catch (error) {
				logger.warn("LCM rebuild failed", { error: errorLabel(error) });
				return null;
			}
		});
	}

	async rebuildProject(snapshots: readonly SourceSnapshot[]): Promise<RebuildResult | null> {
		return this.#enqueue(async () => {
			const context = await this.#ensureOpen();
			const project = this.#project;
			if (!context || !project || this.#disposed || snapshots.length === 0) return null;
			try {
				if (snapshots.some(snapshot => snapshot.scope.projectId !== project.projectId)) {
					throw new Error("LCM project rebuild snapshots belong to another project");
				}
				const active = this.#normalizeActiveBranch(project);
				const activeSnapshot = active.snapshot;
				const result = context.rebuild(snapshots);
				this.#summaryRetryPolicy = undefined;
				this.#clearProviderDegradation(true);
				const activeRebuilt = snapshots.some(
					snapshot =>
						snapshot.scope.sessionId === activeSnapshot.scope.sessionId &&
						snapshot.scope.branchId === activeSnapshot.scope.branchId &&
						snapshot.entries.length === activeSnapshot.entries.length &&
						snapshot.entries.every((entry, index) => {
							const activeEntry = activeSnapshot.entries[index];
							return (
								activeEntry !== undefined &&
								entry.entryId === activeEntry.entryId &&
								entry.contentHash === activeEntry.contentHash
							);
						}),
				);
				if (activeRebuilt) {
					const reconciled = context.reconcile(activeSnapshot, { summarize: false });
					this.#activateBranch(active, reconciled.revision);
				} else this.#clearActiveBranch();
				this.#syncSummaryFailures(context);
				this.#dirty = !activeRebuilt;
				if (result.queuedJobs > 0) this.#startSummaryJobs();
				return result;
			} catch (error) {
				logger.warn("LCM project rebuild failed", { error: errorLabel(error) });
				return null;
			}
		});
	}

	async purge(): Promise<PurgeResult | null> {
		if (!(await this.#requestReconcile(true))) return null;
		return this.#enqueue(() => this.#context?.purge() ?? null);
	}

	async retrySummaries(mode: "due" | "all" = "due"): Promise<SummaryJobAvailability | null> {
		const messages = this.#host.sessionManager.buildSessionContext().messages;
		const limits = this.#host.projectionLimits(messages);
		if (!limits) return null;
		if (!(await this.#requestReconcile(true, { tokenBudget: limits.tokenBudget, freshTail: limits.freshTail }))) {
			return null;
		}
		const availability = await this.#enqueue(() => {
			const context = this.#context;
			const scope = this.#activeBranch?.snapshot.scope;
			if (!context || !scope) return null;
			const policy = this.#configureSummaryRetryPolicy(context);
			if (!policy) return null;
			const availability = context.retrySummaryJobs(
				{ ...scope, tokenBudget: limits.tokenBudget, freshTail: limits.freshTail },
				policy,
				SUMMARY_PROVIDER_RETRY_LIMIT,
				mode,
			);
			this.#syncSummaryFailures(context);
			return availability;
		});
		if (availability?.runnable) this.#startSummaryJobs();
		return availability;
	}

	async search(query: string, options: LcmSearchOptions = {}): Promise<SearchHit[]> {
		if (!(await this.#requestReconcile(true))) return [];
		return this.#enqueue(() => {
			const context = this.#context;
			const scope = this.#activeBranch?.snapshot.scope;
			if (!context || !scope) return [];
			const summary = options.summary;
			if (
				summary &&
				(summary.projectId !== scope.projectId ||
					summary.sessionId !== scope.sessionId ||
					summary.branchId !== scope.branchId)
			) {
				return [];
			}
			return context.search({
				...scope,
				query,
				...(options.limit === undefined ? {} : { limit: options.limit }),
				...(options.offset === undefined ? {} : { offset: options.offset }),
				...(summary ? { summaryHandle: summary.summaryHandle } : {}),
				...(options.mode === undefined ? {} : { mode: options.mode }),
			});
		});
	}

	/**
	 * Cue-path search. Deliberately skips the reconcile `search` performs: the caller runs inside a
	 * turn that just projected, so reconciling again would repeat that work on every provider call.
	 * Scope still comes only from the active branch, never from an argument.
	 */
	async searchProjected(query: string, limit: number, expectedRevision: number): Promise<SearchHit[]> {
		return this.#enqueue(() => {
			const context = this.#context;
			const scope = this.#activeBranch?.snapshot.scope;
			// A reconcile between the projection and this call would leave the cue describing a
			// revision the request no longer contains.
			if (!context || !scope || this.#currentBranch?.revision !== expectedRevision) return [];
			return context.search({ ...scope, query, limit, mode: "text" });
		});
	}

	async describe(handle: LcmHandle): Promise<LcmDescription | null> {
		if (!(await this.#requestReconcile(true))) return null;
		const resolved = await this.#enqueue(() => {
			const context = this.#context;
			const scope = this.#activeBranch?.snapshot.scope;
			if (!context || !scope) return null;
			const reference = handle.kind === "source" ? handle.citation : handle.reference;
			if (
				reference.projectId !== scope.projectId ||
				reference.sessionId !== scope.sessionId ||
				reference.branchId !== scope.branchId
			) {
				return null;
			}
			switch (handle.kind) {
				case "source": {
					const value = context.describe(handle.citation);
					return value ? ({ kind: "source", value } as const) : null;
				}
				case "summary": {
					const value = context.describeSummary(handle.reference);
					return value ? ({ kind: "summary", value } as const) : null;
				}
				case "file": {
					const value = context.describeFile(handle.reference);
					return value
						? ({
								kind: "file",
								value,
								rootPath: this.#project?.rootPath,
								activeReference: this.#activeFileReferences.get(handle.reference.fileId),
							} as const)
						: null;
				}
			}
		});
		if (!resolved) return null;
		if (resolved.kind !== "file") return resolved;
		const rootPath = resolved.rootPath;
		const activeReference = resolved.activeReference;
		let available = false;
		if (
			rootPath &&
			activeReference &&
			activeReference.byteSize === resolved.value.byteSize &&
			activeReference.contentHash === resolved.value.contentHash
		) {
			const root = path.resolve(rootPath);
			const candidate = activeReference.path;
			if (pathIsWithin(root, candidate)) {
				try {
					const stat = await Bun.file(candidate).stat();
					available =
						stat.isFile() &&
						stat.size === activeReference.byteSize &&
						(await fileContentHash(candidate)) === activeReference.contentHash;
				} catch {
					available = false;
				}
			}
		}
		return { kind: "file", value: { ...resolved.value, available } };
	}

	async expand(options: LcmExpandOptions): Promise<LcmResolvedExpansion | null> {
		if (!(await this.#requestReconcile(true))) return null;
		return this.#enqueue(() => {
			const context = this.#context;
			const scope = this.#activeBranch?.snapshot.scope;
			const reference = options.reference;
			if (
				!context ||
				!scope ||
				reference.projectId !== scope.projectId ||
				reference.sessionId !== scope.sessionId ||
				reference.branchId !== scope.branchId
			) {
				return null;
			}
			const expansion = context.expandSummary({ ...reference, ...options });
			if (!expansion) return null;
			return {
				...expansion,
				items: expansion.items.map(item => {
					if (item.kind === "summary") return item;
					const source = context.describe(item.citation);
					return {
						...item,
						available: source !== null,
						...(source ? { redactedText: source.redactedText } : {}),
					};
				}),
			};
		});
	}

	/** Synchronously stop observers/new work; asynchronous close drains below. */
	beginDispose(): void {
		if (this.#disposed) return;
		this.#disposed = true;
		this.#generation++;
		this.#projectionAttempt++;
		this.#summaryRestartRequested = false;
		this.#deferReconcileUntilSummarySettles = false;
		this.#summaryRetryDeferred = false;
		this.#nextPrimaryPressureIntent = undefined;
		this.#unsubscribeDurableEntries?.();
		this.#unsubscribeDurableEntries = undefined;
		this.#summaryAbortController?.abort();
		this.#signalSummaryCapacity();
		this.#clearSummaryWake();
		this.#clearActiveBranch();
	}

	close(): Promise<void> {
		if (this.#closeTask) return this.#closeTask;
		this.beginDispose();
		this.#closeTask = (async () => {
			let summaryError: unknown;
			let summaryRejected = false;
			try {
				await this.#summaryTask;
			} catch (error) {
				summaryRejected = true;
				summaryError = error;
				logger.warn("LCM summary finalizer failed during close", { error: errorLabel(error) });
			}
			await this.#operationTail;
			this.#context?.close();
			this.#context = undefined;
			this.#project = undefined;
			this.#clearActiveBranch();
			this.#lastProjectionRequest = undefined;
			this.#lastPressure = undefined;
			if (summaryRejected) throw summaryError;
		})();
		return this.#closeTask;
	}
}
