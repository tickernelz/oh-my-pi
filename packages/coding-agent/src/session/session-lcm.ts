import * as path from "node:path";
import type {
	ContextProjection,
	DoctorReport,
	LcmContext,
	LcmFileMetadata,
	LcmStatus,
	PurgeResult,
	RebuildResult,
	SearchHit,
	SourceEntry,
	SourceSnapshot,
	SummaryJob,
} from "@oh-my-pi/lcm-context";
import { isLcmSqliteContentionError, openLcmContext } from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import { estimateTokens } from "@oh-my-pi/pi-agent-core/compaction";
import * as AIError from "@oh-my-pi/pi-ai/error";

import { logger, pathIsWithin, prompt } from "@oh-my-pi/pi-utils";
import type {
	LcmDescription,
	LcmExpandOptions,
	LcmHandle,
	LcmResolvedExpansion,
	LcmSearchOptions,
} from "../lcm/operations";
import { type LcmProject, resolveLcmProject } from "../lcm/project-identity";
import lcmSummarySystemPrompt from "../prompts/lcm/summary-system.md" with { type: "text" };
import lcmSummaryUserPrompt from "../prompts/lcm/summary-user.md" with { type: "text" };
import type { SecretObfuscator } from "../secrets/obfuscator";
import { resolveReadPath } from "../tools/path-utils";
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
const SUMMARY_RETRY_DELAY_MS = 30_000;
/** Longest provider-requested delay retained by the durable summary scheduler. */
const SUMMARY_RETRY_AFTER_MAX_MS = 24 * 60 * 60_000;
const SUMMARY_MAX_OUTPUT_TOKENS = 2_048;
const HARD_PROJECTION_WAIT_MS = 30_000;
const SQLITE_CONTENTION_DELAYS_MS = [100, 200, 400] as const;
const MAX_TIMER_DELAY_MS = 2_147_483_647;

const ARTIFACT_REF_PATTERN = /(?:artifact:\/\/\d+|blob:sha256:[a-f0-9]{64})/g;

export type LcmRuntimePhase = "disabled" | "uninitialized" | "idle" | "warming" | "active" | "degraded" | "quarantined";
export class LcmCompletionError extends Error {
	readonly provider: string | undefined;
	readonly retryAfterMs: number | undefined;

	constructor(message: string, options: { provider?: string; retryAfterMs?: number }) {
		super(message.slice(0, 2_048));
		this.name = "LcmCompletionError";
		this.provider = options.provider?.slice(0, 128);
		const retryAfterMs = options.retryAfterMs;
		this.retryAfterMs =
			typeof retryAfterMs === "number" && Number.isFinite(retryAfterMs) && retryAfterMs >= 0
				? Math.min(SUMMARY_RETRY_AFTER_MAX_MS, Math.ceil(retryAfterMs))
				: undefined;
	}
}

export function normalizeLcmMaxConcurrentSummaries(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) && Number.isInteger(value)
		? Math.max(1, Math.min(4, value))
		: 1;
}

export interface LcmProjectionAggregate {
	revision: number;
	sourceTokens: number;
	selectedLevelCounts: Readonly<Record<number, number>>;
	coveredSourceCount: number;
	freshSourceCount: number;
	estimatedTokens: number;
	pendingJobs: number;
}

export interface LcmRuntimeStatus {
	phase: LcmRuntimePhase;
	summaryWorkers: { active: number; limit: number };
	summaryBackoff?: { preferred?: number; fallback?: number };
	summaryModelSelector?: string;
	resolvedSummaryModel?: string;
	lastProjection?: LcmProjectionAggregate;
	lastFailureCategory?: LcmFallbackCategory;
	retryAt?: number;
}

export interface LcmPublicStatus {
	runtime: LcmRuntimeStatus;
	store?: Omit<LcmStatus, "dbPath">;
}

export interface LcmCompletionRequest {
	systemPrompt: string;
	prompt: string;
	maxOutputTokens: number;
	oneshotKind: "lcm_summary" | "lcm_recall";
	modelSelector?: string;
	signal?: AbortSignal;
	/** Reports the concrete provider/model selected for this isolated request. */
	onResolvedModel?: (model: string) => void;
}

export interface LcmProjectionLimits {
	/** Total native request estimate, including stable non-message inputs. */
	sourceTokens: number;
	softThresholdTokens: number;
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
	projectionFits(messages: readonly AgentMessage[]): boolean;
	complete(request: LcmCompletionRequest): Promise<string>;
}

export interface SessionLcmDependencies {
	openContext?: typeof openLcmContext;
	resolveProject?: typeof resolveLcmProject;
	hardWaitMs?: number;
	now?: () => number;
}

export interface SessionLcmOptions {
	agentDir?: string;
	summaryModel?: string;
	maxConcurrentSummaries?: number;
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

export interface SessionLcmProjectResult {
	messages: AgentMessage[];
	owned: boolean;
	/** Present only when a complete, locally fitted projection owns the request. */
	projection?: ContextProjection;
}

type ProjectAttempt = SessionLcmProjectResult;

interface ProjectionCheck {
	result: ProjectAttempt;
	projection?: ContextProjection;
	terminal?: LcmFallbackCategory;
}

type SummaryQueueClass = SummaryJob["queueClass"];

type SummaryWorkerOutcome =
	| {
			status: "completed" | "escalated" | "stale" | "lease_lost" | "unfit" | "aborted";
			jobId: string;
			queueClass: SummaryQueueClass;
	  }
	| { status: "provider_failed"; jobId: string; queueClass: SummaryQueueClass; retryAt: number }
	| { status: "store_failed"; jobId: string; queueClass: SummaryQueueClass; error: unknown };

interface ProjectionRequest {
	limits: LcmProjectionLimits;
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

function referenceOnlyFileMetadata(
	message: Extract<AgentMessage, { role: "fileMention" }>,
	projectId: string,
	redact: (text: string) => string,
	activeFiles?: ActiveFileReferenceCollector,
): LcmFileMetadata[] {
	const metadata: LcmFileMetadata[] = [];
	for (const file of message.files) {
		if (!file.skippedReason) continue;
		const safePath = redact(file.path);
		const byteSize = Math.max(0, file.byteSize ?? 0);
		const contentHash =
			file.contentHash ??
			new Bun.CryptoHasher("sha256")
				.update(`legacy-reference\0${safePath}\0${byteSize}\0${file.skippedReason}`)
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
			explorationSummary: `Reference-only ${file.skippedReason === "tooLarge" ? "oversized" : "binary"} file; bytes remain outside the LCM store.`,
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
	if (message.role !== "historicalContext") return estimateTokens(message);
	const providerMessage = convertToLlm([message])[0];
	return providerMessage ? estimateTokens(providerMessage) : Number.POSITIVE_INFINITY;
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

function historicalText(projection: ContextProjection): string {
	return projection.historical
		.map(item => {
			const sourceIds = [...new Set(item.citations.map(citation => citation.sourceId))];
			const citations = sourceIds.length > 0 ? `\n[Sources: ${sourceIds.map(id => `source:${id}`).join(", ")}]` : "";
			return `${item.redactedText}${citations}`;
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
		freshSourceCount: projection.freshSourceCount,
		estimatedTokens: projection.estimatedTokens,
		pendingJobs: projection.pendingJobs,
	};
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

function deterministicSummary(job: SummaryJob): string {
	const byteBudget = Math.min(512, job.inputTokenCount - 1) * 4;
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
		const files =
			message.role === "fileMention" ? referenceOnlyFileMetadata(message, projectId, redact, activeFiles) : [];
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
	readonly #openContext: typeof openLcmContext;
	readonly #resolveProject: typeof resolveLcmProject;
	readonly #registerProject:
		| ((project: LcmProject, journal: { sessionDir: string; sessionFile?: string }) => Promise<void>)
		| undefined;
	readonly #hardWaitMs: number;
	readonly #now: () => number;
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
	#summaryWakeTimer: NodeJS.Timeout | undefined;
	#summaryWakeAt: number | undefined;
	#summaryWakeTask: Promise<void> | undefined;
	#resolveSummaryWake: (() => void) | undefined;
	#closeTask: Promise<void> | undefined;
	#unsubscribeDurableEntries: (() => void) | undefined;
	#registeredJournalKey: string | undefined;
	#runtimePhase: LcmRuntimePhase = "uninitialized";
	#lastProjection: LcmProjectionAggregate | undefined;
	#lastFailureCategory: LcmFallbackCategory | undefined;
	#pendingFallbackCategory: LcmFallbackCategory | undefined;
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
		this.#openContext = options.dependencies?.openContext ?? openLcmContext;
		this.#resolveProject = options.dependencies?.resolveProject ?? resolveLcmProject;
		this.#registerProject = options.registerProject;
		this.#hardWaitMs = Math.max(1, options.dependencies?.hardWaitMs ?? HARD_PROJECTION_WAIT_MS);
		this.#now = options.dependencies?.now ?? Date.now;
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
		if (nextModel !== this.#summaryModel) this.#resolvedSummaryModel = undefined;
		this.#summaryModel = nextModel;
		this.#maxConcurrentSummaries = normalizeLcmMaxConcurrentSummaries(options.maxConcurrentSummaries);
		this.#signalSummaryCapacity();
		if (this.#context) this.#startSummaryJobs();
	}

	takePendingFallbackCategory(): LcmFallbackCategory | undefined {
		const category = this.#pendingFallbackCategory;
		this.#pendingFallbackCategory = undefined;
		return category;
	}

	#runtimeStatus(): LcmRuntimeStatus {
		const preferred = this.#maxFailureDeadline(this.#preferredFailures);
		const fallback = this.#maxFailureDeadline(this.#fallbackFailures);
		return {
			phase: this.#runtimePhase,
			summaryWorkers: { active: this.#activeSummaryJobs.size, limit: this.#maxConcurrentSummaries },
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
			...(this.#lastProjection ? { lastProjection: this.#lastProjection } : {}),
			...(this.#lastFailureCategory ? { lastFailureCategory: this.#lastFailureCategory } : {}),
			...(this.#retryAt === undefined ? {} : { retryAt: this.#retryAt }),
		};
	}

	#redact(text: string): string {
		return this.#host.obfuscator?.hasSecrets() ? this.#host.obfuscator.obfuscate(text) : text;
	}

	#clearActiveBranch(): void {
		this.#activeBranch = undefined;
		this.#activeFileReferences.clear();
	}

	#activateBranch(normalized: NormalizedBranch): void {
		this.#activeBranch = normalized;
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

	#noteFailure(category: LcmFallbackCategory, retryAt?: number): void {
		this.#runtimePhase = "degraded";
		this.#lastFailureCategory = category;
		this.#retryAt = retryAt;
	}

	#hasPreferredHealthFailure(): boolean {
		return this.#preferredUnfit || this.#preferredFailures.size > 0;
	}

	#failOpen(category: LcmFallbackCategory): void {
		this.#noteFailure(category, this.#retryAt);
		this.#pendingFallbackCategory = category;
	}

	#noteProjection(projection: ContextProjection, active: boolean): void {
		this.#lastProjection = projectionAggregate(projection);
		if (!active) {
			this.#runtimePhase = this.#hasPreferredHealthFailure() ? "degraded" : "warming";
			return;
		}
		this.#preferredUnfit = false;
		this.#runtimePhase = "active";
		this.#pendingFallbackCategory = undefined;
		if (this.#preferredFailures.size === 0 && this.#fallbackFailures.size === 0) {
			this.#lastFailureCategory = undefined;
			this.#retryAt = undefined;
		} else {
			this.#lastFailureCategory = "provider";
		}
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

	#syncSummaryFailures(context: LcmContext): void {
		const failures = context.summaryJobFailures(this.#activeBranch?.snapshot.scope);
		this.#preferredFailures.clear();
		this.#fallbackFailures.clear();
		for (const failure of failures) {
			(failure.queueClass === "preferred" ? this.#preferredFailures : this.#fallbackFailures).set(
				failure.jobId,
				failure.availableAt,
			);
		}
		const preferred = this.#maxFailureDeadline(this.#preferredFailures);
		const fallback = this.#maxFailureDeadline(this.#fallbackFailures);
		this.#retryAt =
			preferred === undefined ? fallback : fallback === undefined ? preferred : Math.max(preferred, fallback);
		if (preferred !== undefined || fallback !== undefined) {
			this.#lastFailureCategory = "provider";
			if (this.#hasPreferredHealthFailure() && this.#runtimePhase !== "active") this.#runtimePhase = "degraded";
		} else if (this.#lastFailureCategory === "provider") {
			this.#lastFailureCategory = this.#preferredUnfit ? "unfit" : undefined;
			if (this.#runtimePhase === "degraded" && !this.#preferredUnfit) this.#runtimePhase = "warming";
		}
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

	async #waitWithinDeadline(
		task: Promise<unknown>,
		signal: AbortSignal | undefined,
		deadline: number,
	): Promise<boolean> {
		if (signal?.aborted) return false;
		const remaining = deadline - this.#now();
		if (remaining <= 0) return false;
		const timeout = Promise.withResolvers<false>();
		const timer = setTimeout(() => timeout.resolve(false), remaining);
		const onAbort = (): void => timeout.resolve(false);
		signal?.addEventListener("abort", onAbort, { once: true });
		try {
			return await Promise.race([task.then(() => true as const), timeout.promise]);
		} finally {
			clearTimeout(timer);
			signal?.removeEventListener("abort", onAbort);
		}
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
		const cwd = this.#host.sessionManager.getCwd();
		if (this.#boundCwd !== undefined && this.#boundCwd !== cwd) {
			this.#pendingFallbackCategory = undefined;
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
			this.#noteFailure("store");
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
			this.#pendingFallbackCategory = undefined;
			this.#preferredUnfit = false;
			this.#preferredFailures.clear();
			this.#fallbackFailures.clear();
			this.#retryAt = undefined;
			if (this.#lastFailureCategory === "provider") this.#lastFailureCategory = undefined;
			this.#resolvedSummaryModel = undefined;
		}

		let context: LcmContext;
		try {
			context = await this.#openContext({ dbPath: project.storePath, recoverCorrupt: true });
		} catch (error) {
			logger.warn("LCM store open failed; using native context", { error: errorLabel(error) });
			this.#noteFailure("store");
			return undefined;
		}
		if (this.#disposed || generation !== this.#generation || cwd !== this.#host.sessionManager.getCwd()) {
			context.close();
			return undefined;
		}

		this.#context = context;
		this.#project = project;
		this.#boundCwd = cwd;
		this.#runtimePhase = "idle";
		return context;
	}

	#normalizeActiveBranch(project: LcmProjectBinding): NormalizedBranch {
		this.#clearActiveBranch();
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
		this.#lastProjection = projectionAggregate(projection);
		return projection;
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
				context.reconcile(normalized.snapshot, { summarize });
				this.#activateBranch(normalized);
				this.#syncSummaryFailures(context);
				reconciled = true;
				const scope = normalized.snapshot.scope;
				const projection =
					summarize === false
						? this.#projectForScheduling(context)
						: context.project({ ...scope, tokenBudget: summarize.tokenBudget, freshTail: summarize.freshTail });
				if (projection) {
					this.#lastProjection = projectionAggregate(projection);
					if (this.#runtimePhase !== "active") {
						this.#runtimePhase = this.#hasPreferredHealthFailure() ? "degraded" : "warming";
					}
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
				const delayMs = context.nextSummaryJobDelayMs(scope, effectiveAllowFallback);
				if (
					this.#runtimePhase !== "active" &&
					!this.#hasPreferredHealthFailure() &&
					(delayMs !== null || fallbackBlocked)
				) {
					this.#runtimePhase = "warming";
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
				this.#noteFailure("store");
				this.#clearActiveBranch();
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
					this.#noteFailure("store");
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
				const activeBeforeBatch = this.#runtimePhase === "active";
				let preferredUnfitObserved = false;
				let unfitObserved = false;
				let progressObserved = false;
				for (const outcome of batch) {
					this.#activeSummaryJobs.delete(outcome.jobId);
					switch (outcome.status) {
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
							} else if (!activeBeforeBatch) {
								unfitObserved = true;
							}
							break;
						case "completed":
						case "escalated":
							progressObserved = true;
							break;
					}
				}
				if (preferredUnfitObserved) this.#preferredUnfit = true;
				if (unfitObserved) this.#noteFailure("unfit");
				else if (progressObserved && this.#runtimePhase !== "active") {
					this.#runtimePhase = this.#hasPreferredHealthFailure() ? "degraded" : "warming";
				}
				try {
					await this.#enqueue(() => this.#syncSummaryFailures(context));
				} catch (error) {
					recordStoreError(error);
				}
			}

			if (storeFailed) this.#summaryAbortController?.abort("LCM summary store failure");
			if (storeFailed || abortObserved) {
				if (this.#activeSummaryJobs.size > 0) {
					await Promise.race(this.#activeSummaryJobs.values());
					continue;
				}
				if (storeFailed) throw firstStoreError;
				this.#summaryRetryDeferred = true;
				this.#summaryRestartRequested = false;
				return;
			}

			let blockedWakeMs: number | null = null;
			while (isCurrent() && this.#activeSummaryJobs.size < this.#maxConcurrentSummaries) {
				let claimed: {
					job: SummaryJob | undefined;
					summaryModel: string;
					delayMs: number | null;
					preferredOnly: boolean;
					cancelled?: boolean;
					handoff?: boolean;
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
						const job = context.claimSummaryJobs({
							workerId: this.#workerId,
							leaseMs: SUMMARY_LEASE_MS,
							limit: 1,
							maxOutputTokens: SUMMARY_MAX_OUTPUT_TOKENS,
							...(scope ? { preferredScope: scope } : {}),
							allowFallback: effectiveAllowFallback,
						})[0];
						const delayMs = job
							? null
							: (context.nextSummaryJobDelayMs(scope, effectiveAllowFallback) ??
								(fallbackBlocked && fallbackRetryAt !== undefined ? fallbackRetryAt - this.#now() : null));
						return {
							job,
							summaryModel: this.#summaryModel ?? "@smol",
							delayMs,
							preferredOnly: !allowFallback,
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
						await this.#enqueue(() => context.releaseSummaryJob(claimed.job!.jobId, claimed.job!.leaseToken));
					} catch (error) {
						recordStoreError(error);
						this.#summaryAbortController?.abort("LCM summary store failure");
					}
					break;
				}
				if (!claimed.job) {
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
					return context.extendSummaryJob(job.jobId, job.leaseToken, SUMMARY_LEASE_MS);
				})
					.then(extended => {
						if (extended === false) {
							leaseLost = true;
							jobController.abort("LCM summary lease lost");
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
						maxOutputTokens: job.outputTokenBudget,
						inputs: job.inputs.map(input => ({ kind: input.kind, id: input.id, text: input.redactedText })),
					});
					promptHash = summaryPromptHash(systemPrompt, userPrompt);
					let output: string;
					try {
						output = await this.#host.complete({
							systemPrompt,
							prompt: userPrompt,
							maxOutputTokens: job.outputTokenBudget,
							oneshotKind: "lcm_summary",
							modelSelector: summaryModel,
							signal: jobSignal,
							onResolvedModel: model => {
								resolvedModel = model;
								this.#resolvedSummaryModel = model;
							},
						});
					} catch (error) {
						if (storeFailed) return { status: "store_failed", ...base, error: firstStoreError };
						if (leaseLost) return { status: "lease_lost", ...base };
						if (
							this.#disposed ||
							jobSignal.aborted ||
							generation !== this.#generation ||
							context !== this.#context ||
							(!(error instanceof LcmCompletionError) && isStructuredAbortError(error))
						) {
							return { status: "aborted", ...base };
						}
						const retryAfterMs = error instanceof LcmCompletionError ? (error.retryAfterMs ?? 0) : 0;
						const failureAt = this.#now();
						const retryDelayMs = Math.min(
							Number.MAX_SAFE_INTEGER - failureAt,
							Math.max(
								retryAfterMs,
								Math.min(300_000, SUMMARY_RETRY_DELAY_MS * 2 ** Math.min(job.transportRetryCount, 4)),
							),
						);
						const provenance = {
							promptHash,
							modelSelector: summaryModel,
							...(resolvedModel ? { resolvedModel } : {}),
							strategy: job.strategy,
						};
						const failed = await this.#enqueue(() =>
							context.failSummaryJob(
								job.jobId,
								job.leaseToken,
								error instanceof LcmCompletionError ? this.#redact(error.message) : "Summary completion failed",
								retryDelayMs,
								provenance,
							),
						);
						if (!failed) return { status: "stale", ...base };
						terminal = true;
						return { status: "provider_failed", ...base, retryAt: failureAt + retryDelayMs };
					}
					redactedText = this.#redact(output).trim();
					if (!redactedText) {
						const retryDelayMs = Math.min(
							300_000,
							SUMMARY_RETRY_DELAY_MS * 2 ** Math.min(job.transportRetryCount, 4),
						);
						const failed = await this.#enqueue(() =>
							context.failSummaryJob(
								job.jobId,
								job.leaseToken,
								"Summary completion returned no text",
								retryDelayMs,
								{
									promptHash,
									modelSelector: summaryModel,
									...(resolvedModel ? { resolvedModel } : {}),
									strategy: job.strategy,
								},
							),
						);
						if (!failed) return { status: "stale", ...base };
						terminal = true;
						return { status: "provider_failed", ...base, retryAt: this.#now() + retryDelayMs };
					}
				}

				if (storeFailed) return { status: "store_failed", ...base, error: firstStoreError };
				if (leaseLost) return { status: "lease_lost", ...base };
				if (jobSignal.aborted || this.#disposed || generation !== this.#generation || context !== this.#context) {
					return { status: "aborted", ...base };
				}
				const provenance = {
					promptHash,
					...(job.stage === "deterministic" ? {} : { modelSelector: summaryModel }),
					...(resolvedModel ? { resolvedModel } : {}),
					strategy: job.strategy,
				};
				const result = await this.#enqueue(() =>
					context.completeSummaryJob(job.jobId, job.leaseToken, { redactedText, provenance }),
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
			if (!terminal) {
				try {
					await this.#enqueue(() => context.releaseSummaryJob(job.jobId, job.leaseToken));
				} catch (error) {
					recordStoreError(error);
				}
			}
			if (storeFailed) outcome = { status: "store_failed", ...base, error: firstStoreError };
		}
		return outcome;
	}

	#projectCurrent(messages: readonly AgentMessage[], limits: LcmProjectionLimits): ProjectionCheck {
		const native: ProjectAttempt = { messages: messages as AgentMessage[], owned: false };
		const context = this.#context;
		const branch = this.#activeBranch;
		if (!context || !branch || this.#dirty) return { result: native, terminal: "store" };
		const projection = context.project({
			...branch.snapshot.scope,
			tokenBudget: limits.tokenBudget,
			freshTail: limits.freshTail,
		});
		this.#noteProjection(projection, false);
		if (projection.pendingJobs > 0 && !this.#summaryRetryDeferred && !this.#summaryTask && !this.#summaryWakeTask) {
			this.#startSummaryJobs();
		}
		if (projection.pendingJobs > 0) return { result: native, projection };
		if (!projection.ready || projection.uncoveredSourceIds.length > 0) {
			return {
				result: native,
				projection,
				...(projection.pendingJobs === 0 ? { terminal: "unfit" as const } : {}),
			};
		}

		if (projection.historical.length === 0) {
			if (!this.#host.projectionFits(messages)) return { result: native, projection, terminal: "unfit" };
			this.#noteProjection(projection, true);
			return { result: { messages: messages as AgentMessage[], owned: true, projection }, projection };
		}

		const firstUserSourceId = branch.firstUserSourceId;
		if (!firstUserSourceId) return { result: native, projection, terminal: "unfit" };
		const wanted = new Set(projection.freshTailSourceIds);
		wanted.add(firstUserSourceId);
		const projected: AgentMessage[] = [];
		let firstUserIndex = -1;
		let foundFresh = 0;
		for (const item of branch.ordered) {
			if (!wanted.has(item.source.entryId)) continue;
			if (projection.freshTailSourceIds.includes(item.source.entryId)) foundFresh++;
			if (item.source.entryId === firstUserSourceId) firstUserIndex = projected.length;
			projected.push(item.message);
		}
		if (foundFresh !== projection.freshTailSourceIds.length || firstUserIndex < 0) {
			return { result: native, projection, terminal: "unfit" };
		}

		projected.push(...liveSuffix(messages, this.#host.sessionManager.buildSessionContext()));
		const activeFinalUserIndex = projected.findLastIndex(message => message.role === "user");
		if (activeFinalUserIndex <= firstUserIndex || projected[firstUserIndex + 1]?.role === "toolResult") {
			return { result: native, projection, terminal: "unfit" };
		}

		const citedContent = historicalText(projection);
		if (!citedContent) return { result: native, projection, terminal: "unfit" };
		const firstUser = projected[firstUserIndex]!;
		projected.splice(
			firstUserIndex + 1,
			0,
			createHistoricalContextMessage({ redactedCitedContent: citedContent, timestamp: firstUser.timestamp }),
		);
		if (!this.#host.projectionFits(projected)) return { result: native, projection, terminal: "unfit" };
		this.#noteProjection(projection, true);
		return { result: { messages: projected, owned: true, projection }, projection };
	}

	async #attemptProjection(messages: readonly AgentMessage[], signal?: AbortSignal): Promise<ProjectAttempt> {
		const native: ProjectAttempt = { messages: messages as AgentMessage[], owned: false };
		if (this.#disposed || signal?.aborted) return native;
		const limits = this.#host.projectionLimits(messages);
		if (!limits) return native;
		if (limits.sourceTokens < limits.softThresholdTokens) return native;
		const atHard = limits.sourceTokens >= limits.hardThresholdTokens;
		if (limits.tokenBudget < 1 || limits.freshTail.maxSources < 1 || limits.freshTail.maxTokens < 1) {
			if (atHard) this.#failOpen("unfit");
			return native;
		}

		this.#summaryRetryDeferred = false;
		this.#lastProjectionRequest = { limits };
		if (this.#runtimePhase !== "active") {
			this.#runtimePhase = this.#hasPreferredHealthFailure() ? "degraded" : "warming";
		}
		const reconcile = this.#requestReconcile(true, {
			tokenBudget: limits.tokenBudget,
			freshTail: limits.freshTail,
		});
		if (!atHard) {
			void reconcile.catch(error => {
				logger.debug("LCM background reconcile failed", { error: errorLabel(error) });
			});
			return native;
		}

		const deadline = this.#now() + this.#hardWaitMs;
		try {
			if (!(await this.#waitWithinDeadline(reconcile, signal, deadline))) {
				if (!signal?.aborted) this.#failOpen("deadline");
				return native;
			}
			if (!(await reconcile) || signal?.aborted) {
				if (!signal?.aborted) this.#failOpen(this.#lastFailureCategory ?? "store");
				return native;
			}

			while (!signal?.aborted) {
				const progressVersion = this.#summaryProgressVersion;
				const settlement = this.#summaryProgressSignal.promise;
				const check = await this.#enqueue(() => this.#projectCurrent(messages, limits));
				if (check.result.owned) return check.result;
				if (check.terminal) {
					this.#failOpen(check.terminal);
					return native;
				}
				if (progressVersion !== this.#summaryProgressVersion) continue;
				const background = this.#summaryTask ?? this.#summaryWakeTask;
				if (!background) {
					this.#failOpen(this.#lastFailureCategory ?? "unfit");
					return native;
				}
				const progress = Promise.race([background, settlement]);
				if (!(await this.#waitWithinDeadline(progress, signal, deadline))) {
					if (!signal?.aborted) this.#failOpen(this.#lastFailureCategory === "provider" ? "provider" : "deadline");
					return native;
				}
			}
			return native;
		} catch (error) {
			logger.warn("LCM projection failed; using native context", { error: errorLabel(error) });
			if (!signal?.aborted) this.#failOpen("store");
			return native;
		}
	}

	/** Project the primary provider request, or return the input unchanged on any unsafe state. */
	async project(messages: AgentMessage[], signal?: AbortSignal): Promise<SessionLcmProjectResult> {
		return this.#attemptProjection(messages, signal);
	}

	/** Whether a ready, locally fitting Lossless projection owns this automatic maintenance request. */
	async ownsRequest(messages: readonly AgentMessage[], signal?: AbortSignal): Promise<boolean> {
		return (await this.#attemptProjection(messages, signal)).owned;
	}

	/** Mark branch/cwd transitions dirty without opening or reconciling below the automatic threshold. */
	async rebind(): Promise<void> {
		if (this.#disposed) return;
		this.#generation++;
		this.#dirty = true;
		this.#summaryRestartRequested = false;
		this.#deferReconcileUntilSummarySettles = false;
		this.#summaryRetryDeferred = false;
		this.#pendingFallbackCategory = undefined;
		this.#preferredUnfit = false;
		this.#lastProjectionRequest = undefined;
		this.#lastProjection = undefined;
		this.#runtimePhase = "idle";
		this.#clearActiveBranch();
		const drain = this.#summaryTask;
		this.#summaryAbortController?.abort("LCM session binding changed");
		this.#signalSummaryCapacity();
		this.#clearSummaryWake();
		try {
			await drain;
		} catch (error) {
			this.#noteFailure("store");
			throw error;
		}
	}

	async status(): Promise<LcmPublicStatus> {
		await this.#requestReconcile(true);
		return this.#enqueue(() => {
			const context = this.#context;
			if (!context || this.#boundCwd !== this.#host.sessionManager.getCwd()) {
				return { runtime: this.#runtimeStatus() };
			}
			this.#syncSummaryFailures(context);
			const { dbPath: _dbPath, ...store } = context.status();
			if (store.quarantined) this.#runtimePhase = "quarantined";
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
				this.#activateBranch(normalized);
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
				if (activeRebuilt) this.#activateBranch(active);
				else this.#clearActiveBranch();
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
			});
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
		this.#summaryRestartRequested = false;
		this.#deferReconcileUntilSummarySettles = false;
		this.#summaryRetryDeferred = false;
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
			if (summaryRejected) throw summaryError;
		})();
		return this.#closeTask;
	}
}
