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
import { openLcmContext } from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";

export type {
	Citation,
	DoctorReport,
	PurgeResult,
	RebuildResult,
	SearchHit,
	SourceDescription,
} from "@oh-my-pi/lcm-context";

import { logger, prompt } from "@oh-my-pi/pi-utils";
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
import type { LcmFallbackCategory } from "./messages";
import {
	bashExecutionToText,
	createBranchSummaryMessage,
	createCustomMessage,
	createHistoricalContextMessage,
	pythonExecutionToText,
} from "./messages";
import type { SessionContext } from "./session-context";
import type { SessionEntry } from "./session-entries";
import type { SessionManager } from "./session-manager";
import { sessionMessagePersistenceKey } from "./turn-persistence";

const SUMMARY_LEASE_MS = 10 * 60_000;
const SUMMARY_RETRY_DELAY_MS = 30_000;
const SUMMARY_MAX_OUTPUT_TOKENS = 2_048;
const SUMMARY_CLAIM_LIMIT = 1;
const HARD_PROJECTION_WAIT_MS = 30_000;

const ARTIFACT_REF_PATTERN = /(?:artifact:\/\/\d+|blob:sha256:[a-f0-9]{64})/g;

export type LcmRuntimePhase = "disabled" | "uninitialized" | "idle" | "warming" | "active" | "degraded" | "quarantined";

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

export type { LcmFallbackCategory } from "./messages";

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

async function fileContentHash(filePath: string): Promise<string> {
	const hasher = new Bun.CryptoHasher("sha256");
	for await (const chunk of Bun.file(filePath).stream()) hasher.update(chunk);
	return hasher.digest("hex");
}

function isPathWithinRoot(rootPath: string, candidatePath: string): boolean {
	const relative = path.relative(rootPath, candidatePath);
	return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
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
			if (isPathWithinRoot(activeFiles.projectRoot, originalPath)) {
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

function liveSuffix(messages: readonly AgentMessage[], persisted: SessionContext): AgentMessage[] {
	if (messages.length === 0) return [];
	let persistedIndex = persisted.messages.length - 1;
	for (let inputIndex = messages.length - 1; inputIndex >= 0; inputIndex--) {
		const key = messageIdentity(messages[inputIndex]!);
		while (persistedIndex >= 0 && messageIdentity(persisted.messages[persistedIndex]!) !== key) persistedIndex--;
		if (persistedIndex >= 0) return messages.slice(inputIndex + 1);
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

	constructor(host: SessionLcmHost, options: SessionLcmOptions) {
		this.#host = host;
		this.#agentDir = options.agentDir;
		this.#summaryModel = options.summaryModel?.trim() || undefined;
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

	setSummaryModel(selector: string | undefined): void {
		const next = selector?.trim() || undefined;
		if (next !== this.#summaryModel) this.#resolvedSummaryModel = undefined;
		this.#summaryModel = next;
	}

	takePendingFallbackCategory(): LcmFallbackCategory | undefined {
		const category = this.#pendingFallbackCategory;
		this.#pendingFallbackCategory = undefined;
		return category;
	}

	#runtimeStatus(): LcmRuntimeStatus {
		return {
			phase: this.#runtimePhase,
			summaryModelSelector: this.#summaryModel?.trim() || "@smol",
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

	#failOpen(category: LcmFallbackCategory): void {
		this.#noteFailure(category, this.#retryAt);
		this.#pendingFallbackCategory = category;
	}

	#noteProjection(projection: ContextProjection, active: boolean): void {
		this.#lastProjection = projectionAggregate(projection);
		this.#runtimePhase = active ? "active" : "warming";
		if (active) {
			this.#lastFailureCategory = undefined;
			this.#pendingFallbackCategory = undefined;
			this.#retryAt = undefined;
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
		const delay = Math.max(1, delayMs);
		const wakeAt = this.#now() + delay;
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
		const result = this.#operationTail.then(operation, operation);
		this.#operationTail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	async #ensureOpen(): Promise<LcmContext | undefined> {
		if (this.#disposed) return undefined;
		const cwd = this.#host.sessionManager.getCwd();
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

		if (this.#context) {
			this.#summaryAbortController?.abort("LCM store binding changed");
			this.#clearSummaryWake();
			this.#context.close();
			this.#context = undefined;
			this.#clearActiveBranch();
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

	async #drainReconcile(open: boolean): Promise<boolean> {
		let reconciled = false;
		while (this.#dirty && !this.#disposed) {
			if (!open && !this.#context) return reconciled;
			const summarize = this.#pendingReconcileSummarize ?? false;
			this.#pendingReconcileSummarize = undefined;
			this.#dirty = false;
			const context = await this.#ensureOpen();
			const project = this.#project;
			if (!context || !project) return false;
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
				const result = context.reconcile(normalized.snapshot, { summarize });
				this.#activateBranch(normalized);
				reconciled = true;
				if (summarize !== false && result.queuedJobs > 0) {
					this.#runtimePhase = "warming";
					this.#startSummaryJobs();
				}
			} catch (error) {
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
		const settled = (): void => {
			if (this.#reconcileTask !== task) return;
			this.#reconcileTask = undefined;
			const next = this.#pendingReconcileSummarize ?? summarize;
			if (this.#dirty && this.#context && !this.#disposed) {
				void this.#requestReconcile(false, next);
			}
		};
		void task.then(settled, settled);
		return task;
	}

	#startSummaryJobs(): void {
		if (this.#disposed || this.#summaryTask || !this.#context) return;
		this.#clearSummaryWake();
		const context = this.#context;
		const generation = this.#generation;
		const controller = new AbortController();
		this.#summaryAbortController = controller;
		const task = this.#runSummaryJobs(context, generation, controller.signal);
		this.#summaryTask = task;
		void task.then(
			() => {
				if (this.#summaryTask === task) this.#summaryTask = undefined;
				if (this.#summaryAbortController === controller) this.#summaryAbortController = undefined;
			},
			error => {
				if (this.#summaryTask === task) this.#summaryTask = undefined;
				if (this.#summaryAbortController === controller) this.#summaryAbortController = undefined;
				if (!controller.signal.aborted && !this.#disposed) {
					this.#noteFailure("store");
					logger.warn("LCM summary worker failed", { error: errorLabel(error) });
				}
			},
		);
	}

	async #runSummaryJobs(context: LcmContext, generation: number, signal: AbortSignal): Promise<void> {
		while (!signal.aborted && !this.#disposed && generation === this.#generation && context === this.#context) {
			const claimed = await this.#enqueue(() => {
				const job = context.claimSummaryJobs({
					workerId: this.#workerId,
					leaseMs: SUMMARY_LEASE_MS,
					limit: SUMMARY_CLAIM_LIMIT,
					maxOutputTokens: SUMMARY_MAX_OUTPUT_TOKENS,
				})[0];
				return { job, delayMs: job ? null : context.nextSummaryJobDelayMs() };
			});
			if (!claimed.job) {
				if (claimed.delayMs !== null) this.#scheduleSummaryWake(context, generation, claimed.delayMs);
				return;
			}
			const summaryModel = this.#summaryModel?.trim() || "@smol";
			await this.#runSummaryJob(context, claimed.job, summaryModel, generation, signal);
		}
	}

	async #runSummaryJob(
		context: LcmContext,
		job: SummaryJob,
		summaryModel: string,
		generation: number,
		signal: AbortSignal,
	): Promise<void> {
		const jobController = new AbortController();
		const jobSignal = AbortSignal.any([signal, jobController.signal]);
		const renewLease = setInterval(
			() => {
				if (jobSignal.aborted || this.#disposed || generation !== this.#generation || context !== this.#context) {
					jobController.abort("LCM summary lease owner changed");
					return;
				}
				void this.#enqueue(() => {
					if (
						jobSignal.aborted ||
						this.#disposed ||
						generation !== this.#generation ||
						context !== this.#context
					) {
						return false;
					}
					const extended = context.extendSummaryJob(job.jobId, job.leaseToken, SUMMARY_LEASE_MS);
					if (!extended) jobController.abort("LCM summary lease lost");
					return extended;
				}).catch(error => {
					logger.debug("LCM summary lease renewal failed", { error: errorLabel(error) });
					jobController.abort("LCM summary lease renewal failed");
				});
			},
			Math.floor(SUMMARY_LEASE_MS / 2),
		);

		try {
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
				const strategyInstructions =
					job.stage === "aggressive"
						? "Use terse bullet points. Keep only essential facts, current state, constraints, errors, and next actions."
						: "Preserve concrete details and causal context while removing repetition and conversational filler.";
				const userPrompt = prompt.render(lcmSummaryUserPrompt, {
					maxOutputTokens: job.outputTokenBudget,
					strategy: job.strategy,
					strategyInstructions,
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
					if (this.#disposed || jobSignal.aborted || generation !== this.#generation || context !== this.#context)
						return;
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
							this.#redact(errorLabel(error)),
							SUMMARY_RETRY_DELAY_MS,
							provenance,
						),
					);
					if (failed) this.#noteFailure("provider", this.#now() + SUMMARY_RETRY_DELAY_MS);
					return;
				}
				redactedText = this.#redact(output).trim();
				if (!redactedText) {
					const failed = await this.#enqueue(() =>
						context.failSummaryJob(
							job.jobId,
							job.leaseToken,
							"Summary completion returned no text",
							SUMMARY_RETRY_DELAY_MS,
							{
								promptHash,
								modelSelector: summaryModel,
								...(resolvedModel ? { resolvedModel } : {}),
								strategy: job.strategy,
							},
						),
					);
					if (failed) this.#noteFailure("provider", this.#now() + SUMMARY_RETRY_DELAY_MS);
					return;
				}
			}

			if (jobSignal.aborted || this.#disposed || generation !== this.#generation || context !== this.#context)
				return;
			const provenance = {
				promptHash,
				...(job.stage === "deterministic" ? {} : { modelSelector: summaryModel }),
				...(resolvedModel ? { resolvedModel } : {}),
				strategy: job.strategy,
			};
			const result = await this.#enqueue(() =>
				context.completeSummaryJob(job.jobId, job.leaseToken, { redactedText, provenance }),
			);
			if (!result.accepted && result.reason === "deterministic_failed") {
				this.#noteFailure("unfit");
			} else if (result.accepted || result.reason === "escalated") {
				this.#retryAt = undefined;
				if (this.#lastFailureCategory === "provider") this.#lastFailureCategory = undefined;
				this.#runtimePhase = "warming";
			}
		} finally {
			clearInterval(renewLease);
			jobController.abort("LCM summary completion settled");
		}
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
		if (projection.pendingJobs > 0 && !this.#summaryTask && !this.#summaryWakeTask) this.#startSummaryJobs();
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

		this.#runtimePhase = "warming";
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
				const check = await this.#enqueue(() => this.#projectCurrent(messages, limits));
				if (check.result.owned) return check.result;
				if (check.terminal) {
					this.#failOpen(check.terminal);
					return native;
				}
				const progress = this.#summaryTask ?? this.#summaryWakeTask;
				if (!progress) {
					this.#failOpen(this.#lastFailureCategory ?? "unfit");
					return native;
				}
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
		this.#clearActiveBranch();
		this.#summaryAbortController?.abort("LCM session binding changed");
		this.#clearSummaryWake();
	}

	async status(): Promise<LcmPublicStatus> {
		await this.#requestReconcile(true);
		return this.#enqueue(() => {
			const context = this.#context;
			if (!context) {
				this.#noteFailure("store");
				return { runtime: this.#runtimeStatus() };
			}
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
			if (isPathWithinRoot(root, candidate)) {
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
		this.#unsubscribeDurableEntries?.();
		this.#unsubscribeDurableEntries = undefined;
		this.#summaryAbortController?.abort();
		this.#clearSummaryWake();
		this.#clearActiveBranch();
	}

	close(): Promise<void> {
		if (this.#closeTask) return this.#closeTask;
		this.beginDispose();
		this.#closeTask = (async () => {
			await this.#summaryTask?.catch(() => undefined);
			await this.#operationTail;
			this.#context?.close();
			this.#context = undefined;
			this.#project = undefined;
			this.#clearActiveBranch();
		})();
		return this.#closeTask;
	}
}
