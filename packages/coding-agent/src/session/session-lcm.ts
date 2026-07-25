import type {
	Citation,
	ContextProjection,
	DoctorReport,
	LcmContext,
	LcmStatus,
	PurgeResult,
	RebuildResult,
	SearchHit,
	SourceDescription,
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
import { type LcmProject, resolveLcmProject } from "../lcm/project-identity";
import lcmSummarySystemPrompt from "../prompts/lcm/summary-system.md" with { type: "text" };
import lcmSummaryUserPrompt from "../prompts/lcm/summary-user.md" with { type: "text" };
import type { SecretObfuscator } from "../secrets/obfuscator";
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

const ARTIFACT_REF_PATTERN = /(?:artifact:\/\/\d+|blob:sha256:[a-f0-9]{64})/g;

export type LcmPublicStatus = Omit<LcmStatus, "dbPath">;

export interface LcmCompletionRequest {
	systemPrompt: string;
	prompt: string;
	maxOutputTokens: number;
	oneshotKind: "lcm_summary" | "lcm_recall";
	modelSelector?: string;
	signal?: AbortSignal;
}

export interface LcmProjectionLimits {
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
	| "getSessionId"
	| "subscribeToDurableEntries"
>;

/** Narrow coding-agent capabilities consumed by the derived LCM lifecycle. */
export interface SessionLcmHost {
	sessionManager: SessionLcmJournal;
	obfuscator?: Pick<SecretObfuscator, "hasSecrets" | "obfuscate">;
	projectionLimits(): LcmProjectionLimits | undefined;
	projectionFits(messages: readonly AgentMessage[]): boolean;
	complete(request: LcmCompletionRequest): Promise<string>;
}

export interface SessionLcmDependencies {
	openContext?: typeof openLcmContext;
	resolveProject?: typeof resolveLcmProject;
}

export interface SessionLcmOptions {
	agentDir?: string;
	summaryModel?: string;
	registerProject?: (project: LcmProject) => Promise<void>;
	dependencies?: SessionLcmDependencies;
}

interface LcmProjectBinding {
	projectId: string;
	rootPath: string;
	storePath: string;
}

interface NormalizedBranch {
	snapshot: SourceSnapshot;
	ordered: Array<{ source: SourceEntry; message: AgentMessage }>;
	firstUserSourceId: string | undefined;
	anchor: string;
}

interface ProjectAttempt {
	messages: AgentMessage[];
	owned: boolean;
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
					content: file.content,
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

/**
 * Owns the coding-agent side of Lossless Context Management. The session journal
 * remains authoritative; this class only maintains and queries a rebuildable,
 * redacted per-project projection.
 */
export class SessionLcm {
	readonly #host: SessionLcmHost;
	readonly #agentDir: string | undefined;
	readonly #summaryModel: string;
	readonly #openContext: typeof openLcmContext;
	readonly #resolveProject: typeof resolveLcmProject;
	readonly #registerProject: ((project: LcmProject) => Promise<void>) | undefined;
	readonly #workerId = `omp-lcm:${Bun.randomUUIDv7()}`;

	#context: LcmContext | undefined;
	#project: LcmProjectBinding | undefined;
	#boundCwd: string | undefined;
	#activeBranch: NormalizedBranch | undefined;
	#dirty = true;
	#generation = 0;
	#disposed = false;
	#operationTail: Promise<void> = Promise.resolve();
	#reconcileTask: Promise<boolean> | undefined;
	#summaryTask: Promise<void> | undefined;
	#summaryAbortController: AbortController | undefined;
	#closeTask: Promise<void> | undefined;
	#unsubscribeDurableEntries: (() => void) | undefined;

	constructor(host: SessionLcmHost, options: SessionLcmOptions) {
		this.#host = host;
		this.#agentDir = options.agentDir;
		this.#summaryModel = options.summaryModel || "@smol";
		this.#openContext = options.dependencies?.openContext ?? openLcmContext;
		this.#resolveProject = options.dependencies?.resolveProject ?? resolveLcmProject;
		this.#registerProject = options.registerProject;
		this.#unsubscribeDurableEntries = host.sessionManager.subscribeToDurableEntries(() => {
			this.#dirty = true;
			if (this.#context && !this.#disposed) void this.#requestReconcile(false);
		});
	}

	get enabled(): boolean {
		return !this.#disposed;
	}

	#redact(text: string): string {
		return this.#host.obfuscator?.hasSecrets() ? this.#host.obfuscator.obfuscate(text) : text;
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
		if (this.#context && this.#boundCwd === cwd) return this.#context;

		const generation = this.#generation;
		let project: LcmProjectBinding;
		try {
			project = await this.#resolveProject(cwd, this.#agentDir);
		} catch (error) {
			logger.warn("LCM project resolution failed; using native context", { error: errorLabel(error) });
			return undefined;
		}
		if (this.#disposed || generation !== this.#generation || cwd !== this.#host.sessionManager.getCwd())
			return undefined;
		if (this.#registerProject) {
			try {
				await this.#registerProject(project);
			} catch (error) {
				logger.warn("LCM project catalog registration failed; continuing without catalog update", {
					projectId: project.projectId,
					error: errorLabel(error),
				});
			}
			if (this.#disposed || generation !== this.#generation || cwd !== this.#host.sessionManager.getCwd()) {
				return undefined;
			}
		}

		if (this.#context) {
			this.#context.close();
			this.#context = undefined;
			this.#activeBranch = undefined;
		}

		let context: LcmContext;
		try {
			context = await this.#openContext({ dbPath: project.storePath, recoverCorrupt: true });
		} catch (error) {
			logger.warn("LCM store open failed; using native context", { error: errorLabel(error) });
			return undefined;
		}
		if (this.#disposed || generation !== this.#generation || cwd !== this.#host.sessionManager.getCwd()) {
			context.close();
			return undefined;
		}

		this.#context = context;
		this.#project = project;
		this.#boundCwd = cwd;
		return context;
	}

	#normalizeActiveBranch(project: LcmProjectBinding): NormalizedBranch {
		const manager = this.#host.sessionManager;
		const entries = manager.getBranch();
		const scope = {
			projectId: project.projectId,
			sessionId: manager.getSessionId(),
			branchId: branchId(manager, entries),
		};
		const ordered: NormalizedBranch["ordered"] = [];
		const sources: SourceEntry[] = [];
		const pendingToolGroups = new Map<string, string>();
		let firstUserSourceId: string | undefined;

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
			// Opaque payload/details never enter redactedText. Retain only explicit,
			// content-addressed references found anywhere on the committed entry.
			collectArtifactRefs(entry, artifactRefs);
			const serialized = serializeMessage(message, artifactRefs);
			if (serialized === undefined) continue;
			const redactedText = this.#redact(serialized);
			const contentHash = new Bun.CryptoHasher("sha256").update(redactedText).digest("hex");
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
		};
	}

	async #drainReconcile(open: boolean): Promise<boolean> {
		let reconciled = false;
		while (this.#dirty && !this.#disposed) {
			if (!open && !this.#context) return reconciled;
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
				const result = context.reconcile(normalized.snapshot);
				this.#activeBranch = normalized;
				reconciled = true;
				if (result.queuedJobs > 0) this.#startSummaryJobs();
			} catch (error) {
				logger.warn("LCM reconcile failed; using native context", { error: errorLabel(error) });
				this.#activeBranch = undefined;
				return false;
			}
		}
		return reconciled;
	}

	#requestReconcile(open: boolean): Promise<boolean> {
		this.#dirty = true;
		if (!open && !this.#context) return Promise.resolve(false);
		if (this.#reconcileTask) return this.#reconcileTask;

		const task = this.#enqueue(() => this.#drainReconcile(open));
		this.#reconcileTask = task;
		void task.then(
			() => {
				if (this.#reconcileTask === task) this.#reconcileTask = undefined;
				if (this.#dirty && this.#context && !this.#disposed) void this.#requestReconcile(false);
			},
			() => {
				if (this.#reconcileTask === task) this.#reconcileTask = undefined;
				if (this.#dirty && this.#context && !this.#disposed) void this.#requestReconcile(false);
			},
		);
		return task;
	}

	#startSummaryJobs(): void {
		if (this.#disposed || this.#summaryTask || !this.#context) return;
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
			() => {
				if (this.#summaryTask === task) this.#summaryTask = undefined;
				if (this.#summaryAbortController === controller) this.#summaryAbortController = undefined;
			},
		);
	}

	async #runSummaryJobs(context: LcmContext, generation: number, signal: AbortSignal): Promise<void> {
		while (!signal.aborted && !this.#disposed && generation === this.#generation && context === this.#context) {
			const jobs = await this.#enqueue(() =>
				context.claimSummaryJobs({
					workerId: this.#workerId,
					leaseMs: SUMMARY_LEASE_MS,
					limit: SUMMARY_CLAIM_LIMIT,
					maxOutputTokens: SUMMARY_MAX_OUTPUT_TOKENS,
				}),
			);
			const job = jobs[0];
			if (!job) return;
			await this.#runSummaryJob(context, job, generation, signal);
		}
	}

	async #runSummaryJob(context: LcmContext, job: SummaryJob, generation: number, signal: AbortSignal): Promise<void> {
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
			const userPrompt = prompt.render(lcmSummaryUserPrompt, {
				maxOutputTokens: job.outputTokenBudget,
				inputs: job.inputs.map(input => ({ kind: input.kind, id: input.id, text: input.redactedText })),
			});
			const output = await this.#host.complete({
				systemPrompt: prompt.render(lcmSummarySystemPrompt),
				prompt: userPrompt,
				maxOutputTokens: job.outputTokenBudget,
				oneshotKind: "lcm_summary",
				modelSelector: this.#summaryModel,
				signal: jobSignal,
			});
			if (jobSignal.aborted || this.#disposed || generation !== this.#generation || context !== this.#context)
				return;
			const redactedText = this.#redact(output).trim();
			if (!redactedText) throw new Error("Summary completion returned no text");
			await this.#enqueue(() => context.completeSummaryJob(job.jobId, job.leaseToken, { redactedText }));
		} catch (error) {
			if (this.#disposed || jobSignal.aborted || generation !== this.#generation || context !== this.#context)
				return;
			const redactedError = this.#redact(errorLabel(error));
			await this.#enqueue(() =>
				context.failSummaryJob(job.jobId, job.leaseToken, redactedError, SUMMARY_RETRY_DELAY_MS),
			);
		} finally {
			clearInterval(renewLease);
			jobController.abort("LCM summary completion settled");
		}
	}

	async #attemptProjection(messages: readonly AgentMessage[], signal?: AbortSignal): Promise<ProjectAttempt> {
		const native = { messages: messages as AgentMessage[], owned: false };
		if (this.#disposed || signal?.aborted) return native;
		const limits = this.#host.projectionLimits();
		if (!limits || limits.tokenBudget < 1 || limits.freshTail.maxSources < 1 || limits.freshTail.maxTokens < 1) {
			return native;
		}
		if (!(await this.#requestReconcile(true)) || signal?.aborted) return native;

		try {
			return await this.#enqueue(() => {
				const context = this.#context;
				const branch = this.#activeBranch;
				if (!context || !branch || this.#dirty) return native;
				const projection = context.project({ ...branch.snapshot.scope, ...limits });
				if (projection.pendingJobs > 0) this.#startSummaryJobs();
				if (!projection.ready || projection.uncoveredSourceIds.length > 0) return native;

				if (projection.historical.length === 0) {
					return this.#host.projectionFits(messages)
						? { messages: messages as AgentMessage[], owned: true }
						: native;
				}

				const firstUserSourceId = branch.firstUserSourceId;
				if (!firstUserSourceId) return native;
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
				if (foundFresh !== projection.freshTailSourceIds.length || firstUserIndex < 0) return native;

				const suffix = liveSuffix(messages, this.#host.sessionManager.buildSessionContext());
				projected.push(...suffix);
				const activeFinalUserIndex = projected.findLastIndex(message => message.role === "user");
				if (activeFinalUserIndex <= firstUserIndex || projected[firstUserIndex + 1]?.role === "toolResult")
					return native;

				const citedContent = historicalText(projection);
				if (!citedContent) return native;
				const firstUser = projected[firstUserIndex]!;
				projected.splice(
					firstUserIndex + 1,
					0,
					createHistoricalContextMessage({ redactedCitedContent: citedContent, timestamp: firstUser.timestamp }),
				);
				if (!this.#host.projectionFits(projected)) return native;
				return { messages: projected, owned: true };
			});
		} catch (error) {
			logger.warn("LCM projection failed; using native context", { error: errorLabel(error) });
			return native;
		}
	}

	/** Project the primary provider request, or return the input unchanged on any unsafe state. */
	async project(messages: AgentMessage[], signal?: AbortSignal): Promise<AgentMessage[]> {
		return (await this.#attemptProjection(messages, signal)).messages;
	}

	/** Whether a ready, locally fitting Lossless projection owns this automatic maintenance request. */
	async ownsRequest(messages: readonly AgentMessage[], signal?: AbortSignal): Promise<boolean> {
		return (await this.#attemptProjection(messages, signal)).owned;
	}

	/** Rebind after a session, branch, or cwd transition; already-open stores reconcile immediately. */
	async rebind(): Promise<void> {
		if (this.#disposed) return;
		this.#generation++;
		this.#dirty = true;
		this.#activeBranch = undefined;
		this.#summaryAbortController?.abort();
		if (this.#context) await this.#requestReconcile(false);
	}

	async status(): Promise<LcmPublicStatus | null> {
		if (!(await this.#requestReconcile(true))) return null;
		return this.#enqueue(() => {
			if (!this.#context) return null;
			const { dbPath: _dbPath, ...status } = this.#context.status();
			return status;
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
				this.#activeBranch = normalized;
				this.#dirty = false;
				if (result.queuedJobs > 0) this.#startSummaryJobs();
				return result;
			} catch (error) {
				logger.warn("LCM rebuild failed", { error: errorLabel(error) });
				return null;
			}
		});
	}

	async purge(): Promise<PurgeResult | null> {
		if (!(await this.#requestReconcile(true))) return null;
		return this.#enqueue(() => this.#context?.purge() ?? null);
	}

	async search(query: string, limit?: number): Promise<SearchHit[]> {
		if (!(await this.#requestReconcile(true))) return [];
		return this.#enqueue(() => {
			const context = this.#context;
			const scope = this.#activeBranch?.snapshot.scope;
			if (!context || !scope) return [];
			return context.search({ ...scope, query, ...(limit === undefined ? {} : { limit }) });
		});
	}

	async describe(citation: Citation): Promise<SourceDescription | null> {
		if (!(await this.#requestReconcile(true))) return null;
		return this.#enqueue(() => {
			const context = this.#context;
			const scope = this.#activeBranch?.snapshot.scope;
			if (
				!context ||
				!scope ||
				citation.projectId !== scope.projectId ||
				citation.sessionId !== scope.sessionId ||
				citation.branchId !== scope.branchId
			) {
				return null;
			}
			return context.describe(citation);
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
			this.#activeBranch = undefined;
		})();
		return this.#closeTask;
	}
}
