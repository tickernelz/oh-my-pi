import { describe, expect, it, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type {
	Citation,
	ContextProjection,
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
	SummaryCompletion,
	SummaryDescription,
	SummaryExpansion,
	SummaryExpansionRequest,
	SummaryJob,
	SummaryReference,
} from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import {
	type LcmCompletionRequest,
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
	reconcileOptions: Array<ReconcileOptions | undefined> = [];
	nextDelayMs: number | null = null;
	deferredClaims = 0;
	jobs: SummaryJob[] = [];
	readonly summaryCompleted = Promise.withResolvers<void>();
	readonly summaryFailed = Promise.withResolvers<void>();
	readonly delayRequested = Promise.withResolvers<void>();
	lastCompletion: SummaryCompletion | undefined;
	job: SummaryJob | undefined;
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
		pendingJobs: this.queuedJobs,
	});

	[Symbol.dispose](): void {
		this.close();
	}

	reconcile(snapshot: SourceSnapshot, options?: ReconcileOptions): ReconcileResult {
		this.reconcileOptions.push(options);
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

	claimSummaryJobs(): SummaryJob[] {
		if (this.deferredClaims > 0) {
			this.deferredClaims--;
			return [];
		}
		const job = this.job ?? this.jobs.shift();
		this.job = undefined;
		return job ? [job] : [];
	}

	nextSummaryJobDelayMs(): number | null {
		this.delayRequested.resolve();
		return this.nextDelayMs;
	}

	extendSummaryJob(): boolean {
		return true;
	}

	completeSummaryJob(_jobId: string, _leaseToken: string, completion: SummaryCompletion) {
		this.lastCompletion = completion;
		this.queuedJobs = Math.max(0, this.queuedJobs - 1);
		this.nextDelayMs = null;
		this.summaryCompleted.resolve();
		return { accepted: true as const, summaryId: "summary-1" };
	}

	failSummaryJob(_jobId: string, _leaseToken: string, redactedError: string): boolean {
		this.failedError = redactedError;
		this.summaryFailed.resolve();
		this.nextDelayMs = 30_000;
		return true;
	}

	async runSummaryJobs() {
		return { claimed: 0, completed: 0, failed: 0, stale: 0, escalated: 0 };
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
		return {
			dbPath: "/secret/context.sqlite",
			schemaVersion: 5,
			journalMode: "wal",
			quarantined: false,
			quarantineReason: null,
			recoveredFrom: null,
			branches: 1,
			activeSources: this.snapshots.at(-1)?.entries.length ?? 0,
			tombstones: 0,
			leafSummaries: 0,
			condensedSummaries: 0,
			jobs: { pending: 0, leased: 0, failed: 0, completed: 0, obsolete: 0 },
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
			queuedJobs: 0,
		};
	}

	purge() {
		return { tombstones: 0, jobs: 0, summaries: 0, sourceContents: 0, files: 0 };
	}

	close(): void {
		this.closed = true;
	}
}

function createHarness(
	manager: SessionManager,
	context = new FakeLcmContext(),
	projectId?: string,
	registerProject?: SessionLcmOptions["registerProject"],
	projectionLimits = () => ({
		sourceTokens: 100,
		softThresholdTokens: 80,
		hardThresholdTokens: 100,
		tokenBudget: 100_000,
		freshTail: { maxSources: 32, maxTokens: 20_000 },
	}),
	hardWaitMs = 20,
	projectRoot?: string,
) {
	const complete = vi.fn(async (_request: LcmCompletionRequest) => "redacted summary");
	const openContext = vi.fn(async () => context as LcmContext);
	const lcm = new SessionLcm(
		{
			sessionManager: manager,
			obfuscator: {
				hasSecrets: () => true,
				obfuscate: text => text.replaceAll("raw-secret", "#SECRET"),
			},
			projectionLimits,
			projectionFits: () => true,
			complete,
		},
		{
			summaryModel: "@smol",
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

function summaryJob(jobId: string): SummaryJob {
	return {
		jobId,
		leaseToken: `lease-${jobId}`,
		leaseExpiresAt: Date.now() + 60_000,
		kind: "leaf",
		level: 0,
		inputs: [{ kind: "source", id: `source-${jobId}`, redactedText: "safe historical facts", tokenCount: 8 }],
		sourceCount: 1,
		inputTokenCount: 8,
		outputTokenBudget: 4,
		stage: "normal",
		strategy: "preserve_details",
		transportRetryCount: 0,
	};
}

describe("SessionLcm", () => {
	it("returns the exact native input below soft without opening the derived store", async () => {
		const manager = SessionManager.inMemory("/below-soft");
		appendUser(manager, "small", 1);
		const { lcm, context, complete, openContext } = createHarness(manager, undefined, undefined, undefined, () => ({
			sourceTokens: 79,
			softThresholdTokens: 80,
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

	it("schedules only above soft and returns native without waiting", async () => {
		const manager = SessionManager.inMemory("/soft-background");
		appendUser(manager, "growing history", 1);
		const context = new FakeLcmContext();
		context.queuedJobs = 1;
		context.job = summaryJob("soft");
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 90,
			softThresholdTokens: 80,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		const completed = context.summaryCompleted.promise;
		const input = manager.buildSessionContext().messages;
		const result = await lcm.project(input);
		expect(result.messages).toBe(input);
		await completed;
		expect(context.reconcileOptions[0]?.summarize).toEqual({
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		});
		expect(complete).toHaveBeenCalledTimes(1);
		await lcm.close();
	});

	it("wakes a delayed retry once without polling", async () => {
		const manager = SessionManager.inMemory("/delayed-retry");
		appendUser(manager, "retryable history", 1);
		const context = new FakeLcmContext();
		context.queuedJobs = 1;
		context.job = summaryJob("delayed");
		context.deferredClaims = 1;
		context.nextDelayMs = 1;
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 90,
			softThresholdTokens: 80,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		await lcm.project(manager.buildSessionContext().messages);
		await context.delayRequested.promise;
		await context.summaryCompleted.promise;
		expect(complete).toHaveBeenCalledTimes(1);
		await lcm.close();
	});

	it("captures the summary selector per claimed job and defaults an absent selector to @smol", async () => {
		const manager = SessionManager.inMemory("/selector-capture");
		appendUser(manager, "two jobs", 1);
		const context = new FakeLcmContext();
		context.queuedJobs = 2;
		context.jobs.push(summaryJob("first"), summaryJob("second"));
		const { lcm, complete } = createHarness(manager, context, undefined, undefined, () => ({
			sourceTokens: 90,
			softThresholdTokens: 80,
			hardThresholdTokens: 100,
			tokenBudget: 80,
			freshTail: { maxSources: 8, maxTokens: 40 },
		}));
		lcm.setSummaryModel(undefined);
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
		await lcm.project(manager.buildSessionContext().messages);
		await firstCalled.promise;
		lcm.setSummaryModel("@next");
		first.resolve();
		await secondCalled.promise;
		expect(selectors).toEqual(["@smol", "@next"]);
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
			softThresholdTokens: 80,
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
		expect(await lcm.status()).not.toHaveProperty("dbPath");

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

	it("inserts one stable transient history after the first user, before the active user, and preserves live tail", async () => {
		const manager = SessionManager.inMemory("/worktree-a");
		const first = appendUser(manager, "first", 1);
		manager.appendMessage({ ...createAssistantMessage("settled"), timestamp: 2 });
		const active = appendUser(manager, "active", 3);
		const live: AgentMessage = { role: "user", content: [{ type: "text", text: "live" }], timestamp: 4 };
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
		context.queuedJobs = 1;
		context.job = {
			...summaryJob("failure"),
			inputs: [{ kind: "source", id: "source-1", redactedText: "safe", tokenCount: 8 }],
		};
		complete.mockRejectedValueOnce(new Error("raw-secret summary failed"));
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
