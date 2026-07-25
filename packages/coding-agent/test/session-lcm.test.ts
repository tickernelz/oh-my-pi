import { describe, expect, it, vi } from "bun:test";
import type {
	ContextProjection,
	LcmContext,
	LcmStatus,
	ProjectionRequest,
	ReconcileResult,
	SourceSnapshot,
	SummaryJob,
} from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import { SessionLcm, type SessionLcmOptions } from "@oh-my-pi/pi-coding-agent/session/session-lcm";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { createAssistantMessage } from "./helpers/agent-session-setup";

class FakeLcmContext implements LcmContext {
	snapshots: SourceSnapshot[] = [];
	closed = false;
	failedError: string | undefined;
	queuedJobs = 0;
	job: SummaryJob | undefined;
	projectImpl: (request: ProjectionRequest, snapshot: SourceSnapshot) => ContextProjection = (_request, snapshot) => ({
		revision: 1,
		ready: false,
		historical: [],
		freshTailSourceIds: snapshot.entries.map(entry => entry.entryId),
		uncoveredSourceIds: [],
		estimatedTokens: 0,
		pendingJobs: this.queuedJobs,
	});

	[Symbol.dispose](): void {
		this.close();
	}

	reconcile(snapshot: SourceSnapshot): ReconcileResult {
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
		const job = this.job;
		this.job = undefined;
		return job ? [job] : [];
	}

	extendSummaryJob(): boolean {
		return true;
	}

	completeSummaryJob() {
		return { accepted: true as const, summaryId: "summary-1" };
	}

	failSummaryJob(_jobId: string, _leaseToken: string, redactedError: string): boolean {
		this.failedError = redactedError;
		return true;
	}

	async runSummaryJobs() {
		return { claimed: 0, completed: 0, failed: 0, stale: 0 };
	}

	search() {
		return [];
	}

	searchProject() {
		return [];
	}

	describe() {
		return null;
	}

	status(): LcmStatus {
		return {
			dbPath: "/secret/context.sqlite",
			schemaVersion: 1,
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
		return { tombstones: 0, jobs: 0, summaries: 0, sourceContents: 0 };
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
) {
	const complete = vi.fn(async () => "redacted summary");
	const openContext = vi.fn(async () => context as LcmContext);
	const lcm = new SessionLcm(
		{
			sessionManager: manager,
			obfuscator: {
				hasSecrets: () => true,
				obfuscate: text => text.replaceAll("raw-secret", "#SECRET"),
			},
			projectionLimits: () => ({ tokenBudget: 100_000, freshTail: { maxSources: 32, maxTokens: 20_000 } }),
			projectionFits: () => true,
			complete,
		},
		{
			summaryModel: "@smol",
			registerProject,
			dependencies: {
				openContext,
				resolveProject: async cwd => ({
					projectId: projectId ?? `project:${Bun.hash(cwd)}`,
					rootPath: cwd,
					storePath: `${cwd}/context.sqlite`,
				}),
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

describe("SessionLcm", () => {
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
		expect(context.snapshots.at(-1)?.scope.sessionId).toBe(manager.getSessionId());
		const reconciles = context.snapshots.length;
		await lcm.close();
		appendUser(manager, "after close", 3);
		await Bun.sleep(0);
		expect(context.snapshots).toHaveLength(reconciles);
		expect(context.closed).toBe(true);
	});

	it("registers each resolved project at lazy runtime initialization and treats catalog failure as nonfatal", async () => {
		const manager = SessionManager.inMemory("/catalog-project");
		appendUser(manager, "first", 1);
		const registerProject = vi.fn(async () => {});
		const registered = createHarness(manager, new FakeLcmContext(), "project:catalog", registerProject);
		await registered.lcm.project(manager.buildSessionContext().messages);
		expect(registerProject).toHaveBeenCalledWith({
			projectId: "project:catalog",
			rootPath: "/catalog-project",
			storePath: "/catalog-project/context.sqlite",
		});
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
				estimatedTokens: 10,
				pendingJobs: 0,
			};
		};
		const input = [...manager.buildSessionContext().messages, live];
		const projected = await lcm.project(input);
		expect(projected[0]).toBe(first);
		expect(projected[1]?.role).toBe("historicalContext");
		expect(projected.filter(message => message.role === "historicalContext")).toHaveLength(1);
		expect(projected).toContain(active);
		expect(projected.at(-1)).toBe(live);
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
		const { lcm, context, complete } = createHarness(manager);
		context.queuedJobs = 1;
		context.job = {
			jobId: "job-1",
			leaseToken: "lease-1",
			leaseExpiresAt: Date.now() + 60_000,
			kind: "leaf",
			level: 0,
			inputs: [{ kind: "source", id: "source-1", redactedText: "safe", tokenCount: 1 }],
			sourceCount: 1,
			inputTokenCount: 1,
			outputTokenBudget: 64,
		};
		complete.mockRejectedValueOnce(new Error("raw-secret summary failed"));
		const input = manager.buildSessionContext().messages;
		const output = await lcm.project(input);
		expect(output).toBe(input);
		for (let attempts = 0; attempts < 20 && context.failedError === undefined; attempts++) await Bun.sleep(0);
		expect(context.failedError).toContain("#SECRET");
		expect(context.failedError).not.toContain("raw-secret");
		expect(manager.getBranch()).toHaveLength(1);
		await lcm.close();
	});
});
