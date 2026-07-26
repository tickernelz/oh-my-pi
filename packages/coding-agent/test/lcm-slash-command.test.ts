import { afterEach, beforeEach, describe, expect, it, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type { Citation, SearchHit, SourceDescription } from "@oh-my-pi/lcm-context";
import { encodeLcmHandle, type LcmDescription, type LcmHandle } from "@oh-my-pi/pi-coding-agent/lcm/operations";
import { registerLcmProject } from "@oh-my-pi/pi-coding-agent/lcm/project-catalog";
import { resolveLcmProject } from "@oh-my-pi/pi-coding-agent/lcm/project-identity";
import type { LcmPublicStatus, LcmRuntimePhase } from "@oh-my-pi/pi-coding-agent/session/session-lcm";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { getSessionDirCandidatesReadOnly } from "@oh-my-pi/pi-coding-agent/session/session-paths";
import { MemorySessionStorage } from "@oh-my-pi/pi-coding-agent/session/session-storage";
import {
	ACP_BUILTIN_SLASH_COMMANDS,
	executeAcpBuiltinSlashCommand,
} from "@oh-my-pi/pi-coding-agent/slash-commands/acp-builtins";
import type { SlashCommandRuntime } from "@oh-my-pi/pi-coding-agent/slash-commands/types";
import { getSessionsDir } from "@oh-my-pi/pi-utils";

const CITATION: Citation = {
	projectId: "v1-project",
	sessionId: "session",
	branchId: "main",
	sourceId: "source",
	sourceKey: "entry",
	contentHash: "hash",
	position: 0,
};

const HIT: SearchHit = {
	kind: "source",
	id: "source",
	redactedText: "redacted result",
	rank: -1,
	citations: [CITATION],
};

const SOURCE_DESCRIPTION: SourceDescription = {
	...CITATION,
	parentId: null,
	timestamp: 1_900_000_000_000,
	kind: "message",
	atomicGroupId: null,
	redactedText: "redacted source description",
	artifactRefs: ["artifact://77"],
	files: [],
};

const DESCRIPTION: LcmDescription = { kind: "source", value: SOURCE_DESCRIPTION };

const STORE: NonNullable<LcmPublicStatus["store"]> = {
	schemaVersion: 5,
	journalMode: "wal",
	quarantined: false,
	quarantineReason: null,
	recoveredFrom: null,
	branches: 2,
	activeSources: 9,
	tombstones: 1,
	leafSummaries: 2,
	condensedSummaries: 1,
	jobs: { pending: 2, leased: 1, failed: 1, completed: 4, obsolete: 1 },
};

function statusFor(phase: LcmRuntimePhase): LcmPublicStatus {
	const withoutStore = phase === "disabled" || phase === "uninitialized";
	return {
		runtime: {
			phase,
			summaryWorkers:
				phase === "disabled" ? { active: 0, limit: 0 } : { active: phase === "active" ? 2 : 0, limit: 4 },
			...(withoutStore ? {} : { summaryModelSelector: "@smol", resolvedSummaryModel: "provider/model" }),
			...(phase === "active"
				? {
						lastProjection: {
							revision: 7,
							sourceTokens: 12_000,
							selectedLevelCounts: { 0: 2, 2: 1 },
							coveredSourceCount: 6,
							freshSourceCount: 3,
							estimatedTokens: 4_000,
							pendingJobs: 2,
						},
						summaryBackoff: { fallback: 1_900_000_000_000 },
						lastFailureCategory: "provider" as const,
						retryAt: 1_900_000_000_000,
					}
				: {}),
			...(phase === "degraded"
				? {
						summaryBackoff: { preferred: 1_900_000_000_000 },
						lastFailureCategory: "provider" as const,
						retryAt: 1_900_000_000_000,
					}
				: {}),
		},
		...(withoutStore
			? {}
			: {
					store: {
						...STORE,
						quarantined: phase === "quarantined",
						quarantineReason: phase === "quarantined" ? "/private/tenant?token=top-secret" : null,
					},
				}),
	};
}

interface RuntimeOptions {
	status?: LcmPublicStatus;
	enabled?: boolean;
	hits?: SearchHit[];
	sessionManager?: SessionManager;
}

function createRuntime(agentDir: string, options: RuntimeOptions = {}) {
	const output: string[] = [];
	const status = options.status ?? statusFor("active");
	const sessionManager = options.sessionManager ?? SessionManager.inMemory("/tmp");
	const session = {
		lcmEnabled: options.enabled ?? status.runtime.phase !== "disabled",
		lcmStatus: vi.fn(async () => status),
		lcmDoctor: vi.fn(async () => ({
			ok: false,
			checks: [{ name: "sqlite-quick-check", ok: false, detail: "/private/token=top-secret" }],
		})),
		lcmRebuildCurrent: vi.fn(async () => ({ branches: 1, activeSources: 2, queuedJobs: 1 })),
		lcmRebuildProject: vi.fn(async (_projectId: string, _journals: readonly SessionManager[]) => ({
			branches: 2,
			activeSources: 3,
			queuedJobs: 1,
		})),
		lcmGc: vi.fn(async () => ({ tombstones: 1, jobs: 2, summaries: 3, sourceContents: 4, files: 5 })),
		lcmSearch: vi.fn(async (_query: string) => options.hits ?? [HIT]),
		lcmDescribe: vi.fn(async (_handle: LcmHandle) => DESCRIPTION),
	};
	const runtime = {
		session,
		sessionManager,
		settings: { getAgentDir: () => agentDir },
		cwd: sessionManager.getCwd(),
		output: (text: string) => {
			output.push(text);
		},
		refreshCommands: () => {},
		reloadPlugins: async () => {},
	} as unknown as SlashCommandRuntime;
	return { output, runtime, session };
}

class FaultySessionStorage extends MemorySessionStorage {
	readonly missing = new Set<string>();
	readonly unreadable = new Set<string>();
	readonly unreadableDirectories = new Set<string>();

	override listFilesSync(dir: string, pattern: string, options?: { strict?: boolean }): string[] {
		if (options?.strict && this.unreadableDirectories.has(dir)) {
			throw new Error("unreadable directory /private/token=top-secret");
		}
		const files = super.listFilesSync(dir, pattern, options);
		for (const file of [...this.missing, ...this.unreadable]) {
			if (path.dirname(file) === dir) files.push(file);
		}
		return files;
	}

	override statSync(file: string) {
		if (this.missing.has(file)) {
			throw Object.assign(new Error("missing /private/token=top-secret"), { code: "ENOENT" });
		}
		if (this.unreadable.has(file)) throw new Error("unreadable /private/token=top-secret");
		return super.statSync(file);
	}
}

function sessionJsonl(cwd: string): string {
	return `${[
		{ type: "session", version: 3, id: "project-session", timestamp: "2026-01-01T00:00:00.000Z", cwd },
		{
			type: "message",
			id: "leaf",
			parentId: null,
			timestamp: "2026-01-01T00:00:00.000Z",
			message: { role: "user", content: [{ type: "text", text: "project source" }], timestamp: 1_767_225_600_000 },
		},
	]
		.map(entry => JSON.stringify(entry))
		.join("\n")}\n`;
}

async function initRepository(root: string): Promise<void> {
	await fs.mkdir(path.join(root, ".git"), { recursive: true });
	await fs.writeFile(path.join(root, ".git", "HEAD"), "ref: refs/heads/main\n");
}

describe("/lcm slash command", () => {
	let tempDir: string;
	let agentDir: string;

	beforeEach(async () => {
		tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-slash-"));
		agentDir = path.join(tempDir, "agent");
	});

	afterEach(async () => {
		await fs.rm(tempDir, { recursive: true, force: true });
	});

	it("advertises one deterministic ACP command surface", () => {
		expect(ACP_BUILTIN_SLASH_COMMANDS).toContainEqual({
			name: "lcm",
			description: "Inspect and maintain LCM derived state",
			input: { hint: "<subcommand>" },
		});
	});

	it("rejects bare, removed purge, and malformed GC operations", async () => {
		const harness = createRuntime(agentDir);
		await executeAcpBuiltinSlashCommand("/lcm", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm purge", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm gc unexpected", harness.runtime);

		expect(harness.session.lcmGc).not.toHaveBeenCalled();
		expect(harness.output).toEqual([
			"Usage: /lcm <status|doctor|rebuild <current|project> --yes|gc|search <query>|describe <handle>|projects>",
			"Usage: /lcm <status|doctor|rebuild <current|project> --yes|gc|search <query>|describe <handle>|projects>",
			"Usage: /lcm <status|doctor|rebuild <current|project> --yes|gc|search <query>|describe <handle>|projects>",
		]);
	});

	it("distinguishes every public runtime phase and preserves quarantine output", async () => {
		for (const phase of [
			"disabled",
			"uninitialized",
			"idle",
			"warming",
			"active",
			"degraded",
			"quarantined",
		] as const) {
			const harness = createRuntime(agentDir, { status: statusFor(phase), enabled: phase !== "disabled" });
			await executeAcpBuiltinSlashCommand("/lcm status", harness.runtime);
			expect(harness.output[0]).toContain(`LCM status: ${phase.toUpperCase()}`);
		}

		const quarantined = createRuntime(agentDir, { status: statusFor("quarantined") });
		await executeAcpBuiltinSlashCommand("/lcm status", quarantined.runtime);
		expect(quarantined.output[0]).toContain("Derived store is quarantined");
		expect(quarantined.output[0]).not.toContain("/private/");
		expect(quarantined.output[0]).not.toContain("top-secret");
	});

	it("renders disabled workers and an unopened project exactly", async () => {
		const harness = createRuntime(agentDir, { status: statusFor("disabled"), enabled: false });
		await executeAcpBuiltinSlashCommand("/lcm status", harness.runtime);

		expect(harness.output).toEqual([
			[
				"LCM status: DISABLED",
				"Authority: session JSONL is authoritative; LCM SQLite is redacted, derived, and rebuildable.",
				"Workers: 0/0 active",
				"SQLite WAL: not initialized",
				"Project jobs: not initialized",
				"Backoff: preferred until none; fallback until none",
				"DAG: no fitted projection yet",
			].join("\n"),
		]);
	});

	it("renders preferred-model backoff while DEGRADED", async () => {
		const harness = createRuntime(agentDir, { status: statusFor("degraded") });
		await executeAcpBuiltinSlashCommand("/lcm status", harness.runtime);

		expect(harness.output[0]).toContain("LCM status: DEGRADED");
		expect(harness.output[0]).toContain("Workers: 0/4 active");
		expect(harness.output[0]).toContain("Project jobs: 2 pending, 1 running, 1 failed, 4 completed, 1 obsolete");
		expect(harness.output[0]).toContain("Backoff: preferred until 2030-03-17T17:46:40.000Z; fallback until none");
		expect(harness.output[0]).toContain("Lossless projection is degraded; native compaction remains active.");
	});

	it("keeps a fitted fallback failure ACTIVE and renders exact status lines", async () => {
		const harness = createRuntime(agentDir, { status: statusFor("active") });
		await executeAcpBuiltinSlashCommand("/lcm status", harness.runtime);

		expect(harness.output).toEqual([
			[
				"LCM status: ACTIVE",
				"Authority: session JSONL is authoritative; LCM SQLite is redacted, derived, and rebuildable.",
				"Summary model: @smol -> provider/model",
				"Workers: 2/4 active",
				"SQLite WAL: enabled; schema: 5",
				"Store: 2 branches, 9 active sources, 1 retained tombstones, 3 summary nodes",
				"Project jobs: 2 pending, 1 running, 1 failed, 4 completed, 1 obsolete",
				"Backoff: preferred until none; fallback until 2030-03-17T17:46:40.000Z",
				"DAG: depth 3, 3 selected nodes, 6 covered sources, 3 fresh sources",
				"Estimated tokens: 12000 -> 4000",
				"Current branch: revision 7; 2 relevant jobs pending",
				"Last fallback: provider",
			].join("\n"),
		]);
	});

	it("keeps status, doctor, and failure output path- and secret-safe", async () => {
		const unsafe = statusFor("quarantined");
		unsafe.runtime.summaryModelSelector = "token=top-secret";
		unsafe.runtime.resolvedSummaryModel = "C:\\private\\secret";
		unsafe.runtime.summaryBackoff = { preferred: "token=top-secret" as unknown as number };
		unsafe.store!.recoveredFrom = "/private/recovered.sqlite?token=top-secret";
		const harness = createRuntime(agentDir, { status: unsafe });
		await executeAcpBuiltinSlashCommand("/lcm status", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm doctor", harness.runtime);
		harness.session.lcmDoctor.mockRejectedValueOnce(new Error("/private/token=top-secret"));
		await executeAcpBuiltinSlashCommand("/lcm doctor", harness.runtime);

		for (const text of harness.output) {
			expect(text).not.toContain("/private/");
			expect(text).not.toContain("top-secret");
		}
		expect(harness.output[0]).toContain("Summary model: [redacted] -> [redacted]");
		expect(harness.output[0]).toContain("Backoff: preferred until none; fallback until none");
		expect(harness.output[1]).toContain("attention required");
		expect(harness.output[2]).toContain("Native context remains available");
	});

	it("requires confirmation and warns before dispatching a destructive current rebuild", async () => {
		const harness = createRuntime(agentDir);
		harness.session.lcmRebuildCurrent.mockImplementationOnce(async () => {
			expect(harness.output).toHaveLength(1);
			expect(harness.output[0]).toContain("destructively resets the entire LCM derived store");
			return { branches: 1, activeSources: 2, queuedJobs: 1 };
		});

		await executeAcpBuiltinSlashCommand("/lcm rebuild current --yes", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm gc", harness.runtime);

		expect(harness.session.lcmRebuildCurrent).toHaveBeenCalledTimes(1);
		expect(harness.session.lcmGc).toHaveBeenCalledTimes(1);
		expect(harness.output[0]).toContain("WARNING");
		expect(harness.output[0]).toContain("only the current session/branch");
		expect(harness.output[0]).toContain("every other session will be removed");
		expect(harness.output[1]).toContain("Session JSONL was not modified");
		expect(harness.output[2]).toContain("retention-aware GC");
		expect(harness.output[2]).toContain("5 unreferenced file records");
		expect(harness.output[2]).toContain("Active lineage and authoritative session JSONL were not changed");
	});

	it("requires an exact rebuild scope and noninteractive confirmation", async () => {
		const harness = createRuntime(agentDir);
		await executeAcpBuiltinSlashCommand("/lcm rebuild", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm rebuild current", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm rebuild project", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm rebuild project --yes extra", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm rebuild current --yes extra", harness.runtime);

		expect(harness.session.lcmRebuildCurrent).not.toHaveBeenCalled();
		expect(harness.session.lcmRebuildProject).not.toHaveBeenCalled();
		for (const text of harness.output) {
			expect(text).toContain("Usage: /lcm rebuild <current|project> --yes");
			expect(text).toContain("incur model cost");
			expect(text).toContain("never modifies JSONL");
		}
	});

	it("rebuilds every discovered journal for only the selected project without mutating JSONL", async () => {
		const projectRoot = path.join(tempDir, "project");
		await initRepository(projectRoot);
		const storage = new MemorySessionStorage();
		const manager = SessionManager.inMemory(projectRoot, storage);
		const journalDir = getSessionDirCandidatesReadOnly(projectRoot, getSessionsDir(agentDir))[0]!;
		const journal = path.join(journalDir, "session.jsonl");
		storage.writeTextSync(journal, sessionJsonl(projectRoot));
		const before = await storage.readText(journal);
		const harness = createRuntime(agentDir, { sessionManager: manager });

		await executeAcpBuiltinSlashCommand("/lcm rebuild project --yes", harness.runtime);

		expect(harness.output[0]).toContain("selected project only");
		expect(harness.output[0]).toContain("incur model cost");
		expect(harness.output[1]).toContain("rebuilt from 1 authoritative JSONL journal");
		expect(harness.session.lcmRebuildProject).toHaveBeenCalledTimes(1);
		const [projectId, journals] = harness.session.lcmRebuildProject.mock.calls[0]!;
		expect(projectId).toMatch(/^v1-[0-9a-f]{64}$/);
		expect(journals.map(item => `${item.getSessionId()}:${item.getLeafId()}`)).toEqual(["project-session:leaf"]);
		expect(await storage.readText(journal)).toBe(before);
	});

	it("blocks a partial project rebuild when one authoritative directory is unreadable", async () => {
		const projectRoot = path.join(tempDir, "project-errors");
		await initRepository(projectRoot);
		const project = await resolveLcmProject(projectRoot, agentDir);
		const storage = new FaultySessionStorage();
		const manager = SessionManager.inMemory(projectRoot, storage);
		const journalDir = getSessionDirCandidatesReadOnly(projectRoot, getSessionsDir(agentDir))[0]!;
		const validJournal = path.join(journalDir, "valid.jsonl");
		storage.writeTextSync(validJournal, sessionJsonl(projectRoot));
		const unreadableDir = path.join(tempDir, "unreadable-sessions");
		storage.unreadableDirectories.add(unreadableDir);
		await registerLcmProject(project, agentDir, 1_900_000_000_000, unreadableDir);
		const before = await storage.readText(validJournal);
		const harness = createRuntime(agentDir, { sessionManager: manager });

		await executeAcpBuiltinSlashCommand("/lcm rebuild project --yes", harness.runtime);

		expect(harness.session.lcmRebuildProject).not.toHaveBeenCalled();
		expect(harness.output[1]).toContain("0 authoritative journal paths were missing and 1 were unreadable");
		expect(harness.output[1]).toContain("Derived state and session JSONL were not changed");
		expect(await storage.readText(validJournal)).toBe(before);
		expect(harness.output.join("\n")).not.toContain("/private/");
		expect(harness.output.join("\n")).not.toContain("top-secret");
	});

	it("keeps disabled, uninitialized, degraded, and genuine no-results search outcomes distinct", async () => {
		const cases: Array<{ phase: LcmRuntimePhase; enabled: boolean; expected: string }> = [
			{ phase: "disabled", enabled: false, expected: "Lossless context is disabled" },
			{ phase: "uninitialized", enabled: true, expected: "derived state is uninitialized" },
			{ phase: "degraded", enabled: true, expected: "runtime is degraded" },
			{ phase: "active", enabled: true, expected: "No LCM matches found" },
		];
		const outcomes: string[] = [];
		for (const item of cases) {
			const harness = createRuntime(agentDir, {
				status: statusFor(item.phase),
				enabled: item.enabled,
				hits: [],
			});
			await executeAcpBuiltinSlashCommand("/lcm search absent", harness.runtime);
			expect(harness.output[0]).toContain(item.expected);
			outcomes.push(harness.output[0]!);
		}
		expect(new Set(outcomes).size).toBe(cases.length);
	});

	it("searches and describes only through the current-session facade", async () => {
		const harness = createRuntime(agentDir);
		const handle = { kind: "source" as const, citation: CITATION };
		const token = encodeLcmHandle(handle);
		await executeAcpBuiltinSlashCommand("/lcm search needle", harness.runtime);
		await executeAcpBuiltinSlashCommand(`/lcm describe ${token}`, harness.runtime);

		expect(harness.session.lcmSearch).toHaveBeenCalledWith("needle");
		expect(harness.output[0]).toContain("redacted result");
		expect(harness.output[0]).toContain(token);
		expect(harness.session.lcmDescribe).toHaveBeenCalledWith(handle);
		expect(harness.output[1]).toContain("redacted source description");
		expect(harness.output[1]).not.toContain("artifact://77");
		expect(harness.output[1]).toContain("[unavailable in current session]");
	});

	it("lists path-safe project selectors without store or root paths", async () => {
		const rootPath = path.join(tempDir, "private-project");
		const harness = createRuntime(agentDir);
		await registerLcmProject(
			{
				projectId: "v1-known",
				rootPath,
				storePath: path.join(agentDir, "lcm", "projects", "v1-known", "context.sqlite"),
			},
			agentDir,
			1_900_000_000_000,
		);
		await executeAcpBuiltinSlashCommand("/lcm projects", harness.runtime);

		expect(harness.output[0]).toContain("path-safe project ID");
		expect(harness.output[0]).toContain("v1-known");
		expect(harness.output[0]).toContain("authoritative journal directories known: 0");
		expect(harness.output[0]).not.toContain(rootPath);
		expect(harness.output[0]).not.toContain("context.sqlite");
	});
});
