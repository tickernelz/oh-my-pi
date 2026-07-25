import { afterEach, beforeEach, describe, expect, it, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type { Citation, SearchHit, SourceDescription } from "@oh-my-pi/lcm-context";
import { encodeLcmCitation } from "@oh-my-pi/pi-coding-agent/lcm/operations";
import { registerLcmProject } from "@oh-my-pi/pi-coding-agent/lcm/project-catalog";
import {
	ACP_BUILTIN_SLASH_COMMANDS,
	executeAcpBuiltinSlashCommand,
} from "@oh-my-pi/pi-coding-agent/slash-commands/acp-builtins";
import type { SlashCommandRuntime } from "@oh-my-pi/pi-coding-agent/slash-commands/types";

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

const DESCRIPTION: SourceDescription = {
	...CITATION,
	parentId: null,
	timestamp: 1_900_000_000_000,
	kind: "message",
	atomicGroupId: null,
	redactedText: "redacted source description",
	artifactRefs: ["artifact://77"],
};

function createRuntime(agentDir: string) {
	const output: string[] = [];
	const session = {
		lcmStatus: vi.fn(async () => ({
			dbPath: path.join(os.homedir(), ".omp", "agent", "lcm", "secret.sqlite"),
			schemaVersion: 3,
			journalMode: "wal",
			quarantined: false,
			quarantineReason: null,
			recoveredFrom: path.join(os.homedir(), ".omp", "agent", "lcm", "quarantined.sqlite"),
			branches: 1,
			activeSources: 2,
			tombstones: 0,
			leafSummaries: 1,
			condensedSummaries: 0,
			jobs: { pending: 0, leased: 0, failed: 0, completed: 1, obsolete: 0 },
		})),
		lcmDoctor: vi.fn(async () => ({ ok: false, checks: [{ name: "integrity", ok: false, detail: "repairable" }] })),
		lcmRebuild: vi.fn(async () => ({ branches: 1, activeSources: 2, queuedJobs: 1 })),
		lcmPurge: vi.fn(async () => ({ tombstones: 1, jobs: 2, summaries: 3, sourceContents: 4 })),
		lcmSearch: vi.fn(async () => [HIT]),
		lcmDescribe: vi.fn(async () => DESCRIPTION),
	};
	const runtime = {
		session,
		sessionManager: {},
		settings: { getAgentDir: () => agentDir },
		cwd: "/tmp",
		output: (text: string) => {
			output.push(text);
		},
		refreshCommands: () => {},
		reloadPlugins: async () => {},
	} as unknown as SlashCommandRuntime;
	return { output, runtime, session };
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

	it("is advertised to ACP as one unified subcommand surface", () => {
		expect(ACP_BUILTIN_SLASH_COMMANDS).toContainEqual({
			name: "lcm",
			description: "Inspect and repair LCM derived state",
			input: { hint: "<subcommand>" },
		});
	});

	it("requires an explicit operation and never treats a bare command as purge", async () => {
		const harness = createRuntime(agentDir);
		await executeAcpBuiltinSlashCommand("/lcm", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm purge unexpected", harness.runtime);

		expect(harness.session.lcmPurge).not.toHaveBeenCalled();
		expect(harness.output).toEqual([
			"Usage: /lcm <status|doctor|rebuild|purge|search <query>|describe <citation>|projects>",
			"Usage: /lcm <status|doctor|rebuild|purge|search <query>|describe <citation>|projects>",
		]);
	});

	it("renders safe status/doctor and operator-only rebuild/purge results", async () => {
		const harness = createRuntime(agentDir);
		await executeAcpBuiltinSlashCommand("/lcm status", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm doctor", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm rebuild", harness.runtime);
		await executeAcpBuiltinSlashCommand("/lcm purge", harness.runtime);

		expect(harness.output[0]).toContain("LCM status: ready");
		expect(harness.output[0]).toContain("Recovered derived store: ~/.omp/agent/lcm/quarantined.sqlite");
		expect(harness.output[0]).not.toContain("secret.sqlite");
		expect(harness.output[1]).toContain("LCM doctor: DEGRADED");
		expect(harness.output[2]).toContain("derived state rebuilt");
		expect(harness.output[3]).toContain("authoritative session journal was not changed");
		expect(harness.session.lcmRebuild).toHaveBeenCalledTimes(1);
		expect(harness.session.lcmPurge).toHaveBeenCalledTimes(1);
	});

	it("searches and describes only through the current-session facade", async () => {
		const harness = createRuntime(agentDir);
		const token = encodeLcmCitation(CITATION);
		await executeAcpBuiltinSlashCommand("/lcm search needle", harness.runtime);
		await executeAcpBuiltinSlashCommand(`/lcm describe ${token}`, harness.runtime);

		expect(harness.session.lcmSearch).toHaveBeenCalledWith("needle");
		expect(harness.output[0]).toContain("redacted result");
		expect(harness.output[0]).toContain(token);
		expect(harness.session.lcmDescribe).toHaveBeenCalledWith(CITATION);
		expect(harness.output[1]).toContain("redacted source description");
		expect(harness.output[1]).not.toContain("artifact://77");
		expect(harness.output[1]).toContain("[unavailable in current session]");
	});

	it("lists registered projects without exposing store paths and states explicit-selection policy", async () => {
		const harness = createRuntime(agentDir);
		await registerLcmProject(
			{
				projectId: "v1-known",
				rootPath: path.join(os.homedir(), "known-project"),
				storePath: path.join(agentDir, "lcm", "projects", "v1-known", "context.sqlite"),
			},
			agentDir,
			1_900_000_000_000,
		);
		await executeAcpBuiltinSlashCommand("/lcm projects", harness.runtime);

		expect(harness.output[0]).toContain("explicit selector");
		expect(harness.output[0]).toContain("v1-known");
		expect(harness.output[0]).toContain("root: ~/known-project");
		expect(harness.output[0]).not.toContain("context.sqlite");
	});
});
