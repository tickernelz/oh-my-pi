import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { registerLcmProject } from "@oh-my-pi/pi-coding-agent/lcm/project-catalog";
import { resolveLcmProject } from "@oh-my-pi/pi-coding-agent/lcm/project-identity";
import { discoverLcmProjectJournals } from "@oh-my-pi/pi-coding-agent/lcm/rebuild";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { getSessionDirCandidatesReadOnly } from "@oh-my-pi/pi-coding-agent/session/session-paths";
import { MemorySessionStorage } from "@oh-my-pi/pi-coding-agent/session/session-storage";
import { getSessionsDir } from "@oh-my-pi/pi-utils";

class FaultySessionStorage extends MemorySessionStorage {
	readonly missing = new Set<string>();
	readonly unreadable = new Set<string>();
	readonly missingDirectories = new Set<string>();
	readonly unreadableDirectories = new Set<string>();

	override listFilesSync(dir: string, pattern: string, options?: { strict?: boolean }): string[] {
		if (options?.strict) {
			if (this.missingDirectories.has(dir)) {
				throw Object.assign(new Error("missing directory /private/project/token=top-secret"), {
					code: "ENOENT",
				});
			}
			if (this.unreadableDirectories.has(dir)) {
				throw new Error("unreadable directory /private/project/token=top-secret");
			}
		}
		const listed = super.listFilesSync(dir, pattern, options);
		for (const file of [...this.missing, ...this.unreadable]) {
			if (path.dirname(file) === dir) listed.push(file);
		}
		return listed;
	}

	override statSync(file: string) {
		if (this.missing.has(file)) {
			throw Object.assign(new Error("missing /private/project/token=top-secret"), { code: "ENOENT" });
		}
		if (this.unreadable.has(file)) throw new Error("unreadable /private/project/token=top-secret");
		return super.statSync(file);
	}
}

function sessionJsonl(cwd: string, sessionId: string, entries: object[]): string {
	const header = {
		type: "session",
		version: 3,
		id: sessionId,
		timestamp: "2026-01-01T00:00:00.000Z",
		cwd,
	};
	return `${[header, ...entries].map(entry => JSON.stringify(entry)).join("\n")}\n`;
}

function message(id: string, parentId: string | null, text: string): object {
	return {
		type: "message",
		id,
		parentId,
		timestamp: "2026-01-01T00:00:00.000Z",
		message: {
			role: "user",
			content: [{ type: "text", text }],
			timestamp: 1_767_225_600_000,
		},
	};
}

async function initRepository(root: string): Promise<void> {
	await fs.mkdir(path.join(root, ".git"), { recursive: true });
	await fs.writeFile(path.join(root, ".git", "HEAD"), "ref: refs/heads/main\n");
}

describe("LCM project journal discovery", () => {
	const tempRoots: string[] = [];

	afterEach(async () => {
		await Promise.all(tempRoots.splice(0).map(root => fs.rm(root, { recursive: true, force: true })));
	});

	it("loads every registered same-project leaf, reports failures, and never mutates JSONL", async () => {
		const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-rebuild-"));
		tempRoots.push(tempRoot);
		const projectRoot = path.join(tempRoot, "project");
		const nestedCwd = path.join(projectRoot, "packages", "worker");
		const foreignRoot = path.join(tempRoot, "foreign");
		const agentDir = path.join(tempRoot, "agent");
		await initRepository(projectRoot);
		await initRepository(foreignRoot);
		await fs.mkdir(nestedCwd, { recursive: true });
		const project = await resolveLcmProject(projectRoot, agentDir);
		const sessionsRoot = getSessionsDir(agentDir);
		const rootSessionDir = getSessionDirCandidatesReadOnly(projectRoot, sessionsRoot)[0]!;
		const nestedSessionDir = getSessionDirCandidatesReadOnly(nestedCwd, sessionsRoot)[0]!;
		await registerLcmProject(project, agentDir, 1_900_000_000_000, nestedSessionDir);

		const storage = new FaultySessionStorage();
		const manager = SessionManager.inMemory(nestedCwd, storage);
		const rootJournal = path.join(rootSessionDir, "root.jsonl");
		const nestedJournal = path.join(nestedSessionDir, "nested.jsonl");
		const foreignJournal = path.join(rootSessionDir, "foreign.jsonl");
		storage.writeTextSync(
			rootJournal,
			sessionJsonl(projectRoot, "root-session", [message("root-leaf", null, "root")]),
		);
		storage.writeTextSync(
			nestedJournal,
			sessionJsonl(nestedCwd, "nested-session", [
				message("fork", null, "fork"),
				message("left-leaf", "fork", "left"),
				message("right-leaf", "fork", "right"),
			]),
		);
		storage.writeTextSync(
			foreignJournal,
			sessionJsonl(foreignRoot, "foreign-session", [message("foreign-leaf", null, "foreign")]),
		);
		storage.missing.add(path.join(rootSessionDir, "missing.jsonl"));
		storage.unreadable.add(path.join(rootSessionDir, "unreadable.jsonl"));
		const before = await storage.readText(rootJournal);

		const discovery = await discoverLcmProjectJournals({ project, sessionManager: manager, agentDir });

		expect(discovery.filesDiscovered).toBe(5);
		expect(discovery.filesLoaded).toBe(2);
		expect(discovery.issues).toEqual({ missing: 1, unreadable: 1, empty: 0, outOfScope: 1 });
		expect(discovery.journals.map(journal => `${journal.getSessionId()}:${journal.getLeafId()}`).sort()).toEqual([
			"nested-session:left-leaf",
			"nested-session:right-leaf",
			"root-session:root-leaf",
		]);
		expect(await storage.readText(rootJournal)).toBe(before);
		expect(JSON.stringify(discovery.issues)).not.toContain("top-secret");
		expect(JSON.stringify(discovery.issues)).not.toContain("/private/");
	});

	it("classifies required directory failures while keeping the legacy fallback best-effort", async () => {
		const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-rebuild-dirs-"));
		tempRoots.push(tempRoot);
		const projectRoot = path.join(tempRoot, "project");
		const agentDir = path.join(tempRoot, "agent");
		await initRepository(projectRoot);
		const project = await resolveLcmProject(projectRoot, agentDir);
		const [canonicalDir, legacyDir] = getSessionDirCandidatesReadOnly(projectRoot, getSessionsDir(agentDir));
		const validDir = path.join(tempRoot, "valid-sessions");
		const unreadableDir = path.join(tempRoot, "unreadable-sessions");
		await registerLcmProject(project, agentDir, 1_900_000_000_000, validDir);
		await registerLcmProject(project, agentDir, 1_900_000_000_001, unreadableDir);

		const storage = new FaultySessionStorage();
		const manager = SessionManager.inMemory(projectRoot, storage);
		const validJournal = path.join(validDir, "valid.jsonl");
		storage.writeTextSync(
			validJournal,
			sessionJsonl(projectRoot, "valid-session", [message("valid-leaf", null, "valid")]),
		);
		storage.missingDirectories.add(canonicalDir!);
		storage.unreadableDirectories.add(unreadableDir);
		if (legacyDir) storage.unreadableDirectories.add(legacyDir);
		const before = await storage.readText(validJournal);

		const discovery = await discoverLcmProjectJournals({ project, sessionManager: manager, agentDir });

		expect(discovery.filesDiscovered).toBe(1);
		expect(discovery.filesLoaded).toBe(1);
		expect(discovery.issues).toEqual({ missing: 1, unreadable: 1, empty: 0, outOfScope: 0 });
		expect(discovery.journals.map(journal => `${journal.getSessionId()}:${journal.getLeafId()}`)).toEqual([
			"valid-session:valid-leaf",
		]);
		expect(await storage.readText(validJournal)).toBe(before);
	});
});
