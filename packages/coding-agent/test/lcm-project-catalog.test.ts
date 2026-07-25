import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import {
	getLcmProjectCatalogPath,
	listLcmProjects,
	readLcmProjectCatalog,
	registerLcmProject,
	resolveLcmProjectSelector,
} from "@oh-my-pi/pi-coding-agent/lcm/project-catalog";
import type { LcmProject } from "@oh-my-pi/pi-coding-agent/lcm/project-identity";

function project(agentDir: string, projectId: string, rootPath: string): LcmProject {
	return {
		projectId,
		rootPath,
		storePath: path.join(agentDir, "lcm", "projects", projectId, "context.sqlite"),
	};
}

describe("LCM project catalog", () => {
	let tempDir: string;
	let agentDir: string;

	beforeEach(async () => {
		tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-catalog-"));
		agentDir = path.join(tempDir, "agent");
	});

	afterEach(async () => {
		await fs.rm(tempDir, { recursive: true, force: true });
	});

	it("publishes concurrent registrations atomically without losing projects", async () => {
		const registrations = Array.from({ length: 12 }, (_, index) =>
			registerLcmProject(
				project(agentDir, `v1-project-${index}`, path.join(tempDir, `project-${index}`)),
				agentDir,
				1_900_000_000_000 + index,
			),
		);
		await Promise.all(registrations);

		const catalog = await readLcmProjectCatalog(agentDir);
		expect(catalog.version).toBe(1);
		expect(catalog.projects.map(entry => entry.projectId).sort()).toEqual(
			Array.from({ length: 12 }, (_, index) => `v1-project-${index}`).sort(),
		);
		const published = JSON.parse(await Bun.file(getLcmProjectCatalogPath(agentDir)).text());
		expect(published).toEqual(catalog);
		const directoryEntries = await fs.readdir(path.dirname(getLcmProjectCatalogPath(agentDir)));
		expect(directoryEntries.some(name => name.endsWith(".tmp") || name.endsWith(".lock"))).toBe(false);
	});

	it("updates lastSeen for the same exact project id instead of duplicating it", async () => {
		const entry = project(agentDir, "v1-stable", path.join(tempDir, "stable"));
		await registerLcmProject(entry, agentDir, 10);
		await registerLcmProject(entry, agentDir, 20);

		expect(await listLcmProjects(agentDir)).toEqual([{ ...entry, lastSeen: 20, journalDirs: [] }]);
	});

	it("merges durable journal-directory discovery hints without duplicates", async () => {
		const entry = project(agentDir, "v1-journals", path.join(tempDir, "stable"));
		const firstDir = path.join(tempDir, "sessions", "root");
		const secondDir = path.join(tempDir, "sessions", "nested");
		await registerLcmProject(entry, agentDir, 10, firstDir);
		await registerLcmProject(entry, agentDir, 20, secondDir);
		await registerLcmProject(entry, agentDir, 30, firstDir);

		expect(await listLcmProjects(agentDir)).toEqual([
			{ ...entry, lastSeen: 30, journalDirs: [secondDir, firstDir].sort() },
		]);
	});

	it("resolves only an exact id or an explicit canonical absolute path", async () => {
		const rootPath = path.join(tempDir, "known-project");
		await fs.mkdir(rootPath, { recursive: true });
		const entry = await registerLcmProject(project(agentDir, "v1-known", rootPath), agentDir, 42);

		expect(await resolveLcmProjectSelector("v1-known", agentDir)).toEqual(entry);
		expect(await resolveLcmProjectSelector(path.join(rootPath, "."), agentDir)).toEqual(entry);
		await expect(resolveLcmProjectSelector("known-project", agentDir)).rejects.toThrow(
			"exact project id or absolute",
		);
		await expect(resolveLcmProjectSelector("", agentDir)).rejects.toThrow("explicit LCM project selector");
		await expect(resolveLcmProjectSelector(path.join(tempDir, "missing"), agentDir)).rejects.toThrow(
			"Unknown LCM project path",
		);
	});

	it("fails closed when a catalog path selector is ambiguous", async () => {
		const rootPath = path.join(tempDir, "same-root");
		const first = { ...project(agentDir, "v1-first", rootPath), lastSeen: 1 };
		const second = { ...project(agentDir, "v2-second", rootPath), lastSeen: 2 };
		const catalogPath = getLcmProjectCatalogPath(agentDir);
		await fs.mkdir(path.dirname(catalogPath), { recursive: true });
		await Bun.write(catalogPath, JSON.stringify({ version: 1, projects: [first, second] }));

		await expect(resolveLcmProjectSelector(rootPath, agentDir)).rejects.toThrow("Ambiguous LCM project path");
		expect((await resolveLcmProjectSelector(first.projectId, agentDir)).projectId).toBe(first.projectId);
	});

	it("rejects ambiguous duplicate ids instead of choosing by order", async () => {
		const first = { ...project(agentDir, "v1-duplicate", path.join(tempDir, "one")), lastSeen: 1 };
		const second = { ...project(agentDir, "v1-duplicate", path.join(tempDir, "two")), lastSeen: 2 };
		const catalogPath = getLcmProjectCatalogPath(agentDir);
		await fs.mkdir(path.dirname(catalogPath), { recursive: true });
		await Bun.write(catalogPath, JSON.stringify({ version: 1, projects: [first, second] }));

		await expect(resolveLcmProjectSelector(first.projectId, agentDir)).rejects.toThrow("Ambiguous LCM project id");
	});
});
