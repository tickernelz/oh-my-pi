import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { getLcmDir, isEnoent, normalizePathForComparison } from "@oh-my-pi/pi-utils";
import { withFileLock } from "../config/file-lock";
import type { LcmProject } from "./project-identity";

const CATALOG_VERSION = 1;
const CATALOG_FILENAME = "projects.json";

export interface LcmProjectCatalogEntry extends LcmProject {
	lastSeen: number;
	/** Read-only discovery hints; every candidate JSONL is revalidated before use. */
	journalDirs: string[];
}

export interface LcmProjectCatalog {
	version: typeof CATALOG_VERSION;
	projects: LcmProjectCatalogEntry[];
}

export function getLcmProjectCatalogPath(agentDir?: string): string {
	return path.join(getLcmDir(agentDir), CATALOG_FILENAME);
}

function emptyCatalog(): LcmProjectCatalog {
	return { version: CATALOG_VERSION, projects: [] };
}

function isCatalogEntry(value: unknown): value is LcmProjectCatalogEntry {
	if (!value || typeof value !== "object") return false;
	const entry = value as Record<string, unknown>;
	return (
		typeof entry.projectId === "string" &&
		entry.projectId.length > 0 &&
		typeof entry.rootPath === "string" &&
		path.isAbsolute(entry.rootPath) &&
		typeof entry.storePath === "string" &&
		path.isAbsolute(entry.storePath) &&
		typeof entry.lastSeen === "number" &&
		Number.isFinite(entry.lastSeen) &&
		entry.lastSeen >= 0 &&
		(entry.journalDirs === undefined ||
			(Array.isArray(entry.journalDirs) &&
				entry.journalDirs.every(directory => typeof directory === "string" && path.isAbsolute(directory))))
	);
}

export async function readLcmProjectCatalog(agentDir?: string): Promise<LcmProjectCatalog> {
	const catalogPath = getLcmProjectCatalogPath(agentDir);
	let value: unknown;
	try {
		value = await Bun.file(catalogPath).json();
	} catch (error) {
		if (isEnoent(error)) return emptyCatalog();
		throw new Error(`Unable to read LCM project catalog: ${catalogPath}`, { cause: error });
	}
	if (!value || typeof value !== "object") {
		throw new Error(`Invalid LCM project catalog: ${catalogPath}`);
	}
	const catalog = value as Record<string, unknown>;
	if (
		catalog.version !== CATALOG_VERSION ||
		!Array.isArray(catalog.projects) ||
		!catalog.projects.every(isCatalogEntry)
	) {
		throw new Error(`Unsupported or invalid LCM project catalog: ${catalogPath}`);
	}
	return {
		version: CATALOG_VERSION,
		projects: catalog.projects.map(project => ({ ...project, journalDirs: [...(project.journalDirs ?? [])] })),
	};
}

async function writeLcmProjectCatalog(catalogPath: string, catalog: LcmProjectCatalog): Promise<void> {
	const temporaryPath = `${catalogPath}.${process.pid}.${randomUUID()}.tmp`;
	try {
		await Bun.write(temporaryPath, `${JSON.stringify(catalog, null, 2)}\n`);
		await fs.rename(temporaryPath, catalogPath);
	} catch (error) {
		await fs.rm(temporaryPath, { force: true }).catch(() => {});
		throw error;
	}
}

/** Register a project in the process-safe, atomically published LCM catalog. */
export async function registerLcmProject(
	project: LcmProject,
	agentDir?: string,
	now = Date.now(),
	journalDir?: string,
): Promise<LcmProjectCatalogEntry> {
	if (!Number.isFinite(now) || now < 0) throw new Error("LCM project lastSeen must be a non-negative timestamp");
	if (journalDir !== undefined && !path.isAbsolute(journalDir)) {
		throw new Error("LCM project journal directory must be absolute");
	}
	const catalogPath = getLcmProjectCatalogPath(agentDir);
	await fs.mkdir(path.dirname(catalogPath), { recursive: true });

	return withFileLock(catalogPath, async () => {
		const catalog = await readLcmProjectCatalog(agentDir);
		const previous = catalog.projects.find(candidate => candidate.projectId === project.projectId);
		const journalDirs = [...(previous?.journalDirs ?? [])];
		if (
			journalDir &&
			!journalDirs.some(
				candidate => normalizePathForComparison(candidate) === normalizePathForComparison(journalDir),
			)
		) {
			journalDirs.push(journalDir);
		}
		journalDirs.sort((left, right) => left.localeCompare(right));
		const entry: LcmProjectCatalogEntry = { ...project, lastSeen: now, journalDirs };
		if (!isCatalogEntry(entry)) throw new Error("Invalid LCM project registration");
		const projects = catalog.projects.filter(candidate => candidate.projectId !== entry.projectId);
		projects.push(entry);
		projects.sort((left, right) => left.projectId.localeCompare(right.projectId));
		await writeLcmProjectCatalog(catalogPath, { version: CATALOG_VERSION, projects });
		return { ...entry, journalDirs: [...entry.journalDirs] };
	});
}

export async function listLcmProjects(agentDir?: string): Promise<LcmProjectCatalogEntry[]> {
	const catalog = await readLcmProjectCatalog(agentDir);
	return catalog.projects
		.map(project => ({ ...project, journalDirs: [...project.journalDirs] }))
		.sort((left, right) => right.lastSeen - left.lastSeen);
}

/** Resolve only an exact project id or an explicit absolute path already present in the catalog. */
export async function resolveLcmProjectSelector(selector: string, agentDir?: string): Promise<LcmProjectCatalogEntry> {
	const value = selector.trim();
	if (!value) throw new Error("An explicit LCM project selector is required");
	const projects = (await readLcmProjectCatalog(agentDir)).projects;
	const idMatches = projects.filter(project => project.projectId === value);
	if (idMatches.length > 1) throw new Error(`Ambiguous LCM project id: ${value}`);
	if (idMatches.length === 1) return { ...idMatches[0], journalDirs: [...idMatches[0]!.journalDirs] };
	if (!path.isAbsolute(value)) {
		throw new Error(`Unknown LCM project selector: ${value}. Use an exact project id or absolute project path.`);
	}
	const canonicalPath = normalizePathForComparison(value);
	const pathMatches = projects.filter(project => normalizePathForComparison(project.rootPath) === canonicalPath);
	if (pathMatches.length > 1) throw new Error(`Ambiguous LCM project path: ${value}`);
	if (pathMatches.length === 0) throw new Error(`Unknown LCM project path: ${value}`);
	return { ...pathMatches[0], journalDirs: [...pathMatches[0]!.journalDirs] };
}
