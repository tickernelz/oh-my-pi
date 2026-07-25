import * as path from "node:path";
import { getSessionsDir, isEnoent, normalizePathForComparison } from "@oh-my-pi/pi-utils";
import type { FileEntry, SessionEntry, SessionHeader } from "../session/session-entries";
import { loadEntriesFromFile } from "../session/session-loader";
import { SessionManager } from "../session/session-manager";
import { migrateToCurrentVersion } from "../session/session-migrations";
import { getSessionDirCandidatesReadOnly } from "../session/session-paths";
import { listLcmProjects } from "./project-catalog";
import { type LcmProject, resolveLcmProject } from "./project-identity";

export interface LcmJournalIssueCounts {
	missing: number;
	unreadable: number;
	empty: number;
	outOfScope: number;
}

export interface LcmProjectJournalDiscovery {
	/** One immutable in-memory journal view per persisted leaf branch. */
	journals: readonly SessionManager[];
	filesDiscovered: number;
	filesLoaded: number;
	issues: LcmJournalIssueCounts;
}

export interface DiscoverLcmProjectJournalsOptions {
	project: LcmProject;
	sessionManager: SessionManager;
	agentDir?: string;
}

function journalManager(header: SessionHeader, entries: SessionEntry[], sessionDir: string): SessionManager {
	const manager = SessionManager.inMemory(header.cwd);
	const empty = manager.captureState();
	manager.restoreState({
		...empty,
		cwd: header.cwd,
		sessionDir,
		sessionId: header.id,
		sessionName: header.title,
		titleSource: header.titleSource,
		sessionFile: undefined,
		titleUpdatedAt: header.timestamp,
		hasTitleSlot: false,
		onDisk: false,
		needsRewrite: false,
		draftOnlySessionCleanupArmed: false,
		header,
		entries,
	});
	return manager;
}

function leafIds(entries: readonly SessionEntry[]): string[] {
	const parents = new Set<string>();
	for (const entry of entries) {
		if (entry.parentId) parents.add(entry.parentId);
	}
	return entries.filter(entry => !parents.has(entry.id)).map(entry => entry.id);
}

/**
 * Discover authoritative journals only in the selected project's deterministic
 * session directories. The scan is read-only and never walks other projects.
 */
export async function discoverLcmProjectJournals(
	options: DiscoverLcmProjectJournalsOptions,
): Promise<LcmProjectJournalDiscovery> {
	const { project, sessionManager } = options;
	const storage = sessionManager.getSessionStorage();
	const registered = (await listLcmProjects(options.agentDir)).find(
		candidate => candidate.projectId === project.projectId,
	);
	const [canonicalDirectory, ...legacyDirectories] = getSessionDirCandidatesReadOnly(
		project.rootPath,
		getSessionsDir(options.agentDir),
	);
	const authoritativeDirectories = [
		...new Set(
			[canonicalDirectory!, sessionManager.getSessionDir(), ...(registered?.journalDirs ?? [])].filter(Boolean),
		),
	];
	const issues: LcmJournalIssueCounts = { missing: 0, unreadable: 0, empty: 0, outOfScope: 0 };
	const discoveredFiles: string[] = [];
	for (const directory of authoritativeDirectories) {
		try {
			discoveredFiles.push(...storage.listFilesSync(directory, "*.jsonl", { strict: true }));
		} catch (error) {
			if (isEnoent(error)) issues.missing++;
			else issues.unreadable++;
		}
	}
	const authoritativeSet = new Set(authoritativeDirectories);
	for (const directory of legacyDirectories) {
		if (!authoritativeSet.has(directory)) {
			discoveredFiles.push(...storage.listFilesSync(directory, "*.jsonl"));
		}
	}
	const files = [...new Set(discoveredFiles)].sort();
	const journals: SessionManager[] = [];
	const seenBranches = new Set<string>();
	const selectedRoot = normalizePathForComparison(project.rootPath);
	let filesLoaded = 0;

	for (const file of files) {
		try {
			storage.statSync(file);
		} catch (error) {
			if (isEnoent(error)) issues.missing++;
			else issues.unreadable++;
			continue;
		}

		let loaded: FileEntry[];
		try {
			loaded = await loadEntriesFromFile(file, storage);
		} catch {
			issues.unreadable++;
			continue;
		}
		if (loaded.length === 0) {
			if (storage.existsSync(file)) issues.unreadable++;
			else issues.missing++;
			continue;
		}

		const header = loaded[0] as SessionHeader;
		let journalProject: LcmProject;
		try {
			journalProject = await resolveLcmProject(header.cwd, options.agentDir);
		} catch {
			issues.unreadable++;
			continue;
		}
		if (
			journalProject.projectId !== project.projectId ||
			normalizePathForComparison(journalProject.rootPath) !== selectedRoot
		) {
			issues.outOfScope++;
			continue;
		}

		try {
			migrateToCurrentVersion(loaded);
		} catch {
			issues.unreadable++;
			continue;
		}
		const entries = loaded.slice(1) as SessionEntry[];
		const leaves = leafIds(entries);
		if (leaves.length === 0) {
			issues.empty++;
			continue;
		}
		for (const leafId of leaves) {
			const branchKey = `${header.id}\0${leafId}`;
			if (seenBranches.has(branchKey)) continue;
			seenBranches.add(branchKey);
			const manager = journalManager(header, entries, path.dirname(file));
			manager.branch(leafId);
			journals.push(manager);
		}
		filesLoaded++;
	}

	return { journals, filesDiscovered: files.length, filesLoaded, issues };
}
