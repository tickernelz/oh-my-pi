import { Database, type SQLQueryBindings } from "bun:sqlite";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { initializeLcmSchema, LCM_SCHEMA_VERSION, summaryHandleForInput, UnsupportedLcmSchemaError } from "./schema";
import type {
	Citation,
	ClaimSummaryJobsOptions,
	CompleteSummaryJobResult,
	ContextProjection,
	ContextScope,
	DoctorCheck,
	DoctorReport,
	FileDescription,
	FileReference,
	JobStatusCounts,
	LcmContext,
	LcmContextOptions,
	LcmFileMetadata,
	LcmStatus,
	ProjectedHistoricalItem,
	ProjectionRequest,
	ProjectSearchRequest,
	PurgeResult,
	RebuildResult,
	ReconcileOptions,
	ReconcileResult,
	SearchHit,
	SearchRequest,
	SourceDescription,
	SourceEntry,
	SourceSnapshot,
	SummaryAttemptProvenance,
	SummaryCompletion,
	SummaryDescription,
	SummaryExpansion,
	SummaryExpansionRequest,
	SummaryJob,
	SummaryJobInput,
	SummaryReference,
	SummaryStage,
	SummaryStrategy,
} from "./types";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const DEFAULT_TOMBSTONE_RETENTION_MS = 30 * 24 * 60 * 60_000;
const DEFAULT_LEAF_MAX_SOURCES = 24;
const DEFAULT_LEAF_MAX_TOKENS = 4_000;
const DEFAULT_CONDENSE_FAN_IN = 4;
const MAX_STORED_DIAGNOSTIC_LENGTH = 2_000;
const SQLITE_OPEN_RETRY_DELAYS_MS = [100, 200, 400] as const;

interface InternalOptions {
	busyTimeoutMs: number;
	tombstoneRetentionMs: number;
	leafMaxSources: number;
	leafMaxTokens: number;
	condenseFanIn: number;
	now: () => number;
}

interface NormalizedEntry extends SourceEntry {
	sourceKey: string;
	artifactRefsJson: string;
	tokenCount: number;
}

interface NormalizedSnapshot {
	scope: ContextScope;
	entries: readonly NormalizedEntry[];
}

interface BranchRow {
	id: number;
	revision: number;
}

interface BranchScheduleRow extends BranchRow {
	session_id: string;
	branch_id: string;
	summary_token_budget: number | null;
	fresh_tail_max_sources: number | null;
	fresh_tail_max_tokens: number | null;
}

interface CurrentSourceRow {
	id: number;
	entry_id: string;
	parent_entry_id: string | null;
	position: number;
	source_key: string;
	atomic_group_id: string | null;
}

interface ActiveSourceRow extends CurrentSourceRow {
	project_id: string;
	session_id: string;
	branch_id: string;
	content_hash: string;
	timestamp_ms: number;
	kind: string;
	redacted_text: string;
	artifact_refs: string;
	token_count: number;
}

interface ProjectSourceRow extends ActiveSourceRow {
	branch_row_id: number;
}

interface SummaryRow {
	summary_id: string;
	stable_handle: string;
	input_hash: string;
	level: number;
	redacted_text: string;
	token_count: number;
	created_at: number;
}

interface FileRow {
	file_id: string;
	project_id: string;
	content_hash: string;
	path: string;
	file_type: string;
	byte_size: number;
	token_count: number;
	exploration_summary: string;
}

interface SummaryCandidate extends SummaryRow {
	lineage: string[];
}

interface SummaryNode {
	summaryId: string;
	level: number;
	lineage: readonly string[];
}

interface JobInputSpec {
	kind: "source" | "summary";
	id: string;
}

interface ScheduleStats {
	queued: number;
	reused: number;
}

interface JobRow {
	job_id: string;
	project_id: string;
	input_hash: string;
	level: number;
	status: string;
	lease_token: string | null;
	lease_expires_at: number | null;
	lease_input_tokens: number | null;
	lease_output_budget: number | null;
	stage: SummaryStage;
	transport_retry_count: number;
}

interface JobInputRow {
	input_kind: "source" | "summary";
	ref_id: string;
}

interface JobStatusRow {
	status: keyof JobStatusCounts;
	count: number;
}

interface CountRow {
	count: number;
}

interface SearchDocumentRow {
	document_kind: "source" | "summary";
	ref_id: string;
	redacted_text: string;
	rank: number;
}

interface StateRow {
	quarantined_at: number | null;
	quarantine_reason: string | null;
	last_recovery_path: string | null;
}

function assertInteger(value: number, name: string, minimum: number): number {
	if (!Number.isSafeInteger(value) || value < minimum) {
		throw new RangeError(`${name} must be a safe integer >= ${minimum}`);
	}
	return value;
}

function assertIdentifier(value: string, name: string): string {
	if (typeof value !== "string" || value.length === 0 || value.trim() !== value) {
		throw new TypeError(`${name} must be a non-empty, trimmed string`);
	}
	return value;
}

function estimateTokens(text: string): number {
	if (text.length === 0) return 0;
	return Math.ceil(Buffer.byteLength(text, "utf8") / 4);
}

function contentAddress(parts: readonly string[]): string {
	const hasher = new Bun.CryptoHasher("sha256");
	for (const part of parts) {
		hasher.update(`${Buffer.byteLength(part, "utf8")}:`);
		hasher.update(part);
	}
	return hasher.digest("hex");
}

function boundedDiagnostic(value: string): string {
	return value.slice(0, MAX_STORED_DIAGNOSTIC_LENGTH);
}

function normalizeOptions(options: LcmContextOptions): InternalOptions {
	assertIdentifier(options.dbPath, "dbPath");
	return {
		busyTimeoutMs: assertInteger(options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS, "busyTimeoutMs", 0),
		tombstoneRetentionMs: assertInteger(
			options.tombstoneRetentionMs ?? DEFAULT_TOMBSTONE_RETENTION_MS,
			"tombstoneRetentionMs",
			0,
		),
		leafMaxSources: assertInteger(
			options.leafChunk?.maxSources ?? DEFAULT_LEAF_MAX_SOURCES,
			"leafChunk.maxSources",
			1,
		),
		leafMaxTokens: assertInteger(options.leafChunk?.maxTokens ?? DEFAULT_LEAF_MAX_TOKENS, "leafChunk.maxTokens", 1),
		condenseFanIn: assertInteger(options.condenseFanIn ?? DEFAULT_CONDENSE_FAN_IN, "condenseFanIn", 2),
		now: options.now ?? Date.now,
	};
}

function normalizeScope(scope: ContextScope): ContextScope {
	return {
		projectId: assertIdentifier(scope.projectId, "projectId"),
		sessionId: assertIdentifier(scope.sessionId, "sessionId"),
		branchId: assertIdentifier(scope.branchId, "branchId"),
	};
}

function normalizeFiles(files: readonly LcmFileMetadata[] | undefined, label: string): LcmFileMetadata[] {
	if (files === undefined) return [];
	if (!Array.isArray(files)) throw new TypeError(`${label} must be an array`);
	const seen = new Set<string>();
	return files.map((file, index) => {
		const prefix = `${label}[${index}]`;
		const fileId = assertIdentifier(file.fileId, `${prefix}.fileId`);
		if (seen.has(fileId)) throw new TypeError(`duplicate file id: ${fileId}`);
		seen.add(fileId);
		const contentHash = assertIdentifier(file.contentHash, `${prefix}.contentHash`);
		const filePath = assertIdentifier(file.path, `${prefix}.path`);
		const fileType = assertIdentifier(file.fileType, `${prefix}.fileType`);
		const byteSize = assertInteger(file.byteSize, `${prefix}.byteSize`, 0);
		const tokenCount = assertInteger(file.tokenCount, `${prefix}.tokenCount`, 0);
		if (typeof file.explorationSummary !== "string") {
			throw new TypeError(`${prefix}.explorationSummary must be a string`);
		}
		return {
			fileId,
			contentHash,
			path: filePath,
			fileType,
			byteSize,
			tokenCount,
			explorationSummary: file.explorationSummary.slice(0, 4_000),
		};
	});
}

function normalizeSnapshot(snapshot: SourceSnapshot): NormalizedSnapshot {
	const scope = normalizeScope(snapshot.scope);
	if (!Array.isArray(snapshot.entries)) throw new TypeError("entries must be an array");
	const seen = new Map<string, number>();
	const normalized: NormalizedEntry[] = [];

	for (let position = 0; position < snapshot.entries.length; position++) {
		const entry = snapshot.entries[position];
		if (
			entry.projectId !== scope.projectId ||
			entry.sessionId !== scope.sessionId ||
			entry.branchId !== scope.branchId
		) {
			throw new TypeError(`entries[${position}] does not match snapshot scope`);
		}
		const entryId = assertIdentifier(entry.entryId, `entries[${position}].entryId`);
		if (seen.has(entryId)) throw new TypeError(`duplicate source entry id: ${entryId}`);
		if (entry.parentId !== null) {
			assertIdentifier(entry.parentId, `entries[${position}].parentId`);
			if (entry.parentId === entryId) throw new TypeError(`source ${entryId} cannot parent itself`);
		}
		assertInteger(entry.timestamp, `entries[${position}].timestamp`, 0);
		assertIdentifier(entry.kind, `entries[${position}].kind`);
		if (entry.atomicGroupId !== undefined) {
			assertIdentifier(entry.atomicGroupId, `entries[${position}].atomicGroupId`);
		}
		if (typeof entry.redactedText !== "string") {
			throw new TypeError(`entries[${position}].redactedText must be a string`);
		}
		assertIdentifier(entry.contentHash, `entries[${position}].contentHash`);
		if (!Array.isArray(entry.artifactRefs)) throw new TypeError(`entries[${position}].artifactRefs must be an array`);
		const artifactRefs = [
			...new Set(
				entry.artifactRefs.map((ref: string, index: number) => assertIdentifier(ref, `artifactRefs[${index}]`)),
			),
		];
		artifactRefs.sort();
		const artifactRefsJson = JSON.stringify(artifactRefs);
		const files = normalizeFiles(entry.files, `entries[${position}].files`);
		const sourceIdentity = [
			"lcm-source-v1",
			scope.projectId,
			entry.contentHash,
			String(entry.timestamp),
			entry.kind,
			entry.redactedText,
			artifactRefsJson,
		];
		if (files.length > 0) sourceIdentity.push(JSON.stringify(files));
		const sourceKey = contentAddress(sourceIdentity);
		normalized.push({
			...entry,
			artifactRefs,
			files,
			sourceKey,
			artifactRefsJson,
			tokenCount: estimateTokens(entry.redactedText),
		});
		seen.set(entryId, position);
	}

	for (let position = 0; position < normalized.length; position++) {
		const parentId = normalized[position]?.parentId;
		if (parentId === null || parentId === undefined) continue;
		const parentPosition = seen.get(parentId);
		if (parentPosition !== undefined && parentPosition >= position) {
			throw new TypeError(
				`source ${normalized[position]?.entryId} references a parent that is not earlier in the branch`,
			);
		}
	}
	return { scope, entries: normalized };
}

function sameNullable(left: string | null, right: string | null): boolean {
	return left === right;
}

function atomicUnits<T extends { atomic_group_id: string | null }>(rows: readonly T[]): T[][] {
	const lastByGroup = new Map<string, number>();
	for (let position = 0; position < rows.length; position++) {
		const groupId = rows[position]?.atomic_group_id;
		if (groupId) lastByGroup.set(groupId, position);
	}
	const units: T[][] = [];
	let start = 0;
	while (start < rows.length) {
		let end = start;
		for (let position = start; position <= end; position++) {
			const groupId = rows[position]?.atomic_group_id;
			if (groupId) end = Math.max(end, lastByGroup.get(groupId) ?? end);
		}
		units.push(rows.slice(start, end + 1));
		start = end + 1;
	}
	return units;
}

interface FreshTailSelection {
	start: number;
	tokens: number;
	count: number;
}

function selectFreshTail<T extends { atomic_group_id: string | null; token_count: number }>(
	rows: readonly T[],
	tokenBudget: number,
	maxSources: number,
	maxTokens: number,
): FreshTailSelection {
	const units = atomicUnits(rows);
	let start = rows.length;
	let tokens = 0;
	let count = 0;
	const tokenLimit = Math.min(maxTokens, tokenBudget);
	for (let unitIndex = units.length - 1; unitIndex >= 0; unitIndex--) {
		const unit = units[unitIndex];
		if (!unit || count >= maxSources) break;
		const unitTokens = unit.reduce((total, row) => total + row.token_count, 0);
		const expandsSourceLimit = count + unit.length > maxSources;
		const isAtomicClosure = unit.some(row => row.atomic_group_id !== null);
		if ((expandsSourceLimit && !isAtomicClosure) || tokens + unitTokens > tokenLimit) break;
		start -= unit.length;
		tokens += unitTokens;
		count += unit.length;
	}
	return { start, tokens, count };
}

function strategyForStage(stage: SummaryStage): SummaryStrategy {
	switch (stage) {
		case "normal":
			return "preserve_details";
		case "aggressive":
			return "bullet_points";
		case "deterministic":
			return "deterministic_truncate";
	}
}

function outputBudgetForStage(stage: SummaryStage, maxOutputTokens: number, inputTokenCount: number): number {
	const normalBudget = Math.min(maxOutputTokens, inputTokenCount - 1);
	if (stage === "normal") return normalBudget;
	if (stage === "aggressive") return Math.min(inputTokenCount - 1, Math.max(1, Math.floor(normalBudget / 2)));
	return Math.min(512, inputTokenCount - 1);
}

function nextStage(stage: SummaryStage): SummaryStage | null {
	if (stage === "normal") return "aggressive";
	if (stage === "aggressive") return "deterministic";
	return null;
}

function findAlignedSequences<T extends { source_key: string; atomic_group_id: string | null }>(
	rows: readonly T[],
	lineage: readonly string[],
): number[] {
	if (lineage.length === 0 || lineage.length > rows.length) return [];
	const boundaries = new Set<number>([0]);
	let boundary = 0;
	for (const unit of atomicUnits(rows)) {
		boundary += unit.length;
		boundaries.add(boundary);
	}
	const starts: number[] = [];
	for (const start of boundaries) {
		const end = start + lineage.length;
		if (!boundaries.has(end)) continue;
		let matches = true;
		for (let offset = 0; offset < lineage.length; offset++) {
			if (rows[start + offset]?.source_key !== lineage[offset]) {
				matches = false;
				break;
			}
		}
		if (matches) starts.push(start);
	}
	return starts;
}

function findAlignedSequence<T extends { source_key: string; atomic_group_id: string | null }>(
	rows: readonly T[],
	lineage: readonly string[],
): number {
	return findAlignedSequences(rows, lineage)[0] ?? -1;
}

function containsSequence(sequence: readonly string[], candidate: readonly string[]): boolean {
	if (candidate.length === 0 || candidate.length > sequence.length) return false;
	for (let start = 0; start <= sequence.length - candidate.length; start++) {
		let matches = true;
		for (let offset = 0; offset < candidate.length; offset++) {
			if (sequence[start + offset] !== candidate[offset]) {
				matches = false;
				break;
			}
		}
		if (matches) return true;
	}
	return false;
}

function parseArtifactRefs(serialized: string): string[] {
	const parsed: unknown = JSON.parse(serialized);
	if (!Array.isArray(parsed) || !parsed.every(value => typeof value === "string")) {
		throw new Error("LCM artifact reference record is malformed");
	}
	return parsed;
}

function visitErrorCauses(error: unknown, visitor: (error: { code?: unknown; message?: unknown }) => void): void {
	const seen = new Set<object>();
	let current = error;
	while (current !== null && typeof current === "object") {
		if (seen.has(current)) return;
		seen.add(current);
		const candidate = current as { cause?: unknown; code?: unknown; message?: unknown };
		visitor(candidate);
		current = candidate.cause;
	}
}

export function isLcmSqliteContentionError(error: unknown): boolean {
	let sawExplicitCode = false;
	let sawContentionCode = false;
	let sawCanonicalLockMessage = false;
	visitErrorCauses(error, candidate => {
		if (candidate.code !== undefined) {
			sawExplicitCode = true;
			if (
				typeof candidate.code === "string" &&
				(candidate.code.startsWith("SQLITE_BUSY") || candidate.code.startsWith("SQLITE_LOCKED"))
			) {
				sawContentionCode = true;
			}
			return;
		}
		if (candidate.message === "database is locked" || candidate.message === "database table is locked") {
			sawCanonicalLockMessage = true;
		}
	});
	return sawContentionCode || (!sawExplicitCode && sawCanonicalLockMessage);
}

export function isLcmSqliteCorruptionError(error: unknown): boolean {
	let matched = false;
	visitErrorCauses(error, candidate => {
		const code = candidate.code;
		if (
			typeof code === "string" &&
			(code.startsWith("SQLITE_CORRUPT") || code === "SQLITE_IOERR_CORRUPTFS" || code === "SQLITE_NOTADB")
		) {
			matched = true;
		}
	});
	return matched;
}

function isMissingFile(error: unknown): boolean {
	return error instanceof Error && "code" in error && error.code === "ENOENT";
}

async function prepareDatabaseParent(dbPath: string): Promise<void> {
	if (dbPath === ":memory:" || dbPath.startsWith("file:")) return;
	await fs.mkdir(path.dirname(dbPath), { recursive: true });
}

async function quarantineDatabaseFiles(dbPath: string, now: number): Promise<string> {
	const quarantinePath = `${dbPath}.quarantine-${now}-${crypto.randomUUID()}`;
	let movedMain = false;
	for (const suffix of ["", "-wal", "-shm"] as const) {
		try {
			await fs.rename(`${dbPath}${suffix}`, `${quarantinePath}${suffix}`);
			if (suffix === "") movedMain = true;
		} catch (error) {
			if (!isMissingFile(error)) throw error;
		}
	}
	if (!movedMain) throw new Error(`Cannot quarantine missing LCM database: ${dbPath}`);
	return quarantinePath;
}

function databaseFilePath(dbPath: string): string | undefined {
	if (dbPath === ":memory:") return undefined;
	if (!dbPath.startsWith("file:")) return dbPath;
	const queryIndex = dbPath.indexOf("?");
	const encodedPath = dbPath.slice("file:".length, queryIndex === -1 ? undefined : queryIndex);
	const mode = queryIndex === -1 ? null : new URLSearchParams(dbPath.slice(queryIndex + 1)).get("mode");
	if (!encodedPath || encodedPath === ":memory:" || mode === "memory") return undefined;
	let decodedPath: string;
	try {
		decodedPath = decodeURIComponent(encodedPath);
	} catch {
		return undefined;
	}
	if (decodedPath === ":memory:") return undefined;
	if (!decodedPath.startsWith("//")) return decodedPath;
	const pathIndex = decodedPath.indexOf("/", 2);
	if (pathIndex === -1) return undefined;
	const authority = decodedPath.slice(2, pathIndex);
	if (authority && authority.toLowerCase() !== "localhost") return undefined;
	return decodedPath.slice(pathIndex) || undefined;
}

function openDatabaseRecoveryGuard(lockPath: string, busyTimeoutMs: number): Database {
	let guard: Database | undefined;
	try {
		guard = new Database(lockPath, { create: true, readwrite: true, strict: true });
		guard.run(`PRAGMA busy_timeout = ${busyTimeoutMs}`);
		// BEGIN EXCLUSIVE excludes readers only in rollback-journal mode.
		const journalMode = guard.query<{ journal_mode: string }, []>("PRAGMA journal_mode = DELETE").get();
		if (journalMode?.journal_mode !== "delete") throw new Error("LCM recovery guard requires DELETE journal mode");
		guard.run("CREATE TABLE IF NOT EXISTS lcm_owner_guard (id INTEGER PRIMARY KEY)");
		return guard;
	} catch (error) {
		guard?.close();
		throw error;
	}
}

function closeDatabaseRecoveryGuard(guard: Database | undefined): void {
	if (!guard) return;
	try {
		guard.run("ROLLBACK");
	} catch {}
	guard.close();
}

function acquireDatabaseOwnerGuard(dbPath: string, busyTimeoutMs: number): Database | undefined {
	const databasePath = databaseFilePath(dbPath);
	if (!databasePath) return undefined;
	const guard = openDatabaseRecoveryGuard(`${databasePath}.recovery-lock`, busyTimeoutMs);
	try {
		guard.run("BEGIN");
		// BEGIN is deferred; this read acquires the shared lock retained by the live context.
		guard.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM lcm_owner_guard").get();
		return guard;
	} catch (error) {
		closeDatabaseRecoveryGuard(guard);
		throw error;
	}
}

async function withDatabaseRecoveryLock<T>(
	lockPath: string,
	busyTimeoutMs: number,
	operation: () => Promise<T>,
): Promise<T> {
	let lock: Database | undefined;
	try {
		lock = openDatabaseRecoveryGuard(lockPath, Math.max(busyTimeoutMs, 1_000));
		lock.run("BEGIN EXCLUSIVE");
		return await operation();
	} finally {
		closeDatabaseRecoveryGuard(lock);
	}
}

class SqliteLcmContext implements LcmContext {
	#db: Database;
	#dbPath: string;
	#options: InternalOptions;
	#ownerGuard: Database | undefined;
	#closed = false;
	#quarantined = false;

	constructor(db: Database, dbPath: string, options: InternalOptions, ownerGuard: Database | undefined) {
		this.#db = db;
		this.#dbPath = dbPath;
		this.#options = options;
		this.#ownerGuard = ownerGuard;
		const state = this.#readState();
		this.#quarantined = state.quarantined_at !== null;
	}

	#assertOpen(): void {
		if (this.#closed) throw new Error("LCM context is closed");
	}

	#assertAvailable(): void {
		this.#assertOpen();
		if (this.#quarantined) throw new Error("LCM context is quarantined and must be rebuilt");
	}

	#readState(): StateRow {
		const row = this.#db
			.query<StateRow, []>(
				"SELECT quarantined_at, quarantine_reason, last_recovery_path FROM store_state WHERE id = 1",
			)
			.get();
		if (!row) throw new Error("LCM store state is missing");
		return row;
	}

	recordRecovery(quarantinePath: string, reason: string): void {
		const now = this.#options.now();
		const transaction = this.#db.transaction(() => {
			this.#db.run("UPDATE store_state SET last_recovery_path = ? WHERE id = 1", [quarantinePath]);
			this.#db.run("INSERT INTO recovery_events (quarantine_path, reason, created_at) VALUES (?, ?, ?)", [
				quarantinePath,
				boundedDiagnostic(reason),
				now,
			]);
		});
		transaction.immediate();
	}

	reconcile(snapshot: SourceSnapshot, options?: ReconcileOptions): ReconcileResult {
		this.#assertAvailable();
		const normalized = normalizeSnapshot(snapshot);
		const transaction = this.#db.transaction(() =>
			this.#reconcileNormalized(normalized, this.#options.now(), options?.summarize),
		);
		return transaction.immediate();
	}

	#reconcileNormalized(
		snapshot: NormalizedSnapshot,
		now: number,
		summarize?: false | Pick<ProjectionRequest, "tokenBudget" | "freshTail">,
	): ReconcileResult {
		const branch = this.#ensureBranch(snapshot.scope, now);
		const current = this.#activeRows(branch.id);
		const unchanged =
			current.length === snapshot.entries.length &&
			current.every((row, position) => {
				const entry = snapshot.entries[position];
				return (
					entry !== undefined &&
					row.position === position &&
					row.entry_id === entry.entryId &&
					row.source_key === entry.sourceKey &&
					sameNullable(row.parent_entry_id, entry.parentId) &&
					sameNullable(row.atomic_group_id, entry.atomicGroupId ?? null)
				);
			});

		let insertedSources = 0;
		let tombstonedSources = 0;
		let revision = branch.revision;
		if (!unchanged) {
			this.#db.run("UPDATE branch_sources SET position = -id WHERE branch_row_id = ? AND active = 1", [branch.id]);
			const currentByEntry = new Map(current.map(row => [row.entry_id, row]));
			const incomingIds = new Set<string>();

			for (let position = 0; position < snapshot.entries.length; position++) {
				const entry = snapshot.entries[position];
				if (!entry) continue;
				incomingIds.add(entry.entryId);
				this.#insertSourceContent(snapshot.scope.projectId, entry, now);
				const previous = currentByEntry.get(entry.entryId);
				if (
					previous &&
					previous.source_key === entry.sourceKey &&
					sameNullable(previous.parent_entry_id, entry.parentId) &&
					sameNullable(previous.atomic_group_id, entry.atomicGroupId ?? null)
				) {
					this.#db.run("UPDATE branch_sources SET position = ? WHERE id = ?", [position, previous.id]);
					continue;
				}
				if (previous) {
					this.#db.run("UPDATE branch_sources SET active = 0, tombstoned_at = ? WHERE id = ? AND active = 1", [
						now,
						previous.id,
					]);
					tombstonedSources++;
				}
				this.#db.run(
					`INSERT INTO branch_sources
						(branch_row_id, entry_id, parent_entry_id, position, source_key, atomic_group_id, active, created_at)
					 VALUES (?, ?, ?, ?, ?, ?, 1, ?)`,
					[branch.id, entry.entryId, entry.parentId, position, entry.sourceKey, entry.atomicGroupId ?? null, now],
				);
				insertedSources++;
			}

			for (const previous of current) {
				if (incomingIds.has(previous.entry_id)) continue;
				this.#db.run("UPDATE branch_sources SET active = 0, tombstoned_at = ? WHERE id = ? AND active = 1", [
					now,
					previous.id,
				]);
				tombstonedSources++;
			}
			revision++;
			this.#db.run("UPDATE branches SET revision = ?, reconciled_at = ? WHERE id = ?", [revision, now, branch.id]);
		}

		if (summarize && typeof summarize === "object") {
			const tokenBudget = assertInteger(summarize.tokenBudget, "summarize.tokenBudget", 0);
			const maxSources = assertInteger(summarize.freshTail.maxSources, "summarize.freshTail.maxSources", 0);
			const maxTokens = assertInteger(summarize.freshTail.maxTokens, "summarize.freshTail.maxTokens", 0);
			this.#db.run(
				`UPDATE branches SET summary_token_budget = ?, fresh_tail_max_sources = ?, fresh_tail_max_tokens = ?
				 WHERE id = ?`,
				[tokenBudget, maxSources, maxTokens, branch.id],
			);
		}
		const schedule =
			summarize === false
				? { queued: 0, reused: 0 }
				: this.#scheduleBranch(branch.id, snapshot.scope, revision, now, summarize);
		return {
			changed: !unchanged,
			revision,
			activeSources: snapshot.entries.length,
			insertedSources,
			tombstonedSources,
			queuedJobs: schedule.queued,
			reusedSummaries: schedule.reused,
		};
	}

	#ensureBranch(scope: ContextScope, now: number): BranchRow {
		const existing = this.#db
			.query<BranchRow, [string, string, string]>(
				"SELECT id, revision FROM branches WHERE project_id = ? AND session_id = ? AND branch_id = ?",
			)
			.get(scope.projectId, scope.sessionId, scope.branchId);
		if (existing) return existing;
		const inserted = this.#db.run(
			"INSERT INTO branches (project_id, session_id, branch_id, reconciled_at) VALUES (?, ?, ?, ?)",
			[scope.projectId, scope.sessionId, scope.branchId, now],
		);
		return { id: Number(inserted.lastInsertRowid), revision: 0 };
	}

	#insertSourceContent(projectId: string, entry: NormalizedEntry, now: number): void {
		this.#db.run(
			`INSERT OR IGNORE INTO source_contents
				(source_key, project_id, content_hash, timestamp_ms, kind, redacted_text, artifact_refs, token_count, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[
				entry.sourceKey,
				projectId,
				entry.contentHash,
				entry.timestamp,
				entry.kind,
				entry.redactedText,
				entry.artifactRefsJson,
				entry.tokenCount,
				now,
			],
		);
		for (let ordinal = 0; ordinal < (entry.files?.length ?? 0); ordinal++) {
			const file = entry.files![ordinal]!;
			const existing = this.#db
				.query<
					{
						project_id: string;
						content_hash: string;
						path: string;
						file_type: string;
						byte_size: number;
						token_count: number;
						exploration_summary: string;
					},
					[string]
				>(
					`SELECT project_id, content_hash, path, file_type, byte_size, token_count, exploration_summary
					 FROM file_records WHERE file_id = ?`,
				)
				.get(file.fileId);
			if (
				existing &&
				(existing.project_id !== projectId ||
					existing.content_hash !== file.contentHash ||
					existing.path !== file.path ||
					existing.file_type !== file.fileType ||
					existing.byte_size !== file.byteSize ||
					existing.token_count !== file.tokenCount ||
					existing.exploration_summary !== file.explorationSummary)
			) {
				throw new TypeError(`file metadata collision: ${file.fileId}`);
			}
			if (!existing) {
				this.#db.run(
					`INSERT INTO file_records
						(file_id, project_id, content_hash, path, file_type, byte_size, token_count, exploration_summary, created_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					[
						file.fileId,
						projectId,
						file.contentHash,
						file.path,
						file.fileType,
						file.byteSize,
						file.tokenCount,
						file.explorationSummary,
						now,
					],
				);
			}
			this.#db.run("INSERT OR IGNORE INTO source_files (source_key, ordinal, file_id) VALUES (?, ?, ?)", [
				entry.sourceKey,
				ordinal,
				file.fileId,
			]);
		}
	}

	#activeRows(branchRowId: number): CurrentSourceRow[] {
		return this.#db
			.query<CurrentSourceRow, [number]>(
				`SELECT id, entry_id, parent_entry_id, position, source_key, atomic_group_id
				 FROM branch_sources WHERE branch_row_id = ? AND active = 1 ORDER BY position`,
			)
			.all(branchRowId);
	}

	#activeSourceRows(scope: ContextScope): ActiveSourceRow[] {
		return this.#db
			.query<ActiveSourceRow, [string, string, string]>(
				`SELECT bs.id, bs.entry_id, bs.parent_entry_id, bs.position, bs.source_key, bs.atomic_group_id,
					b.project_id, b.session_id, b.branch_id, sc.content_hash, sc.timestamp_ms,
					sc.kind, sc.redacted_text, sc.artifact_refs, sc.token_count
				 FROM branches b
				 JOIN branch_sources bs ON bs.branch_row_id = b.id AND bs.active = 1
				 JOIN source_contents sc ON sc.source_key = bs.source_key
				 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ?
				 ORDER BY bs.position`,
			)
			.all(scope.projectId, scope.sessionId, scope.branchId);
	}

	#activeProjectSourceRows(projectId: string): ProjectSourceRow[] {
		return this.#db
			.query<ProjectSourceRow, [string]>(
				`SELECT bs.id, bs.branch_row_id, bs.entry_id, bs.parent_entry_id, bs.position,
					bs.source_key, bs.atomic_group_id, b.project_id, b.session_id, b.branch_id,
					sc.content_hash, sc.timestamp_ms, sc.kind, sc.redacted_text, sc.artifact_refs, sc.token_count
				 FROM branches b
				 JOIN branch_sources bs ON bs.branch_row_id = b.id AND bs.active = 1
				 JOIN source_contents sc ON sc.source_key = bs.source_key
				 WHERE b.project_id = ? ORDER BY bs.branch_row_id, bs.position`,
			)
			.all(projectId);
	}

	#scheduleBranch(
		branchRowId: number,
		scope: ContextScope,
		revision: number,
		now: number,
		summarize?: Pick<ProjectionRequest, "tokenBudget" | "freshTail">,
	): ScheduleStats {
		let rows = this.#db
			.query<Pick<ActiveSourceRow, "source_key" | "token_count" | "atomic_group_id">, [number]>(
				`SELECT bs.source_key, sc.token_count, bs.atomic_group_id
				 FROM branch_sources bs JOIN source_contents sc ON sc.source_key = bs.source_key
				 WHERE bs.branch_row_id = ? AND bs.active = 1 ORDER BY bs.position`,
			)
			.all(branchRowId);
		const stats: ScheduleStats = { queued: 0, reused: 0 };
		if (summarize) {
			const tail = selectFreshTail(
				rows,
				summarize.tokenBudget,
				summarize.freshTail.maxSources,
				summarize.freshTail.maxTokens,
			);
			rows = rows.slice(0, tail.start);
		}
		if (rows.length === 0) return stats;

		const chunks: Array<typeof rows> = [];
		let chunk: typeof rows = [];
		let chunkTokens = 0;
		for (const unit of atomicUnits(rows)) {
			const unitTokens = unit.reduce((total, row) => total + row.token_count, 0);
			if (
				chunk.length > 0 &&
				(chunk.length + unit.length > this.#options.leafMaxSources ||
					chunkTokens + unitTokens > this.#options.leafMaxTokens)
			) {
				chunks.push(chunk);
				chunk = [];
				chunkTokens = 0;
			}
			chunk.push(...unit);
			chunkTokens += unitTokens;
		}
		if (chunk.length > 0) chunks.push(chunk);

		let nodes: SummaryNode[] = [];
		let waiting = false;
		for (const leaf of chunks) {
			const lineage = leaf.map(row => row.source_key);
			const resolved = this.#resolveOrQueueJob({
				projectId: scope.projectId,
				branchRowId,
				revision,
				level: 0,
				inputs: lineage.map(id => ({ kind: "source" as const, id })),
				lineage,
				now,
				stats,
			});
			if (resolved) nodes.push(resolved);
			else waiting = true;
		}
		if (waiting) return stats;

		while (nodes.length > 1) {
			const next: SummaryNode[] = [];
			waiting = false;
			for (let start = 0; start < nodes.length; start += this.#options.condenseFanIn) {
				const group = nodes.slice(start, start + this.#options.condenseFanIn);
				if (group.length === 1) {
					const only = group[0];
					if (only) next.push(only);
					continue;
				}
				const lineage = group.flatMap(node => node.lineage);
				const level = Math.max(...group.map(node => node.level)) + 1;
				const resolved = this.#resolveOrQueueJob({
					projectId: scope.projectId,
					branchRowId,
					revision,
					level,
					inputs: group.map(node => ({ kind: "summary" as const, id: node.summaryId })),
					lineage,
					now,
					stats,
				});
				if (resolved) next.push(resolved);
				else waiting = true;
			}
			if (waiting) return stats;
			nodes = next;
		}
		return stats;
	}

	#resolveOrQueueJob(params: {
		projectId: string;
		branchRowId: number;
		revision: number;
		level: number;
		inputs: readonly JobInputSpec[];
		lineage: readonly string[];
		now: number;
		stats: ScheduleStats;
	}): SummaryNode | null {
		const inputHash = contentAddress([
			"lcm-summary-input-v1",
			params.projectId,
			String(params.level),
			...params.inputs.flatMap(input => [input.kind, input.id]),
		]);
		const summary = this.#db
			.query<Pick<SummaryRow, "summary_id" | "level">, [string, string]>(
				"SELECT summary_id, level FROM summaries WHERE project_id = ? AND input_hash = ?",
			)
			.get(params.projectId, inputHash);
		if (summary) {
			params.stats.reused++;
			return { summaryId: summary.summary_id, level: summary.level, lineage: [...params.lineage] };
		}

		const jobId = `job_${inputHash}`;
		const existing = this.#db
			.query<{ status: string }, [string]>("SELECT status FROM summary_jobs WHERE job_id = ?")
			.get(jobId);
		if (!existing) {
			this.#db.run(
				`INSERT INTO summary_jobs
					(job_id, project_id, input_hash, level, origin_branch_row_id, origin_revision,
					 status, available_at, created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
				[
					jobId,
					params.projectId,
					inputHash,
					params.level,
					params.branchRowId,
					params.revision,
					params.now,
					params.now,
					params.now,
				],
			);
			for (let ordinal = 0; ordinal < params.inputs.length; ordinal++) {
				const input = params.inputs[ordinal];
				if (!input) continue;
				this.#db.run("INSERT INTO job_inputs (job_id, ordinal, input_kind, ref_id) VALUES (?, ?, ?, ?)", [
					jobId,
					ordinal,
					input.kind,
					input.id,
				]);
			}
			for (let ordinal = 0; ordinal < params.lineage.length; ordinal++) {
				this.#db.run("INSERT INTO job_lineage (job_id, ordinal, source_key) VALUES (?, ?, ?)", [
					jobId,
					ordinal,
					params.lineage[ordinal]!,
				]);
			}
			params.stats.queued++;
		} else if (existing.status === "obsolete" || existing.status === "completed") {
			this.#db.run(
				`UPDATE summary_jobs SET status = 'pending', origin_branch_row_id = ?, origin_revision = ?,
					available_at = ?, result_summary_id = NULL, updated_at = ? WHERE job_id = ?`,
				[params.branchRowId, params.revision, params.now, params.now, jobId],
			);
			params.stats.queued++;
		}
		return null;
	}

	project(request: ProjectionRequest): ContextProjection {
		this.#assertAvailable();
		const scope = normalizeScope(request);
		const tokenBudget = assertInteger(request.tokenBudget, "tokenBudget", 0);
		const maxTailSources = assertInteger(request.freshTail.maxSources, "freshTail.maxSources", 0);
		const maxTailTokens = assertInteger(request.freshTail.maxTokens, "freshTail.maxTokens", 0);
		const branch = this.#db
			.query<BranchRow, [string, string, string]>(
				"SELECT id, revision FROM branches WHERE project_id = ? AND session_id = ? AND branch_id = ?",
			)
			.get(scope.projectId, scope.sessionId, scope.branchId);
		if (!branch) {
			return {
				revision: 0,
				ready: true,
				historical: [],
				freshTailSourceIds: [],
				uncoveredSourceIds: [],
				sourceTokens: 0,
				selectedLevelCounts: {},
				coveredSourceCount: 0,
				freshSourceCount: 0,
				estimatedTokens: 0,
				pendingJobs: 0,
			};
		}
		const rows = this.#activeSourceRows(scope);
		const units = atomicUnits(rows);
		const atomicBoundaries = new Set<number>([0]);
		let boundary = 0;
		for (const unit of units) {
			boundary += unit.length;
			atomicBoundaries.add(boundary);
		}
		const tail = selectFreshTail(rows, tokenBudget, maxTailSources, maxTailTokens);
		const tailStart = tail.start;
		const tailTokens = tail.tokens;

		const historical: ProjectedHistoricalItem[] = [];
		const selectedLevelCounts: Record<number, number> = {};
		const candidates = this.#summaryCandidates(scope.projectId);
		const byFirstSource = new Map<string, SummaryCandidate[]>();
		for (const candidate of candidates) {
			const first = candidate.lineage[0];
			if (!first) continue;
			const group = byFirstSource.get(first);
			if (group) group.push(candidate);
			else byFirstSource.set(first, [candidate]);
		}
		for (const group of byFirstSource.values()) {
			group.sort(
				(left, right) =>
					right.lineage.length - left.lineage.length ||
					right.level - left.level ||
					left.token_count - right.token_count ||
					left.summary_id.localeCompare(right.summary_id),
			);
		}

		let cursor = 0;
		let usedTokens = tailTokens;
		while (cursor < tailStart) {
			const row = rows[cursor];
			if (!row) break;
			const group = byFirstSource.get(row.source_key) ?? [];
			let selected: SummaryCandidate | undefined;
			for (const candidate of group) {
				if (!atomicBoundaries.has(cursor + candidate.lineage.length)) continue;
				if (cursor + candidate.lineage.length > tailStart || usedTokens + candidate.token_count > tokenBudget)
					continue;
				let matches = true;
				for (let offset = 0; offset < candidate.lineage.length; offset++) {
					if (rows[cursor + offset]?.source_key !== candidate.lineage[offset]) {
						matches = false;
						break;
					}
				}
				if (matches) {
					selected = candidate;
					break;
				}
			}
			if (!selected) break;
			const coveredRows = rows.slice(cursor, cursor + selected.lineage.length);
			historical.push({
				kind: "summary",
				summaryId: selected.summary_id,
				summaryHandle: selected.stable_handle,
				level: selected.level,
				redactedText: selected.redacted_text,
				tokenCount: selected.token_count,
				sourceIds: coveredRows.map(source => source.entry_id),
				citations: coveredRows.map(source => this.#citation(source)),
			});
			selectedLevelCounts[selected.level] = (selectedLevelCounts[selected.level] ?? 0) + 1;
			cursor += selected.lineage.length;
			usedTokens += selected.token_count;
		}

		return {
			revision: branch.revision,
			ready: cursor === tailStart,
			historical,
			freshTailSourceIds: rows.slice(tailStart).map(row => row.entry_id),
			uncoveredSourceIds: rows.slice(cursor, tailStart).map(row => row.entry_id),
			sourceTokens: rows.reduce((total, row) => total + row.token_count, 0),
			selectedLevelCounts,
			coveredSourceCount: cursor,
			freshSourceCount: tail.count,
			estimatedTokens: usedTokens,
			pendingJobs: this.#relevantPendingJobCount(scope.projectId, rows, tailStart),
		};
	}

	#relevantPendingJobCount(
		projectId: string,
		branchRows: readonly Pick<ActiveSourceRow, "source_key" | "atomic_group_id">[],
		tailStart: number,
	): number {
		const rows = this.#db
			.query<{ job_id: string; source_key: string }, [string]>(
				`SELECT j.job_id, jl.source_key FROM summary_jobs j
				 JOIN job_lineage jl ON jl.job_id = j.job_id
				 WHERE j.project_id = ? AND j.status IN ('pending', 'leased', 'failed')
				 ORDER BY j.job_id, jl.ordinal`,
			)
			.all(projectId);
		const lineages = new Map<string, string[]>();
		for (const row of rows) {
			const lineage = lineages.get(row.job_id);
			if (lineage) lineage.push(row.source_key);
			else lineages.set(row.job_id, [row.source_key]);
		}
		let count = 0;
		for (const lineage of lineages.values()) {
			const start = findAlignedSequence(branchRows, lineage);
			if (start >= 0 && start + lineage.length <= tailStart) count++;
		}
		return count;
	}

	#summaryCandidates(projectId: string): SummaryCandidate[] {
		const rows = this.#db
			.query<SummaryRow & { ordinal: number; source_key: string }, [string]>(
				`SELECT s.summary_id, s.stable_handle, s.input_hash, s.level, s.redacted_text, s.token_count,
					s.created_at, sl.ordinal, sl.source_key
				 FROM summaries s JOIN summary_lineage sl ON sl.summary_id = s.summary_id
				 WHERE s.project_id = ? ORDER BY s.summary_id, sl.ordinal`,
			)
			.all(projectId);
		const candidates: SummaryCandidate[] = [];
		let current: SummaryCandidate | undefined;
		for (const row of rows) {
			if (!current || current.summary_id !== row.summary_id) {
				current = {
					summary_id: row.summary_id,
					stable_handle: row.stable_handle,
					input_hash: row.input_hash,
					level: row.level,
					redacted_text: row.redacted_text,
					token_count: row.token_count,
					created_at: row.created_at,
					lineage: [],
				};
				candidates.push(current);
			}
			current.lineage.push(row.source_key);
		}
		return candidates;
	}

	claimSummaryJobs(options: ClaimSummaryJobsOptions): SummaryJob[] {
		this.#assertAvailable();
		const workerId = assertIdentifier(options.workerId, "workerId");
		const leaseMs = assertInteger(options.leaseMs, "leaseMs", 1);
		const limit = assertInteger(options.limit, "limit", 1);
		const maxOutputTokens = assertInteger(options.maxOutputTokens, "maxOutputTokens", 1);
		const preferredScope = options.preferredScope === undefined ? undefined : normalizeScope(options.preferredScope);
		const allowFallback = options.allowFallback ?? true;
		const now = this.#options.now();
		const transaction = this.#db.transaction(() => {
			const preferredBranch = this.#preferredBranchRows(preferredScope);
			const candidates = this.#db
				.query<Pick<JobRow, "job_id" | "project_id" | "stage">, [number, number]>(
					`SELECT job_id, project_id, stage FROM summary_jobs
					 WHERE available_at <= ? AND (
						status IN ('pending', 'failed') OR
						(status = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
					 )
					 ORDER BY level, created_at, job_id`,
				)
				.all(now, now);
			const lineages = this.#groupJobLineages(
				this.#db
					.query<{ job_id: string; source_key: string }, [number, number]>(
						`SELECT jl.job_id, jl.source_key
						 FROM job_lineage jl JOIN summary_jobs j ON j.job_id = jl.job_id
						 WHERE j.available_at <= ? AND (
							j.status IN ('pending', 'failed') OR
							(j.status = 'leased' AND j.lease_expires_at IS NOT NULL AND j.lease_expires_at <= ?)
						 ) ORDER BY jl.job_id, jl.ordinal`,
					)
					.all(now, now),
			);
			const activeProjects = allowFallback ? this.#activeBranchesByProject() : new Map();
			const preferred: Array<{
				candidate: (typeof candidates)[number];
				lineage: string[];
				queueClass: "preferred";
			}> = [];
			const fallback: Array<{
				candidate: (typeof candidates)[number];
				lineage: string[];
				queueClass: "fallback";
			}> = [];
			for (const candidate of candidates) {
				const lineage = lineages.get(candidate.job_id) ?? [];
				if (
					preferredBranch?.projectId === candidate.project_id &&
					findAlignedSequence(preferredBranch.rows, lineage) >= 0
				) {
					preferred.push({ candidate, lineage, queueClass: "preferred" });
				} else if (allowFallback) {
					fallback.push({ candidate, lineage, queueClass: "fallback" });
				}
			}

			const claimed: SummaryJob[] = [];
			for (const { candidate, lineage, queueClass } of [...preferred, ...fallback]) {
				if (claimed.length >= limit) break;
				if (
					queueClass === "fallback" &&
					!this.#lineageActiveSomewhere(candidate.project_id, lineage, activeProjects)
				) {
					this.#db.run(
						`UPDATE summary_jobs SET status = 'obsolete', worker_id = NULL, lease_token = NULL,
							lease_expires_at = NULL, updated_at = ? WHERE job_id = ?`,
						[now, candidate.job_id],
					);
					continue;
				}
				const inputs = this.#loadJobInputs(candidate.job_id);
				if (!inputs) {
					this.#db.run("UPDATE summary_jobs SET status = 'obsolete', updated_at = ? WHERE job_id = ?", [
						now,
						candidate.job_id,
					]);
					continue;
				}
				const inputTokenCount = inputs.reduce((total, input) => total + input.tokenCount, 0);
				const outputTokenBudget = outputBudgetForStage(candidate.stage, maxOutputTokens, inputTokenCount);
				if (outputTokenBudget < 1) {
					this.#db.run(
						"UPDATE summary_jobs SET status = 'obsolete', last_error = 'input too small to compress', updated_at = ? WHERE job_id = ?",
						[now, candidate.job_id],
					);
					continue;
				}
				const leaseToken = crypto.randomUUID();
				const leaseExpiresAt = now + leaseMs;
				const changed = this.#db.run(
					`UPDATE summary_jobs SET status = 'leased', worker_id = ?, lease_token = ?, lease_expires_at = ?,
						lease_input_tokens = ?, lease_output_budget = ?, attempt_count = attempt_count + 1,
						last_strategy = ?, last_input_tokens = ?, updated_at = ?
					 WHERE job_id = ? AND available_at <= ? AND (
						status IN ('pending', 'failed') OR
						(status = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
					 )`,
					[
						workerId,
						leaseToken,
						leaseExpiresAt,
						inputTokenCount,
						outputTokenBudget,
						strategyForStage(candidate.stage),
						inputTokenCount,
						now,
						candidate.job_id,
						now,
						now,
					],
				);
				if (Number(changed.changes) === 0) continue;
				const job = this.#loadClaimedJob(
					candidate.job_id,
					leaseToken,
					leaseExpiresAt,
					inputs,
					lineage.length,
					inputTokenCount,
					outputTokenBudget,
					queueClass,
				);
				if (job) claimed.push(job);
				else {
					this.#db.run(
						`UPDATE summary_jobs SET status = 'obsolete', worker_id = NULL, lease_token = NULL,
							lease_expires_at = NULL, updated_at = ? WHERE job_id = ? AND lease_token = ?`,
						[now, candidate.job_id, leaseToken],
					);
				}
			}
			return claimed;
		});
		return transaction.immediate();
	}

	nextSummaryJobDelayMs(preferredScope?: ContextScope, allowFallback = true): number | null {
		this.#assertAvailable();
		const scope = preferredScope === undefined ? undefined : normalizeScope(preferredScope);
		const preferredBranch = this.#preferredBranchRows(scope);
		const now = this.#options.now();
		const rows = this.#db
			.query<
				{
					job_id: string;
					project_id: string;
					status: string;
					available_at: number;
					lease_expires_at: number | null;
				},
				[]
			>(
				`SELECT job_id, project_id, status, available_at, lease_expires_at FROM summary_jobs
				 WHERE status IN ('pending', 'failed', 'leased')`,
			)
			.all();
		const lineages = this.#groupJobLineages(
			this.#db
				.query<{ job_id: string; source_key: string }, []>(
					`SELECT jl.job_id, jl.source_key
					 FROM job_lineage jl JOIN summary_jobs j ON j.job_id = jl.job_id
					 WHERE j.status IN ('pending', 'failed', 'leased')
					 ORDER BY jl.job_id, jl.ordinal`,
				)
				.all(),
		);
		const activeProjects = allowFallback ? this.#activeBranchesByProject() : new Map();
		let availableAt: number | null = null;
		for (const row of rows) {
			const lineage = lineages.get(row.job_id) ?? [];
			const isPreferred =
				preferredBranch?.projectId === row.project_id && findAlignedSequence(preferredBranch.rows, lineage) >= 0;
			if (!isPreferred) {
				if (!allowFallback || !this.#lineageActiveSomewhere(row.project_id, lineage, activeProjects)) continue;
			}
			const candidateAt = row.status === "leased" ? row.lease_expires_at : row.available_at;
			if (candidateAt !== null && (availableAt === null || candidateAt < availableAt)) availableAt = candidateAt;
		}
		return availableAt === null ? null : Math.max(0, availableAt - now);
	}

	#loadClaimedJob(
		jobId: string,
		leaseToken: string,
		leaseExpiresAt: number,
		inputs: readonly SummaryJobInput[],
		sourceCount: number,
		inputTokenCount: number,
		outputTokenBudget: number,
		queueClass: SummaryJob["queueClass"],
	): SummaryJob | null {
		const job = this.#db
			.query<Pick<JobRow, "job_id" | "level" | "stage" | "transport_retry_count">, [string]>(
				"SELECT job_id, level, stage, transport_retry_count FROM summary_jobs WHERE job_id = ?",
			)
			.get(jobId);
		if (!job) return null;
		return {
			jobId,
			leaseToken,
			leaseExpiresAt,
			queueClass,
			kind: job.level === 0 ? "leaf" : "condensed",
			level: job.level,
			inputs,
			sourceCount,
			inputTokenCount,
			outputTokenBudget,
			stage: job.stage,
			strategy: strategyForStage(job.stage),
			transportRetryCount: job.transport_retry_count,
		};
	}

	#loadJobInputs(jobId: string): SummaryJobInput[] | null {
		const inputRows = this.#db
			.query<JobInputRow, [string]>("SELECT input_kind, ref_id FROM job_inputs WHERE job_id = ? ORDER BY ordinal")
			.all(jobId);
		const inputs: SummaryJobInput[] = [];
		for (const input of inputRows) {
			if (input.input_kind === "source") {
				const source = this.#db
					.query<{ redacted_text: string; token_count: number }, [string]>(
						"SELECT redacted_text, token_count FROM source_contents WHERE source_key = ?",
					)
					.get(input.ref_id);
				if (!source) return null;
				inputs.push({
					kind: "source",
					id: input.ref_id,
					redactedText: source.redacted_text,
					tokenCount: source.token_count,
				});
			} else {
				const summary = this.#db
					.query<{ redacted_text: string; token_count: number }, [string]>(
						"SELECT redacted_text, token_count FROM summaries WHERE summary_id = ?",
					)
					.get(input.ref_id);
				if (!summary) return null;
				inputs.push({
					kind: "summary",
					id: input.ref_id,
					redactedText: summary.redacted_text,
					tokenCount: summary.token_count,
				});
			}
		}
		return inputs;
	}

	#groupJobLineages(rows: readonly { job_id: string; source_key: string }[]): Map<string, string[]> {
		const lineages = new Map<string, string[]>();
		for (const row of rows) {
			const lineage = lineages.get(row.job_id);
			if (lineage) lineage.push(row.source_key);
			else lineages.set(row.job_id, [row.source_key]);
		}
		return lineages;
	}

	#activeBranchesByProject(): Map<string, Map<number, Array<{ source_key: string; atomic_group_id: string | null }>>> {
		const rows = this.#db
			.query<{ project_id: string; branch_row_id: number; source_key: string; atomic_group_id: string | null }, []>(
				`SELECT b.project_id, bs.branch_row_id, bs.source_key, bs.atomic_group_id
				 FROM branch_sources bs JOIN branches b ON b.id = bs.branch_row_id
				 WHERE bs.active = 1 ORDER BY b.project_id, bs.branch_row_id, bs.position`,
			)
			.all();
		const projects = new Map<string, Map<number, Array<{ source_key: string; atomic_group_id: string | null }>>>();
		for (const row of rows) {
			let branches = projects.get(row.project_id);
			if (!branches) {
				branches = new Map();
				projects.set(row.project_id, branches);
			}
			let branch = branches.get(row.branch_row_id);
			if (!branch) {
				branch = [];
				branches.set(row.branch_row_id, branch);
			}
			branch.push({ source_key: row.source_key, atomic_group_id: row.atomic_group_id });
		}
		return projects;
	}

	#jobLineage(jobId: string): string[] {
		return this.#db
			.query<{ source_key: string }, [string]>(
				"SELECT source_key FROM job_lineage WHERE job_id = ? ORDER BY ordinal",
			)
			.all(jobId)
			.map(row => row.source_key);
	}

	#preferredBranchRows(scope?: ContextScope): { projectId: string; rows: CurrentSourceRow[] } | null {
		if (!scope) return null;
		const branch = this.#db
			.query<Pick<BranchRow, "id">, [string, string, string]>(
				"SELECT id FROM branches WHERE project_id = ? AND session_id = ? AND branch_id = ?",
			)
			.get(scope.projectId, scope.sessionId, scope.branchId);
		return branch ? { projectId: scope.projectId, rows: this.#activeRows(branch.id) } : null;
	}

	#lineageActiveSomewhere(
		projectId: string,
		lineage: readonly string[],
		projects: ReadonlyMap<
			string,
			ReadonlyMap<number, readonly { source_key: string; atomic_group_id: string | null }[]>
		> = this.#activeBranchesByProject(),
	): boolean {
		if (lineage.length === 0) return false;
		for (const branch of projects.get(projectId)?.values() ?? []) {
			if (findAlignedSequence(branch, lineage) >= 0) return true;
		}
		return false;
	}

	summaryJobFailures(preferredScope?: ContextScope): readonly {
		jobId: string;
		availableAt: number;
		queueClass: "preferred" | "fallback";
	}[] {
		this.#assertAvailable();
		const scope = preferredScope === undefined ? undefined : normalizeScope(preferredScope);
		const preferredBranch = this.#preferredBranchRows(scope);
		const rows = this.#db
			.query<{ job_id: string; project_id: string; available_at: number }, []>(
				`SELECT job_id, project_id, available_at FROM summary_jobs
				 WHERE status = 'failed' ORDER BY available_at, job_id`,
			)
			.all();
		const lineages = this.#groupJobLineages(
			this.#db
				.query<{ job_id: string; source_key: string }, []>(
					`SELECT jl.job_id, jl.source_key
					 FROM job_lineage jl JOIN summary_jobs j ON j.job_id = jl.job_id
					 WHERE j.status = 'failed' ORDER BY jl.job_id, jl.ordinal`,
				)
				.all(),
		);
		const activeProjects = this.#activeBranchesByProject();
		const failures: Array<{
			jobId: string;
			availableAt: number;
			queueClass: "preferred" | "fallback";
		}> = [];
		for (const row of rows) {
			const lineage = lineages.get(row.job_id) ?? [];
			const isPreferred =
				preferredBranch?.projectId === row.project_id && findAlignedSequence(preferredBranch.rows, lineage) >= 0;
			if (!isPreferred && !this.#lineageActiveSomewhere(row.project_id, lineage, activeProjects)) continue;
			failures.push({
				jobId: row.job_id,
				availableAt: row.available_at,
				queueClass: isPreferred ? "preferred" : "fallback",
			});
		}
		return failures;
	}

	extendSummaryJob(jobId: string, leaseToken: string, leaseMs: number): boolean {
		this.#assertAvailable();
		assertIdentifier(jobId, "jobId");
		assertIdentifier(leaseToken, "leaseToken");
		assertInteger(leaseMs, "leaseMs", 1);
		const now = this.#options.now();
		const result = this.#db.run(
			`UPDATE summary_jobs SET lease_expires_at = ?, updated_at = ?
			 WHERE job_id = ? AND status = 'leased' AND lease_token = ? AND lease_expires_at > ?`,
			[now + leaseMs, now, jobId, leaseToken, now],
		);
		return Number(result.changes) > 0;
	}

	releaseSummaryJob(jobId: string, leaseToken: string): boolean {
		this.#assertAvailable();
		assertIdentifier(jobId, "jobId");
		assertIdentifier(leaseToken, "leaseToken");
		const now = this.#options.now();
		const result = this.#db.run(
			`UPDATE summary_jobs SET status = 'pending', worker_id = NULL, lease_token = NULL,
				lease_expires_at = NULL, lease_input_tokens = NULL, lease_output_budget = NULL,
				available_at = ?, updated_at = ?
			 WHERE job_id = ? AND status = 'leased' AND lease_token = ?`,
			[now, now, jobId, leaseToken],
		);
		return Number(result.changes) > 0;
	}

	completeSummaryJob(jobId: string, leaseToken: string, completion: SummaryCompletion): CompleteSummaryJobResult {
		this.#assertAvailable();
		assertIdentifier(jobId, "jobId");
		assertIdentifier(leaseToken, "leaseToken");
		if (typeof completion.redactedText !== "string" || completion.redactedText.trim().length === 0) {
			throw new TypeError("completion.redactedText must be a non-empty string");
		}
		const locallyMeasuredTokens = estimateTokens(completion.redactedText);
		const reportedTokens =
			completion.tokenCount === undefined
				? locallyMeasuredTokens
				: assertInteger(completion.tokenCount, "completion.tokenCount", 0);
		const tokenCount = Math.max(locallyMeasuredTokens, reportedTokens);
		const provenance = completion.provenance;
		if (provenance) {
			assertIdentifier(provenance.promptHash, "completion.provenance.promptHash");
			if (provenance.modelSelector !== undefined)
				assertIdentifier(provenance.modelSelector, "completion.provenance.modelSelector");
			if (provenance.resolvedModel !== undefined)
				assertIdentifier(provenance.resolvedModel, "completion.provenance.resolvedModel");
		}
		const now = this.#options.now();
		const transaction = this.#db.transaction((): CompleteSummaryJobResult => {
			const job = this.#db
				.query<JobRow, [string]>(
					`SELECT job_id, project_id, input_hash, level, status, lease_token, lease_expires_at,
						lease_input_tokens, lease_output_budget, stage, transport_retry_count
					 FROM summary_jobs WHERE job_id = ?`,
				)
				.get(jobId);
			if (
				job?.status !== "leased" ||
				job.lease_token !== leaseToken ||
				job.lease_expires_at === null ||
				job.lease_input_tokens === null ||
				job.lease_output_budget === null ||
				job.lease_expires_at <= now
			) {
				return { accepted: false, reason: "lease_lost" };
			}
			const strategy = strategyForStage(job.stage);
			if (provenance && provenance.strategy !== strategy) {
				throw new TypeError(`completion provenance strategy ${provenance.strategy} does not match ${strategy}`);
			}
			const promptHash = provenance?.promptHash ?? null;
			const modelSelector = provenance?.modelSelector ? boundedDiagnostic(provenance.modelSelector) : null;
			const resolvedModel = provenance?.resolvedModel ? boundedDiagnostic(provenance.resolvedModel) : null;
			const lineage = this.#jobLineage(jobId);
			if (!this.#lineageActiveSomewhere(job.project_id, lineage)) {
				this.#db.run(
					`UPDATE summary_jobs SET status = 'obsolete', worker_id = NULL, lease_token = NULL,
						lease_expires_at = NULL, updated_at = ? WHERE job_id = ? AND lease_token = ?`,
					[now, jobId, leaseToken],
				);
				return { accepted: false, reason: "stale" };
			}
			if (tokenCount >= job.lease_input_tokens || tokenCount > job.lease_output_budget) {
				const advanced = nextStage(job.stage);
				if (advanced) {
					this.#db.run(
						`UPDATE summary_jobs SET status = 'pending', stage = ?, worker_id = NULL, lease_token = NULL,
							lease_expires_at = NULL, available_at = ?, last_error = 'summary did not compress input',
							non_compression_count = non_compression_count + 1, last_strategy = ?, last_prompt_hash = ?,
							last_model_selector = ?, last_resolved_model = ?, last_input_tokens = ?, last_output_tokens = ?,
							updated_at = ? WHERE job_id = ? AND lease_token = ?`,
						[
							advanced,
							now,
							strategy,
							promptHash,
							modelSelector,
							resolvedModel,
							job.lease_input_tokens,
							tokenCount,
							now,
							jobId,
							leaseToken,
						],
					);
					return { accepted: false, reason: "escalated", stage: advanced };
				}
				this.#db.run(
					`UPDATE summary_jobs SET status = 'obsolete', worker_id = NULL, lease_token = NULL,
						lease_expires_at = NULL, last_error = 'deterministic summary did not compress input',
						non_compression_count = non_compression_count + 1, last_strategy = ?, last_prompt_hash = ?,
						last_model_selector = ?, last_resolved_model = ?, last_input_tokens = ?, last_output_tokens = ?,
						updated_at = ? WHERE job_id = ? AND lease_token = ?`,
					[
						strategy,
						promptHash,
						modelSelector,
						resolvedModel,
						job.lease_input_tokens,
						tokenCount,
						now,
						jobId,
						leaseToken,
					],
				);
				return { accepted: false, reason: "deterministic_failed" };
			}

			let summary = this.#db
				.query<{ summary_id: string }, [string, string]>(
					"SELECT summary_id FROM summaries WHERE project_id = ? AND input_hash = ?",
				)
				.get(job.project_id, job.input_hash);
			const jobInputs = this.#db
				.query<JobInputRow, [string]>("SELECT input_kind, ref_id FROM job_inputs WHERE job_id = ? ORDER BY ordinal")
				.all(jobId);
			const handleInputs = jobInputs.map(input => {
				if (input.input_kind === "source") return { kind: "source" as const, id: input.ref_id };
				const child = this.#db
					.query<{ stable_handle: string }, [string]>("SELECT stable_handle FROM summaries WHERE summary_id = ?")
					.get(input.ref_id);
				if (!child) throw new Error(`Missing child summary ${input.ref_id}`);
				return { kind: "summary" as const, id: child.stable_handle };
			});
			const stableHandle = summaryHandleForInput(job.project_id, job.level, handleInputs);
			if (!summary) {
				const summaryId = `sum_${contentAddress([
					"lcm-summary-v1",
					job.project_id,
					job.input_hash,
					completion.redactedText,
				])}`;
				this.#db.run(
					`INSERT INTO summaries
						(summary_id, stable_handle, project_id, input_hash, level, redacted_text, token_count, strategy,
						 prompt_hash, model_selector, resolved_model, input_token_count, output_token_count, created_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					[
						summaryId,
						stableHandle,
						job.project_id,
						job.input_hash,
						job.level,
						completion.redactedText,
						tokenCount,
						strategy,
						promptHash,
						modelSelector,
						resolvedModel,
						job.lease_input_tokens,
						tokenCount,
						now,
					],
				);
				for (let ordinal = 0; ordinal < lineage.length; ordinal++) {
					this.#db.run("INSERT INTO summary_lineage (summary_id, ordinal, source_key) VALUES (?, ?, ?)", [
						summaryId,
						ordinal,
						lineage[ordinal]!,
					]);
				}
				const children = jobInputs.filter(input => input.input_kind === "summary");
				for (let ordinal = 0; ordinal < children.length; ordinal++) {
					this.#db.run("INSERT INTO summary_children (summary_id, ordinal, child_summary_id) VALUES (?, ?, ?)", [
						summaryId,
						ordinal,
						children[ordinal]!.ref_id,
					]);
				}
				summary = { summary_id: summaryId };
			}
			this.#db.run(
				`UPDATE summary_jobs SET status = 'completed', result_summary_id = ?, worker_id = NULL,
					lease_token = NULL, lease_expires_at = NULL, last_error = NULL, last_strategy = ?,
					last_prompt_hash = ?, last_model_selector = ?, last_resolved_model = ?,
					last_input_tokens = ?, last_output_tokens = ?, updated_at = ?
				 WHERE job_id = ? AND lease_token = ?`,
				[
					summary.summary_id,
					strategy,
					promptHash,
					modelSelector,
					resolvedModel,
					job.lease_input_tokens,
					tokenCount,
					now,
					jobId,
					leaseToken,
				],
			);
			this.#scheduleProject(job.project_id, now);
			return { accepted: true, summaryId: summary.summary_id };
		});
		return transaction.immediate();
	}

	#scheduleProject(projectId: string, now: number): ScheduleStats {
		const branches = this.#db
			.query<BranchScheduleRow, [string]>(
				`SELECT id, session_id, branch_id, revision, summary_token_budget,
					fresh_tail_max_sources, fresh_tail_max_tokens
				 FROM branches WHERE project_id = ? ORDER BY id`,
			)
			.all(projectId);
		const total: ScheduleStats = { queued: 0, reused: 0 };
		for (const branch of branches) {
			const summarize =
				branch.summary_token_budget !== null &&
				branch.fresh_tail_max_sources !== null &&
				branch.fresh_tail_max_tokens !== null
					? {
							tokenBudget: branch.summary_token_budget,
							freshTail: {
								maxSources: branch.fresh_tail_max_sources,
								maxTokens: branch.fresh_tail_max_tokens,
							},
						}
					: undefined;
			const result = this.#scheduleBranch(
				branch.id,
				{ projectId, sessionId: branch.session_id, branchId: branch.branch_id },
				branch.revision,
				now,
				summarize,
			);
			total.queued += result.queued;
			total.reused += result.reused;
		}
		return total;
	}

	failSummaryJob(
		jobId: string,
		leaseToken: string,
		redactedError: string,
		retryDelayMs: number,
		provenance?: SummaryAttemptProvenance,
	): boolean {
		this.#assertAvailable();
		assertIdentifier(jobId, "jobId");
		assertIdentifier(leaseToken, "leaseToken");
		if (typeof redactedError !== "string") throw new TypeError("redactedError must be a string");
		assertInteger(retryDelayMs, "retryDelayMs", 0);
		if (provenance) {
			assertIdentifier(provenance.promptHash, "provenance.promptHash");
			if (provenance.modelSelector !== undefined)
				assertIdentifier(provenance.modelSelector, "provenance.modelSelector");
			if (provenance.resolvedModel !== undefined)
				assertIdentifier(provenance.resolvedModel, "provenance.resolvedModel");
		}
		const now = this.#options.now();
		const result = this.#db.run(
			`UPDATE summary_jobs SET status = 'failed', worker_id = NULL, lease_token = NULL,
				lease_expires_at = NULL, available_at = ?, last_error = ?,
				transport_retry_count = transport_retry_count + 1,
				last_strategy = COALESCE(?, last_strategy), last_prompt_hash = COALESCE(?, last_prompt_hash),
				last_model_selector = COALESCE(?, last_model_selector),
				last_resolved_model = COALESCE(?, last_resolved_model),
				last_input_tokens = lease_input_tokens, updated_at = ?
			 WHERE job_id = ? AND status = 'leased' AND lease_token = ? AND lease_expires_at > ?`,
			[
				now + retryDelayMs,
				boundedDiagnostic(redactedError),
				provenance?.strategy ?? null,
				provenance?.promptHash ?? null,
				provenance?.modelSelector ? boundedDiagnostic(provenance.modelSelector) : null,
				provenance?.resolvedModel ? boundedDiagnostic(provenance.resolvedModel) : null,
				now,
				jobId,
				leaseToken,
				now,
			],
		);
		return Number(result.changes) > 0;
	}

	search(request: SearchRequest): SearchHit[] {
		this.#assertAvailable();
		const scope = normalizeScope(request);
		const limit = Math.min(assertInteger(request.limit ?? 20, "limit", 1), 100);
		const offset = Math.min(assertInteger(request.offset ?? 0, "offset", 0), 1_000);
		const match = this.#ftsMatch(request.query);
		if (!match) return [];
		const branchRows = this.#activeSourceRows(scope);
		if (branchRows.length === 0) return [];
		let scopedLineage: string[] | undefined;
		if (request.summaryHandle !== undefined) {
			const handle = assertIdentifier(request.summaryHandle, "summaryHandle");
			const root = this.#summaryByHandle(scope.projectId, handle);
			if (!root) return [];
			scopedLineage = this.#summaryLineage(root.summary_id);
			if (findAlignedSequence(branchRows, scopedLineage) < 0) return [];
		}
		const hits: SearchHit[] = [];
		for (const document of this.#searchDocuments(scope.projectId, match, offset + limit)) {
			if (document.document_kind === "source") {
				if (scopedLineage && !scopedLineage.includes(document.ref_id)) continue;
				const matches = branchRows.filter(row => row.source_key === document.ref_id);
				if (matches.length === 0) continue;
				hits.push({
					kind: "source",
					id: document.ref_id,
					redactedText: document.redacted_text,
					rank: Number(document.rank),
					citations: matches.map(row => this.#citation(row)),
				});
				continue;
			}
			const summary = this.#summaryById(document.ref_id);
			if (!summary) continue;
			const lineage = this.#summaryLineage(summary.summary_id);
			if (scopedLineage && !containsSequence(scopedLineage, lineage)) continue;
			const start = findAlignedSequence(branchRows, lineage);
			if (start < 0) continue;
			hits.push({
				kind: "summary",
				id: summary.summary_id,
				summaryHandle: summary.stable_handle,
				redactedText: document.redacted_text,
				rank: Number(document.rank),
				citations: branchRows.slice(start, start + lineage.length).map(row => this.#citation(row)),
			});
		}
		return hits.slice(offset, offset + limit);
	}

	searchProject(request: ProjectSearchRequest): SearchHit[] {
		this.#assertAvailable();
		const projectId = assertIdentifier(request.projectId, "projectId");
		const limit = Math.min(assertInteger(request.limit ?? 20, "limit", 1), 100);
		const offset = Math.min(assertInteger(request.offset ?? 0, "offset", 0), 1_000);
		const match = this.#ftsMatch(request.query);
		if (!match) return [];
		const projectRows = this.#activeProjectSourceRows(projectId);
		if (projectRows.length === 0) return [];
		const byBranch = new Map<number, ProjectSourceRow[]>();
		for (const row of projectRows) {
			const rows = byBranch.get(row.branch_row_id);
			if (rows) rows.push(row);
			else byBranch.set(row.branch_row_id, [row]);
		}

		const hits: SearchHit[] = [];
		for (const document of this.#searchDocuments(projectId, match, offset + limit)) {
			if (document.document_kind === "source") {
				const matches = projectRows.filter(row => row.source_key === document.ref_id);
				if (matches.length === 0) continue;
				hits.push({
					kind: "source",
					id: document.ref_id,
					redactedText: document.redacted_text,
					rank: Number(document.rank),
					citations: matches.map(row => this.#citation(row)),
				});
				continue;
			}
			const summary = this.#summaryById(document.ref_id);
			if (!summary) continue;
			const lineage = this.#summaryLineage(summary.summary_id);
			const citations: Citation[] = [];
			for (const branchRows of byBranch.values()) {
				for (const start of findAlignedSequences(branchRows, lineage)) {
					citations.push(...branchRows.slice(start, start + lineage.length).map(row => this.#citation(row)));
				}
			}
			if (citations.length === 0) continue;
			hits.push({
				kind: "summary",
				id: summary.summary_id,
				summaryHandle: summary.stable_handle,
				redactedText: document.redacted_text,
				rank: Number(document.rank),
				citations,
			});
		}
		return hits.slice(offset, offset + limit);
	}

	#searchDocuments(projectId: string, match: string, requested: number): SearchDocumentRow[] {
		const candidateLimit = Math.min(Math.max(requested * 16, 64), 1_000);
		return this.#db
			.query<SearchDocumentRow, [string, string, number]>(
				`SELECT d.document_kind, d.ref_id, d.redacted_text, bm25(search_fts) AS rank
				 FROM search_fts JOIN search_documents d ON d.id = search_fts.rowid
				 WHERE search_fts MATCH ? AND d.project_id = ? ORDER BY rank, d.id LIMIT ?`,
			)
			.all(match, projectId, candidateLimit);
	}

	#summaryById(summaryId: string): SummaryRow | null {
		return (
			this.#db
				.query<SummaryRow, [string]>(
					`SELECT summary_id, stable_handle, input_hash, level, redacted_text, token_count, created_at
					 FROM summaries WHERE summary_id = ?`,
				)
				.get(summaryId) ?? null
		);
	}

	#summaryByHandle(projectId: string, summaryHandle: string): SummaryRow | null {
		return (
			this.#db
				.query<SummaryRow, [string, string]>(
					`SELECT summary_id, stable_handle, input_hash, level, redacted_text, token_count, created_at
					 FROM summaries WHERE project_id = ? AND stable_handle = ?`,
				)
				.get(projectId, summaryHandle) ?? null
		);
	}

	#summaryLineage(summaryId: string): string[] {
		return this.#db
			.query<{ source_key: string }, [string]>(
				"SELECT source_key FROM summary_lineage WHERE summary_id = ? ORDER BY ordinal",
			)
			.all(summaryId)
			.map(row => row.source_key);
	}

	#ftsMatch(query: string): string | null {
		if (typeof query !== "string") throw new TypeError("query must be a string");
		const tokens = query.normalize("NFKC").match(/[\p{L}\p{N}_]+/gu);
		if (!tokens || tokens.length === 0) return null;
		return [...new Set(tokens)].map(token => `"${token.replaceAll('"', '""')}"`).join(" AND ");
	}

	#toFileMetadata(row: FileRow): LcmFileMetadata {
		return {
			fileId: row.file_id,
			contentHash: row.content_hash,
			path: row.path,
			fileType: row.file_type,
			byteSize: row.byte_size,
			tokenCount: row.token_count,
			explorationSummary: row.exploration_summary,
		};
	}

	#filesForSource(sourceKey: string): LcmFileMetadata[] {
		return this.#db
			.query<FileRow, [string]>(
				`SELECT f.file_id, f.project_id, f.content_hash, f.path, f.file_type, f.byte_size,
					f.token_count, f.exploration_summary
				 FROM source_files sf JOIN file_records f ON f.file_id = sf.file_id
				 WHERE sf.source_key = ? ORDER BY sf.ordinal`,
			)
			.all(sourceKey)
			.map(row => this.#toFileMetadata(row));
	}

	#filesForLineage(lineage: readonly string[]): LcmFileMetadata[] {
		const files = new Map<string, LcmFileMetadata>();
		for (const sourceKey of lineage) {
			for (const file of this.#filesForSource(sourceKey)) files.set(file.fileId, file);
		}
		return [...files.values()];
	}

	describe(citation: Citation): SourceDescription | null {
		this.#assertAvailable();
		const scope = normalizeScope(citation);
		assertIdentifier(citation.sourceId, "sourceId");
		assertIdentifier(citation.sourceKey, "sourceKey");
		assertIdentifier(citation.contentHash, "contentHash");
		const row = this.#db
			.query<ActiveSourceRow, [string, string, string, string, string, string]>(
				`SELECT bs.id, bs.entry_id, bs.parent_entry_id, bs.position, bs.source_key, bs.atomic_group_id,
					b.project_id, b.session_id, b.branch_id, sc.content_hash, sc.timestamp_ms,
					sc.kind, sc.redacted_text, sc.artifact_refs, sc.token_count
				 FROM branches b
				 JOIN branch_sources bs ON bs.branch_row_id = b.id AND bs.active = 1
				 JOIN source_contents sc ON sc.source_key = bs.source_key
				 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ?
					AND bs.entry_id = ? AND bs.source_key = ? AND sc.content_hash = ?`,
			)
			.get(
				scope.projectId,
				scope.sessionId,
				scope.branchId,
				citation.sourceId,
				citation.sourceKey,
				citation.contentHash,
			);
		if (!row) return null;
		return {
			...this.#citation(row),
			parentId: row.parent_entry_id,
			timestamp: row.timestamp_ms,
			kind: row.kind,
			atomicGroupId: row.atomic_group_id,
			redactedText: row.redacted_text,
			artifactRefs: parseArtifactRefs(row.artifact_refs),
			files: this.#filesForSource(row.source_key),
		};
	}

	#describeSummaryRow(
		scope: ContextScope,
		row: SummaryRow,
		branchRows: readonly ActiveSourceRow[],
	): SummaryDescription | null {
		const lineage = this.#summaryLineage(row.summary_id);
		if (findAlignedSequence(branchRows, lineage) < 0) return null;
		const parentHandles: string[] = [];
		for (const parent of this.#db
			.query<{ summary_id: string; stable_handle: string }, [string]>(
				`SELECT parent.summary_id, parent.stable_handle FROM summary_children edge
				 JOIN summaries parent ON parent.summary_id = edge.summary_id
				 WHERE edge.child_summary_id = ? ORDER BY parent.level, parent.summary_id`,
			)
			.all(row.summary_id)) {
			if (findAlignedSequence(branchRows, this.#summaryLineage(parent.summary_id)) >= 0) {
				parentHandles.push(parent.stable_handle);
			}
		}
		return {
			...scope,
			summaryHandle: row.stable_handle,
			kind: row.level === 0 ? "leaf" : "condensed",
			level: row.level,
			redactedText: row.redacted_text,
			tokenCount: row.token_count,
			sourceCount: lineage.length,
			childCount: this.#count("SELECT COUNT(*) AS count FROM summary_children WHERE summary_id = ?", row.summary_id),
			parentHandles: [...new Set(parentHandles)],
			files: this.#filesForLineage(lineage),
		};
	}

	describeSummary(reference: SummaryReference): SummaryDescription | null {
		this.#assertAvailable();
		const scope = normalizeScope(reference);
		const summaryHandle = assertIdentifier(reference.summaryHandle, "summaryHandle");
		const row = this.#summaryByHandle(scope.projectId, summaryHandle);
		if (!row) return null;
		return this.#describeSummaryRow(scope, row, this.#activeSourceRows(scope));
	}

	describeFile(reference: FileReference): FileDescription | null {
		this.#assertAvailable();
		const scope = normalizeScope(reference);
		const fileId = assertIdentifier(reference.fileId, "fileId");
		const row = this.#db
			.query<FileRow, [string, string]>(
				`SELECT file_id, project_id, content_hash, path, file_type, byte_size, token_count, exploration_summary
				 FROM file_records WHERE project_id = ? AND file_id = ?`,
			)
			.get(scope.projectId, fileId);
		if (!row) return null;
		const sources = this.#activeSourceRows(scope)
			.filter(source => this.#filesForSource(source.source_key).some(file => file.fileId === fileId))
			.map(source => this.#citation(source));
		if (sources.length === 0) return null;
		return { ...scope, ...this.#toFileMetadata(row), sources };
	}

	expandSummary(request: SummaryExpansionRequest): SummaryExpansion | null {
		this.#assertAvailable();
		const scope = normalizeScope(request);
		const summaryHandle = assertIdentifier(request.summaryHandle, "summaryHandle");
		const depth = Math.min(assertInteger(request.depth ?? 1, "depth", 1), 4);
		const offset = Math.min(assertInteger(request.offset ?? 0, "offset", 0), 1_000);
		const limit = Math.min(assertInteger(request.limit ?? 20, "limit", 1), 50);
		const maxTokens = Math.min(assertInteger(request.maxTokens ?? 4_000, "maxTokens", 1), 8_000);
		const branchRows = this.#activeSourceRows(scope);
		const rootRow = this.#summaryByHandle(scope.projectId, summaryHandle);
		if (!rootRow) return null;
		const root = this.#describeSummaryRow(scope, rootRow, branchRows);
		if (!root) return null;
		const rootLineage = this.#summaryLineage(rootRow.summary_id);
		const rootStart = findAlignedSequence(branchRows, rootLineage);
		if (rootStart < 0) return null;
		const rootRows = branchRows.slice(rootStart, rootStart + rootLineage.length);
		const sourcesByKey = new Map<string, ActiveSourceRow[]>();
		for (const source of rootRows) {
			const occurrences = sourcesByKey.get(source.source_key);
			if (occurrences) occurrences.push(source);
			else sourcesByKey.set(source.source_key, [source]);
		}
		const sourceOffsets = new Map<string, number>();
		const candidates: SummaryExpansion["items"][number][] = [];
		const visit = (summary: SummaryRow, itemDepth: number): void => {
			const children = this.#db
				.query<{ summary_id: string }, [string]>(
					"SELECT child_summary_id AS summary_id FROM summary_children WHERE summary_id = ? ORDER BY ordinal",
				)
				.all(summary.summary_id);
			if (children.length > 0) {
				for (const childRef of children) {
					const child = this.#summaryById(childRef.summary_id);
					if (!child) continue;
					const description = this.#describeSummaryRow(scope, child, branchRows);
					if (!description) continue;
					candidates.push({ kind: "summary", depth: itemDepth, summary: description });
					if (itemDepth < depth) visit(child, itemDepth + 1);
				}
				return;
			}
			for (const sourceKey of this.#summaryLineage(summary.summary_id)) {
				const offset = sourceOffsets.get(sourceKey) ?? 0;
				const source = sourcesByKey.get(sourceKey)?.[offset];
				if (!source) continue;
				sourceOffsets.set(sourceKey, offset + 1);
				candidates.push({
					kind: "source",
					depth: itemDepth,
					citation: this.#citation(source),
					tokenCount: source.token_count,
					files: this.#filesForSource(source.source_key),
				});
			}
		};
		visit(rootRow, 1);
		const items: SummaryExpansion["items"][number][] = [];
		let estimatedTokens = 0;
		for (const item of candidates.slice(offset)) {
			if (items.length >= limit) break;
			const rawTokens = item.kind === "summary" ? item.summary.tokenCount : item.tokenCount;
			const itemTokens = Math.min(Math.max(1, rawTokens), maxTokens);
			if (items.length > 0 && estimatedTokens + itemTokens > maxTokens) break;
			items.push(item);
			estimatedTokens += itemTokens;
		}
		const next = offset + items.length;
		return {
			root,
			items,
			offset,
			totalItems: candidates.length,
			estimatedTokens,
			truncated: next < candidates.length,
			...(next < candidates.length ? { nextOffset: next } : {}),
		};
	}

	#citation(row: ActiveSourceRow): Citation {
		return {
			projectId: row.project_id,
			sessionId: row.session_id,
			branchId: row.branch_id,
			sourceId: row.entry_id,
			sourceKey: row.source_key,
			contentHash: row.content_hash,
			position: row.position,
		};
	}

	status(): LcmStatus {
		this.#assertOpen();
		const state = this.#readState();
		const jobCounts: JobStatusCounts = { pending: 0, leased: 0, failed: 0, completed: 0, obsolete: 0 };
		for (const row of this.#db
			.query<JobStatusRow, []>("SELECT status, COUNT(*) AS count FROM summary_jobs GROUP BY status")
			.all()) {
			if (row.status in jobCounts) jobCounts[row.status] = row.count;
		}
		const schema = this.#db.query<{ user_version: number }, []>("PRAGMA user_version").get();
		const journal = this.#db.query<{ journal_mode: string }, []>("PRAGMA journal_mode").get();
		return {
			dbPath: this.#dbPath,
			schemaVersion: schema?.user_version ?? 0,
			journalMode: journal?.journal_mode ?? "unknown",
			quarantined: state.quarantined_at !== null,
			quarantineReason: state.quarantine_reason,
			recoveredFrom: state.last_recovery_path,
			branches: this.#count("SELECT COUNT(*) AS count FROM branches"),
			activeSources: this.#count("SELECT COUNT(*) AS count FROM branch_sources WHERE active = 1"),
			tombstones: this.#count("SELECT COUNT(*) AS count FROM branch_sources WHERE active = 0"),
			leafSummaries: this.#count("SELECT COUNT(*) AS count FROM summaries WHERE level = 0"),
			condensedSummaries: this.#count("SELECT COUNT(*) AS count FROM summaries WHERE level > 0"),
			jobs: jobCounts,
		};
	}

	doctor(): DoctorReport {
		this.#assertOpen();
		const checks: DoctorCheck[] = [];
		const check = (name: string, operation: () => string | null): void => {
			try {
				const detail = operation();
				checks.push(detail === null ? { name, ok: true } : { name, ok: false, detail });
			} catch (error) {
				checks.push({ name, ok: false, detail: boundedDiagnostic(String(error)) });
			}
		};
		check("schema-version", () => {
			const row = this.#db.query<{ user_version: number }, []>("PRAGMA user_version").get();
			return row?.user_version === LCM_SCHEMA_VERSION
				? null
				: `expected ${LCM_SCHEMA_VERSION}, found ${row?.user_version ?? 0}`;
		});
		check("sqlite-quick-check", () => {
			const rows = this.#db.query<Record<string, string>, []>("PRAGMA quick_check").all();
			return rows.length === 1 && Object.values(rows[0] ?? {})[0] === "ok" ? null : JSON.stringify(rows);
		});
		check("foreign-keys", () => {
			const rows = this.#db.query<Record<string, SQLQueryBindings>, []>("PRAGMA foreign_key_check").all();
			return rows.length === 0 ? null : `${rows.length} foreign-key violation(s)`;
		});
		check("branch-sequences", () => {
			const invalid = this.#db
				.query<{ id: number }, []>(
					`SELECT b.id FROM branches b JOIN branch_sources bs ON bs.branch_row_id = b.id AND bs.active = 1
					 GROUP BY b.id HAVING MIN(bs.position) <> 0 OR MAX(bs.position) <> COUNT(*) - 1
						OR COUNT(DISTINCT bs.position) <> COUNT(*) LIMIT 1`,
				)
				.get();
			return invalid ? `branch ${invalid.id} has a non-contiguous active sequence` : null;
		});
		check("fts-index", () => {
			this.#db.run("INSERT INTO search_fts(search_fts, rank) VALUES('integrity-check', 1)");
			return null;
		});
		check("search-documents", () => {
			const missing = this.#count(
				`SELECT COUNT(*) AS count FROM search_documents d WHERE
					(d.document_kind = 'source' AND NOT EXISTS (SELECT 1 FROM source_contents s WHERE s.source_key = d.ref_id)) OR
					(d.document_kind = 'summary' AND NOT EXISTS (SELECT 1 FROM summaries s WHERE s.summary_id = d.ref_id))`,
			);
			return missing === 0 ? null : `${missing} orphan search document(s)`;
		});
		check("quarantine", () => {
			const state = this.#readState();
			return state.quarantined_at === null ? null : (state.quarantine_reason ?? "store is quarantined");
		});
		return { ok: checks.every(item => item.ok), checks };
	}

	rebuild(snapshots: readonly SourceSnapshot[]): RebuildResult {
		this.#assertOpen();
		if (!Array.isArray(snapshots)) throw new TypeError("snapshots must be an array");
		const normalized = snapshots.map(normalizeSnapshot);
		const scopes = new Set<string>();
		for (const snapshot of normalized) {
			const scopeKey = JSON.stringify([snapshot.scope.projectId, snapshot.scope.sessionId, snapshot.scope.branchId]);
			if (scopes.has(scopeKey)) throw new TypeError("rebuild snapshots contain a duplicate branch scope");
			scopes.add(scopeKey);
		}
		const now = this.#options.now();
		const transaction = this.#db.transaction((): RebuildResult => {
			this.#db.run("DELETE FROM summary_jobs");
			this.#db.run("DELETE FROM summary_children");
			this.#db.run("DELETE FROM summaries");
			this.#db.run("DELETE FROM branches");
			this.#db.run("DELETE FROM source_contents");
			this.#db.run("DELETE FROM file_records");
			this.#db.run("DELETE FROM search_documents");
			this.#db.run("INSERT INTO search_fts(search_fts) VALUES('rebuild')");
			let activeSources = 0;
			let queuedJobs = 0;
			for (const snapshot of normalized) {
				const result = this.#reconcileNormalized(snapshot, now);
				activeSources += result.activeSources;
				queuedJobs += result.queuedJobs;
			}
			this.#db.run(
				`UPDATE store_state SET quarantined_at = NULL, quarantine_reason = NULL, rebuilt_at = ?,
					rebuild_count = rebuild_count + 1 WHERE id = 1`,
				[now],
			);
			return { branches: normalized.length, activeSources, queuedJobs };
		});
		const result = transaction.immediate();
		this.#quarantined = false;
		return result;
	}

	purge(): PurgeResult {
		this.#assertAvailable();
		const now = this.#options.now();
		const cutoff = now - this.#options.tombstoneRetentionMs;
		const transaction = this.#db.transaction((): PurgeResult => {
			let jobs = 0;
			const jobCandidates = this.#db
				.query<{ job_id: string; project_id: string; status: string }, [number]>(
					"SELECT job_id, project_id, status FROM summary_jobs WHERE updated_at <= ? ORDER BY updated_at",
				)
				.all(cutoff);
			for (const job of jobCandidates) {
				if (
					job.status !== "completed" &&
					job.status !== "obsolete" &&
					this.#lineageActiveSomewhere(job.project_id, this.#jobLineage(job.job_id))
				) {
					continue;
				}
				jobs += Number(this.#db.run("DELETE FROM summary_jobs WHERE job_id = ?", [job.job_id]).changes);
			}

			let summaries = 0;
			const summaryCandidates = this.#db
				.query<{ summary_id: string; project_id: string }, [number]>(
					"SELECT summary_id, project_id FROM summaries WHERE created_at <= ? ORDER BY level DESC, created_at",
				)
				.all(cutoff);
			for (const summary of summaryCandidates) {
				const lineage = this.#db
					.query<{ source_key: string }, [string]>(
						"SELECT source_key FROM summary_lineage WHERE summary_id = ? ORDER BY ordinal",
					)
					.all(summary.summary_id)
					.map(row => row.source_key);
				if (this.#lineageActiveSomewhere(summary.project_id, lineage)) continue;
				try {
					summaries += Number(
						this.#db.run("DELETE FROM summaries WHERE summary_id = ?", [summary.summary_id]).changes,
					);
				} catch {
					// A surviving parent summary still owns this child; retain it with the parent.
				}
			}

			const tombstones = Number(
				this.#db.run("DELETE FROM branch_sources WHERE active = 0 AND tombstoned_at <= ?", [cutoff]).changes,
			);
			const sourceContents = Number(
				this.#db.run(`
					DELETE FROM source_contents
					WHERE NOT EXISTS (SELECT 1 FROM branch_sources bs WHERE bs.source_key = source_contents.source_key)
					  AND NOT EXISTS (SELECT 1 FROM summary_lineage sl WHERE sl.source_key = source_contents.source_key)
					  AND NOT EXISTS (SELECT 1 FROM job_lineage jl WHERE jl.source_key = source_contents.source_key)
				`).changes,
			);
			const files = Number(
				this.#db.run(`
					DELETE FROM file_records
					WHERE NOT EXISTS (SELECT 1 FROM source_files sf WHERE sf.file_id = file_records.file_id)
				`).changes,
			);
			return { tombstones, jobs, summaries, sourceContents, files };
		});
		return transaction.immediate();
	}

	#count(sql: string, ...bindings: SQLQueryBindings[]): number {
		const row = this.#db.query<CountRow, SQLQueryBindings[]>(sql).get(...bindings);
		return row?.count ?? 0;
	}

	close(): void {
		if (this.#closed) return;
		try {
			this.#db.close();
		} finally {
			closeDatabaseRecoveryGuard(this.#ownerGuard);
			this.#ownerGuard = undefined;
			this.#closed = true;
		}
	}

	[Symbol.dispose](): void {
		this.close();
	}
}

function assertLcmDatabaseIntegrity(db: Database): void {
	const result = db.query<{ quick_check: string }, []>("PRAGMA quick_check(1)").get();
	if (result?.quick_check === "ok") return;
	throw Object.assign(new Error("LCM SQLite integrity check failed"), { code: "SQLITE_CORRUPT" });
}

function createSqliteLcmContext(
	dbPath: string,
	options: InternalOptions,
	verifyIntegrity: boolean,
	holdOwnerGuard = true,
): SqliteLcmContext {
	let db: Database | undefined;
	let ownerGuard: Database | undefined;
	try {
		if (holdOwnerGuard) ownerGuard = acquireDatabaseOwnerGuard(dbPath, options.busyTimeoutMs);
		db = new Database(dbPath, { create: true, readwrite: true, strict: true });
		if (verifyIntegrity) {
			db.run(`PRAGMA busy_timeout = ${options.busyTimeoutMs}`);
			const version = db.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version ?? 0;
			if (version > LCM_SCHEMA_VERSION) throw new UnsupportedLcmSchemaError(version);
			assertLcmDatabaseIntegrity(db);
		}
		initializeLcmSchema(db, options.busyTimeoutMs);
		return new SqliteLcmContext(db, dbPath, options, ownerGuard);
	} catch (error) {
		try {
			db?.close();
		} finally {
			closeDatabaseRecoveryGuard(ownerGuard);
		}
		throw error;
	}
}

async function createSqliteLcmContextWithRetry(
	dbPath: string,
	options: InternalOptions,
	verifyIntegrity: boolean,
	holdOwnerGuard = true,
): Promise<SqliteLcmContext> {
	for (let attempt = 0; ; attempt++) {
		try {
			return createSqliteLcmContext(dbPath, options, verifyIntegrity, holdOwnerGuard);
		} catch (error) {
			if (!isLcmSqliteContentionError(error) || attempt >= SQLITE_OPEN_RETRY_DELAYS_MS.length) throw error;
			await Bun.sleep(SQLITE_OPEN_RETRY_DELAYS_MS[attempt]!);
		}
	}
}

export async function openLcmContext(options: LcmContextOptions): Promise<LcmContext> {
	const normalized = normalizeOptions(options);
	await prepareDatabaseParent(options.dbPath);
	try {
		return await createSqliteLcmContextWithRetry(options.dbPath, normalized, options.recoverCorrupt === true);
	} catch (error) {
		const databasePath = databaseFilePath(options.dbPath);
		if (
			!options.recoverCorrupt ||
			!databasePath ||
			error instanceof UnsupportedLcmSchemaError ||
			!isLcmSqliteCorruptionError(error)
		) {
			throw error;
		}
		// Recheck or rebuild under the exclusive guard, but close the main handle before releasing it.
		await withDatabaseRecoveryLock(`${databasePath}.recovery-lock`, normalized.busyTimeoutMs, async () => {
			let context: SqliteLcmContext | undefined;
			try {
				try {
					context = await createSqliteLcmContextWithRetry(options.dbPath, normalized, true, false);
				} catch (currentError) {
					if (currentError instanceof UnsupportedLcmSchemaError || !isLcmSqliteCorruptionError(currentError)) {
						throw currentError;
					}
					const quarantinePath = await quarantineDatabaseFiles(databasePath, normalized.now());
					context = await createSqliteLcmContextWithRetry(options.dbPath, normalized, true, false);
					context.recordRecovery(quarantinePath, String(currentError));
				}
			} finally {
				context?.close();
			}
		});
		// Reopen through the normal path so the returned context retains its own shared guard.
		return await createSqliteLcmContextWithRetry(options.dbPath, normalized, true);
	}
}
