import { Database, type SQLQueryBindings } from "bun:sqlite";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { initializeLcmSchema, LCM_SCHEMA_VERSION, UnsupportedLcmSchemaError } from "./schema";
import type {
	Citation,
	ClaimSummaryJobsOptions,
	CompleteSummaryJobResult,
	ContextProjection,
	ContextScope,
	DoctorCheck,
	DoctorReport,
	JobStatusCounts,
	LcmContext,
	LcmContextOptions,
	LcmStatus,
	ProjectedHistoricalItem,
	ProjectionRequest,
	ProjectSearchRequest,
	PurgeResult,
	RebuildResult,
	ReconcileResult,
	RunSummaryJobsOptions,
	RunSummaryJobsResult,
	SearchHit,
	SearchRequest,
	SourceDescription,
	SourceEntry,
	SourceSnapshot,
	SummaryCompletion,
	SummaryCompletionCallback,
	SummaryJob,
	SummaryJobInput,
} from "./types";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const DEFAULT_TOMBSTONE_RETENTION_MS = 30 * 24 * 60 * 60_000;
const DEFAULT_LEAF_MAX_SOURCES = 24;
const DEFAULT_LEAF_MAX_TOKENS = 4_000;
const DEFAULT_CONDENSE_FAN_IN = 4;
const MAX_STORED_DIAGNOSTIC_LENGTH = 2_000;

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
	input_hash: string;
	level: number;
	redacted_text: string;
	token_count: number;
	created_at: number;
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

function errorName(error: unknown): string {
	return error instanceof Error ? error.name : "CompletionError";
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
		const sourceKey = contentAddress([
			"lcm-source-v1",
			scope.projectId,
			entry.contentHash,
			String(entry.timestamp),
			entry.kind,
			entry.redactedText,
			artifactRefsJson,
		]);
		normalized.push({
			...entry,
			artifactRefs,
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

function parseArtifactRefs(serialized: string): string[] {
	const parsed: unknown = JSON.parse(serialized);
	if (!Array.isArray(parsed) || !parsed.every(value => typeof value === "string")) {
		throw new Error("LCM artifact reference record is malformed");
	}
	return parsed;
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

class SqliteLcmContext implements LcmContext {
	#db: Database;
	#dbPath: string;
	#options: InternalOptions;
	#closed = false;
	#quarantined = false;

	constructor(db: Database, dbPath: string, options: InternalOptions) {
		this.#db = db;
		this.#dbPath = dbPath;
		this.#options = options;
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

	reconcile(snapshot: SourceSnapshot): ReconcileResult {
		this.#assertAvailable();
		const normalized = normalizeSnapshot(snapshot);
		const transaction = this.#db.transaction(() => this.#reconcileNormalized(normalized, this.#options.now()));
		return transaction.immediate();
	}

	#reconcileNormalized(snapshot: NormalizedSnapshot, now: number): ReconcileResult {
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

		const schedule = this.#scheduleBranch(branch.id, snapshot.scope, revision, now);
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

	#scheduleBranch(branchRowId: number, scope: ContextScope, revision: number, now: number): ScheduleStats {
		const rows = this.#db
			.query<Pick<ActiveSourceRow, "source_key" | "token_count" | "atomic_group_id">, [number]>(
				`SELECT bs.source_key, sc.token_count, bs.atomic_group_id
				 FROM branch_sources bs JOIN source_contents sc ON sc.source_key = bs.source_key
				 WHERE bs.branch_row_id = ? AND bs.active = 1 ORDER BY bs.position`,
			)
			.all(branchRowId);
		const stats: ScheduleStats = { queued: 0, reused: 0 };
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
		let tailStart = rows.length;
		let tailTokens = 0;
		let tailCount = 0;
		const effectiveTailTokenLimit = Math.min(maxTailTokens, tokenBudget);
		for (let unitIndex = units.length - 1; unitIndex >= 0; unitIndex--) {
			const unit = units[unitIndex];
			if (!unit || tailCount >= maxTailSources) break;
			const unitTokens = unit.reduce((total, row) => total + row.token_count, 0);
			const expandsSourceLimit = tailCount + unit.length > maxTailSources;
			const isAtomicClosure = unit.some(row => row.atomic_group_id !== null);
			if ((expandsSourceLimit && !isAtomicClosure) || tailTokens + unitTokens > effectiveTailTokenLimit) break;
			tailStart -= unit.length;
			tailTokens += unitTokens;
			tailCount += unit.length;
		}

		const historical: ProjectedHistoricalItem[] = [];
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
				level: selected.level,
				redactedText: selected.redacted_text,
				tokenCount: selected.token_count,
				sourceIds: coveredRows.map(source => source.entry_id),
				citations: coveredRows.map(source => this.#citation(source)),
			});
			cursor += selected.lineage.length;
			usedTokens += selected.token_count;
		}

		return {
			revision: branch.revision,
			ready: cursor === tailStart,
			historical,
			freshTailSourceIds: rows.slice(tailStart).map(row => row.entry_id),
			uncoveredSourceIds: rows.slice(cursor, tailStart).map(row => row.entry_id),
			estimatedTokens: usedTokens,
			pendingJobs: this.#count(
				"SELECT COUNT(*) AS count FROM summary_jobs WHERE project_id = ? AND status IN ('pending', 'leased', 'failed')",
				scope.projectId,
			),
		};
	}

	#summaryCandidates(projectId: string): SummaryCandidate[] {
		const rows = this.#db
			.query<SummaryRow & { ordinal: number; source_key: string }, [string]>(
				`SELECT s.summary_id, s.input_hash, s.level, s.redacted_text, s.token_count, s.created_at,
					sl.ordinal, sl.source_key
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
		const now = this.#options.now();
		const transaction = this.#db.transaction(() => {
			const candidateLimit = Math.min(Math.max(limit * 16, limit), 1_000);
			const candidates = this.#db
				.query<Pick<JobRow, "job_id" | "project_id">, [number, number, number]>(
					`SELECT job_id, project_id FROM summary_jobs
					 WHERE available_at <= ? AND (
						status IN ('pending', 'failed') OR
						(status = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
					 )
					 ORDER BY level, created_at, job_id LIMIT ?`,
				)
				.all(now, now, candidateLimit);
			const claimed: SummaryJob[] = [];
			for (const candidate of candidates) {
				if (claimed.length >= limit) break;
				const lineage = this.#jobLineage(candidate.job_id);
				if (!this.#lineageActiveSomewhere(candidate.project_id, lineage)) {
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
				const outputTokenBudget = Math.min(maxOutputTokens, inputTokenCount - 1);
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
						last_error = NULL, updated_at = ?
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
					inputTokenCount,
					outputTokenBudget,
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

	#loadClaimedJob(
		jobId: string,
		leaseToken: string,
		leaseExpiresAt: number,
		inputs: readonly SummaryJobInput[],
		inputTokenCount: number,
		outputTokenBudget: number,
	): SummaryJob | null {
		const job = this.#db
			.query<Pick<JobRow, "job_id" | "level">, [string]>("SELECT job_id, level FROM summary_jobs WHERE job_id = ?")
			.get(jobId);
		if (!job) return null;
		return {
			jobId,
			leaseToken,
			leaseExpiresAt,
			kind: job.level === 0 ? "leaf" : "condensed",
			level: job.level,
			inputs,
			sourceCount: this.#jobLineage(jobId).length,
			inputTokenCount,
			outputTokenBudget,
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

	#jobLineage(jobId: string): string[] {
		return this.#db
			.query<{ source_key: string }, [string]>(
				"SELECT source_key FROM job_lineage WHERE job_id = ? ORDER BY ordinal",
			)
			.all(jobId)
			.map(row => row.source_key);
	}

	#lineageActiveSomewhere(projectId: string, lineage: readonly string[]): boolean {
		if (lineage.length === 0) return false;
		const rows = this.#db
			.query<{ branch_row_id: number; source_key: string; atomic_group_id: string | null }, [string]>(
				`SELECT bs.branch_row_id, bs.source_key, bs.atomic_group_id
				 FROM branch_sources bs JOIN branches b ON b.id = bs.branch_row_id
				 WHERE b.project_id = ? AND bs.active = 1 ORDER BY bs.branch_row_id, bs.position`,
			)
			.all(projectId);
		let branchId = -1;
		let sequence: Array<{ source_key: string; atomic_group_id: string | null }> = [];
		for (const row of rows) {
			if (branchId !== row.branch_row_id) {
				if (findAlignedSequence(sequence, lineage) >= 0) return true;
				branchId = row.branch_row_id;
				sequence = [];
			}
			sequence.push({ source_key: row.source_key, atomic_group_id: row.atomic_group_id });
		}
		return findAlignedSequence(sequence, lineage) >= 0;
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
		const now = this.#options.now();
		const transaction = this.#db.transaction((): CompleteSummaryJobResult => {
			const job = this.#db
				.query<JobRow, [string]>(
					`SELECT job_id, project_id, input_hash, level, status, lease_token, lease_expires_at,
						lease_input_tokens, lease_output_budget
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
				this.#db.run(
					`UPDATE summary_jobs SET status = 'failed', worker_id = NULL, lease_token = NULL,
						lease_expires_at = NULL, available_at = ?, last_error = 'summary did not compress input', updated_at = ?
					 WHERE job_id = ? AND lease_token = ?`,
					[now, now, jobId, leaseToken],
				);
				return { accepted: false, reason: "not_compressed" };
			}

			let summary = this.#db
				.query<{ summary_id: string }, [string, string]>(
					"SELECT summary_id FROM summaries WHERE project_id = ? AND input_hash = ?",
				)
				.get(job.project_id, job.input_hash);
			if (!summary) {
				const summaryId = `sum_${contentAddress([
					"lcm-summary-v1",
					job.project_id,
					job.input_hash,
					completion.redactedText,
				])}`;
				this.#db.run(
					`INSERT INTO summaries
						(summary_id, project_id, input_hash, level, redacted_text, token_count, created_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?)`,
					[summaryId, job.project_id, job.input_hash, job.level, completion.redactedText, tokenCount, now],
				);
				for (let ordinal = 0; ordinal < lineage.length; ordinal++) {
					this.#db.run("INSERT INTO summary_lineage (summary_id, ordinal, source_key) VALUES (?, ?, ?)", [
						summaryId,
						ordinal,
						lineage[ordinal]!,
					]);
				}
				const children = this.#db
					.query<JobInputRow, [string]>(
						"SELECT input_kind, ref_id FROM job_inputs WHERE job_id = ? ORDER BY ordinal",
					)
					.all(jobId)
					.filter(input => input.input_kind === "summary");
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
					lease_token = NULL, lease_expires_at = NULL, last_error = NULL, updated_at = ?
				 WHERE job_id = ? AND lease_token = ?`,
				[summary.summary_id, now, jobId, leaseToken],
			);
			this.#scheduleProject(job.project_id, now);
			return { accepted: true, summaryId: summary.summary_id };
		});
		return transaction.immediate();
	}

	#scheduleProject(projectId: string, now: number): ScheduleStats {
		const branches = this.#db
			.query<{ id: number; session_id: string; branch_id: string; revision: number }, [string]>(
				"SELECT id, session_id, branch_id, revision FROM branches WHERE project_id = ? ORDER BY id",
			)
			.all(projectId);
		const total: ScheduleStats = { queued: 0, reused: 0 };
		for (const branch of branches) {
			const result = this.#scheduleBranch(
				branch.id,
				{ projectId, sessionId: branch.session_id, branchId: branch.branch_id },
				branch.revision,
				now,
			);
			total.queued += result.queued;
			total.reused += result.reused;
		}
		return total;
	}

	failSummaryJob(jobId: string, leaseToken: string, redactedError: string, retryDelayMs: number): boolean {
		this.#assertAvailable();
		assertIdentifier(jobId, "jobId");
		assertIdentifier(leaseToken, "leaseToken");
		if (typeof redactedError !== "string") throw new TypeError("redactedError must be a string");
		assertInteger(retryDelayMs, "retryDelayMs", 0);
		const now = this.#options.now();
		const result = this.#db.run(
			`UPDATE summary_jobs SET status = 'failed', worker_id = NULL, lease_token = NULL,
				lease_expires_at = NULL, available_at = ?, last_error = ?, updated_at = ?
			 WHERE job_id = ? AND status = 'leased' AND lease_token = ? AND lease_expires_at > ?`,
			[now + retryDelayMs, boundedDiagnostic(redactedError), now, jobId, leaseToken, now],
		);
		return Number(result.changes) > 0;
	}

	async runSummaryJobs(
		options: RunSummaryJobsOptions,
		complete: SummaryCompletionCallback,
	): Promise<RunSummaryJobsResult> {
		assertInteger(options.retryDelayMs, "retryDelayMs", 0);
		const jobs = this.claimSummaryJobs(options);
		const result: RunSummaryJobsResult = { claimed: jobs.length, completed: 0, failed: 0, stale: 0 };
		for (const job of jobs) {
			try {
				const completion = await complete(job);
				const accepted = this.completeSummaryJob(job.jobId, job.leaseToken, completion);
				if (accepted.accepted) result.completed++;
				else if (accepted.reason === "not_compressed") result.failed++;
				else result.stale++;
			} catch (error) {
				if (this.failSummaryJob(job.jobId, job.leaseToken, errorName(error), options.retryDelayMs)) result.failed++;
				else result.stale++;
			}
		}
		return result;
	}

	search(request: SearchRequest): SearchHit[] {
		this.#assertAvailable();
		const scope = normalizeScope(request);
		const limit = assertInteger(request.limit ?? 20, "limit", 1);
		const match = this.#ftsMatch(request.query);
		if (!match) return [];
		const branchRows = this.#activeSourceRows(scope);
		if (branchRows.length === 0) return [];
		const documents = this.#searchDocuments(scope.projectId, match, limit);
		const hits: SearchHit[] = [];
		for (const document of documents) {
			if (hits.length >= limit) break;
			if (document.document_kind === "source") {
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
			const lineage = this.#summaryLineage(document.ref_id);
			const start = findAlignedSequence(branchRows, lineage);
			if (start < 0) continue;
			const citedRows = branchRows.slice(start, start + lineage.length);
			hits.push({
				kind: "summary",
				id: document.ref_id,
				redactedText: document.redacted_text,
				rank: Number(document.rank),
				citations: citedRows.map(row => this.#citation(row)),
			});
		}
		return hits;
	}

	searchProject(request: ProjectSearchRequest): SearchHit[] {
		this.#assertAvailable();
		const projectId = assertIdentifier(request.projectId, "projectId");
		const limit = assertInteger(request.limit ?? 20, "limit", 1);
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
		for (const document of this.#searchDocuments(projectId, match, limit)) {
			if (hits.length >= limit) break;
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
			const lineage = this.#summaryLineage(document.ref_id);
			const citations: Citation[] = [];
			for (const branchRows of byBranch.values()) {
				for (const start of findAlignedSequences(branchRows, lineage)) {
					citations.push(...branchRows.slice(start, start + lineage.length).map(row => this.#citation(row)));
				}
			}
			if (citations.length === 0) continue;
			hits.push({
				kind: "summary",
				id: document.ref_id,
				redactedText: document.redacted_text,
				rank: Number(document.rank),
				citations,
			});
		}
		return hits;
	}

	#searchDocuments(projectId: string, match: string, limit: number): SearchDocumentRow[] {
		const candidateLimit = Math.min(Math.max(limit * 16, 64), 1_000);
		return this.#db
			.query<SearchDocumentRow, [string, string, number]>(
				`SELECT d.document_kind, d.ref_id, d.redacted_text, bm25(search_fts) AS rank
				 FROM search_fts JOIN search_documents d ON d.id = search_fts.rowid
				 WHERE search_fts MATCH ? AND d.project_id = ? ORDER BY rank, d.id LIMIT ?`,
			)
			.all(match, projectId, candidateLimit);
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

	quarantine(redactedReason: string): void {
		this.#assertOpen();
		if (typeof redactedReason !== "string" || redactedReason.trim().length === 0) {
			throw new TypeError("redactedReason must be a non-empty string");
		}
		this.#db.run("UPDATE store_state SET quarantined_at = ?, quarantine_reason = ? WHERE id = 1", [
			this.#options.now(),
			boundedDiagnostic(redactedReason),
		]);
		this.#quarantined = true;
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
			return { tombstones, jobs, summaries, sourceContents };
		});
		return transaction.immediate();
	}

	#count(sql: string, ...bindings: SQLQueryBindings[]): number {
		const row = this.#db.query<CountRow, SQLQueryBindings[]>(sql).get(...bindings);
		return row?.count ?? 0;
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#db.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}

function createSqliteLcmContext(dbPath: string, options: InternalOptions): SqliteLcmContext {
	let db: Database | undefined;
	try {
		db = new Database(dbPath, { create: true, readwrite: true, strict: true });
		initializeLcmSchema(db, options.busyTimeoutMs);
		return new SqliteLcmContext(db, dbPath, options);
	} catch (error) {
		db?.close();
		throw error;
	}
}

export async function openLcmContext(options: LcmContextOptions): Promise<LcmContext> {
	const normalized = normalizeOptions(options);
	await prepareDatabaseParent(options.dbPath);
	try {
		return createSqliteLcmContext(options.dbPath, normalized);
	} catch (error) {
		if (!options.recoverCorrupt || options.dbPath === ":memory:" || error instanceof UnsupportedLcmSchemaError)
			throw error;
		const quarantinePath = await quarantineDatabaseFiles(options.dbPath, normalized.now());
		const context = createSqliteLcmContext(options.dbPath, normalized);
		context.recordRecovery(quarantinePath, String(error));
		return context;
	}
}
