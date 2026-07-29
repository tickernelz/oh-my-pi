import { Database, type SQLQueryBindings } from "bun:sqlite";
import { type Dirent, existsSync, lstatSync, readdirSync, readFileSync, unlinkSync } from "node:fs";
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
	LcmPerformanceCounters,
	LcmRecoveryCategory,
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
	SummaryAttemptOutcome,
	SummaryAttemptProvenance,
	SummaryCompletion,
	SummaryDescription,
	SummaryExpansion,
	SummaryExpansionRequest,
	SummaryFailureAttemptOutcome,
	SummaryJob,
	SummaryJobInput,
	SummaryLocalAttemptOutcome,
	SummaryProviderAttempt,
	SummaryProviderAttemptStart,
	SummaryProviderUsage,
	SummaryReference,
	SummaryStage,
	SummaryStrategy,
} from "./types";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const DEFAULT_TOMBSTONE_RETENTION_MS = 30 * 24 * 60 * 60_000;
const DEFAULT_QUARANTINE_RETENTION_MS = 30 * 24 * 60 * 60_000;
const DEFAULT_LEAF_MAX_SOURCES = 24;
const DEFAULT_LEAF_MAX_TOKENS = 4_000;
const DEFAULT_CONDENSE_FAN_IN = 4;
const MAX_STORED_DIAGNOSTIC_LENGTH = 2_000;
const SQLITE_OPEN_RETRY_DELAYS_MS = [100, 200, 400] as const;
const MAX_SEARCH_LIMIT = 100;
const MAX_SEARCH_OFFSET = 1_000;
const MAX_SEARCH_CANDIDATES = MAX_SEARCH_OFFSET + MAX_SEARCH_LIMIT;

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

interface SpanRow {
	level: number;
	start_position: number;
	end_position: number;
	input_hash: string;
	summary_id: string | null;
	frontier: number;
}

interface SummaryResolution {
	inputHash: string;
	jobId: string;
	node: SummaryNode | null;
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

interface AttemptRow {
	job_id: string;
	project_id: string;
	outcome: string;
	started_at: number;
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

interface RecoveryEventRow {
	reason: string;
	created_at: number;
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

function assertAmount(value: number, name: string): number {
	if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
		throw new RangeError(`${name} must be a finite number >= 0`);
	}
	return value;
}

function optionalCount(value: number | undefined, name: string): number | null {
	return value === undefined ? null : assertInteger(value, name, 0);
}

function optionalAmount(value: number | undefined, name: string): number | null {
	return value === undefined ? null : assertAmount(value, name);
}

/**
 * Ordered `summary_attempts` usage/cost bindings. Absent optional detail and a
 * missing usage snapshot stay SQL NULL so unknown never reads as measured zero.
 */
function attemptUsageBindings(usage: SummaryProviderUsage | undefined): (number | null)[] {
	if (!usage) return new Array<number | null>(19).fill(null);
	return [
		assertInteger(usage.input, "attempt.usage.input", 0),
		assertInteger(usage.output, "attempt.usage.output", 0),
		assertInteger(usage.cacheRead, "attempt.usage.cacheRead", 0),
		assertInteger(usage.cacheWrite, "attempt.usage.cacheWrite", 0),
		assertInteger(usage.totalTokens, "attempt.usage.totalTokens", 0),
		optionalCount(usage.orchestration?.input, "attempt.usage.orchestration.input"),
		optionalCount(usage.orchestration?.cacheRead, "attempt.usage.orchestration.cacheRead"),
		optionalCount(usage.orchestration?.output, "attempt.usage.orchestration.output"),
		optionalCount(usage.reasoningTokens, "attempt.usage.reasoningTokens"),
		optionalAmount(usage.premiumRequests, "attempt.usage.premiumRequests"),
		optionalCount(usage.cttl?.ephemeral5m, "attempt.usage.cttl.ephemeral5m"),
		optionalCount(usage.cttl?.ephemeral1h, "attempt.usage.cttl.ephemeral1h"),
		optionalCount(usage.server?.webSearch, "attempt.usage.server.webSearch"),
		optionalCount(usage.server?.webFetch, "attempt.usage.server.webFetch"),
		assertAmount(usage.cost.input, "attempt.usage.cost.input"),
		assertAmount(usage.cost.output, "attempt.usage.cost.output"),
		assertAmount(usage.cost.cacheRead, "attempt.usage.cost.cacheRead"),
		assertAmount(usage.cost.cacheWrite, "attempt.usage.cost.cacheWrite"),
		assertAmount(usage.cost.total, "attempt.usage.cost.total"),
	];
}

function assertProviderAttemptStart(attempt: SummaryProviderAttemptStart, label: string): void {
	assertIdentifier(attempt.attemptId, `${label}.attemptId`);
	assertInteger(attempt.startedAt, `${label}.startedAt`, 0);
	assertIdentifier(attempt.provider, `${label}.provider`);
	assertIdentifier(attempt.model, `${label}.model`);
}

function assertProviderAttempt(attempt: SummaryProviderAttempt, label: string): void {
	assertProviderAttemptStart(attempt, label);
	if (assertInteger(attempt.completedAt, `${label}.completedAt`, 0) < attempt.startedAt) {
		throw new RangeError(`${label}.completedAt must not precede ${label}.startedAt`);
	}
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

const DATABASE_QUARANTINE_SIDECAR_SUFFIXES = ["-wal", "-shm"] as const;
const MAX_PENDING_DATABASE_QUARANTINE_BYTES = 32_768;
const DATABASE_QUARANTINE_UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

interface PendingDatabaseQuarantine {
	quarantinePath: string;
	reason: string;
}

class PendingDatabaseQuarantineError extends Error {}

function pendingDatabaseQuarantinePath(dbPath: string): string {
	return `${dbPath}.quarantine-pending`;
}

async function databaseFileExists(filePath: string): Promise<boolean> {
	try {
		await fs.stat(filePath);
		return true;
	} catch (error) {
		if (isMissingFile(error)) return false;
		throw error;
	}
}

async function syncDatabaseDirectory(dbPath: string): Promise<void> {
	const directory = await fs.open(path.dirname(path.resolve(dbPath)), "r");
	try {
		await directory.sync();
	} finally {
		await directory.close();
	}
}

function isValidDatabaseQuarantinePath(dbPath: string, quarantinePath: string): boolean {
	const normalizedDatabasePath = path.normalize(dbPath);
	if (quarantinePath !== path.normalize(quarantinePath)) return false;
	if (path.dirname(quarantinePath) !== path.dirname(normalizedDatabasePath)) return false;
	const prefix = `${path.basename(normalizedDatabasePath)}.quarantine-`;
	const basename = path.basename(quarantinePath);
	if (!basename.startsWith(prefix)) return false;
	const suffix = basename.slice(prefix.length);
	const uuidIndex = suffix.length - 36;
	if (uuidIndex <= 1 || suffix[uuidIndex - 1] !== "-") return false;
	const timestamp = suffix.slice(0, uuidIndex - 1);
	return (
		/^(0|[1-9]\d*)$/.test(timestamp) &&
		Number.isSafeInteger(Number(timestamp)) &&
		DATABASE_QUARANTINE_UUID_PATTERN.test(suffix.slice(uuidIndex))
	);
}

interface DatabaseQuarantineFile {
	path: string;
	bytes: number;
}

interface DatabaseQuarantineUnit {
	path: string;
	timestamp: number;
	files: readonly DatabaseQuarantineFile[];
}

function regularFileBytes(filePath: string): number {
	try {
		const stats = lstatSync(filePath);
		return stats.isFile() ? stats.size : 0;
	} catch {
		return 0;
	}
}

function databaseQuarantineTimestamp(dbPath: string, quarantinePath: string): number | undefined {
	if (!isValidDatabaseQuarantinePath(dbPath, quarantinePath)) return undefined;
	const prefix = `${path.basename(path.normalize(dbPath))}.quarantine-`;
	const suffix = path.basename(quarantinePath).slice(prefix.length);
	return Number(suffix.slice(0, -37));
}

function databaseQuarantineUnits(dbPath: string): DatabaseQuarantineUnit[] {
	const normalizedDatabasePath = path.normalize(dbPath);
	const directory = path.dirname(normalizedDatabasePath);
	const databaseName = path.basename(normalizedDatabasePath);
	let entries: Dirent[];
	try {
		entries = readdirSync(directory, { withFileTypes: true, encoding: "utf8" });
	} catch {
		return [];
	}
	const units = new Map<
		string,
		{ timestamp: number; valid: boolean; hasMain: boolean; files: DatabaseQuarantineFile[] }
	>();
	for (const entry of entries) {
		if (!entry.name.startsWith(`${databaseName}.quarantine-`)) continue;
		const sidecar = DATABASE_QUARANTINE_SIDECAR_SUFFIXES.find(suffix => entry.name.endsWith(suffix));
		const mainName = sidecar === undefined ? entry.name : entry.name.slice(0, -sidecar.length);
		const quarantinePath = path.join(directory, mainName);
		const timestamp = databaseQuarantineTimestamp(normalizedDatabasePath, quarantinePath);
		if (timestamp === undefined) continue;
		let unit = units.get(quarantinePath);
		if (!unit) {
			unit = { timestamp, valid: true, hasMain: false, files: [] };
			units.set(quarantinePath, unit);
		}
		if (!entry.isFile()) {
			unit.valid = false;
			continue;
		}
		const filePath = path.join(directory, entry.name);
		const bytes = regularFileBytes(filePath);
		if (bytes === 0) {
			try {
				if (!lstatSync(filePath).isFile()) unit.valid = false;
			} catch {
				unit.valid = false;
			}
		}
		if (!unit.valid) continue;
		unit.files.push({ path: filePath, bytes });
		if (sidecar === undefined) unit.hasMain = true;
	}
	return [...units.entries()]
		.filter(([, unit]) => unit.valid && unit.hasMain)
		.map(([quarantinePath, unit]) => ({ path: quarantinePath, timestamp: unit.timestamp, files: unit.files }))
		.sort((left, right) => left.timestamp - right.timestamp || left.path.localeCompare(right.path));
}

function pendingDatabaseQuarantineTarget(dbPath: string): string | null | undefined {
	const markerPath = pendingDatabaseQuarantinePath(dbPath);
	let serialized: Buffer;
	try {
		const stats = lstatSync(markerPath);
		if (!stats.isFile() || stats.size > MAX_PENDING_DATABASE_QUARANTINE_BYTES) return null;
		serialized = readFileSync(markerPath);
	} catch (error) {
		return isMissingFile(error) ? undefined : null;
	}
	try {
		const parsed: unknown = JSON.parse(serialized.toString("utf8"));
		if (
			typeof parsed !== "object" ||
			parsed === null ||
			!("quarantinePath" in parsed) ||
			typeof parsed.quarantinePath !== "string" ||
			!isValidDatabaseQuarantinePath(dbPath, parsed.quarantinePath) ||
			!("reason" in parsed) ||
			typeof parsed.reason !== "string" ||
			parsed.reason.length > MAX_STORED_DIAGNOSTIC_LENGTH
		) {
			return null;
		}
		return parsed.quarantinePath;
	} catch {
		return null;
	}
}

function recoveryCategory(reason: string): LcmRecoveryCategory {
	if (reason.includes("LCM SQLite integrity check failed")) return "integrity_check";
	if (/SQLITE_(?:CORRUPT|NOTADB|IOERR_CORRUPTFS)|corrupt|malformed|not a database/i.test(reason)) {
		return "corruption";
	}
	return "unknown";
}

async function publishPendingDatabaseQuarantine(dbPath: string, pending: PendingDatabaseQuarantine): Promise<void> {
	const markerPath = pendingDatabaseQuarantinePath(dbPath);
	const payload = Buffer.from(JSON.stringify(pending), "utf8");
	if (payload.byteLength > MAX_PENDING_DATABASE_QUARANTINE_BYTES) {
		throw new Error(`Pending LCM quarantine manifest is too large: ${markerPath}`);
	}
	if (await databaseFileExists(markerPath)) {
		throw new PendingDatabaseQuarantineError(`LCM database quarantine recovery is pending: ${dbPath}`);
	}
	const temporaryPath = path.join(
		path.dirname(markerPath),
		`.${path.basename(markerPath)}.${process.pid}.${crypto.randomUUID()}.tmp`,
	);
	let removeTemporary = false;
	try {
		const handle = await fs.open(temporaryPath, "wx", 0o600);
		removeTemporary = true;
		try {
			await handle.writeFile(payload);
			await handle.sync();
		} finally {
			await handle.close();
		}
		await fs.rename(temporaryPath, markerPath);
		removeTemporary = false;
		await syncDatabaseDirectory(dbPath);
	} finally {
		if (removeTemporary) await fs.rm(temporaryPath, { force: true }).catch(() => {});
	}
}

async function readPendingDatabaseQuarantine(dbPath: string): Promise<PendingDatabaseQuarantine | undefined> {
	const markerPath = pendingDatabaseQuarantinePath(dbPath);
	let serialized: Buffer;
	try {
		serialized = await fs.readFile(markerPath);
	} catch (error) {
		if (isMissingFile(error)) return undefined;
		throw error;
	}
	if (serialized.byteLength > MAX_PENDING_DATABASE_QUARANTINE_BYTES) {
		throw new Error(`Invalid pending LCM quarantine manifest: ${markerPath}`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(serialized.toString("utf8"));
	} catch (error) {
		throw new Error(`Invalid pending LCM quarantine manifest: ${markerPath}`, { cause: error });
	}
	if (
		typeof parsed !== "object" ||
		parsed === null ||
		!("quarantinePath" in parsed) ||
		typeof parsed.quarantinePath !== "string" ||
		!isValidDatabaseQuarantinePath(dbPath, parsed.quarantinePath) ||
		!("reason" in parsed) ||
		typeof parsed.reason !== "string" ||
		parsed.reason.length > MAX_STORED_DIAGNOSTIC_LENGTH
	) {
		throw new Error(`Invalid pending LCM quarantine manifest: ${markerPath}`);
	}
	return { quarantinePath: parsed.quarantinePath, reason: parsed.reason };
}

async function removePendingDatabaseQuarantine(dbPath: string): Promise<void> {
	try {
		await fs.unlink(pendingDatabaseQuarantinePath(dbPath));
	} catch (error) {
		if (isMissingFile(error)) return;
		throw error;
	}
	await syncDatabaseDirectory(dbPath);
}

async function databaseUnitIsFullyOriginal(dbPath: string, quarantinePath: string): Promise<boolean> {
	if (!(await databaseFileExists(dbPath)) || (await databaseFileExists(quarantinePath))) return false;
	for (const suffix of DATABASE_QUARANTINE_SIDECAR_SUFFIXES) {
		if (await databaseFileExists(`${quarantinePath}${suffix}`)) return false;
	}
	return true;
}

async function completeDatabaseQuarantine(dbPath: string, quarantinePath: string): Promise<void> {
	const originalSidecars: (typeof DATABASE_QUARANTINE_SIDECAR_SUFFIXES)[number][] = [];
	for (const suffix of DATABASE_QUARANTINE_SIDECAR_SUFFIXES) {
		const originalExists = await databaseFileExists(`${dbPath}${suffix}`);
		const quarantinedExists = await databaseFileExists(`${quarantinePath}${suffix}`);
		if (originalExists && quarantinedExists) {
			throw new Error(`Pending LCM quarantine has conflicting sidecars: ${dbPath}${suffix}`);
		}
		if (originalExists) originalSidecars.push(suffix);
	}
	for (const suffix of originalSidecars) {
		await fs.rename(`${dbPath}${suffix}`, `${quarantinePath}${suffix}`);
	}
	await syncDatabaseDirectory(dbPath);

	const originalMainExists = await databaseFileExists(dbPath);
	const quarantinedMainExists = await databaseFileExists(quarantinePath);
	if (originalMainExists && quarantinedMainExists) {
		throw new Error(`Pending LCM quarantine has conflicting database files: ${dbPath}`);
	}
	if (!quarantinedMainExists) {
		if (!originalMainExists) throw new Error(`Cannot quarantine missing LCM database: ${dbPath}`);
		await fs.rename(dbPath, quarantinePath);
	}
	await syncDatabaseDirectory(dbPath);
}

async function restoreOriginalDatabaseUnit(dbPath: string, quarantinePath: string): Promise<void> {
	const originalMainExists = await databaseFileExists(dbPath);
	const quarantinedMainExists = await databaseFileExists(quarantinePath);
	if (originalMainExists && quarantinedMainExists) {
		throw new Error(`Pending LCM quarantine has conflicting database files: ${dbPath}`);
	}
	if (!originalMainExists && !quarantinedMainExists) {
		throw new Error(`Pending LCM quarantine is missing both database files: ${dbPath}`);
	}
	const quarantinedSidecars: (typeof DATABASE_QUARANTINE_SIDECAR_SUFFIXES)[number][] = [];
	for (const suffix of DATABASE_QUARANTINE_SIDECAR_SUFFIXES) {
		const originalExists = await databaseFileExists(`${dbPath}${suffix}`);
		const quarantinedExists = await databaseFileExists(`${quarantinePath}${suffix}`);
		if (originalExists && quarantinedExists) {
			throw new Error(`Pending LCM quarantine has conflicting sidecars: ${dbPath}${suffix}`);
		}
		if (quarantinedExists) quarantinedSidecars.push(suffix);
	}
	for (const suffix of quarantinedSidecars) {
		await fs.rename(`${quarantinePath}${suffix}`, `${dbPath}${suffix}`);
	}
	await syncDatabaseDirectory(dbPath);
	if (quarantinedMainExists) {
		await fs.rename(quarantinePath, dbPath);
		await syncDatabaseDirectory(dbPath);
	}
}

async function recoverPendingDatabaseQuarantine(dbPath: string): Promise<PendingDatabaseQuarantine | undefined> {
	const pending = await readPendingDatabaseQuarantine(dbPath);
	if (!pending) return undefined;
	const originalMainExists = await databaseFileExists(dbPath);
	const quarantinedMainExists = await databaseFileExists(pending.quarantinePath);
	if (quarantinedMainExists) return pending;
	if (!originalMainExists) {
		throw new Error(`Pending LCM quarantine is missing both database files: ${dbPath}`);
	}
	try {
		await restoreOriginalDatabaseUnit(dbPath, pending.quarantinePath);
	} catch (rollbackError) {
		if (await databaseUnitIsFullyOriginal(dbPath, pending.quarantinePath)) throw rollbackError;
		try {
			await completeDatabaseQuarantine(dbPath, pending.quarantinePath);
			return pending;
		} catch (completionError) {
			throw new AggregateError(
				[rollbackError, completionError],
				`Failed to settle pending LCM database quarantine: ${dbPath}`,
			);
		}
	}
	await removePendingDatabaseQuarantine(dbPath);
	return undefined;
}

async function quarantineDatabaseFiles(dbPath: string, now: number, reason: string): Promise<string> {
	const normalizedDatabasePath = path.normalize(dbPath);
	const quarantinePath = path.join(
		path.dirname(normalizedDatabasePath),
		`${path.basename(normalizedDatabasePath)}.quarantine-${assertInteger(now, "now", 0)}-${crypto.randomUUID()}`,
	);
	await publishPendingDatabaseQuarantine(dbPath, {
		quarantinePath,
		reason: boundedDiagnostic(reason),
	});
	try {
		await completeDatabaseQuarantine(dbPath, quarantinePath);
		return quarantinePath;
	} catch (forwardError) {
		try {
			await restoreOriginalDatabaseUnit(dbPath, quarantinePath);
		} catch (rollbackError) {
			if (await databaseUnitIsFullyOriginal(dbPath, quarantinePath)) {
				throw new AggregateError(
					[forwardError, rollbackError],
					`Failed to quarantine LCM database after restoring the original unit: ${dbPath}`,
				);
			}
			try {
				await completeDatabaseQuarantine(dbPath, quarantinePath);
				return quarantinePath;
			} catch (completionError) {
				const errors: unknown[] = [forwardError, rollbackError, completionError];
				try {
					await restoreOriginalDatabaseUnit(dbPath, quarantinePath);
				} catch (finalRestoreError) {
					errors.push(finalRestoreError);
				}
				throw new AggregateError(errors, `Failed to settle LCM database quarantine: ${dbPath}`);
			}
		}
		try {
			await removePendingDatabaseQuarantine(dbPath);
		} catch (cleanupError) {
			throw new AggregateError(
				[forwardError, cleanupError],
				`Failed to quarantine LCM database after restoring the original unit: ${dbPath}`,
			);
		}
		throw forwardError;
	}
}

function sqliteUriParameters(dbPath: string): URLSearchParams | undefined {
	const queryIndex = dbPath.indexOf("?");
	const fragmentIndex = dbPath.indexOf("#");
	if (queryIndex === -1 || (fragmentIndex !== -1 && fragmentIndex < queryIndex)) return undefined;
	const query = dbPath.slice(queryIndex + 1, fragmentIndex === -1 ? undefined : fragmentIndex);
	return new URLSearchParams(query.replaceAll("+", "%2B"));
}

function sqliteUriBoolean(value: string | null | undefined, fallback: boolean): boolean {
	if (value === undefined || value === null) return fallback;
	switch (value.toLowerCase()) {
		case "yes":
		case "true":
		case "on":
			return true;
		case "no":
		case "false":
		case "off":
			return false;
	}
	const numericPrefix = /^[+-]?\d+/.exec(value)?.[0];
	return numericPrefix === undefined ? fallback : !/^[+-]?0+$/.test(numericPrefix);
}

function databaseFilePath(dbPath: string): string | undefined {
	if (dbPath === ":memory:") return undefined;
	if (!dbPath.startsWith("file:")) return dbPath;
	const queryIndex = dbPath.indexOf("?");
	const fragmentIndex = dbPath.indexOf("#");
	const hasQuery = queryIndex !== -1 && (fragmentIndex === -1 || queryIndex < fragmentIndex);
	const pathEnd = hasQuery ? queryIndex : fragmentIndex === -1 ? undefined : fragmentIndex;
	const encodedPath = dbPath.slice("file:".length, pathEnd);
	const mode = sqliteUriParameters(dbPath)?.get("mode");
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

function databaseRecoveryFilePath(dbPath: string): string | undefined {
	const databasePath = databaseFilePath(dbPath);
	if (!databasePath || !dbPath.startsWith("file:")) return databasePath;
	const parameters = sqliteUriParameters(dbPath);
	const mode = parameters?.get("mode")?.toLowerCase();
	if (mode === "ro" || mode === "rw" || sqliteUriBoolean(parameters?.get("immutable"), false)) return undefined;
	return databasePath;
}

function openDatabaseRecoveryGuard(lockPath: string, busyTimeoutMs: number): Database {
	let guard: Database | undefined;
	try {
		guard = new Database(lockPath, { create: true, readwrite: true, strict: true });
		guard.run(`PRAGMA busy_timeout = ${busyTimeoutMs}`);
		// BEGIN EXCLUSIVE excludes readers only in rollback-journal mode. Established
		// owner guards must remain read-only so another live owner cannot block here.
		const currentMode = guard.query<{ journal_mode: string }, []>("PRAGMA journal_mode").get();
		const journalMode =
			currentMode?.journal_mode === "delete"
				? currentMode
				: guard.query<{ journal_mode: string }, []>("PRAGMA journal_mode = DELETE").get();
		if (journalMode?.journal_mode !== "delete") throw new Error("LCM recovery guard requires DELETE journal mode");
		const ownerTable = guard
			.query<{ name: string }, []>(
				"SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'lcm_owner_guard'",
			)
			.get();
		if (!ownerTable) guard.run("CREATE TABLE IF NOT EXISTS lcm_owner_guard (id INTEGER PRIMARY KEY)");
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
	#performance: LcmPerformanceCounters = {
		projectionCalls: 0,
		projectionWallMs: 0,
		projectionCpuMs: 0,
		projectionLineageRowsRead: 0,
		schedulerBranchPasses: 0,
	};

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

	hasRecordedRecovery(quarantinePath: string): boolean {
		return this.#readState().last_recovery_path === quarantinePath;
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
		this.flushRecoveryProvenance();
	}

	flushRecoveryProvenance(): void {
		const checkpoint = this.#db
			.query<{ busy: number; log: number; checkpointed: number }, []>("PRAGMA wal_checkpoint(FULL)")
			.get();
		if (checkpoint?.busy !== 0 || checkpoint.log !== checkpoint.checkpointed) {
			throw new Error("LCM recovery provenance checkpoint did not complete");
		}
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
		// Longest unchanged positional prefix, computed BEFORE branch_sources mutates.
		let unchangedPrefix = 0;
		while (unchangedPrefix < current.length && unchangedPrefix < snapshot.entries.length) {
			const row = current[unchangedPrefix]!;
			const entry = snapshot.entries[unchangedPrefix]!;
			if (
				row.position !== unchangedPrefix ||
				row.entry_id !== entry.entryId ||
				row.source_key !== entry.sourceKey ||
				!sameNullable(row.parent_entry_id, entry.parentId) ||
				!sameNullable(row.atomic_group_id, entry.atomicGroupId ?? null)
			) {
				break;
			}
			unchangedPrefix++;
		}

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
			this.#carryForwardSpans(branch.id, branch.revision, revision, unchangedPrefix);
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

	/**
	 * Move spans onto the new revision, keeping only those wholly inside the unchanged
	 * prefix and truncated to the greatest current atomic boundary within it. An append
	 * that extends the final atomic group therefore cannot preserve a leaf that now
	 * splits one indivisible unit.
	 */
	#carryForwardSpans(branchRowId: number, oldRevision: number, newRevision: number, unchangedPrefix: number): void {
		const units = atomicUnits(this.#activeRows(branchRowId));
		let safePrefix = 0;
		let boundary = 0;
		for (const unit of units) {
			boundary += unit.length;
			if (boundary > unchangedPrefix) break;
			safePrefix = boundary;
		}
		if (safePrefix > 0) {
			this.#db.run(
				`INSERT INTO branch_summary_spans
					(branch_row_id, revision, level, start_position, end_position, input_hash, summary_id, frontier)
				 SELECT branch_row_id, ?, level, start_position, end_position, input_hash, summary_id, 0
				 FROM branch_summary_spans
				 WHERE branch_row_id = ? AND revision = ? AND end_position <= ?
				 ON CONFLICT(branch_row_id, revision, level, start_position, end_position) DO NOTHING`,
				[newRevision, branchRowId, oldRevision, safePrefix],
			);
		}
		this.#db.run("DELETE FROM branch_summary_spans WHERE branch_row_id = ? AND revision = ?", [
			branchRowId,
			oldRevision,
		]);
		this.#rebuildFrontier(branchRowId, newRevision);
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

	#branchSpans(branchRowId: number, revision: number): SpanRow[] {
		return this.#db
			.query<SpanRow, [number, number]>(
				`SELECT level, start_position, end_position, input_hash, summary_id, frontier
				 FROM branch_summary_spans WHERE branch_row_id = ? AND revision = ?
				 ORDER BY start_position, level, end_position`,
			)
			.all(branchRowId, revision);
	}

	#insertSpan(
		branchRowId: number,
		revision: number,
		span: {
			level: number;
			start: number;
			end: number;
			inputHash: string;
			summaryId: string | null;
			frontier: boolean;
		},
	): void {
		this.#db.run(
			`INSERT INTO branch_summary_spans
				(branch_row_id, revision, level, start_position, end_position, input_hash, summary_id, frontier)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(branch_row_id, revision, level, start_position, end_position) DO UPDATE SET
				input_hash = excluded.input_hash,
				summary_id = COALESCE(excluded.summary_id, branch_summary_spans.summary_id),
				frontier = excluded.frontier`,
			[
				branchRowId,
				revision,
				span.level,
				span.start,
				span.end,
				span.inputHash,
				span.summaryId,
				span.frontier ? 1 : 0,
			],
		);
	}

	/** End of the contiguous frontier cover starting at position zero. */
	#frontierCoverEnd(spans: readonly SpanRow[]): number {
		let cursor = 0;
		for (;;) {
			const next = spans.find(span => span.frontier === 1 && span.start_position === cursor);
			if (!next) return cursor;
			cursor = next.end_position;
		}
	}

	/**
	 * Recompute frontier flags for one revision by walking from zero and taking the
	 * longest completed span at each cursor, falling back to an unresolved leaf, until
	 * the first gap. Used after copying spans across a revision advance.
	 */
	#rebuildFrontier(branchRowId: number, revision: number): void {
		this.#db.run("UPDATE branch_summary_spans SET frontier = 0 WHERE branch_row_id = ? AND revision = ?", [
			branchRowId,
			revision,
		]);
		const spans = this.#branchSpans(branchRowId, revision);
		let cursor = 0;
		for (;;) {
			const candidates = spans.filter(span => span.start_position === cursor);
			if (candidates.length === 0) return;
			const completed = candidates
				.filter(span => span.summary_id !== null)
				.sort((left, right) => right.end_position - left.end_position || right.level - left.level)[0];
			const chosen = completed ?? candidates.find(span => span.level === 0);
			if (!chosen) return;
			this.#db.run(
				`UPDATE branch_summary_spans SET frontier = 1
				 WHERE branch_row_id = ? AND revision = ? AND level = ? AND start_position = ? AND end_position = ?`,
				[branchRowId, revision, chosen.level, chosen.start_position, chosen.end_position],
			);
			cursor = chosen.end_position;
		}
	}

	#scheduleBranch(
		branchRowId: number,
		scope: ContextScope,
		revision: number,
		now: number,
		summarize?: Pick<ProjectionRequest, "tokenBudget" | "freshTail">,
	): ScheduleStats {
		this.#performance.schedulerBranchPasses++;
		const stats: ScheduleStats = { queued: 0, reused: 0 };
		const rows = this.#db
			.query<Pick<ActiveSourceRow, "source_key" | "token_count" | "atomic_group_id">, [number]>(
				`SELECT bs.source_key, sc.token_count, bs.atomic_group_id
				 FROM branch_sources bs JOIN source_contents sc ON sc.source_key = bs.source_key
				 WHERE bs.branch_row_id = ? AND bs.active = 1 ORDER BY bs.position`,
			)
			.all(branchRowId);
		if (rows.length === 0) return stats;

		this.#repairUnresolvedSpans(branchRowId, scope, revision, rows, now, stats);

		let tailStart = rows.length;
		if (summarize) {
			tailStart = selectFreshTail(
				rows,
				summarize.tokenBudget,
				summarize.freshTail.maxSources,
				summarize.freshTail.maxTokens,
			).start;
		}
		// An immutable leaf must never be split: pull the boundary back to the start of
		// any already-scheduled level-0 span the fresh tail would cut through.
		const existing = this.#branchSpans(branchRowId, revision);
		for (const span of existing) {
			if (span.level === 0 && span.start_position < tailStart && span.end_position > tailStart) {
				tailStart = span.start_position;
			}
		}
		if (tailStart <= 0) return stats;

		const coverEnd = this.#frontierCoverEnd(existing);
		if (coverEnd < tailStart) {
			const eligible = rows.slice(coverEnd, tailStart);
			let chunkStart = coverEnd;
			let chunk: typeof rows = [];
			let chunkTokens = 0;
			const flush = (): void => {
				if (chunk.length === 0) return;
				const lineage = chunk.map(row => row.source_key);
				const resolution = this.#resolveOrQueueJob({
					projectId: scope.projectId,
					branchRowId,
					revision,
					level: 0,
					inputs: lineage.map(id => ({ kind: "source" as const, id })),
					lineage,
					now,
					stats,
				});
				this.#insertSpan(branchRowId, revision, {
					level: 0,
					start: chunkStart,
					end: chunkStart + chunk.length,
					inputHash: resolution.inputHash,
					summaryId: resolution.node?.summaryId ?? null,
					frontier: true,
				});
				chunkStart += chunk.length;
				chunk = [];
				chunkTokens = 0;
			};
			for (const unit of atomicUnits(eligible)) {
				const unitTokens = unit.reduce((total, row) => total + row.token_count, 0);
				if (
					chunk.length > 0 &&
					(chunk.length + unit.length > this.#options.leafMaxSources ||
						chunkTokens + unitTokens > this.#options.leafMaxTokens)
				) {
					flush();
				}
				chunk.push(...unit);
				chunkTokens += unitTokens;
			}
			flush();
		}

		this.#advanceBranchFrontier(branchRowId, scope.projectId, revision, now, stats);
		return stats;
	}

	/**
	 * Restore a current span whose job vanished. The recomputed hash must equal the
	 * stored one; a mismatch leaves projection unready and fails `doctor()` rather than
	 * silently relabelling a span.
	 */
	#repairUnresolvedSpans(
		branchRowId: number,
		scope: ContextScope,
		revision: number,
		rows: readonly Pick<ActiveSourceRow, "source_key">[],
		now: number,
		stats: ScheduleStats,
	): void {
		const broken = this.#db
			.query<SpanRow, [number, number]>(
				`SELECT s.level, s.start_position, s.end_position, s.input_hash, s.summary_id, s.frontier
				 FROM branch_summary_spans s
				 WHERE s.branch_row_id = ? AND s.revision = ? AND s.summary_id IS NULL
				   AND NOT EXISTS (
					SELECT 1 FROM summary_jobs j
					WHERE j.input_hash = s.input_hash AND j.status IN ('pending', 'leased', 'failed')
				   )
				 ORDER BY s.level, s.start_position`,
			)
			.all(branchRowId, revision);
		for (const span of broken) {
			const inputs =
				span.level === 0
					? rows
							.slice(span.start_position, span.end_position)
							.map(row => ({ kind: "source" as const, id: row.source_key }))
					: this.#childSpanInputs(branchRowId, revision, span);
			if (!inputs) continue;
			const lineage = rows.slice(span.start_position, span.end_position).map(row => row.source_key);
			const resolution = this.#resolveOrQueueJob({
				projectId: scope.projectId,
				branchRowId,
				revision,
				level: span.level,
				inputs,
				lineage,
				now,
				stats,
			});
			if (resolution.inputHash !== span.input_hash) continue;
			if (resolution.node) {
				this.#db.run(
					`UPDATE branch_summary_spans SET summary_id = ?
					 WHERE branch_row_id = ? AND revision = ? AND level = ? AND start_position = ? AND end_position = ?`,
					[resolution.node.summaryId, branchRowId, revision, span.level, span.start_position, span.end_position],
				);
			}
		}
	}

	/** Ordered completed child summary ids exactly covering a parent span, or null. */
	#childSpanInputs(branchRowId: number, revision: number, span: SpanRow): JobInputSpec[] | null {
		const children = this.#db
			.query<SpanRow, [number, number, number, number, number]>(
				`SELECT level, start_position, end_position, input_hash, summary_id, frontier
				 FROM branch_summary_spans
				 WHERE branch_row_id = ? AND revision = ? AND level = ?
				   AND start_position >= ? AND end_position <= ?
				 ORDER BY start_position`,
			)
			.all(branchRowId, revision, span.level - 1, span.start_position, span.end_position);
		let cursor = span.start_position;
		const inputs: JobInputSpec[] = [];
		for (const child of children) {
			if (child.start_position !== cursor || child.summary_id === null) return null;
			inputs.push({ kind: "summary", id: child.summary_id });
			cursor = child.end_position;
		}
		return cursor === span.end_position && inputs.length > 1 ? inputs : null;
	}

	/**
	 * Collapse each same-level contiguous frontier run into disjoint exact-fan-in
	 * parents. Never condenses a partial or singleton group, so no synthetic root is
	 * manufactured and every parent is reproducible from its ordered children.
	 */
	#advanceBranchFrontier(
		branchRowId: number,
		projectId: string,
		revision: number,
		now: number,
		stats: ScheduleStats,
	): void {
		const fanIn = this.#options.condenseFanIn;
		for (let pass = 0; pass < 64; pass++) {
			const frontier = this.#branchSpans(branchRowId, revision).filter(span => span.frontier === 1);
			let progressed = false;
			let index = 0;
			while (index < frontier.length) {
				const level = frontier[index]!.level;
				let runEnd = index;
				while (
					runEnd + 1 < frontier.length &&
					frontier[runEnd + 1]!.level === level &&
					frontier[runEnd + 1]!.start_position === frontier[runEnd]!.end_position
				) {
					runEnd++;
				}
				const run = frontier.slice(index, runEnd + 1);
				index = runEnd + 1;
				for (let start = 0; start + fanIn <= run.length; start += fanIn) {
					const group = run.slice(start, start + fanIn);
					if (group.some(child => child.summary_id === null)) continue;
					const parentStart = group[0]!.start_position;
					const parentEnd = group.at(-1)!.end_position;
					const parentLevel = level + 1;
					const inputs = group.map(child => ({ kind: "summary" as const, id: child.summary_id! }));
					const existingParent = this.#db
						.query<{ summary_id: string | null; frontier: number }, [number, number, number, number, number]>(
							`SELECT summary_id, frontier FROM branch_summary_spans
							 WHERE branch_row_id = ? AND revision = ? AND level = ?
							   AND start_position = ? AND end_position = ?`,
						)
						.get(branchRowId, revision, parentLevel, parentStart, parentEnd);
					// A pending parent is not revisited until completion changes state, and an
					// already-collapsed one must not re-report progress or every pass would run.
					if (existingParent?.summary_id === null) continue;
					if (existingParent && existingParent.frontier === 1) continue;
					const lineage = this.#db
						.query<{ source_key: string }, [number, number, number]>(
							`SELECT source_key FROM branch_sources
							 WHERE branch_row_id = ? AND active = 1 AND position >= ? AND position < ?
							 ORDER BY position`,
						)
						.all(branchRowId, parentStart, parentEnd)
						.map(row => row.source_key);
					const resolution = this.#resolveOrQueueJob({
						projectId,
						branchRowId,
						revision,
						level: parentLevel,
						inputs,
						lineage,
						now,
						stats,
					});
					if (existingParent && resolution.node === null) continue;
					this.#insertSpan(branchRowId, revision, {
						level: parentLevel,
						start: parentStart,
						end: parentEnd,
						inputHash: resolution.inputHash,
						summaryId: resolution.node?.summaryId ?? null,
						frontier: resolution.node !== null,
					});
					if (resolution.node) {
						for (const child of group) {
							this.#db.run(
								`UPDATE branch_summary_spans SET frontier = 0
								 WHERE branch_row_id = ? AND revision = ? AND level = ?
								   AND start_position = ? AND end_position = ?`,
								[branchRowId, revision, child.level, child.start_position, child.end_position],
							);
						}
					}
					progressed = true;
				}
			}
			if (!progressed) return;
		}
	}

	#writeJobPayload(jobId: string, inputs: readonly JobInputSpec[], lineage: readonly string[]): void {
		for (let ordinal = 0; ordinal < inputs.length; ordinal++) {
			const input = inputs[ordinal];
			if (!input) continue;
			this.#db.run("INSERT INTO job_inputs (job_id, ordinal, input_kind, ref_id) VALUES (?, ?, ?, ?)", [
				jobId,
				ordinal,
				input.kind,
				input.id,
			]);
		}
		for (let ordinal = 0; ordinal < lineage.length; ordinal++) {
			this.#db.run("INSERT INTO job_lineage (job_id, ordinal, source_key) VALUES (?, ?, ?)", [
				jobId,
				ordinal,
				lineage[ordinal]!,
			]);
		}
	}

	#compactTerminalJob(jobId: string): void {
		this.#db.run("DELETE FROM job_inputs WHERE job_id = ?", [jobId]);
		this.#db.run("DELETE FROM job_lineage WHERE job_id = ?", [jobId]);
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
	}): SummaryResolution {
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
		const jobId = `job_${inputHash}`;
		if (summary) {
			params.stats.reused++;
			return {
				inputHash,
				jobId,
				node: { summaryId: summary.summary_id, level: summary.level, lineage: [...params.lineage] },
			};
		}

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
			this.#writeJobPayload(jobId, params.inputs, params.lineage);
			params.stats.queued++;
		} else if (existing.status === "obsolete" || existing.status === "completed") {
			this.#compactTerminalJob(jobId);
			this.#writeJobPayload(jobId, params.inputs, params.lineage);
			this.#db.run(
				`UPDATE summary_jobs SET status = 'pending', origin_branch_row_id = ?, origin_revision = ?,
					available_at = ?, result_summary_id = NULL, updated_at = ? WHERE job_id = ?`,
				[params.branchRowId, params.revision, params.now, params.now, jobId],
			);
			params.stats.queued++;
		}
		return { inputHash, jobId, node: null };
	}

	project(request: ProjectionRequest): ContextProjection {
		const startedWall = Bun.nanoseconds();
		const startedCpu = process.cpuUsage();
		try {
			return this.#projectInternal(request);
		} finally {
			const cpu = process.cpuUsage(startedCpu);
			this.#performance.projectionCalls++;
			this.#performance.projectionWallMs += (Bun.nanoseconds() - startedWall) / 1e6;
			this.#performance.projectionCpuMs += (cpu.user + cpu.system) / 1_000;
		}
	}

	#projectInternal(request: ProjectionRequest): ContextProjection {
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
		let tailStart = tail.start;
		const spans = this.#db
			.query<SpanRow & { stable_handle: string; redacted_text: string; token_count: number }, [number, number]>(
				`SELECT s.level, s.start_position, s.end_position, s.input_hash, s.summary_id, s.frontier,
					m.stable_handle, m.redacted_text, m.token_count
				 FROM branch_summary_spans s
				 JOIN summaries m ON m.summary_id = s.summary_id
				 WHERE s.branch_row_id = ? AND s.revision = ?
				 ORDER BY s.start_position, s.end_position DESC, s.level DESC`,
			)
			.all(branch.id, branch.revision);
		// A scheduled leaf is indivisible, so an enlarged fresh tail moves left rather
		// than cutting one; project-time fitting handles the oversized remainder.
		for (const span of spans) {
			if (span.level === 0 && span.start_position < tailStart && span.end_position > tailStart) {
				tailStart = span.start_position;
			}
		}
		const tailTokens = rows.slice(tailStart).reduce((total, row) => total + row.token_count, 0);
		const byStart = new Map<number, typeof spans>();
		for (const span of spans) {
			const group = byStart.get(span.start_position);
			if (group) group.push(span);
			else byStart.set(span.start_position, [span]);
		}
		for (const group of byStart.values()) {
			group.sort(
				(left, right) =>
					right.end_position - left.end_position ||
					right.level - left.level ||
					left.token_count - right.token_count ||
					left.summary_id!.localeCompare(right.summary_id!),
			);
		}

		const historical: ProjectedHistoricalItem[] = [];
		const selectedLevelCounts: Record<number, number> = {};
		let cursor = 0;
		let usedTokens = tailTokens;
		while (cursor < tailStart) {
			const selected = (byStart.get(cursor) ?? []).find(
				span =>
					span.end_position <= tailStart &&
					atomicBoundaries.has(span.end_position) &&
					usedTokens + span.token_count <= tokenBudget,
			);
			if (!selected) break;
			const coveredRows = rows.slice(selected.start_position, selected.end_position);
			historical.push({
				kind: "summary",
				summaryId: selected.summary_id!,
				summaryHandle: selected.stable_handle,
				level: selected.level,
				redactedText: selected.redacted_text,
				tokenCount: selected.token_count,
				sourceIds: coveredRows.map(source => source.entry_id),
				citations: coveredRows.map(source => this.#citation(source)),
			});
			selectedLevelCounts[selected.level] = (selectedLevelCounts[selected.level] ?? 0) + 1;
			cursor = selected.end_position;
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
			freshSourceCount: rows.length - tailStart,
			estimatedTokens: usedTokens,
			pendingJobs: this.#pendingSpanCount(branch.id, branch.revision, tailStart),
		};
	}

	/**
	 * Unresolved frontier spans intersecting the historical prefix. A pending parent
	 * whose completed children already cover its range is excluded: projection can use
	 * those children while condensation continues in the background.
	 */
	#pendingSpanCount(branchRowId: number, revision: number, tailStart: number): number {
		const unresolved = this.#db
			.query<SpanRow, [number, number, number]>(
				`SELECT level, start_position, end_position, input_hash, summary_id, frontier
				 FROM branch_summary_spans
				 WHERE branch_row_id = ? AND revision = ? AND summary_id IS NULL
				   AND frontier = 1 AND start_position < ?
				 ORDER BY start_position`,
			)
			.all(branchRowId, revision, tailStart);
		let count = 0;
		for (const span of unresolved) {
			if (span.level > 0 && this.#childSpanInputs(branchRowId, revision, span) !== null) continue;
			count++;
		}
		return count;
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
			const preferredBranchRowId = this.#preferredBranchRowId(preferredScope);
			const candidates = this.#db
				.query<Pick<JobRow, "job_id" | "project_id" | "input_hash" | "stage">, [number, number]>(
					`SELECT job_id, project_id, input_hash, stage FROM summary_jobs
					 WHERE available_at <= ? AND (
						status IN ('pending', 'failed') OR
						(status = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
					 )
					 ORDER BY level, created_at, job_id`,
				)
				.all(now, now);
			const preferred: Array<{ candidate: (typeof candidates)[number]; queueClass: "preferred" }> = [];
			const fallback: Array<{ candidate: (typeof candidates)[number]; queueClass: "fallback" }> = [];
			const obsolete: string[] = [];
			for (const candidate of candidates) {
				const placement = this.#jobSpanClass(candidate.project_id, candidate.input_hash, preferredBranchRowId);
				if (placement === "preferred") preferred.push({ candidate, queueClass: "preferred" });
				else if (placement === null) obsolete.push(candidate.job_id);
				else if (allowFallback) fallback.push({ candidate, queueClass: "fallback" });
			}
			for (const jobId of obsolete) {
				this.#db.run(
					`UPDATE summary_jobs SET status = 'obsolete', worker_id = NULL, lease_token = NULL,
						lease_expires_at = NULL, updated_at = ? WHERE job_id = ?`,
					[now, jobId],
				);
				this.#compactTerminalJob(jobId);
			}

			const claimed: SummaryJob[] = [];
			for (const { candidate, queueClass } of [...preferred, ...fallback]) {
				if (claimed.length >= limit) break;
				const inputs = this.#loadJobInputs(candidate.job_id);
				if (!inputs) {
					this.#db.run("UPDATE summary_jobs SET status = 'obsolete', updated_at = ? WHERE job_id = ?", [
						now,
						candidate.job_id,
					]);
					this.#compactTerminalJob(candidate.job_id);
					continue;
				}
				const inputTokenCount = inputs.reduce((total, input) => total + input.tokenCount, 0);
				const outputTokenBudget = outputBudgetForStage(candidate.stage, maxOutputTokens, inputTokenCount);
				if (outputTokenBudget < 1) {
					this.#db.run(
						"UPDATE summary_jobs SET status = 'obsolete', last_error = 'input too small to compress', updated_at = ? WHERE job_id = ?",
						[now, candidate.job_id],
					);
					this.#compactTerminalJob(candidate.job_id);
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
					this.#count("SELECT COUNT(*) AS count FROM job_lineage WHERE job_id = ?", candidate.job_id),
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
					this.#compactTerminalJob(candidate.job_id);
				}
			}
			return claimed;
		});
		return transaction.immediate();
	}

	nextSummaryJobDelayMs(preferredScope?: ContextScope, allowFallback = true): number | null {
		this.#assertAvailable();
		const scope = preferredScope === undefined ? undefined : normalizeScope(preferredScope);
		const preferredBranchRowId = this.#preferredBranchRowId(scope);
		const now = this.#options.now();
		const rows = this.#db
			.query<
				{
					job_id: string;
					project_id: string;
					input_hash: string;
					status: string;
					available_at: number;
					lease_expires_at: number | null;
				},
				[]
			>(
				`SELECT job_id, project_id, input_hash, status, available_at, lease_expires_at FROM summary_jobs
				 WHERE status IN ('pending', 'failed', 'leased')`,
			)
			.all();
		let availableAt: number | null = null;
		for (const row of rows) {
			const placement = this.#jobSpanClass(row.project_id, row.input_hash, preferredBranchRowId);
			if (placement === null) continue;
			if (placement === "fallback" && !allowFallback) continue;
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

	/**
	 * Where a content-addressed job is currently placed. `null` means no current-revision
	 * span in the project still wants it, so the job is obsolete. Retrieval authorization
	 * keeps using stored lineage; the frontier is never an authorization shortcut.
	 */
	#jobSpanClass(
		projectId: string,
		inputHash: string,
		preferredBranchRowId: number | undefined,
	): "preferred" | "fallback" | null {
		const row = this.#db
			.query<{ total: number; preferred: number }, [number, string, string]>(
				`SELECT COUNT(*) AS total,
					MAX(CASE WHEN s.branch_row_id = ? THEN 1 ELSE 0 END) AS preferred
				 FROM branch_summary_spans s
				 JOIN branches b ON b.id = s.branch_row_id AND b.revision = s.revision
				 WHERE b.project_id = ? AND s.input_hash = ? AND s.summary_id IS NULL`,
			)
			.get(preferredBranchRowId ?? -1, projectId, inputHash);
		if (!row || row.total === 0) return null;
		return row.preferred === 1 ? "preferred" : "fallback";
	}

	#preferredBranchRowId(scope?: ContextScope): number | undefined {
		if (!scope) return undefined;
		return this.#db
			.query<Pick<BranchRow, "id">, [string, string, string]>(
				"SELECT id FROM branches WHERE project_id = ? AND session_id = ? AND branch_id = ?",
			)
			.get(scope.projectId, scope.sessionId, scope.branchId)?.id;
	}

	/** Whether any current-revision span in the project still references this summary. */
	#summaryPlacedInProject(projectId: string, summaryId: string): boolean {
		return (
			this.#count(
				`SELECT COUNT(*) AS count FROM branch_summary_spans s
				 JOIN branches b ON b.id = s.branch_row_id AND b.revision = s.revision
				 WHERE b.project_id = ? AND s.summary_id = ?`,
				projectId,
				summaryId,
			) > 0
		);
	}

	#jobLineage(jobId: string): string[] {
		return this.#db
			.query<{ source_key: string }, [string]>(
				"SELECT source_key FROM job_lineage WHERE job_id = ? ORDER BY ordinal",
			)
			.all(jobId)
			.map(row => row.source_key);
	}

	summaryJobFailures(preferredScope?: ContextScope): readonly {
		jobId: string;
		availableAt: number;
		queueClass: "preferred" | "fallback";
	}[] {
		this.#assertAvailable();
		const scope = preferredScope === undefined ? undefined : normalizeScope(preferredScope);
		const preferredBranchRowId = this.#preferredBranchRowId(scope);
		const rows = this.#db
			.query<{ job_id: string; project_id: string; input_hash: string; available_at: number }, []>(
				`SELECT job_id, project_id, input_hash, available_at FROM summary_jobs
				 WHERE status = 'failed' ORDER BY available_at, job_id`,
			)
			.all();
		const failures: Array<{
			jobId: string;
			availableAt: number;
			queueClass: "preferred" | "fallback";
		}> = [];
		for (const row of rows) {
			const placement = this.#jobSpanClass(row.project_id, row.input_hash, preferredBranchRowId);
			if (placement === null) continue;
			failures.push({ jobId: row.job_id, availableAt: row.available_at, queueClass: placement });
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
		const attempt = completion.attempt;
		if (attempt) assertProviderAttempt(attempt, "completion.attempt");
		const markObsolete = (): void => {
			this.#db.run(
				`UPDATE summary_jobs SET status = 'obsolete', worker_id = NULL, lease_token = NULL,
					lease_expires_at = NULL, updated_at = ? WHERE job_id = ? AND lease_token = ?`,
				[now, jobId, leaseToken],
			);
			this.#compactTerminalJob(jobId);
		};
		// Settles before any job mutation so a late billed response never stays
		// `in_flight`. A non-null result means the settler overrode the branch intent.
		const settle = (requested: SummaryAttemptOutcome): CompleteSummaryJobResult | null => {
			if (!attempt) return null;
			const settled = this.#settleAttempt(jobId, leaseToken, attempt, requested, now);
			if (settled === requested) return null;
			if (settled === "stale") {
				markObsolete();
				return { accepted: false, reason: "stale" };
			}
			return { accepted: false, reason: "lease_lost" };
		};
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
				return settle("lease_lost") ?? { accepted: false, reason: "lease_lost" };
			}
			const strategy = strategyForStage(job.stage);
			if (provenance && provenance.strategy !== strategy) {
				throw new TypeError(`completion provenance strategy ${provenance.strategy} does not match ${strategy}`);
			}
			const promptHash = provenance?.promptHash ?? null;
			const modelSelector = provenance?.modelSelector ? boundedDiagnostic(provenance.modelSelector) : null;
			const resolvedModel = provenance?.resolvedModel ? boundedDiagnostic(provenance.resolvedModel) : null;
			const lineage = this.#jobLineage(jobId);
			if (!this.#jobPlacementActive(jobId, job.project_id)) {
				const overridden = settle("stale");
				if (overridden) return overridden;
				markObsolete();
				return { accepted: false, reason: "stale" };
			}
			if (tokenCount >= job.lease_input_tokens || tokenCount > job.lease_output_budget) {
				const overridden = settle("non_compressing");
				if (overridden) return overridden;
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
				this.#compactTerminalJob(jobId);
				return { accepted: false, reason: "deterministic_failed" };
			}

			const settledCompletion = settle("completed");
			if (settledCompletion) return settledCompletion;
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
			this.#compactTerminalJob(jobId);
			this.#fillCompletedSpans(job.project_id, job.input_hash, summary.summary_id, jobInputs, now);
			return { accepted: true, summaryId: summary.summary_id };
		});
		return transaction.immediate();
	}

	/**
	 * Attach a finished summary to every current-revision span that requested it and
	 * advance only the branches actually affected. An old-revision or mismatched parent
	 * stays non-frontier; the next reconcile repairs it.
	 */
	#fillCompletedSpans(
		projectId: string,
		inputHash: string,
		summaryId: string,
		jobInputs: readonly JobInputRow[],
		now: number,
	): void {
		const matches = this.#db
			.query<
				{ branch_row_id: number; revision: number; level: number; start_position: number; end_position: number },
				[string, string]
			>(
				`SELECT s.branch_row_id, s.revision, s.level, s.start_position, s.end_position
				 FROM branch_summary_spans s
				 JOIN branches b ON b.id = s.branch_row_id AND b.revision = s.revision
				 WHERE b.project_id = ? AND s.input_hash = ? AND s.summary_id IS NULL
				 ORDER BY s.branch_row_id, s.level, s.start_position`,
			)
			.all(projectId, inputHash);
		if (matches.length === 0) return;
		const orderedInputs = jobInputs.filter(input => input.input_kind === "summary").map(input => input.ref_id);
		const affected = new Map<number, number>();
		for (const match of matches) {
			const span: SpanRow = {
				level: match.level,
				start_position: match.start_position,
				end_position: match.end_position,
				input_hash: inputHash,
				summary_id: null,
				frontier: 0,
			};
			let promote = match.level === 0;
			if (match.level > 0) {
				const children = this.#childSpanInputs(match.branch_row_id, match.revision, span);
				promote =
					children !== null &&
					children.length === orderedInputs.length &&
					children.every((child, index) => child.id === orderedInputs[index]);
			}
			this.#db.run(
				`UPDATE branch_summary_spans SET summary_id = ?, frontier = ?
				 WHERE branch_row_id = ? AND revision = ? AND level = ? AND start_position = ? AND end_position = ?`,
				[
					summaryId,
					promote ? 1 : 0,
					match.branch_row_id,
					match.revision,
					match.level,
					match.start_position,
					match.end_position,
				],
			);
			if (promote && match.level > 0) {
				this.#db.run(
					`UPDATE branch_summary_spans SET frontier = 0
					 WHERE branch_row_id = ? AND revision = ? AND level = ?
					   AND start_position >= ? AND end_position <= ?`,
					[match.branch_row_id, match.revision, match.level - 1, match.start_position, match.end_position],
				);
			}
			if (promote) affected.set(match.branch_row_id, match.revision);
		}
		const stats: ScheduleStats = { queued: 0, reused: 0 };
		for (const [branchRowId, revision] of affected) {
			this.#advanceBranchFrontier(branchRowId, projectId, revision, now, stats);
		}
	}

	failSummaryJob(
		jobId: string,
		leaseToken: string,
		redactedError: string,
		retryDelayMs: number,
		provenance?: SummaryAttemptProvenance,
		failedAttempt?: { attempt: SummaryProviderAttempt; outcome: SummaryFailureAttemptOutcome },
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
		if (failedAttempt) assertProviderAttempt(failedAttempt.attempt, "failedAttempt.attempt");
		const now = this.#options.now();
		const applyFailure = (): boolean => {
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
		};
		if (!failedAttempt) return applyFailure();
		// Only the requested provider failure retries: a superseded or re-leased job
		// records its billed attempt without resurrecting obsolete work.
		const transaction = this.#db.transaction((): boolean => {
			const settled = this.#settleAttempt(jobId, leaseToken, failedAttempt.attempt, failedAttempt.outcome, now);
			return settled === failedAttempt.outcome && applyFailure();
		});
		return transaction.immediate();
	}

	/**
	 * Whether this job is still wanted by a current-revision span. Retrieval
	 * authorization keeps using stored lineage; this is scheduling and billing only.
	 */
	#jobPlacementActive(jobId: string, projectId: string): boolean {
		const inputHash = this.#db
			.query<{ input_hash: string }, [string]>("SELECT input_hash FROM summary_jobs WHERE job_id = ?")
			.get(jobId)?.input_hash;
		return inputHash !== undefined && this.#jobSpanClass(projectId, inputHash, undefined) !== null;
	}

	beginSummaryAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttemptStart,
		provenance: SummaryAttemptProvenance,
	): boolean {
		this.#assertAvailable();
		assertIdentifier(jobId, "jobId");
		assertIdentifier(leaseToken, "leaseToken");
		assertProviderAttemptStart(attempt, "attempt");
		assertIdentifier(provenance.promptHash, "provenance.promptHash");
		if (provenance.modelSelector !== undefined)
			assertIdentifier(provenance.modelSelector, "provenance.modelSelector");
		if (provenance.resolvedModel !== undefined)
			assertIdentifier(provenance.resolvedModel, "provenance.resolvedModel");
		const now = this.#options.now();
		const transaction = this.#db.transaction((): boolean => {
			const job = this.#db
				.query<
					Pick<JobRow, "project_id" | "input_hash" | "status" | "lease_token" | "lease_expires_at" | "stage"> & {
						attempt_count: number;
					},
					[string]
				>(
					`SELECT project_id, input_hash, status, lease_token, lease_expires_at, stage, attempt_count
					 FROM summary_jobs WHERE job_id = ?`,
				)
				.get(jobId);
			if (
				job?.status !== "leased" ||
				job.lease_token !== leaseToken ||
				job.lease_expires_at === null ||
				job.lease_expires_at <= now
			) {
				return false;
			}
			if (!this.#jobPlacementActive(jobId, job.project_id)) return false;
			const inserted = this.#db.run(
				`INSERT INTO summary_attempts
					(attempt_id, job_id, project_id, input_hash, attempt_count, started_at, outcome,
					 model_selector, provider, model, stage, strategy, prompt_hash)
				 VALUES (?, ?, ?, ?, ?, ?, 'in_flight', ?, ?, ?, ?, ?, ?)
				 ON CONFLICT(attempt_id) DO NOTHING`,
				[
					attempt.attemptId,
					jobId,
					job.project_id,
					job.input_hash,
					Math.max(1, job.attempt_count),
					attempt.startedAt,
					provenance.modelSelector ? boundedDiagnostic(provenance.modelSelector) : null,
					boundedDiagnostic(attempt.provider),
					boundedDiagnostic(attempt.model),
					job.stage,
					provenance.strategy,
					provenance.promptHash,
				],
			);
			return Number(inserted.changes) > 0;
		});
		return transaction.immediate();
	}

	settleSummaryAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttempt,
		requestedOutcome: SummaryLocalAttemptOutcome,
	): SummaryAttemptOutcome | null {
		this.#assertAvailable();
		assertIdentifier(jobId, "jobId");
		assertIdentifier(leaseToken, "leaseToken");
		assertProviderAttempt(attempt, "attempt");
		const now = this.#options.now();
		const transaction = this.#db.transaction((): SummaryAttemptOutcome | null =>
			this.#settleAttempt(jobId, leaseToken, attempt, requestedOutcome, now),
		);
		return transaction.immediate();
	}

	/**
	 * Finish one `in_flight` ledger row inside the caller's transaction. A lost lease
	 * outranks a removed placement, which outranks the requested outcome, so billed
	 * cost is never attributed to a successor attempt. Missing or already-terminal
	 * rows return null and mutate nothing.
	 */
	#settleAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttempt,
		requestedOutcome: SummaryAttemptOutcome,
		now: number,
	): SummaryAttemptOutcome | null {
		const row = this.#db
			.query<AttemptRow, [string]>(
				"SELECT job_id, project_id, outcome, started_at FROM summary_attempts WHERE attempt_id = ?",
			)
			.get(attempt.attemptId);
		if (!row || row.job_id !== jobId || row.outcome !== "in_flight") return null;
		const job = this.#db
			.query<Pick<JobRow, "project_id" | "status" | "lease_token" | "lease_expires_at">, [string]>(
				"SELECT project_id, status, lease_token, lease_expires_at FROM summary_jobs WHERE job_id = ?",
			)
			.get(jobId);
		let outcome: SummaryAttemptOutcome;
		if (
			job?.status !== "leased" ||
			job.lease_token !== leaseToken ||
			job.lease_expires_at === null ||
			job.lease_expires_at <= now
		) {
			outcome = "lease_lost";
		} else if (!this.#jobPlacementActive(jobId, job.project_id)) {
			outcome = "stale";
		} else {
			outcome = requestedOutcome;
		}
		const changed = this.#db.run(
			`UPDATE summary_attempts SET completed_at = ?, outcome = ?,
				input_tokens = ?, output_tokens = ?, cache_read_tokens = ?, cache_write_tokens = ?, total_tokens = ?,
				orchestration_input_tokens = ?, orchestration_cache_read_tokens = ?, orchestration_output_tokens = ?,
				reasoning_tokens = ?, premium_requests = ?, cache_write_5m_tokens = ?, cache_write_1h_tokens = ?,
				server_web_search_requests = ?, server_web_fetch_requests = ?,
				cost_input = ?, cost_output = ?, cost_cache_read = ?, cost_cache_write = ?, cost_total = ?
			 WHERE attempt_id = ? AND job_id = ? AND outcome = 'in_flight'`,
			[
				Math.max(row.started_at, attempt.completedAt),
				outcome,
				...attemptUsageBindings(attempt.usage),
				attempt.attemptId,
				jobId,
			],
		);
		return Number(changed.changes) > 0 ? outcome : null;
	}

	search(request: SearchRequest): SearchHit[] {
		this.#assertAvailable();
		const scope = normalizeScope(request);
		const limit = Math.min(assertInteger(request.limit ?? 20, "limit", 1), MAX_SEARCH_LIMIT);
		const offset = Math.min(assertInteger(request.offset ?? 0, "offset", 0), MAX_SEARCH_OFFSET);
		const match = this.#ftsMatch(request.query);
		if (!match) return [];
		const transaction = this.#db.transaction((): SearchHit[] => {
			const branchRows = this.#activeSourceRows(scope);
			if (branchRows.length === 0) return [];
			let scopedLineage: string[] | undefined;
			let scopedSummaryId: string | undefined;
			if (request.summaryHandle !== undefined) {
				const handle = assertIdentifier(request.summaryHandle, "summaryHandle");
				const root = this.#summaryByHandle(scope.projectId, handle);
				if (!root) return [];
				scopedLineage = this.#summaryLineage(root.summary_id);
				scopedSummaryId = root.summary_id;
				if (findAlignedSequence(branchRows, scopedLineage) < 0) return [];
			}
			const hits: SearchHit[] = [];
			for (const document of this.#searchDocuments(scope.projectId, match, offset + limit, scope, scopedSummaryId)) {
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
		});
		return transaction.deferred();
	}

	searchProject(request: ProjectSearchRequest): SearchHit[] {
		this.#assertAvailable();
		const projectId = assertIdentifier(request.projectId, "projectId");
		const limit = Math.min(assertInteger(request.limit ?? 20, "limit", 1), MAX_SEARCH_LIMIT);
		const offset = Math.min(assertInteger(request.offset ?? 0, "offset", 0), MAX_SEARCH_OFFSET);
		const match = this.#ftsMatch(request.query);
		if (!match) return [];
		const transaction = this.#db.transaction((): SearchHit[] => {
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
		});
		return transaction.deferred();
	}

	#searchDocuments(
		projectId: string,
		match: string,
		requested: number,
		scope?: ContextScope,
		scopedSummaryId?: string,
	): SearchDocumentRow[] {
		const sessionId = scope?.sessionId ?? null;
		const branchId = scope?.branchId ?? null;
		const summaryId = scopedSummaryId ?? null;
		const candidateLimit = Math.min(requested, MAX_SEARCH_CANDIDATES);
		return this.#db
			.query<
				SearchDocumentRow,
				[
					string,
					string | null,
					string | null,
					string | null,
					string,
					string,
					string | null,
					string | null,
					string | null,
					string | null,
					number,
				]
			>(
				`WITH requested_branches AS (
					SELECT id FROM branches
					WHERE project_id = ? AND (? IS NULL OR (session_id = ? AND branch_id = ?))
				 )
				 SELECT d.document_kind, d.ref_id, d.redacted_text, bm25(search_fts) AS rank
				 FROM search_fts JOIN search_documents d ON d.id = search_fts.rowid
				 WHERE search_fts MATCH ? AND d.project_id = ? AND (
					(d.document_kind = 'source'
					 AND EXISTS (
						SELECT 1 FROM requested_branches rb
						JOIN branch_sources bs ON bs.branch_row_id = rb.id AND bs.active = 1
						WHERE bs.source_key = d.ref_id
					 )
					 AND (? IS NULL OR EXISTS (
						SELECT 1 FROM summary_lineage root_source
						WHERE root_source.summary_id = ? AND root_source.source_key = d.ref_id
					 )))
					OR
					(d.document_kind = 'summary'
					 AND EXISTS (
						SELECT 1 FROM requested_branches rb
						JOIN summary_lineage first_lineage
						  ON first_lineage.summary_id = d.ref_id AND first_lineage.ordinal = 0
						JOIN branch_sources start_source
						  ON start_source.branch_row_id = rb.id AND start_source.active = 1
						 AND start_source.source_key = first_lineage.source_key
						WHERE NOT EXISTS (
							SELECT 1 FROM summary_lineage candidate_lineage
							LEFT JOIN branch_sources placed_source
							  ON placed_source.branch_row_id = rb.id AND placed_source.active = 1
							 AND placed_source.position = start_source.position + candidate_lineage.ordinal
							 AND placed_source.source_key = candidate_lineage.source_key
							WHERE candidate_lineage.summary_id = d.ref_id AND placed_source.id IS NULL
						)
						AND NOT EXISTS (
							SELECT 1 FROM branch_sources atomic_source
							WHERE atomic_source.branch_row_id = rb.id AND atomic_source.active = 1
							  AND atomic_source.atomic_group_id IS NOT NULL
							GROUP BY atomic_source.atomic_group_id
							HAVING (
								start_source.position BETWEEN MIN(atomic_source.position) + 1 AND MAX(atomic_source.position)
								OR start_source.position + (
									SELECT COUNT(*) FROM summary_lineage lineage_size
									WHERE lineage_size.summary_id = d.ref_id
								) BETWEEN MIN(atomic_source.position) + 1 AND MAX(atomic_source.position)
							)
						)
					 )
					 AND (? IS NULL OR EXISTS (
						SELECT 1 FROM summary_lineage root_start
						WHERE root_start.summary_id = ? AND NOT EXISTS (
							SELECT 1 FROM summary_lineage candidate_lineage
							LEFT JOIN summary_lineage root_lineage
							  ON root_lineage.summary_id = root_start.summary_id
							 AND root_lineage.ordinal = root_start.ordinal + candidate_lineage.ordinal
							 AND root_lineage.source_key = candidate_lineage.source_key
							WHERE candidate_lineage.summary_id = d.ref_id AND root_lineage.summary_id IS NULL
						)
					 )))
				 )
				 ORDER BY rank, d.id LIMIT ?`,
			)
			.all(
				projectId,
				sessionId,
				sessionId,
				branchId,
				match,
				projectId,
				summaryId,
				summaryId,
				summaryId,
				summaryId,
				candidateLimit,
			);
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
		const databasePath = databaseFilePath(this.#dbPath);
		const quarantineUnits = databasePath ? databaseQuarantineUnits(databasePath) : [];
		const recovery = this.#db
			.query<RecoveryEventRow, []>(
				"SELECT reason, created_at FROM recovery_events ORDER BY created_at DESC, id DESC LIMIT 1",
			)
			.get();
		return {
			schemaVersion: schema?.user_version ?? 0,
			journalMode: journal?.journal_mode ?? "unknown",
			quarantined: state.quarantined_at !== null,
			storage: {
				databaseBytes: databasePath ? regularFileBytes(databasePath) : 0,
				walBytes: databasePath ? regularFileBytes(`${databasePath}-wal`) : 0,
				quarantineBytes: quarantineUnits.reduce(
					(total, unit) => total + unit.files.reduce((unitTotal, file) => unitTotal + file.bytes, 0),
					0,
				),
			},
			latestRecovery: recovery
				? { occurredAt: recovery.created_at, category: recoveryCategory(recovery.reason) }
				: null,
			branches: this.#count("SELECT COUNT(*) AS count FROM branches"),
			activeSources: this.#count("SELECT COUNT(*) AS count FROM branch_sources WHERE active = 1"),
			tombstones: this.#count("SELECT COUNT(*) AS count FROM branch_sources WHERE active = 0"),
			leafSummaries: this.#count("SELECT COUNT(*) AS count FROM summaries WHERE level = 0"),
			condensedSummaries: this.#count("SELECT COUNT(*) AS count FROM summaries WHERE level > 0"),
			jobs: jobCounts,
			performance: { ...this.#performance },
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
		check("branch-summary-spans", () => {
			const overlap = this.#db
				.query<{ branch_row_id: number }, []>(
					`SELECT a.branch_row_id FROM branch_summary_spans a
					 JOIN branches b ON b.id = a.branch_row_id AND b.revision = a.revision
					 JOIN branch_summary_spans c
					   ON c.branch_row_id = a.branch_row_id AND c.revision = a.revision AND c.frontier = 1
					  AND (c.level <> a.level OR c.start_position <> a.start_position OR c.end_position <> a.end_position)
					  AND c.start_position < a.end_position AND c.end_position > a.start_position
					 WHERE a.frontier = 1 LIMIT 1`,
				)
				.get();
			if (overlap) return `branch ${overlap.branch_row_id} has an overlapping frontier`;
			const mismatch = this.#db
				.query<{ summary_id: string; expected: number; actual: number }, []>(
					`SELECT s.summary_id,
						(s.end_position - s.start_position) AS expected,
						(SELECT COUNT(*) FROM summary_lineage l WHERE l.summary_id = s.summary_id) AS actual
					 FROM branch_summary_spans s
					 JOIN branches b ON b.id = s.branch_row_id AND b.revision = s.revision
					 WHERE s.summary_id IS NOT NULL
					   AND (s.end_position - s.start_position) <>
						(SELECT COUNT(*) FROM summary_lineage l WHERE l.summary_id = s.summary_id)
					 LIMIT 1`,
				)
				.get();
			if (mismatch) {
				return `summary ${mismatch.summary_id} covers ${mismatch.expected} positions but has ${mismatch.actual} lineage rows`;
			}
			const gap = this.#db
				.query<{ branch_row_id: number; start_position: number }, []>(
					`SELECT s.branch_row_id, s.start_position FROM branch_summary_spans s
					 JOIN branches b ON b.id = s.branch_row_id AND b.revision = s.revision
					 WHERE s.frontier = 1 AND s.start_position > 0 AND NOT EXISTS (
						SELECT 1 FROM branch_summary_spans p
						WHERE p.branch_row_id = s.branch_row_id AND p.revision = s.revision
						  AND p.frontier = 1 AND p.end_position = s.start_position
					 ) LIMIT 1`,
				)
				.get();
			return gap ? `branch ${gap.branch_row_id} frontier has a gap before position ${gap.start_position}` : null;
		});
		check("quarantine", () => (this.#readState().quarantined_at === null ? null : "store is quarantined"));
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
			this.#db.run("DELETE FROM branch_summary_spans");
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
					this.#jobPlacementActive(job.job_id, job.project_id)
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
				if (this.#summaryPlacedInProject(summary.project_id, summary.summary_id)) continue;
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
			return { tombstones, jobs, summaries, sourceContents, files, quarantineFiles: 0, quarantineBytes: 0 };
		});
		const result = transaction.immediate();
		const databasePath = databaseRecoveryFilePath(this.#dbPath);
		if (!databasePath) return result;
		const pendingPath = pendingDatabaseQuarantineTarget(databasePath);
		if (pendingPath === null) return result;
		const latestRecoveryPath = this.#readState().last_recovery_path;
		const latestRecoveryEventPath = this.#db
			.query<{ quarantine_path: string }, []>(
				"SELECT quarantine_path FROM recovery_events ORDER BY created_at DESC, id DESC LIMIT 1",
			)
			.get()?.quarantine_path;
		const quarantineCutoff = now - DEFAULT_QUARANTINE_RETENTION_MS;
		let quarantineFiles = 0;
		let quarantineBytes = 0;
		for (const unit of databaseQuarantineUnits(databasePath)) {
			if (
				unit.timestamp >= quarantineCutoff ||
				unit.path === pendingPath ||
				unit.path === latestRecoveryPath ||
				unit.path === latestRecoveryEventPath
			) {
				continue;
			}
			const files = [...unit.files].sort(
				(left, right) => Number(left.path === unit.path) - Number(right.path === unit.path),
			);
			for (const file of files) {
				try {
					unlinkSync(file.path);
					quarantineFiles++;
					quarantineBytes += file.bytes;
				} catch {
					break;
				}
			}
		}
		return { ...result, quarantineFiles, quarantineBytes };
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
		const databasePath = databaseFilePath(dbPath);
		if (holdOwnerGuard && databasePath && existsSync(pendingDatabaseQuarantinePath(databasePath))) {
			throw new PendingDatabaseQuarantineError(`LCM database quarantine recovery is pending: ${databasePath}`);
		}
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
	for (;;) {
		try {
			return await createSqliteLcmContextWithRetry(options.dbPath, normalized, options.recoverCorrupt === true);
		} catch (error) {
			const databasePath = databaseRecoveryFilePath(options.dbPath);
			const pendingFailure = error instanceof PendingDatabaseQuarantineError;
			if (pendingFailure) {
				if (!options.recoverCorrupt || !databasePath) throw error;
			} else if (
				!options.recoverCorrupt ||
				!databasePath ||
				error instanceof UnsupportedLcmSchemaError ||
				!isLcmSqliteCorruptionError(error)
			) {
				throw error;
			}
			let completedRecovery = false;
			// Recheck or rebuild under the exclusive guard, but close the main handle before releasing it.
			await withDatabaseRecoveryLock(`${databasePath}.recovery-lock`, normalized.busyTimeoutMs, async () => {
				let context: SqliteLcmContext | undefined;
				try {
					const pending = await recoverPendingDatabaseQuarantine(databasePath);
					if (pending) {
						context = await createSqliteLcmContextWithRetry(options.dbPath, normalized, true, false);
						if (!context.hasRecordedRecovery(pending.quarantinePath)) {
							context.recordRecovery(pending.quarantinePath, pending.reason);
						} else {
							context.flushRecoveryProvenance();
						}
						await removePendingDatabaseQuarantine(databasePath);
						completedRecovery = true;
						return;
					}
					if (pendingFailure) return;
					try {
						context = await createSqliteLcmContextWithRetry(options.dbPath, normalized, true, false);
					} catch (currentError) {
						if (currentError instanceof UnsupportedLcmSchemaError || !isLcmSqliteCorruptionError(currentError)) {
							throw currentError;
						}
						const reason = String(currentError);
						const quarantinePath = await quarantineDatabaseFiles(databasePath, normalized.now(), reason);
						context = await createSqliteLcmContextWithRetry(options.dbPath, normalized, true, false);
						context.recordRecovery(quarantinePath, reason);
						await removePendingDatabaseQuarantine(databasePath);
					}
					completedRecovery = true;
				} finally {
					context?.close();
				}
			});
			if (!completedRecovery) continue;
			// Reopen through the normal path so the returned context retains its own shared guard.
			return await createSqliteLcmContextWithRetry(options.dbPath, normalized, true);
		}
	}
}
