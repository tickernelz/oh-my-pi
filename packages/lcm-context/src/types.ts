export interface ContextScope {
	projectId: string;
	sessionId: string;
	branchId: string;
}

/**
 * A committed journal entry normalized by the caller.
 *
 * `redactedText` is the only source text accepted by this package. The exact
 * journal entry remains caller-owned and is never persisted in the LCM store.
 */
export interface SourceEntry extends ContextScope {
	entryId: string;
	parentId: string | null;
	timestamp: number;
	kind: string;
	/** Entries sharing an id form one indivisible projection closure (for example, a tool call and all parallel results). */
	atomicGroupId?: string;
	redactedText: string;
	contentHash: string;
	artifactRefs: readonly string[];
}

export interface SourceSnapshot {
	scope: ContextScope;
	/** Complete, committed branch history in projection order. */
	entries: readonly SourceEntry[];
}

export interface LeafChunkPolicy {
	maxSources: number;
	maxTokens: number;
}

export interface LcmContextOptions {
	/** Caller-owned SQLite path. `:memory:` is supported for isolated use. */
	dbPath: string;
	busyTimeoutMs?: number;
	tombstoneRetentionMs?: number;
	leafChunk?: Partial<LeafChunkPolicy>;
	condenseFanIn?: number;
	/** Move an unreadable database aside and create a rebuildable empty store. */
	recoverCorrupt?: boolean;
	now?: () => number;
}

export interface ReconcileResult {
	changed: boolean;
	revision: number;
	activeSources: number;
	insertedSources: number;
	tombstonedSources: number;
	queuedJobs: number;
	reusedSummaries: number;
}

export interface FreshTailLimits {
	/** Target source count; an indivisible `atomicGroupId` closure may expand it. */
	maxSources: number;
	/** Hard fresh-tail token ceiling, including any atomic closure expansion. */
	maxTokens: number;
}

export interface ProjectionRequest extends ContextScope {
	/** Total estimated-token ceiling across summaries and the fresh tail. */
	tokenBudget: number;
	freshTail: FreshTailLimits;
}

export interface Citation extends ContextScope {
	sourceId: string;
	sourceKey: string;
	contentHash: string;
	position: number;
}

export interface ProjectedHistoricalItem {
	kind: "summary";
	summaryId: string;
	level: number;
	redactedText: string;
	tokenCount: number;
	sourceIds: readonly string[];
	citations: readonly Citation[];
}

export interface ContextProjection {
	revision: number;
	/** False means the caller must fail open to its native history path. */
	ready: boolean;
	historical: readonly ProjectedHistoricalItem[];
	freshTailSourceIds: readonly string[];
	uncoveredSourceIds: readonly string[];
	estimatedTokens: number;
	pendingJobs: number;
}

export interface SummaryJobInput {
	kind: "source" | "summary";
	id: string;
	redactedText: string;
	tokenCount: number;
}

export interface SummaryJob {
	jobId: string;
	leaseToken: string;
	leaseExpiresAt: number;
	kind: "leaf" | "condensed";
	level: number;
	inputs: readonly SummaryJobInput[];
	sourceCount: number;
	inputTokenCount: number;
	outputTokenBudget: number;
}

export interface ClaimSummaryJobsOptions {
	workerId: string;
	leaseMs: number;
	limit: number;
	/** Hard output ceiling requested from the completion callback. */
	maxOutputTokens: number;
}

export interface SummaryCompletion {
	/** Caller-redacted summary text. */
	redactedText: string;
	/** Optional provider measurement; the package also measures text locally and uses the larger value. */
	tokenCount?: number;
}

export type SummaryCompletionCallback = (job: SummaryJob) => Promise<SummaryCompletion>;

export type CompleteSummaryJobResult =
	| { accepted: true; summaryId: string }
	| { accepted: false; reason: "lease_lost" | "stale" | "not_compressed" };

export interface RunSummaryJobsOptions extends ClaimSummaryJobsOptions {
	retryDelayMs: number;
}

export interface RunSummaryJobsResult {
	claimed: number;
	completed: number;
	failed: number;
	stale: number;
}

export interface SearchRequest extends ContextScope {
	query: string;
	limit?: number;
}

export interface ProjectSearchRequest {
	projectId: string;
	query: string;
	limit?: number;
}

export interface SearchHit {
	kind: "source" | "summary";
	id: string;
	redactedText: string;
	/** SQLite FTS5 BM25 rank; lower values are better. */
	rank: number;
	citations: readonly Citation[];
}

export interface SourceDescription extends Citation {
	parentId: string | null;
	timestamp: number;
	kind: string;
	atomicGroupId: string | null;
	redactedText: string;
	artifactRefs: readonly string[];
}

export interface JobStatusCounts {
	pending: number;
	leased: number;
	failed: number;
	completed: number;
	obsolete: number;
}

export interface LcmStatus {
	dbPath: string;
	schemaVersion: number;
	journalMode: string;
	quarantined: boolean;
	quarantineReason: string | null;
	recoveredFrom: string | null;
	branches: number;
	activeSources: number;
	tombstones: number;
	leafSummaries: number;
	condensedSummaries: number;
	jobs: JobStatusCounts;
}

export interface DoctorCheck {
	name: string;
	ok: boolean;
	detail?: string;
}

export interface DoctorReport {
	ok: boolean;
	checks: readonly DoctorCheck[];
}

export interface PurgeResult {
	tombstones: number;
	jobs: number;
	summaries: number;
	sourceContents: number;
}

export interface RebuildResult {
	branches: number;
	activeSources: number;
	queuedJobs: number;
}

export interface LcmContext extends Disposable {
	reconcile(snapshot: SourceSnapshot): ReconcileResult;
	project(request: ProjectionRequest): ContextProjection;
	claimSummaryJobs(options: ClaimSummaryJobsOptions): SummaryJob[];
	extendSummaryJob(jobId: string, leaseToken: string, leaseMs: number): boolean;
	completeSummaryJob(jobId: string, leaseToken: string, completion: SummaryCompletion): CompleteSummaryJobResult;
	failSummaryJob(jobId: string, leaseToken: string, redactedError: string, retryDelayMs: number): boolean;
	runSummaryJobs(options: RunSummaryJobsOptions, complete: SummaryCompletionCallback): Promise<RunSummaryJobsResult>;
	search(request: SearchRequest): SearchHit[];
	searchProject(request: ProjectSearchRequest): SearchHit[];
	describe(citation: Citation): SourceDescription | null;
	status(): LcmStatus;
	doctor(): DoctorReport;
	/** Logically quarantine this derived store until `rebuild` succeeds. */
	quarantine(redactedReason: string): void;
	rebuild(snapshots: readonly SourceSnapshot[]): RebuildResult;
	purge(): PurgeResult;
	close(): void;
}
