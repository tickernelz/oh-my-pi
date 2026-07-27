export interface ContextScope {
	projectId: string;
	sessionId: string;
	branchId: string;
}

/** Metadata-only reference to content owned by the journal, artifact store, or filesystem. */
export interface LcmFileMetadata {
	fileId: string;
	contentHash: string;
	path: string;
	fileType: string;
	byteSize: number;
	tokenCount: number;
	explorationSummary: string;
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
	/** Large files are metadata-only; their bytes never enter the derived store. */
	files?: readonly LcmFileMetadata[];
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
export interface ReconcileOptions {
	/** Schedule summaries only for the historical prefix outside this protected tail. */
	summarize?: false | Pick<ProjectionRequest, "tokenBudget" | "freshTail">;
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
	/** Stable executable identity derived from canonical summarized inputs. */
	summaryHandle: string;
	level: number;
	redactedText: string;
	tokenCount: number;
	sourceIds: readonly string[];
	citations: readonly Citation[];
}

export type SummaryStage = "normal" | "aggressive" | "deterministic";

export type SummaryStrategy = "preserve_details" | "bullet_points" | "deterministic_truncate";

export interface SummaryAttemptProvenance {
	promptHash: string;
	modelSelector?: string;
	resolvedModel?: string;
	strategy: SummaryStrategy;
}

export interface ContextProjection {
	revision: number;
	/** False means the caller must fail open to its native history path. */
	ready: boolean;
	historical: readonly ProjectedHistoricalItem[];
	freshTailSourceIds: readonly string[];
	uncoveredSourceIds: readonly string[];
	/** Estimated tokens in every active source before projection. */
	sourceTokens: number;
	/** Number of selected summaries at each hierarchy level. */
	selectedLevelCounts: Readonly<Record<number, number>>;
	coveredSourceCount: number;
	freshSourceCount: number;
	/** Estimated tokens after projection, including the protected fresh tail. */
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
	queueClass: "preferred" | "fallback";
	kind: "leaf" | "condensed";
	level: number;
	inputs: readonly SummaryJobInput[];
	sourceCount: number;
	inputTokenCount: number;
	outputTokenBudget: number;
	stage: SummaryStage;
	strategy: SummaryStrategy;
	transportRetryCount: number;
}

export interface ClaimSummaryJobsOptions {
	workerId: string;
	leaseMs: number;
	limit: number;
	/** Hard output ceiling requested from the completion provider. */
	maxOutputTokens: number;
	preferredScope?: ContextScope;
	/** Defaults to true to preserve project-wide draining when no scope is supplied. */
	allowFallback?: boolean;
}

export interface SummaryCompletion {
	/** Caller-redacted summary text. */
	redactedText: string;
	/** Optional provider measurement; the package also measures text locally and uses the larger value. */
	tokenCount?: number;
	provenance?: SummaryAttemptProvenance;
}

export type CompleteSummaryJobResult =
	| { accepted: true; summaryId: string }
	| { accepted: false; reason: "lease_lost" | "stale" }
	| { accepted: false; reason: "escalated"; stage: SummaryStage }
	| { accepted: false; reason: "deterministic_failed" };

export interface SearchRequest extends ContextScope {
	query: string;
	limit?: number;
	/** Zero-based offset after scope filtering. */
	offset?: number;
	/** Restrict matches to the active placement of this summary handle. */
	summaryHandle?: string;
}

export interface ProjectSearchRequest {
	projectId: string;
	query: string;
	limit?: number;
	/** Zero-based offset after project/branch filtering. */
	offset?: number;
}

export interface SearchHit {
	kind: "source" | "summary";
	id: string;
	/** Present for summary hits; unlike `id`, stable across regenerated prose. */
	summaryHandle?: string;
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
	files: readonly LcmFileMetadata[];
}

export interface SummaryReference extends ContextScope {
	summaryHandle: string;
}

export interface FileReference extends ContextScope {
	fileId: string;
}

export interface SummaryDescription extends SummaryReference {
	kind: "leaf" | "condensed";
	level: number;
	redactedText: string;
	tokenCount: number;
	sourceCount: number;
	childCount: number;
	parentHandles: readonly string[];
	files: readonly LcmFileMetadata[];
}

export interface FileDescription extends FileReference, LcmFileMetadata {
	sources: readonly Citation[];
}

export interface SummaryExpansionRequest extends SummaryReference {
	depth?: number;
	offset?: number;
	limit?: number;
	maxTokens?: number;
}

export type SummaryExpansionItem =
	| { kind: "summary"; depth: number; summary: SummaryDescription }
	| { kind: "source"; depth: number; citation: Citation; tokenCount: number; files: readonly LcmFileMetadata[] };

export interface SummaryExpansion {
	root: SummaryDescription;
	items: readonly SummaryExpansionItem[];
	offset: number;
	totalItems: number;
	estimatedTokens: number;
	truncated: boolean;
	nextOffset?: number;
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
	files: number;
}

export interface RebuildResult {
	branches: number;
	activeSources: number;
	queuedJobs: number;
}

export interface LcmContext extends Disposable {
	reconcile(snapshot: SourceSnapshot, options?: ReconcileOptions): ReconcileResult;
	project(request: ProjectionRequest): ContextProjection;
	claimSummaryJobs(options: ClaimSummaryJobsOptions): SummaryJob[];
	nextSummaryJobDelayMs(preferredScope?: ContextScope, allowFallback?: boolean): number | null;
	summaryJobFailures(preferredScope?: ContextScope): readonly {
		jobId: string;
		availableAt: number;
		queueClass: "preferred" | "fallback";
	}[];
	extendSummaryJob(jobId: string, leaseToken: string, leaseMs: number): boolean;
	releaseSummaryJob(jobId: string, leaseToken: string): boolean;
	completeSummaryJob(jobId: string, leaseToken: string, completion: SummaryCompletion): CompleteSummaryJobResult;
	failSummaryJob(
		jobId: string,
		leaseToken: string,
		redactedError: string,
		retryDelayMs: number,
		provenance?: SummaryAttemptProvenance,
	): boolean;
	search(request: SearchRequest): SearchHit[];
	searchProject(request: ProjectSearchRequest): SearchHit[];
	describe(citation: Citation): SourceDescription | null;
	describeSummary(reference: SummaryReference): SummaryDescription | null;
	describeFile(reference: FileReference): FileDescription | null;
	expandSummary(request: SummaryExpansionRequest): SummaryExpansion | null;
	status(): LcmStatus;
	doctor(): DoctorReport;
	rebuild(snapshots: readonly SourceSnapshot[]): RebuildResult;
	purge(): PurgeResult;
	close(): void;
}
