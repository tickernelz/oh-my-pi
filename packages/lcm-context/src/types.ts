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

/**
 * Host-supplied regex engine. This package keeps zero runtime dependencies, so the
 * caller injects one; it MUST be linear-time (RE2/Rust-style) because patterns reach
 * it straight from a model and a backtracking engine could stall the store.
 */
export interface LcmRegexEngine {
	/** Returns a predicate for `pattern`, or throws a `TypeError` when it will not compile. */
	compile(pattern: string): (text: string) => boolean;
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
	/** Enables `SearchRequest.mode: "regex"`; absent makes that mode throw. */
	regexEngine?: LcmRegexEngine;
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
	/** Files referenced by the summarized sources, so compaction never loses file awareness. */
	files: readonly LcmFileMetadata[];
}

export type SummaryStage = "normal" | "aggressive" | "deterministic";

export type SummaryStrategy = "preserve_details" | "bullet_points" | "deterministic_truncate";

export interface SummaryAttemptProvenance {
	promptHash: string;
	modelSelector?: string;
	resolvedModel?: string;
	strategy: SummaryStrategy;
}

/** Canonical provider usage and cost for one dispatched summary attempt. */
export interface SummaryProviderUsage {
	input: number;
	output: number;
	cacheRead: number;
	cacheWrite: number;
	totalTokens: number;
	orchestration?: { input?: number; cacheRead?: number; output?: number };
	premiumRequests?: number;
	reasoningTokens?: number;
	cttl?: { ephemeral5m?: number; ephemeral1h?: number };
	server?: { webSearch?: number; webFetch?: number };
	cost: { input: number; output: number; cacheRead: number; cacheWrite: number; total: number };
}

export interface SummaryProviderAttemptStart {
	attemptId: string;
	startedAt: number;
	provider: string;
	model: string;
}

export interface SummaryProviderAttempt extends SummaryProviderAttemptStart {
	completedAt: number;
	usage?: SummaryProviderUsage;
}

export type SummaryAttemptOutcome =
	| "completed"
	| "provider_error"
	| "transport_error"
	| "empty_output"
	| "aborted"
	| "non_compressing"
	| "stale"
	| "lease_lost";

export type SummaryFailureAttemptOutcome = "provider_error" | "transport_error" | "empty_output";

export type SummaryLocalAttemptOutcome = "aborted" | "stale" | "lease_lost";

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
	/** Dispatched provider attempt whose ledger row this completion settles. */
	attempt?: SummaryProviderAttempt;
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
	/** Matcher for `query`: FTS5 token conjunction (default) or a linear-time regex scan. */
	mode?: "text" | "regex";
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
	/** FTS5 BM25 rank in text mode, or the zero-based match ordinal in regex mode; lower sorts first. */
	rank: number;
	/** Branch position of a source hit; absent for summary hits. */
	position?: number;
	/** Stable handle of the summary node currently covering a source hit, when one exists. */
	coveringSummaryHandle?: string;
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

export type LcmRecoveryCategory = "integrity_check" | "corruption" | "unknown";

/**
 * Process-local, content-free projection and scheduling counters. They reset on
 * restart and exist so benchmarks can observe hot-path cost without sampling.
 */
export interface LcmPerformanceCounters {
	projectionCalls: number;
	projectionWallMs: number;
	projectionCpuMs: number;
	projectionLineageRowsRead: number;
	schedulerBranchPasses: number;
}

export interface LcmStatus {
	schemaVersion: number;
	journalMode: string;
	quarantined: boolean;
	storage: {
		databaseBytes: number;
		walBytes: number;
		quarantineBytes: number;
	};
	latestRecovery: { occurredAt: number; category: LcmRecoveryCategory } | null;
	branches: number;
	activeSources: number;
	tombstones: number;
	leafSummaries: number;
	condensedSummaries: number;
	jobs: JobStatusCounts;
	/** Always present on the SQLite context; test fakes may omit it. */
	performance?: LcmPerformanceCounters;
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
	quarantineFiles: number;
	quarantineBytes: number;
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
	/**
	 * Record a dispatched provider attempt before it starts. Returns false when the
	 * lease is stale, the job is no longer placed on a current branch, or the
	 * attempt id already exists — the caller must then dispatch nothing.
	 */
	beginSummaryAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttemptStart,
		provenance: SummaryAttemptProvenance,
	): boolean;
	/** Finish an in-flight attempt without mutating job retry state. */
	settleSummaryAttempt(
		jobId: string,
		leaseToken: string,
		attempt: SummaryProviderAttempt,
		requestedOutcome: SummaryLocalAttemptOutcome,
	): SummaryAttemptOutcome | null;
	failSummaryJob(
		jobId: string,
		leaseToken: string,
		redactedError: string,
		retryDelayMs: number,
		provenance?: SummaryAttemptProvenance,
		failedAttempt?: { attempt: SummaryProviderAttempt; outcome: SummaryFailureAttemptOutcome },
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
