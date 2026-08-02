import { Database } from "bun:sqlite";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import {
	activeSourceFingerprint,
	type ContextProjection,
	type LcmContext,
	openLcmContext,
	type SourceSnapshot,
	type SummaryProviderAttempt,
	type SummaryProviderAttemptStart,
} from "@oh-my-pi/lcm-context";
import { Agent, type AgentMessage, type AgentTool, type StreamFn } from "@oh-my-pi/pi-agent-core";
import { estimateTokens, resolveThresholdTokens } from "@oh-my-pi/pi-agent-core/compaction";
import type { Api, AssistantMessage, Context, Model, ModelSpec, SimpleStreamOptions } from "@oh-my-pi/pi-ai";
import { AssistantMessageEventStream } from "@oh-my-pi/pi-ai/utils/event-stream";
import { buildModel } from "@oh-my-pi/pi-catalog/build";
import { getBlobsDir } from "@oh-my-pi/pi-utils";
import { Settings } from "../src/config/settings";
import type { Skill } from "../src/extensibility/skills";
import { decodeLcmHandle, encodeLcmHandle } from "../src/lcm/operations";
import { computeNonMessageTokens } from "../src/modes/utils/context-usage";
import { AgentSession } from "../src/session/agent-session";
import { BlobStore } from "../src/session/blob-store";
import { convertToLlm, wrapSteeringForModel } from "../src/session/messages";
import type { FileEntry, SessionEntry } from "../src/session/session-entries";
import {
	estimateLcmProjectionMessageTokens,
	estimateLcmProjectionMessageTokenUpperBound,
	LcmCompletionError,
	type LcmCompletionRequest,
	type LcmCompletionResult,
	type LcmPrimaryRequestRoute,
	normalizeLcmBranch,
	SessionLcm,
} from "../src/session/session-lcm";
import { resolveBlobRefsInEntries } from "../src/session/session-loader";
import { SessionManager } from "../src/session/session-manager";
import { MemorySessionStorage } from "../src/session/session-storage";
import harnessCompatibility from "./lcm-replay-compatibility.json" with { type: "json" };

const HARNESS_SCHEMA = "lcm-replay/v4";
const HARNESS_IDENTITY_SCHEMA = "lcm-replay-harness-source/v1";
const ESTIMATOR_SCHEMA = "pi-estimate-tokens/v1";
const MODEL_ID = "lcm-replay-summary";
const MODEL_SELECTOR = "@smol";
const CONTEXT_WINDOW = 400_000;
const DEFAULT_THRESHOLD_TOKENS = 340_000;
const DEFAULT_SAMPLES = 5;
const COST_PER_MILLION = { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 } as const;
const CONTENT_FREE_LEAF_SOURCES = 24;
const CONTENT_FREE_LEAF_TOKENS = 80_000;
const CONTENT_FREE_CONDENSE_FAN_IN = 4_096;
const CONTENT_FREE_FRESH_TOKENS = 16_000;
const CONTENT_FREE_MIN_SOURCES = 40;
const CONTENT_FREE_EXPECTED_WORK = {
	projectionReads: 3,
	lineageReads: 0,
	schedulerBranchPasses: 1,
} as const;
interface SyntheticWorkloadDescriptor {
	fixtureHash: string;
	sourceCount: number;
	boundary: ContentFreeBoundary;
}

interface HarnessCompatibilityPair {
	baseline: string;
	candidate: string;
	reason: string;
	baselineArtifact: {
		path: string;
		sha256: string;
	};
	syntheticWorkloadMigration?: {
		baseline: SyntheticWorkloadDescriptor;
		candidate: SyntheticWorkloadDescriptor;
		justification: string;
	};
}

const harnessCompatibilityManifest = harnessCompatibility as {
	schema: string;
	pairs: HarnessCompatibilityPair[];
};
if (harnessCompatibilityManifest.schema !== "lcm-replay-harness-compatibility/v1") {
	throw new Error("unsupported LCM replay harness compatibility manifest");
}
const HARNESS_SOURCE_COMPATIBILITY = new Map(
	harnessCompatibilityManifest.pairs.map(pair => [`${pair.baseline}:${pair.candidate}`, pair]),
);
const HISTORICAL_BASELINE_SOURCE_HASHES = new Set(harnessCompatibilityManifest.pairs.map(pair => pair.baseline));
const HISTORICAL_HARD_PROJECTION_WAIT_MS = 30_000;
const REPO_ROOT = path.resolve(import.meta.dir, "../../..");
const ARTIFACT_ROOT = path.join(REPO_ROOT, "compaction-results/lcm-replay");
const LCM_HANDLE_TOKEN_PATTERN = /lcm-handle:v1:[A-Za-z0-9_-]+/g;
const ABANDONED_ATTEMPT_OUTCOMES = new Set(["aborted", "lease_lost", "stale"]);
const BLOB_REF_PATTERN = /blob:sha256:[a-f0-9]{64}/g;

type FixtureName =
	| "real"
	| "oversized-tool-output"
	| "boundary-summary"
	| "single-child-frontier"
	| "minimal-marker-budget"
	| "provider-backoff"
	| "model-change"
	| "stale-lease"
	| "cancellation";

type Mode = "capture" | "baseline" | "compare";

interface CaptureOptions {
	mode: "capture" | "baseline";
	out: string;
	fixture: FixtureName;
	agentDir?: string;
	replayPath?: string;
	markerId?: string;
	snapshotIn?: string;
	snapshotOut: string;
	samples: number;
	treatment: string;
	requestTokensFloor?: number;
	runtimeEnvelopeIn?: string;
	runtimeEnvelope?: CapturedRuntimeEnvelope;
	sourceIn?: string;
	thresholdTokens: number;
	sourceOut: string;
	storePath?: string;
	workRoot?: string;
}

interface CompareOptions {
	mode: "compare";
	baseline: string;
	candidate: string;
}

type CliOptions = CaptureOptions | CompareOptions;

interface MarkerIdentity {
	markerId: string;
	parentId: string;
	markerOrdinal: number;
	sessionId: string;
	branchId: string;
	selectedEntriesHash: string;
	selectedEntries: number;
	journalFileBytesAtCapture: number;
	journalSuffixHash: string;
	sessionTimestamp: string;
}

interface CapturedRuntimeEnvelope {
	projectId: string;
	sessionId: string;
	branchId: string;
	orderedSourceKeys: string[];
	contextWindow: number;
	summaryModelSelector: string;
	systemPrompt: string[];
	toolSchemas: unknown[];
	skills: unknown[];
	nonMessageTokens: number;
	settings: {
		thresholdTokens: number;
		freshTailMaxSources: number;
		freshTailMaxTokens: number;
		maxConcurrentSummaries: number;
	};
}

interface ReconstructionQualification {
	projectId: string;
	sessionId: string;
	branchId: string;
	orderedSourceKeys: string[];
	runtimeEnvelopeAuthoritative: boolean;
	capturedStoreAuthoritative?: boolean;
	readyStore?: boolean;
}

interface BaselineEligibility {
	classification: "exact-historical-replay" | "historical-reconstruction-impossible";
	preChangeContractReproduced: boolean;
}

interface BlobEvidence {
	verifiedRefs: string[];
	digestMismatchRefs: string[];
	readFailureRefs: string[];
	missingRefs?: string[];
}

interface JobEvidence {
	relevant: number;
	pending: number;
	leased: number;
	backoff: number;
	exhausted: number;
	missing: number;
}
export interface LogicalTableEvidence {
	name: string;
	columns: string[];
	rows: Record<string, unknown>[];
}

export interface LogicalTableHash {
	name: string;
	hash: string;
}

export interface PreparedTemplateEvidence {
	reserialized: boolean;
	quickCheck: string;
	byteHash: string;
	logicalHash: string;
	schemaRows: Array<{ type: string; name: string; tblName: string; sql: string | null }>;
	logicalTables?: LogicalTableEvidence[];
	logicalTableHashes?: LogicalTableHash[];
	sourceRows: SourceRow[];
	summaryRows: Record<string, unknown>[];
	summaryLineageRows?: SummaryLineageEvidenceRow[];
	jobRows: Record<string, unknown>[];
	policyRows: Record<string, unknown>[];
	projectIds: string[];
	retryEpochs: number[];
}

interface ProviderMetrics {
	calls: number;
	successes: number;
	active: number;
	peak: number;
	input: number;
	output: number;
	reasoning: number;
	cacheRead: number;
	cacheWrite: number;
	cost: number;
	promptCounts: Map<string, number>;
	firstBackoffInjected: boolean;
	onStart?: () => void;
	onIdle?: () => void;
	onAbort?: () => void;
	releaseAfterAbort?: Promise<void>;
}

interface SourceRow {
	position: number;
	entryId: string;
	sourceKey: string;
}

export interface SummaryLineageEvidenceRow {
	summaryId: string;
	summaryHandle: string | null;
	projectId: string;
	branchRevision: number;
	level: number;
	startPosition: number;
	endPosition: number;
	frontier: number;
	ordinal: number;
	sourceKey: string;
	entryId: string | null;
	position: number | null;
}

interface SelectedSpan {
	summaryId: string;
	summaryHandle: string;
	level: number;
	tokenCount: number;
	sourceCount: number;
	summaryRow: Record<string, unknown> | null;
	sourceIds: string[];
	lineageRows: SummaryLineageEvidenceRow[];
}

interface ReconstructionStatus {
	classification: "exact-historical-replay" | "historical-reconstruction-impossible";
	preChangeContractReproduced: boolean;
	originalRedactionIdentityAvailable: boolean;
	missingBlobRefs: string[];
	syntheticFixture: { kind: "content-free-shape"; sourceCount: number } | null;
	baselineEligibility?: BaselineEligibility;
	qualification?: ReconstructionQualification;
}

type ReplayControlEvidence =
	| {
			kind: "oversized-tool-output";
			inputBytes: number;
			projected: boolean;
			sourceCoverageComplete: boolean;
	  }
	| { kind: "single-child-frontier"; childJobs: number; parentJobs: number; unresolvedJobs: number }
	| {
			kind: "minimal-marker-budget";
			fallbackReason: "minimum_representation" | "irreducible_input";
			candidateTokens: number;
			budget: number;
	  }
	| { kind: "provider-backoff"; providerAttempts: number; retries: number; recovered: boolean }
	| { kind: "model-change"; oldPolicyCompletionAccepted: boolean; newPolicyEpoch: number }
	| { kind: "stale-lease"; staleCompletionAccepted: boolean; replacementLeaseTokenChanged: boolean }
	| {
			kind: "cancellation";
			started: boolean;
			claimedJobs: number;
			aborted: boolean;
			projectionReturnedBeforeProviderRelease: boolean;
			abortLatencyMs: number;
			cleanupLatencyMs: number;
			providerAttempts: number;
			providerWrites: number;
			inFlightAttempts: number;
			billedAttempts: number;
			missingUsage: number;
			attemptOutcomes: string[];
			staleSummaries: number;
			activeProviders: number;
			cleanupComplete: boolean;
			storeRowsChanged: number;
	  };

interface ReplayFailureControl {
	name: ReplayFailureControlName;
	route: string;
	category: string | null;
	reason: string | null;
	providerCalls: number;
	storeRowsChanged: number;
}

type ReplayFailureControlName =
	| "store"
	| "provider-exhausted"
	| "provider-key-mismatch"
	| "assembly-invalid"
	| "fit-invariant"
	| "irreducible-input"
	| "minimum-representation";

const FAILURE_CONTROL_ORDER: readonly ReplayFailureControlName[] = [
	"store",
	"provider-exhausted",
	"provider-key-mismatch",
	"assembly-invalid",
	"fit-invariant",
	"irreducible-input",
	"minimum-representation",
];

const FAILURE_CONTROL_EXPECTATIONS: Record<ReplayFailureControlName, Omit<ReplayFailureControl, "name">> = {
	store: { route: "native_fallback", category: "store", reason: null, providerCalls: 0, storeRowsChanged: 0 },
	"provider-exhausted": {
		route: "native_fallback",
		category: "provider",
		reason: "provider_exhausted",
		providerCalls: 1,
		storeRowsChanged: 1,
	},
	"provider-key-mismatch": {
		route: "native_fallback",
		category: "provider",
		reason: "provider_key_mismatch",
		providerCalls: 0,
		storeRowsChanged: 0,
	},
	"assembly-invalid": {
		route: "native_fallback",
		category: "unfit",
		reason: "assembly_invalid",
		providerCalls: 0,
		storeRowsChanged: 0,
	},
	"fit-invariant": {
		route: "native_fallback",
		category: "unfit",
		reason: "fit_invariant",
		providerCalls: 0,
		storeRowsChanged: 0,
	},
	"irreducible-input": {
		route: "native_fallback",
		category: "unfit",
		reason: "irreducible_input",
		providerCalls: 0,
		storeRowsChanged: 0,
	},
	"minimum-representation": {
		route: "native_fallback",
		category: "unfit",
		reason: "minimum_representation",
		providerCalls: 0,
		storeRowsChanged: 0,
	},
};

interface SampleReport {
	sample: number;
	latencyMs: number;
	peakProviderConcurrency: number;
	providerUsage: {
		input: number;
		output: number;
		reasoning: number;
		cacheRead: number;
		cacheWrite: number;
		cost: number;
	};
	retries: number;
	cacheReuse: number;
	controlEvidence?: ReplayControlEvidence;
	fitProof: {
		owned: boolean;
		ready: boolean;
		complete: boolean;
		revision: number | null;
		uncoveredSources: number;
	};
	fallbackCategory: string | null;
	selectedSpans: SelectedSpan[];
	sourceCoverage: {
		active: number;
		covered: number;
		fresh: number;
		uncovered: number;
		complete: boolean;
		activeSourceKeys: string[];
		historicalSourceKeys: string[];
		freshSourceKeys: string[];
		projectedSourceKeys: string[];
	};
	promptInputTokens: number;
	cpuMs: number;
	postStoreHash?: string;
	postSnapshotHash?: string;
	underfillRatio: number;
	storeRowsChanged: number;
	sqliteQuickCheck: string;
	serializedStoreHash: string;
	schemaVersion: number;
	sourceRows: SourceRow[];
	status?: {
		health: string;
		coverageReadiness: string | null;
		route: string;
		committed?: boolean;
		hasMetrics?: boolean;
		failureCategory: string | null;
		failureReason: string | null;
	};
	tokens?: { request: number; nonMessage: number; candidate: number; routeCandidate?: number; budget: number };
	handles?: {
		count: number;
		unique: number;
		allPresent: boolean;
		allResolved: boolean;
		allMatchStore: boolean;
		providerVisible?: boolean;
		tokens?: string[];
	};
	attempts: SummaryAttemptReport;
	counters?: Record<string, number>;
	providerAttempts?: number;
	jobs: JobEvidence;
	maintenance?: {
		localRequestTokens: number;
		requestTokensFloor: number;
		authoritative: boolean;
		decision: { kind: "owned" | "native" | "aborted"; fallback?: unknown };
	};
}

export interface ReplayReport {
	harnessSchema: string;
	workloadFingerprint: string;
	sourceSnapshotHash: string;
	artifacts?: {
		root: string;
		harnessSource?: string;
		rawPrefix?: string;
		resolvedPrefix?: string;
		sourceSnapshot?: string;
		sqliteSnapshot?: string;
		treatmentTemplate?: string;
		migratedTemplate?: string;
	};
	fixture: {
		name: FixtureName;
		markerId: string;
		parentId: string;
		sessionId: string;
		branchId: string;
		selectedEntries: number;
		projectId: string;
		markerOrdinal: number;
		sessionTimestamp: string;
		selectedEntriesHash: string;
		journalFileBytesAtCapture: number;
		journalSuffixHash: string;
		requestTokensFloor?: number;
		activeSources: number;
		sourceTokens: number;
		summaryModelSelector: string;
		contextWindow: number;
		settings: {
			thresholdTokens: number;
			freshTailMaxSources: number;
			freshTailMaxTokens: number;
			maxConcurrentSummaries: number;
		};
		estimatorSchema: string;
		harnessSourceHash?: string;
		harnessIdentityHash?: string;
		nonMessageTokens?: number;
		systemPromptHash?: string;
		sqliteSnapshotHash?: string;
		logicalStoreHash?: string;
		toolSchemaHash?: string;
		skillHash?: string;
		reconstruction: ReconstructionStatus;
		prefixIdentity?: { rawHash: string; resolvedHash: string };
		blobEvidence?: BlobEvidence;
	};
	treatment: {
		label: string;
		hardProjectionWaitMs: number | null;
		schemaVersion: number;
		retryKey?: string;
	};
	storeEvidence?: {
		pristine: { path: string; byteHash: string; logicalHash: string; quickCheck: string };
		migratedTemplate: PreparedTemplateEvidence;
		preparedTemplate: PreparedTemplateEvidence;
	};
	syntheticPair?: {
		kind: "content-free-shape";
		fixtureHash: string;
		sourceCount: number;
		boundary: ContentFreeBoundary;
		scope: SourceSnapshot["scope"];
		templatePath: string;
		templateEvidence: PreparedTemplateEvidence;
		samples: SampleReport[];
	};
	cancellationControl?: {
		kind: "cancellation";
		fixtureHash: string;
		sourceCount: number;
		boundary: ContentFreeBoundary;
		scope: SourceSnapshot["scope"];
		templatePath: string;
		templateEvidence: PreparedTemplateEvidence;
		sample: SampleReport;
	};
	failureControls?: ReplayFailureControl[];
	candidateOutcome?: { route: string; reproducedOldFallback: boolean };
	samples: SampleReport[];
	metrics: {
		latencyMs: { median: number; mad: number; p95: number };
		cpuMs: { median: number; mad: number; p95: number };
		peakProviderConcurrency: number;
		providerUsage: SampleReport["providerUsage"];
		retries: number;
		cacheReuse: number;
		fitProof: SampleReport["fitProof"];
		fallbackCategory: string | null;
		selectedSpans: SelectedSpan[];
		sourceCoverage: SampleReport["sourceCoverage"];
		promptInputTokens: number;
		underfillRatio: number;
		storeRowsChanged: number;
		sqliteQuickCheck: string;
		serializedStoreHash: string;
	};
}

function hashText(text: string): string {
	return new Bun.CryptoHasher("sha256").update(text).digest("hex");
}

function hashBytes(value: Uint8Array): string {
	return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

function canonicalValue(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(canonicalValue);
	if (!value || typeof value !== "object") return value;
	return Object.fromEntries(
		Object.entries(value as Record<string, unknown>)
			.filter(([, child]) => child !== undefined)
			.sort(([left], [right]) => left.localeCompare(right))
			.map(([key, child]) => [key, canonicalValue(child)]),
	);
}

function canonicalJson(value: unknown): string {
	return JSON.stringify(canonicalValue(value));
}

export function replayHarnessIdentityHash(harnessSourceHash: string): string {
	return hashText(canonicalJson({ schema: HARNESS_IDENTITY_SCHEMA, harnessSourceHash }));
}

function lexicalArtifactPath(candidate: string, label: string): string {
	const resolved = path.resolve(candidate);
	const relative = path.relative(ARTIFACT_ROOT, resolved);
	if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
		throw new Error(`${label} must be inside compaction-results/lcm-replay`);
	}
	return resolved;
}

async function realPathThroughNearestParent(target: string, label: string): Promise<string> {
	let cursor = target;
	const missing: string[] = [];
	for (;;) {
		try {
			return path.resolve(await fs.realpath(cursor), ...missing);
		} catch (error) {
			if (!error || typeof error !== "object" || !("code" in error) || error.code !== "ENOENT") throw error;
			try {
				await fs.lstat(cursor);
				throw new Error(`${label} must resolve inside compaction-results/lcm-replay`);
			} catch (statError) {
				if (!statError || typeof statError !== "object" || !("code" in statError) || statError.code !== "ENOENT") {
					throw statError;
				}
			}
			const parent = path.dirname(cursor);
			if (parent === cursor) throw error;
			missing.unshift(path.basename(cursor));
			cursor = parent;
		}
	}
}

async function assertArtifactPath(candidate: string, label: string, access: "read" | "write"): Promise<string> {
	const resolved = lexicalArtifactPath(candidate, label);
	if (access === "write") {
		try {
			if ((await fs.lstat(resolved)).isSymbolicLink()) {
				throw new Error(`${label} must not be a symlink inside compaction-results/lcm-replay`);
			}
		} catch (error) {
			if (!error || typeof error !== "object" || !("code" in error) || error.code !== "ENOENT") throw error;
		}
	}
	const canonicalRoot = await realPathThroughNearestParent(ARTIFACT_ROOT, label);
	const canonicalTarget = await realPathThroughNearestParent(resolved, label);
	const relative = path.relative(canonicalRoot, canonicalTarget);
	if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
		throw new Error(`${label} must resolve inside compaction-results/lcm-replay`);
	}
	return resolved;
}

function canonicalTokens(value: unknown): number {
	const text = typeof value === "string" ? value : JSON.stringify(value);
	return text.length === 0 ? 0 : Math.ceil(Buffer.byteLength(text, "utf8") / 4);
}

interface RenderedSummaryHandle {
	token: string;
	projectId: string;
	sessionId: string;
	branchId: string;
	summaryHandle: string;
}

function renderedSummaryHandles(messages: readonly unknown[]): { valid: boolean; handles: RenderedSummaryHandle[] } {
	let valid = true;
	const handles: RenderedSummaryHandle[] = [];
	for (const message of messages) {
		if (!message || typeof message !== "object" || Array.isArray(message)) continue;
		const content = (message as Record<string, unknown>).content;
		const fragments =
			typeof content === "string"
				? [content]
				: Array.isArray(content)
					? content.flatMap(part =>
							part &&
							typeof part === "object" &&
							!Array.isArray(part) &&
							typeof (part as Record<string, unknown>).text === "string"
								? [(part as Record<string, unknown>).text as string]
								: [],
						)
					: [];
		for (const fragment of fragments) {
			for (const token of fragment.match(LCM_HANDLE_TOKEN_PATTERN) ?? []) {
				try {
					const decoded = decodeLcmHandle(token);
					if (decoded.kind !== "summary") continue;
					handles.push({ token, ...decoded.reference });
				} catch {
					valid = false;
				}
			}
		}
	}
	return { valid, handles };
}

interface ReproducibleRuntimeInputs {
	tools: AgentTool[];
	skills: Skill[];
	nonMessageTokens: number;
}

function reproducibleRuntimeInputs(
	envelope: CapturedRuntimeEnvelope | undefined,
): ReproducibleRuntimeInputs | undefined {
	if (!envelope?.systemPrompt.every(part => typeof part === "string")) return undefined;
	const tools: AgentTool[] = [];
	for (const value of envelope.toolSchemas) {
		if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
		const schema = value as Record<string, unknown>;
		if (
			typeof schema.name !== "string" ||
			typeof schema.description !== "string" ||
			!schema.parameters ||
			typeof schema.parameters !== "object" ||
			Array.isArray(schema.parameters) ||
			canonicalJson(schema) !==
				canonicalJson({ name: schema.name, description: schema.description, parameters: schema.parameters })
		) {
			return undefined;
		}
		tools.push({
			name: schema.name,
			label: schema.name,
			description: schema.description,
			parameters: schema.parameters as AgentTool["parameters"],
			execute: async () => {
				throw new Error("captured replay tools cannot execute");
			},
		});
	}
	const skills: Skill[] = [];
	for (const value of envelope.skills) {
		if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
		const skill = value as Record<string, unknown>;
		if (
			typeof skill.name !== "string" ||
			typeof skill.description !== "string" ||
			typeof skill.filePath !== "string" ||
			typeof skill.baseDir !== "string" ||
			typeof skill.source !== "string" ||
			(skill.hide !== undefined && typeof skill.hide !== "boolean")
		) {
			return undefined;
		}
		skills.push({ ...skill } as unknown as Skill);
	}
	const nonMessageTokens = computeNonMessageTokens({
		systemPrompt: envelope.systemPrompt,
		agent: { state: { tools } },
		skills,
	});
	if (nonMessageTokens !== envelope.nonMessageTokens) return undefined;
	return { tools, skills, nonMessageTokens };
}

function capturedProjectionPolicy(options: CaptureOptions): {
	tokenBudget: number;
	freshTail: { maxSources: number; maxTokens: number };
} {
	const systemPrompt = options.runtimeEnvelope?.systemPrompt ?? ["system prompt"];
	const runtimeInputs = reproducibleRuntimeInputs(options.runtimeEnvelope);
	const settings = Settings.isolated({
		"compaction.enabled": true,
		"compaction.thresholdTokens": options.thresholdTokens,
		"context.engine": "lossless",
		"context.lossless.retrievalCues": false,
		modelRoles: { smol: MODEL_ID },
	});
	const hardThresholdTokens = resolveThresholdTokens(
		options.runtimeEnvelope?.contextWindow ?? CONTEXT_WINDOW,
		settings.getGroup("compaction"),
	);
	const nonMessageTokens = computeNonMessageTokens({
		systemPrompt,
		agent: { state: { tools: runtimeInputs?.tools ?? [] } },
		skills: runtimeInputs?.skills ?? [],
	});
	const tokenBudget = Math.floor(hardThresholdTokens - nonMessageTokens);
	return {
		tokenBudget,
		freshTail: {
			maxSources: settings.get("context.lossless.freshTailSources"),
			maxTokens: Math.max(1, Math.min(16_000, Math.floor(tokenBudget / 2))),
		},
	};
}

function integer(value: string | undefined, label: string, fallback?: number): number {
	if (value === undefined && fallback !== undefined) return fallback;
	if (!value || !/^\d+$/.test(value)) throw new Error(`${label} must be a non-negative base-10 integer`);
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed)) throw new Error(`${label} must be a safe integer`);
	return parsed;
}

function option(args: readonly string[], name: string): string | undefined {
	const index = args.indexOf(name);
	if (index < 0) return undefined;
	const value = args[index + 1];
	if (!value || value.startsWith("--")) throw new Error(`${name} requires a value`);
	return value;
}

function parseCli(argv: readonly string[]): CliOptions {
	const mode = argv[0] as Mode | undefined;
	if (mode === "compare") {
		const baseline = option(argv, "--baseline");
		const candidate = option(argv, "--candidate");
		if (!baseline || !candidate) throw new Error("compare requires --baseline and --candidate");
		return {
			mode,
			baseline: lexicalArtifactPath(baseline, "--baseline"),
			candidate: lexicalArtifactPath(candidate, "--candidate"),
		};
	}
	if (mode !== "capture" && mode !== "baseline") {
		throw new Error("usage: lcm-replay.bench.ts capture|baseline|compare [options]");
	}
	const outOption = option(argv, "--out");
	if (!outOption) throw new Error(`${mode} requires --out`);
	const fixture = (option(argv, "--fixture") ?? "real") as FixtureName;
	const validFixtures: readonly FixtureName[] = [
		"real",
		"oversized-tool-output",
		"single-child-frontier",
		"minimal-marker-budget",
		"provider-backoff",
		"model-change",
		"stale-lease",
		"cancellation",
	];
	if (!validFixtures.includes(fixture)) throw new Error(`unsupported fixture: ${fixture}`);
	const replayPath = option(argv, "--replay");
	if (fixture === "real" && !replayPath) throw new Error(`the real fixture requires --replay`);
	const markerId = option(argv, "--marker");
	const runtimeEnvelopeIn = option(argv, "--runtime-envelope-in");
	if (runtimeEnvelopeIn) lexicalArtifactPath(runtimeEnvelopeIn, "--runtime-envelope-in");
	const rawRequestTokensFloor = option(argv, "--request-tokens-floor");
	const requestTokensFloor =
		rawRequestTokensFloor === undefined ? undefined : integer(rawRequestTokensFloor, "--request-tokens-floor");
	if (option(argv, "--hard-wait-ms") !== undefined) {
		throw new Error("--hard-wait-ms is unsupported because the current runtime has no hard projection wait");
	}
	const out = lexicalArtifactPath(outOption, "--out");
	const snapshotOut = lexicalArtifactPath(option(argv, "--snapshot-out") ?? `${out}.sqlite`, "--snapshot-out");
	const sourceOut = lexicalArtifactPath(option(argv, "--source-out") ?? `${out}.sources.json`, "--source-out");
	const sourceIn = option(argv, "--source-in");
	if (sourceIn) lexicalArtifactPath(sourceIn, "--source-in");
	const snapshotIn = option(argv, "--snapshot-in");
	if (snapshotIn) lexicalArtifactPath(snapshotIn, "--snapshot-in");
	const samples = integer(option(argv, "--samples"), "--samples", DEFAULT_SAMPLES);
	if (samples < 1) throw new Error("--samples must be at least 1");
	return {
		mode,
		out,
		fixture,
		replayPath,
		markerId,
		agentDir: option(argv, "--agent-dir"),
		requestTokensFloor,
		runtimeEnvelopeIn,
		sourceIn,
		treatment: option(argv, "--treatment") ?? "replay",
		snapshotIn,
		snapshotOut,
		sourceOut,
		storePath: option(argv, "--store"),
		samples,
		thresholdTokens: integer(option(argv, "--threshold-tokens"), "--threshold-tokens", DEFAULT_THRESHOLD_TOKENS),
	};
}

async function validateCliArtifactPaths(options: CliOptions): Promise<void> {
	if (options.mode === "compare") {
		await Promise.all([
			assertArtifactPath(options.baseline, "--baseline", "read"),
			assertArtifactPath(options.candidate, "--candidate", "read"),
		]);
		return;
	}
	const paths: Array<[string, string, "read" | "write"]> = [
		[options.out, "--out", "write"],
		[options.snapshotOut, "--snapshot-out", "write"],
		[options.sourceOut, "--source-out", "write"],
	];
	if (options.snapshotIn) paths.push([options.snapshotIn, "--snapshot-in", "read"]);
	if (options.sourceIn) paths.push([options.sourceIn, "--source-in", "read"]);
	if (options.runtimeEnvelopeIn) paths.push([options.runtimeEnvelopeIn, "--runtime-envelope-in", "read"]);
	await Promise.all(paths.map(([candidate, label, access]) => assertArtifactPath(candidate, label, access)));
}

async function createOwnedPrivateDirectory(directory: string): Promise<void> {
	await fs.mkdir(directory, { recursive: true, mode: 0o700 });
	await fs.chmod(directory, 0o700);
}

async function writePrivateArtifact(filePath: string, data: string | Uint8Array): Promise<void> {
	await fs.mkdir(path.dirname(filePath), { recursive: true });
	const file = await fs.open(filePath, "w", 0o600);
	try {
		await file.chmod(0o600);
		await file.writeFile(data);
	} finally {
		await file.close();
	}
	await fs.chmod(filePath, 0o600);
}

function percentile(values: readonly number[], fraction: number): number {
	if (values.length === 0) return 0;
	const sorted = [...values].sort((left, right) => left - right);
	return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * fraction) - 1))]!;
}

function median(values: readonly number[]): number {
	const sorted = [...values].sort((left, right) => left - right);
	const middle = sorted.length >> 1;
	return sorted.length % 2 === 1 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2;
}

function medianAbsoluteDeviation(values: readonly number[]): number {
	const center = median(values);
	return median(values.map(value => Math.abs(value - center)));
}

function createModel(contextWindow = CONTEXT_WINDOW): Model<Api> {
	return buildModel({
		id: MODEL_ID,
		name: "LCM Replay Summary",
		api: "anthropic",
		provider: "lcm-replay",
		baseUrl: "https://example.invalid",
		reasoning: false,
		input: ["text"],
		cost: COST_PER_MILLION,
		contextWindow,
		maxTokens: 4096,
	} as ModelSpec<Api>) as Model<Api>;
}

async function createSyntheticManager(root: string, fixture: FixtureName): Promise<SessionManager> {
	const storage = new MemorySessionStorage();
	const replayPath = path.join(root, `${fixture}.jsonl`);
	const header = {
		type: "session",
		version: 3,
		id: `synthetic-${fixture}`,
		timestamp: "2026-07-31T00:00:00.000Z",
		cwd: root,
	};
	const entries: SessionEntry[] = [];
	let parentId: string | null = null;
	let ordinal = 0;
	const append = (message: AgentMessage, timestamp: number) => {
		const id = `synthetic-${String(ordinal++).padStart(4, "0")}`;
		entries.push({ type: "message", id, parentId, timestamp: new Date(timestamp).toISOString(), message });
		parentId = id;
	};
	append({ role: "user", content: [{ type: "text", text: "opening question" }], timestamp: 1 }, 1);
	const sources = fixture === "single-child-frontier" ? 17 : fixture === "minimal-marker-budget" ? 3 : 48;
	for (let index = 0; index < sources; index++) {
		append(
			{
				role: "user",
				content: [
					{
						type: "text",
						text: `Fixture ${fixture} source ${index}: ${"alpha beta gamma delta epsilon ".repeat(index % 7 === 0 ? 300 : 40)}`,
					},
				],
				timestamp: index + 2,
			},
			index + 2,
		);
	}
	if (fixture === "oversized-tool-output") {
		const callId = "fixture-oversized-call";
		append(
			{
				role: "assistant",
				content: [{ type: "toolCall", id: callId, name: "grep", arguments: { pattern: "alpha" } }],
				api: "anthropic",
				provider: "lcm-replay",
				model: MODEL_ID,
				usage: {
					input: 1,
					output: 1,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: 2,
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				},
				stopReason: "toolUse",
				timestamp: 1_000,
			},
			1_000,
		);
		append(
			{
				role: "toolResult",
				toolCallId: callId,
				toolName: "grep",
				content: [{ type: "text", text: "match line\n".repeat(24_000) }],
				isError: false,
				timestamp: 1_001,
			},
			1_001,
		);
	}
	append({ role: "user", content: [{ type: "text", text: "closing request" }], timestamp: 2_000 }, 2_000);
	storage.writeTextSync(replayPath, `${[header, ...entries].map(entry => JSON.stringify(entry)).join("\n")}\n`);
	return await SessionManager.open(replayPath, root, storage, {
		initialCwd: process.cwd(),
		suppressBreadcrumb: true,
	});
}

async function createContentFreeManager(root: string, sourceCount: number): Promise<SessionManager> {
	const storage = new MemorySessionStorage();
	const replayPath = path.join(root, "content-free-shape.jsonl");
	const count = Math.max(CONTENT_FREE_MIN_SOURCES, sourceCount);
	const header = {
		type: "session",
		version: 3,
		id: `content-free-shape-${count}`,
		timestamp: "2026-07-31T00:00:00.000Z",
		cwd: root,
	};
	const entries: SessionEntry[] = [];
	for (let index = 0; index < count; index++) {
		entries.push({
			type: "message",
			id: `shape-${String(index).padStart(6, "0")}`,
			parentId: index === 0 ? null : `shape-${String(index - 1).padStart(6, "0")}`,
			timestamp: new Date(index + 1).toISOString(),
			message: {
				role: "user",
				content: [{ type: "text", text: `shape-${index} ${"unit ".repeat(3_000)}` }],
				timestamp: index + 1,
			},
		});
	}
	storage.writeTextSync(replayPath, `${[header, ...entries].map(entry => JSON.stringify(entry)).join("\n")}\n`);
	return SessionManager.open(replayPath, root, storage, {
		initialCwd: process.cwd(),
		suppressBreadcrumb: true,
	});
}

async function loadRealManager(
	replayPath: string,
	markerId: string | undefined,
	agentDir?: string,
): Promise<{
	manager: SessionManager;
	identity: MarkerIdentity;
	rawPrefix: string;
	resolvedPrefix: string;
	missingBlobRefs: string[];
	blobEvidence: BlobEvidence;
	prefixIdentity: { rawHash: string; resolvedHash: string };
}> {
	const raw = await Bun.file(replayPath).text();
	const physical = raw.match(/[^\n]*(?:\n|$)/g)?.filter(line => line.trim().length > 0) ?? [];
	const parsed = physical.map(line => JSON.parse(line) as FileEntry);
	const markerOrdinal = markerId
		? parsed.findIndex(entry => entry.id === markerId)
		: parsed.findLastIndex(entry => entry.type === "compaction");
	if (markerOrdinal < 0) throw new Error("no replay marker found");
	const marker = parsed[markerOrdinal] as unknown as SessionEntry;
	if (!marker.parentId) throw new Error(`marker ${marker.id} has no parentId`);
	const rawPrefix = physical.slice(0, markerOrdinal + 1).join("");
	const rawSuffix = physical.slice(markerOrdinal + 1).join("");
	const blobRefs = [...new Set(rawPrefix.match(BLOB_REF_PATTERN) ?? [])].sort();
	const blobStore = new BlobStore(agentDir ? path.join(agentDir, "blobs", "data") : getBlobsDir());
	const blobEvidence: BlobEvidence = { verifiedRefs: [], digestMismatchRefs: [], readFailureRefs: [] };
	const missingBlobRefs: string[] = [];
	for (const ref of blobRefs) {
		const expectedHash = ref.slice("blob:sha256:".length);
		try {
			const bytes = await blobStore.get(expectedHash);
			if (!bytes) {
				missingBlobRefs.push(ref);
				continue;
			}
			if (hashBytes(bytes) !== expectedHash) {
				blobEvidence.digestMismatchRefs.push(ref);
				continue;
			}
			blobEvidence.verifiedRefs.push(ref);
		} catch {
			blobEvidence.readFailureRefs.push(ref);
		}
	}
	const resolvedEntries = parsed.slice(0, markerOrdinal + 1);
	await resolveBlobRefsInEntries(resolvedEntries, blobStore);
	const resolvedPrefix = `${resolvedEntries.map(entry => JSON.stringify(entry)).join("\n")}\n`;
	if (missingBlobRefs.length > 0) blobEvidence.missingRefs = [...missingBlobRefs];
	const unavailableBlobRefs = [
		...missingBlobRefs,
		...blobEvidence.readFailureRefs,
		...blobEvidence.digestMismatchRefs,
	].sort();
	const storage = new MemorySessionStorage();
	storage.writeTextSync(replayPath, resolvedPrefix);
	const manager = await SessionManager.open(replayPath, path.dirname(replayPath), storage, {
		initialCwd: process.cwd(),
		suppressBreadcrumb: true,
	});
	manager.branch(marker.parentId);
	const selected = manager.getBranch();
	const header = manager.getHeader();
	return {
		manager,
		rawPrefix,
		resolvedPrefix,
		missingBlobRefs: unavailableBlobRefs,
		blobEvidence,
		prefixIdentity: { rawHash: hashText(rawPrefix), resolvedHash: hashText(resolvedPrefix) },
		identity: {
			markerId: marker.id,
			parentId: marker.parentId,
			markerOrdinal,
			sessionId: manager.getSessionId(),
			branchId: hashText(selected.map(entry => entry.id).join("\0")).slice(0, 16),
			selectedEntriesHash: hashText(rawPrefix),
			selectedEntries: selected.length,
			journalFileBytesAtCapture: Buffer.byteLength(raw, "utf8"),
			journalSuffixHash: hashText(rawSuffix),
			sessionTimestamp: header?.timestamp ?? "",
		},
	};
}

function syntheticIdentity(manager: SessionManager, fixture: FixtureName): MarkerIdentity {
	const selected = manager.getBranch();
	const selectedText = canonicalJson(selected);
	return {
		markerId: `synthetic:${fixture}`,
		parentId: manager.getLeafId() ?? "root",
		markerOrdinal: selected.length,
		sessionId: manager.getSessionId(),
		branchId: hashText(selected.map(entry => entry.id).join("\0")).slice(0, 16),
		selectedEntriesHash: hashText(selectedText),
		selectedEntries: selected.length,
		journalFileBytesAtCapture: Buffer.byteLength(selectedText, "utf8"),
		journalSuffixHash: hashText(""),
		sessionTimestamp: manager.getHeader()?.timestamp ?? "",
	};
}

function providerStream(metrics: ProviderMetrics, fixture: FixtureName): StreamFn {
	return (_model: Model<Api>, context: Context, options?: SimpleStreamOptions) => {
		const promptText = JSON.stringify(context);
		const promptHash = hashText(promptText);
		const inputTokens = canonicalTokens(promptText);
		const detailTokens =
			fixture === "boundary-summary" ? Math.max(1, Math.floor((options?.maxTokens ?? 2) / 2) - 32) : 0;
		const outputText =
			fixture === "boundary-summary" ? `Boundary summary ${promptHash}: ${"detail ".repeat(detailTokens)}` : ".";
		const outputTokens = canonicalTokens(outputText);
		metrics.calls++;
		metrics.active++;
		metrics.peak = Math.max(metrics.peak, metrics.active);
		metrics.onStart?.();
		metrics.input += inputTokens;
		metrics.output += outputTokens;
		metrics.promptCounts.set(promptHash, (metrics.promptCounts.get(promptHash) ?? 0) + 1);
		const inputCost = (inputTokens * COST_PER_MILLION.input) / 1e6;
		const outputCost = (outputTokens * COST_PER_MILLION.output) / 1e6;
		metrics.cost += inputCost + outputCost;
		const stream = new AssistantMessageEventStream();
		void (async () => {
			try {
				if (fixture === "cancellation") {
					const signal = options?.signal;
					if (!signal) {
						stream.fail(new Error("cancellation fixture requires an abort signal"));
						return;
					}
					if (!signal.aborted) {
						await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }));
					}
					metrics.onAbort?.();
					await metrics.releaseAfterAbort;
				} else {
					await Bun.sleep(1);
					if (fixture === "provider-backoff" && !metrics.firstBackoffInjected) {
						metrics.firstBackoffInjected = true;
						const error = new Error("fixture provider backoff");
						Object.assign(error, { status: 429, retryAfter: 1 });
						stream.fail(error);
						return;
					}
				}
				const usage = {
					input: inputTokens,
					output: outputTokens,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: inputTokens + outputTokens,
					cost: {
						input: inputCost,
						output: outputCost,
						cacheRead: 0,
						cacheWrite: 0,
						total: inputCost + outputCost,
					},
				};
				const message: AssistantMessage = {
					role: "assistant",
					content: [{ type: "text", text: outputText }],
					api: "anthropic",
					provider: "lcm-replay",
					model: MODEL_ID,
					usage,
					stopReason: "stop",
					timestamp: Date.now(),
				};
				metrics.successes++;
				stream.push({ type: "done", reason: "stop", message });
			} finally {
				metrics.active--;
				if (metrics.active === 0) metrics.onIdle?.();
			}
		})();
		return stream;
	};
}

export interface SummaryAttemptEvidence {
	attemptId: string;
	jobId: string;
	projectId: string;
	outcome: string;
	startedAt: number;
	completedAt: number | null;
	usage: {
		inputTokens: number | null;
		outputTokens: number | null;
		cacheReadTokens: number | null;
		cacheWriteTokens: number | null;
		totalTokens: number | null;
		reasoningTokens: number | null;
		costTotal: number | null;
	} | null;
}

function normalizedSummaryAttempt(row: Record<string, unknown>): SummaryAttemptEvidence {
	const numberOrNull = (key: string): number | null => (typeof row[key] === "number" ? row[key] : null);
	const totalTokens = numberOrNull("total_tokens");
	const costTotal = numberOrNull("cost_total");
	return {
		attemptId: String(row.attempt_id ?? ""),
		jobId: String(row.job_id ?? ""),
		projectId: String(row.project_id ?? ""),
		outcome: String(row.outcome ?? ""),
		startedAt: Number(row.started_at ?? 0),
		completedAt: numberOrNull("completed_at"),
		usage:
			totalTokens === null && costTotal === null
				? null
				: {
						inputTokens: numberOrNull("input_tokens"),
						outputTokens: numberOrNull("output_tokens"),
						cacheReadTokens: numberOrNull("cache_read_tokens"),
						cacheWriteTokens: numberOrNull("cache_write_tokens"),
						totalTokens,
						reasoningTokens: numberOrNull("reasoning_tokens"),
						costTotal,
					},
	};
}

interface SummaryAttemptReport {
	rows: SummaryAttemptEvidence[];
	inFlight: number;
	billed: number;
	missingUsage: number;
}

function summaryAttemptReport(rows: SummaryAttemptEvidence[]): SummaryAttemptReport {
	return {
		rows,
		inFlight: rows.filter(row => row.outcome === "in_flight").length,
		billed: rows.filter(row => (row.usage?.totalTokens ?? 0) > 0 || (row.usage?.costTotal ?? 0) > 0).length,
		missingUsage: rows.filter(row => row.outcome !== "in_flight" && row.usage === null).length,
	};
}

export interface SqliteReport {
	quickCheck: string;
	serializedStoreHash: string;
	snapshotHash: string;
	snapshotBytes: Uint8Array;
	rows: number;
	schemaVersion: number;
	schemaRows: Array<{ type: string; name: string; tblName: string; sql: string | null }>;
	logicalTables: LogicalTableEvidence[];
	logicalTablesComplete: boolean;
	sourceRows: SourceRow[];
	summaryRows: Record<string, unknown>[];
	summaryLineageRows: SummaryLineageEvidenceRow[];
	jobRows: Record<string, unknown>[];
	relevantJobRows: Record<string, unknown>[];
	attemptRows: SummaryAttemptEvidence[];
	relevantAttemptRows: SummaryAttemptEvidence[];
	policyRows: Record<string, unknown>[];
	projectIds: string[];
	retryEpochs: number[];
}

function logicalValue(value: unknown): unknown {
	if (value instanceof Uint8Array) return { $blobSha256: hashBytes(value), $blobBytes: value.byteLength };
	return value;
}

export function sqliteReport(
	storePath: string,
	projectId: string | undefined,
	sessionId: string | undefined,
	branchId: string | undefined,
): SqliteReport {
	const db = new Database(storePath, { readonly: true, strict: true });
	try {
		const quickCheck = db.query<{ quick_check: string }, []>("PRAGMA quick_check").get()?.quick_check ?? "missing";
		const schemaVersion = Number(
			db.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version ?? 0,
		);
		const schema = db
			.query<{ type: string; name: string; tblName: string; sql: string | null }, []>(
				`SELECT type, name, tbl_name AS tblName, sql FROM sqlite_schema
				 WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`,
			)
			.all();
		const logicalTables: LogicalTableEvidence[] = [];
		let rows = 0;
		for (const table of schema.filter(entry => entry.type === "table")) {
			const quotedTable = `"${table.name.replaceAll('"', '""')}"`;
			const columns = db.query<{ name: string; pk: number }, []>(`PRAGMA table_info(${quotedTable})`).all();
			const names = columns.map(column => column.name);
			const primary = columns
				.filter(column => column.pk > 0)
				.sort((left, right) => left.pk - right.pk)
				.map(column => column.name);
			const order = primary.length > 0 ? primary : names;
			const quotedColumns = names.map(name => `"${name.replaceAll('"', '""')}"`);
			const orderBy = order.map(name => `"${name.replaceAll('"', '""')}"`).join(", ");
			const tableRows =
				names.length === 0
					? []
					: db
							.query<Record<string, unknown>, []>(
								`SELECT ${quotedColumns.join(", ")} FROM ${quotedTable} ORDER BY ${orderBy}`,
							)
							.all()
							.map(row =>
								Object.fromEntries(Object.entries(row).map(([key, value]) => [key, logicalValue(value)])),
							);
			rows += tableRows.length;
			logicalTables.push({ name: table.name, columns: names, rows: tableRows });
		}
		const rowsFor = (name: string): Record<string, unknown>[] =>
			logicalTables.find(table => table.name === name)?.rows ?? [];
		const summaryRows = rowsFor("summaries");
		const jobRows = rowsFor("summary_jobs");
		const policyRows = rowsFor("summary_retry_policies");
		const attemptRows = rowsFor("summary_attempts").map(normalizedSummaryAttempt);
		const projectIds = [
			...new Set(
				[...rowsFor("branches"), ...jobRows]
					.map(row => row.project_id)
					.filter((value): value is string => typeof value === "string"),
			),
		].sort();
		const relevantJobRows =
			projectId && sessionId && branchId && schemaVersion >= 8
				? db
						.query<Record<string, unknown>, [string, string, string]>(
							`SELECT DISTINCT sj.* FROM branches b
							 JOIN branch_summary_spans span ON span.branch_row_id = b.id AND span.revision = b.revision
							 JOIN summary_jobs sj ON sj.project_id = b.project_id AND sj.input_hash = span.input_hash
							 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ?
							   AND span.frontier = 1 AND span.summary_id IS NULL
							 ORDER BY sj.job_id`,
						)
						.all(projectId, sessionId, branchId)
				: [];
		const relevantAttemptRows =
			projectId && sessionId && branchId && schemaVersion >= 8
				? db
						.query<Record<string, unknown>, [string, string, string]>(
							`SELECT DISTINCT attempt.* FROM branches b
							 JOIN branch_summary_spans span ON span.branch_row_id = b.id AND span.revision = b.revision
							 JOIN summary_jobs sj ON sj.project_id = b.project_id AND sj.input_hash = span.input_hash
							 JOIN summary_attempts attempt ON attempt.project_id = b.project_id AND attempt.job_id = sj.job_id
							 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ?
							 ORDER BY attempt.attempt_id`,
						)
						.all(projectId, sessionId, branchId)
						.map(normalizedSummaryAttempt)
				: [];
		const retryEpochs = [
			...new Set(
				[...jobRows.map(row => row.retry_epoch), ...policyRows.map(row => row.epoch)].filter(
					(value): value is number => typeof value === "number",
				),
			),
		].sort((left, right) => left - right);
		const sourceRows =
			projectId && sessionId && branchId
				? db
						.query<SourceRow, [string, string, string]>(
							`SELECT bs.position, bs.entry_id AS entryId, bs.source_key AS sourceKey
							 FROM branches b JOIN branch_sources bs ON bs.branch_row_id = b.id AND bs.active = 1
							 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ? ORDER BY bs.position`,
						)
						.all(projectId, sessionId, branchId)
				: [];
		const summaryLineageRows =
			projectId && sessionId && branchId && schemaVersion >= 8
				? db
						.query<SummaryLineageEvidenceRow, [string, string, string]>(
							`SELECT span.summary_id AS summaryId, summary.stable_handle AS summaryHandle,
							        summary.project_id AS projectId, b.revision AS branchRevision, span.level,
							        span.start_position AS startPosition, span.end_position AS endPosition,
							        span.frontier, lineage.ordinal, lineage.source_key AS sourceKey,
							        placed.entry_id AS entryId, placed.position
							 FROM branches b
							 JOIN branch_summary_spans span
							   ON span.branch_row_id = b.id AND span.revision = b.revision AND span.summary_id IS NOT NULL
							 JOIN summaries summary ON summary.summary_id = span.summary_id AND summary.project_id = b.project_id
							 JOIN summary_lineage lineage ON lineage.summary_id = summary.summary_id
							 LEFT JOIN branch_sources placed
							   ON placed.branch_row_id = b.id AND placed.active = 1
							  AND placed.position = span.start_position + lineage.ordinal
							  AND placed.source_key = lineage.source_key
							 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ?
							 ORDER BY span.start_position, span.end_position, span.level, span.summary_id, lineage.ordinal`,
						)
						.all(projectId, sessionId, branchId)
				: [];
		const snapshotBytes = Uint8Array.from(db.serialize());
		return {
			quickCheck,
			serializedStoreHash: hashText(canonicalJson({ schemaVersion, schema, tables: logicalTables })),
			snapshotHash: hashBytes(snapshotBytes),
			snapshotBytes,
			rows,
			schemaVersion,
			schemaRows: schema,
			logicalTables,
			logicalTablesComplete: true,
			sourceRows,
			summaryRows,
			summaryLineageRows,
			jobRows,
			relevantJobRows,
			attemptRows,
			relevantAttemptRows,
			policyRows,
			projectIds,
			retryEpochs,
		};
	} finally {
		db.close(false);
	}
}

async function sourceSnapshotStoreReport(storePath: string, snapshot: SourceSnapshot): Promise<SqliteReport> {
	const context = await openLcmContext({ dbPath: storePath });
	try {
		context.reconcile(snapshot, { summarize: false });
	} finally {
		context.close();
	}
	return sqliteReport(storePath, snapshot.scope.projectId, snapshot.scope.sessionId, snapshot.scope.branchId);
}

function jobEvidence(rows: readonly Record<string, unknown>[], missing = 0): JobEvidence {
	const statusCount = (status: string): number => rows.filter(row => row.status === status).length;
	return {
		relevant: rows.length + missing,
		pending: statusCount("pending"),
		leased: statusCount("leased"),
		backoff: statusCount("failed"),
		exhausted: rows.filter(row => Number(row.transport_retry_count ?? 0) >= 5).length,
		missing,
	};
}

function normalizedRuntimeStatus(status: unknown): NonNullable<SampleReport["status"]> {
	const statusRecord = status && typeof status === "object" ? (status as Record<string, unknown>) : {};
	const runtime =
		statusRecord.runtime && typeof statusRecord.runtime === "object"
			? (statusRecord.runtime as Record<string, unknown>)
			: {};
	const phase = typeof runtime.phase === "string" ? runtime.phase : undefined;
	const health =
		typeof runtime.health === "string"
			? runtime.health
			: phase === "disabled" || phase === "uninitialized" || phase === "degraded"
				? phase
				: "healthy";
	const coverageReadiness =
		typeof runtime.coverageReadiness === "string"
			? runtime.coverageReadiness
			: phase === "active"
				? "ready"
				: phase === "warming"
					? "warming"
					: phase === "idle"
						? "idle"
						: null;
	const lastRoute =
		runtime.lastRequestRoute && typeof runtime.lastRequestRoute === "object"
			? (runtime.lastRequestRoute as Record<string, unknown>)
			: undefined;
	const lastFailure =
		runtime.lastFailure && typeof runtime.lastFailure === "object"
			? (runtime.lastFailure as Record<string, unknown>)
			: undefined;
	const legacyFailure = typeof runtime.lastFailureCategory === "string" ? runtime.lastFailureCategory : null;
	const route = typeof lastRoute?.kind === "string" ? lastRoute.kind : "missing";
	const routeFailure = route === "native_fallback" ? (lastRoute ?? lastFailure) : undefined;
	return {
		health,
		coverageReadiness,
		route,
		committed: route !== "missing",
		hasMetrics: Boolean(lastRoute?.metrics && typeof lastRoute.metrics === "object"),
		failureCategory:
			typeof routeFailure?.category === "string"
				? routeFailure.category
				: route === "native_fallback"
					? legacyFailure
					: null,
		failureReason: typeof routeFailure?.reason === "string" ? routeFailure.reason : null,
	};
}
async function publicOwnershipDecision(
	session: AgentSession,
	messages: AgentMessage[],
	signal: AbortSignal,
	requestTokensFloor: number,
): Promise<Pick<NonNullable<SampleReport["maintenance"]>, "authoritative" | "decision">> {
	const publicSession = session as unknown as {
		losslessOwnsRequest?: (
			messages: AgentMessage[],
			signal?: AbortSignal,
			requestTokensFloor?: number,
		) => Promise<boolean | { kind: "owned" | "native" | "aborted"; fallback?: unknown } | undefined>;
	};
	if (typeof publicSession.losslessOwnsRequest !== "function") {
		return { authoritative: false, decision: { kind: "native" } };
	}
	const decision = await publicSession.losslessOwnsRequest(messages, signal, requestTokensFloor);
	if (decision && typeof decision === "object" && "kind" in decision) {
		return { authoritative: true, decision };
	}
	if (decision === true) return { authoritative: true, decision: { kind: "owned" } };
	if (signal.aborted) return { authoritative: true, decision: { kind: "aborted" } };
	return { authoritative: true, decision: { kind: "native" } };
}

function controlSnapshot(
	name: "single-child-frontier" | "model-change" | "stale-lease",
	sourceCount: number,
): SourceSnapshot {
	const scope = {
		projectId: `control-${name}`,
		sessionId: `control-${name}-session`,
		branchId: `control-${name}-branch`,
	};
	return {
		scope,
		entries: Array.from({ length: sourceCount }, (_, index) => {
			const entryId = `${name}-${index + 1}`;
			const redactedText = `${name} source ${index + 1}: ${"alpha beta gamma delta ".repeat(32)}`;
			return {
				...scope,
				entryId,
				parentId: index === 0 ? null : `${name}-${index}`,
				timestamp: 1_800_000_000_000 + index,
				kind: "message",
				redactedText,
				contentHash: hashText(`${entryId}\0${redactedText}`),
				artifactRefs: [],
			};
		}),
	};
}

async function singleChildControl(root: string): Promise<ReplayControlEvidence> {
	const storePath = path.join(root, "single-child-control.sqlite");
	const context = await openLcmContext({
		dbPath: storePath,
		leafChunk: { maxSources: 1, maxTokens: 10_000 },
		condenseFanIn: 4,
	});
	const source = controlSnapshot("single-child-frontier", 4);
	try {
		const reconcile = context.reconcile(source);
		if (reconcile.queuedJobs !== 4) throw new Error("single-child control did not queue four leaf jobs");
		const policy = context.configureSummaryRetryPolicy(source.scope.projectId, "control/single-child");
		if (policy.kind !== "ready") throw new Error("single-child control retry policy conflicted");
		const children = context.claimSummaryJobs({
			...policy,
			workerId: "single-child-setup",
			leaseMs: 60_000,
			limit: 4,
			maxOutputTokens: 64,
			maxTransportRetries: 5,
			preferredScope: source.scope,
			allowFallback: false,
		});
		if (children.length !== 4 || children.some(child => child.level !== 0)) {
			throw new Error("single-child control did not claim four leaf jobs");
		}
		for (const child of children.slice(0, 3)) {
			const completion = context.completeSummaryJob(child, { redactedText: "." });
			if (!completion.accepted) throw new Error("single-child sibling setup did not complete");
		}
		const beforeTrigger = sqliteReport(
			storePath,
			source.scope.projectId,
			source.scope.sessionId,
			source.scope.branchId,
		);
		const selectedChild = children[3]!;
		const childCompletion = context.completeSummaryJob(selectedChild, { redactedText: "." });
		if (!childCompletion.accepted) throw new Error("single-child trigger did not complete");
		const afterTrigger = sqliteReport(
			storePath,
			source.scope.projectId,
			source.scope.sessionId,
			source.scope.branchId,
		);
		const beforeJobIds = new Set(
			beforeTrigger.jobRows.flatMap(row => (typeof row.job_id === "string" ? [row.job_id] : [])),
		);
		const parentRows = afterTrigger.jobRows.filter(
			row =>
				typeof row.job_id === "string" &&
				!beforeJobIds.has(row.job_id) &&
				Number(row.level) === selectedChild.level + 1 &&
				row.status === "pending",
		);
		if (parentRows.length !== 1) throw new Error("single-child trigger did not create one direct parent job");
		const parentJobId = parentRows[0]!.job_id;
		if (typeof parentJobId !== "string") throw new Error("single-child parent job id was invalid");
		const observer = new Database(storePath, { readonly: true, strict: true });
		try {
			const edge = observer
				.query<{ inputs: number; selected: number }, [string, string]>(
					`SELECT COUNT(*) AS inputs, COUNT(CASE WHEN ref_id = ? THEN 1 END) AS selected
					 FROM job_inputs WHERE job_id = ? AND input_kind = 'summary'`,
				)
				.get(childCompletion.summaryId, parentJobId);
			if (edge?.inputs !== 4 || edge.selected !== 1) {
				throw new Error("single-child trigger did not create the expected four-input parent edge");
			}
		} finally {
			observer.close(false);
		}
		const [parent] = context.claimSummaryJobs({
			...policy,
			workerId: "single-child-parent",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 64,
			maxTransportRetries: 5,
			preferredScope: source.scope,
			allowFallback: false,
		});
		if (!parent || parent.jobId !== parentJobId) {
			throw new Error("single-child control did not claim the direct parent job");
		}
		const parentCompletion = context.completeSummaryJob(parent, { redactedText: "." });
		if (!parentCompletion.accepted) throw new Error("single-child direct parent did not complete");
		const final = sqliteReport(storePath, source.scope.projectId, source.scope.sessionId, source.scope.branchId);
		return {
			kind: "single-child-frontier",
			childJobs: final.jobRows.filter(
				row =>
					row.job_id === selectedChild.jobId &&
					Number(row.level) === selectedChild.level &&
					row.status === "completed",
			).length,
			parentJobs: final.jobRows.filter(
				row => row.job_id === parent.jobId && Number(row.level) === parent.level && row.status === "completed",
			).length,
			unresolvedJobs: final.jobRows.filter(
				row => row.status === "pending" || row.status === "leased" || row.status === "failed",
			).length,
		};
	} finally {
		context.close();
	}
}

async function modelChangeControl(root: string): Promise<ReplayControlEvidence> {
	const storePath = path.join(root, "model-change-control.sqlite");
	const context = await openLcmContext({ dbPath: storePath });
	const source = controlSnapshot("model-change", 1);
	try {
		context.reconcile(source);
		const oldPolicy = context.configureSummaryRetryPolicy(source.scope.projectId, "control/model-a");
		if (oldPolicy.kind !== "ready") throw new Error("model-change control retry policy conflicted");
		const [oldLease] = context.claimSummaryJobs({
			...oldPolicy,
			workerId: "model-change-old",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 64,
			maxTransportRetries: 5,
			preferredScope: source.scope,
			allowFallback: false,
		});
		if (!oldLease) throw new Error("model-change control did not claim a job");
		const newPolicy = context.configureSummaryRetryPolicy(source.scope.projectId, "control/model-b", {
			expected: oldPolicy,
		});
		if (newPolicy.kind !== "ready") throw new Error("model-change control did not rotate retry policy");
		const completion = context.completeSummaryJob(oldLease, { redactedText: "." });
		return {
			kind: "model-change",
			oldPolicyCompletionAccepted: completion.accepted,
			newPolicyEpoch: newPolicy.retryEpoch,
		};
	} finally {
		context.close();
	}
}

async function staleLeaseControl(root: string): Promise<ReplayControlEvidence> {
	let now = 1_800_000_000_000;
	const storePath = path.join(root, "stale-lease-control.sqlite");
	const context = await openLcmContext({ dbPath: storePath, now: () => now });
	const source = controlSnapshot("stale-lease", 1);
	try {
		context.reconcile(source);
		const policy = context.configureSummaryRetryPolicy(source.scope.projectId, "control/stale-lease");
		if (policy.kind !== "ready") throw new Error("stale-lease control retry policy conflicted");
		const [staleLease] = context.claimSummaryJobs({
			...policy,
			workerId: "stale-lease-old",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 64,
			maxTransportRetries: 5,
			preferredScope: source.scope,
			allowFallback: false,
		});
		if (!staleLease) throw new Error("stale-lease control did not claim the initial job");
		now += 101;
		const [replacement] = context.claimSummaryJobs({
			...policy,
			workerId: "stale-lease-replacement",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 64,
			maxTransportRetries: 5,
			preferredScope: source.scope,
			allowFallback: false,
		});
		if (!replacement) throw new Error("stale-lease control did not reclaim the expired job");
		const completion = context.completeSummaryJob(staleLease, { redactedText: "." });
		return {
			kind: "stale-lease",
			staleCompletionAccepted: completion.accepted,
			replacementLeaseTokenChanged: replacement.leaseToken !== staleLease.leaseToken,
		};
	} finally {
		context.close();
	}
}

function failureControlProjection(
	snapshot: SourceSnapshot,
	revision: number,
	invalidFingerprint = false,
): ContextProjection {
	const historical = snapshot.entries.slice(0, -1);
	const fresh = snapshot.entries.at(-1)!;
	const activeIds = snapshot.entries.map(entry => entry.entryId);
	return {
		revision,
		ready: true,
		historical: [
			{
				kind: "summary",
				summaryId: "failure-control-summary",
				summaryHandle: "failure-control-handle",
				level: 0,
				redactedText: "failure control historical context",
				tokenCount: 4,
				sourceIds: historical.map(entry => entry.entryId),
				citations: [],
				files: [],
			},
		],
		activeSourceFingerprint: activeSourceFingerprint(invalidFingerprint ? [...activeIds].reverse() : activeIds),
		freshTailSourceIds: [fresh.entryId],
		uncoveredSourceIds: [],
		sourceTokens: snapshot.entries.length,
		selectedLevelCounts: { 0: 1 },
		coveredSourceCount: historical.length,
		freshSourceCount: 1,
		estimatedTokens: 10,
		pendingJobs: 0,
	};
}

async function executeFailureControl(root: string, name: ReplayFailureControlName): Promise<ReplayFailureControl> {
	const manager = SessionManager.inMemory(path.join(root, name));
	for (const [text, timestamp] of [
		["first control source", 1],
		["older control source", 2],
		["active control source", 3],
	] as const) {
		manager.appendMessage({ role: "user", content: [{ type: "text", text }], timestamp });
	}
	const projectId = `failure-control-${name}`;
	const snapshot = normalizeLcmBranch(manager, projectId, String);
	const storePath = path.join(root, `${name}.sqlite`);
	const freshTail = { maxSources: 1, maxTokens: 1 };
	const now = 1_900_000_000_000;
	let context: LcmContext | undefined;
	let evidenceBaselineHash: string | undefined;
	if (name !== "store" && name !== "irreducible-input") {
		context = await openLcmContext({
			dbPath: storePath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			now: () => now,
		});
		const schedulingControl = name === "provider-exhausted" || name === "provider-key-mismatch";
		const reconciled = context.reconcile(
			snapshot,
			schedulingControl ? { summarize: { tokenBudget: 100, freshTail } } : { summarize: false },
		);
		const retryKey = name === "provider-key-mismatch" ? "provider/persisted-model" : "provider/current-model";
		const policy = context.configureSummaryRetryPolicy(projectId, retryKey);
		if (policy.kind !== "ready") throw new Error(`${name} control could not initialize its retry policy`);
		evidenceBaselineHash = sqliteReport(
			storePath,
			snapshot.scope.projectId,
			snapshot.scope.sessionId,
			snapshot.scope.branchId,
		).serializedStoreHash;
		if (name === "assembly-invalid" || name === "minimum-representation" || name === "fit-invariant") {
			const reconcile = context.reconcile.bind(context);
			context.reconcile = source => reconcile(source, { summarize: false });
			context.project = () => {
				evidenceBaselineHash = sqliteReport(
					storePath,
					snapshot.scope.projectId,
					snapshot.scope.sessionId,
					snapshot.scope.branchId,
				).serializedStoreHash;
				return failureControlProjection(snapshot, reconciled.revision, name === "assembly-invalid");
			};
		}
		if (name === "provider-exhausted") {
			const claim = context.claimSummaryJobs.bind(context);
			let seededRetryCount = false;
			context.claimSummaryJobs = options => {
				if (!seededRetryCount) {
					evidenceBaselineHash = sqliteReport(
						storePath,
						snapshot.scope.projectId,
						snapshot.scope.sessionId,
						snapshot.scope.branchId,
					).serializedStoreHash;
					const db = new Database(storePath, { strict: true });
					try {
						db.run(
							"UPDATE summary_jobs SET transport_retry_count = 4, retry_epoch = ? WHERE project_id = ? AND status = 'pending'",
							[options.retryEpoch, projectId],
						);
					} finally {
						db.close(false);
					}
					seededRetryCount = true;
				}
				return claim(options);
			};
		}
	}
	let providerCalls = 0;
	const complete = async (request: LcmCompletionRequest): Promise<LcmCompletionResult> => {
		providerCalls++;
		request.onResolvedModel?.("provider/current-model");
		const start: SummaryProviderAttemptStart = {
			attemptId: `failure-control-attempt-${providerCalls}`,
			startedAt: now,
			provider: "failure-control",
			model: "current-model",
		};
		if (request.onAttemptStart && !(await request.onAttemptStart(start))) {
			throw new LcmCompletionError("failure control was superseded before dispatch", {
				provider: start.provider,
				category: "aborted",
			});
		}
		const attempt: SummaryProviderAttempt = { ...start, completedAt: now };
		if (name === "provider-exhausted") {
			throw new LcmCompletionError("deterministic fifth provider failure", {
				provider: start.provider,
				attempt,
			});
		}
		return { text: ".", attempt };
	};
	const lcm = new SessionLcm(
		{
			sessionManager: manager,
			projectionLimits: () => ({
				sourceTokens: 201,
				prewarmThresholdTokens: 40,
				hardThresholdTokens: 100,
				tokenBudget: name === "irreducible-input" ? 0 : 100,
				freshTail,
			}),
			projectionTokenMeasurements: messages => {
				const tokens =
					name === "minimum-representation"
						? messages.some(message => message.role === "historicalContext")
							? 101
							: 60
						: 60;
				const upperBound = messages.reduce(
					(total, message) => total + estimateLcmProjectionMessageTokenUpperBound(message),
					0,
				);
				return { tokens, upperBound: Math.max(tokens, upperBound) };
			},
			complete,
			resolveSummaryModel: () => "provider/current-model",
		},
		{
			summaryModel: "provider/current-model",
			maxConcurrentSummaries: 1,
			dependencies: {
				openContext: async () => {
					if (name === "store") throw new Error("deterministic store failure");
					if (!context) throw new Error(`${name} control context is unavailable`);
					return context;
				},
				resolveProject: async () => ({ projectId, rootPath: root, storePath }),
				now: () => now,
				peerPollMs: 1,
			},
		},
	);
	let route: LcmPrimaryRequestRoute | undefined;
	try {
		const result = await lcm.project(manager.buildSessionContext().messages);
		if (name === "fit-invariant") {
			if (!result.owned) throw new Error("fit-invariant control did not first own its fitting projection");
			lcm.recordPendingPrimaryProviderTokens(
				result.routeKey,
				(result.messageTokenBudget ?? 100) + 1,
				result.projectionTokenMeasurements,
			);
		}
		if (!lcm.commitPrimaryRequestRoute(result.routeKey)) throw new Error(`${name} control route did not commit`);
		route = (await lcm.status()).runtime.lastRequestRoute;
	} finally {
		await lcm.close();
	}
	if (route?.kind !== "native_fallback") throw new Error(`${name} control did not dispatch native fallback`);
	if (route.category === "deadline") throw new Error(`${name} control emitted the removed deadline fallback`);
	const postStoreHash = context
		? sqliteReport(storePath, snapshot.scope.projectId, snapshot.scope.sessionId, snapshot.scope.branchId)
				.serializedStoreHash
		: undefined;
	const control: ReplayFailureControl = {
		name,
		route: route.kind,
		category: route.category,
		reason: route.reason ?? null,
		providerCalls,
		storeRowsChanged: evidenceBaselineHash && postStoreHash !== evidenceBaselineHash ? 1 : 0,
	};
	if (canonicalJson(control) !== canonicalJson({ name, ...FAILURE_CONTROL_EXPECTATIONS[name] })) {
		throw new Error(`${name} failure control contradicted its route, provider-call, or store-write contract`);
	}
	return control;
}

async function executeFailureControls(root: string): Promise<ReplayFailureControl[]> {
	await createOwnedPrivateDirectory(root);
	const controls: ReplayFailureControl[] = [];
	for (const name of FAILURE_CONTROL_ORDER) controls.push(await executeFailureControl(root, name));
	return controls;
}

function providerBackoffLeafChunk(
	fixture: FixtureName,
	snapshot: SourceSnapshot | undefined,
): { maxSources: number; maxTokens: number } | undefined {
	if (fixture !== "provider-backoff" || !snapshot) return undefined;
	return {
		maxSources: Math.max(1, snapshot.entries.length),
		maxTokens: Math.max(
			1,
			snapshot.entries.reduce((total, entry) => total + canonicalTokens(entry.redactedText), 0),
		),
	};
}

async function runSample(
	baseManager: SessionManager,
	fixture: FixtureName,
	capture: CaptureOptions,
	sample: number,
	snapshot: { value?: SourceSnapshot },
	logicalSnapshot: { bytes?: Uint8Array },
	options: {
		contextOptions?: { leafChunk: { maxSources: number; maxTokens: number }; condenseFanIn: number };
		recordControlEvidence?: boolean;
	} = {},
): Promise<SampleReport> {
	const workRoot = capture.workRoot ?? path.join(path.dirname(capture.out), ".work");
	await fs.mkdir(workRoot, { recursive: true });
	const root = await fs.mkdtemp(path.join(workRoot, "sample-"));
	const projectRoot = path.join(root, "project");
	const storePath = path.join(root, "context.sqlite");
	await createOwnedPrivateDirectory(projectRoot);
	if (capture.snapshotIn) await fs.copyFile(capture.snapshotIn, storePath);
	const initialSqlite = capture.snapshotIn
		? sqliteReport(
				storePath,
				snapshot.value?.scope.projectId,
				snapshot.value?.scope.sessionId,
				snapshot.value?.scope.branchId,
			)
		: undefined;
	const manager = baseManager.cloneCurrentSession({ persist: false });
	const model = createModel(capture.runtimeEnvelope?.contextWindow ?? CONTEXT_WINDOW);
	const metrics: ProviderMetrics = {
		calls: 0,
		successes: 0,
		active: 0,
		peak: 0,
		input: 0,
		output: 0,
		reasoning: 0,
		cacheRead: 0,
		cacheWrite: 0,
		cost: 0,
		promptCounts: new Map(),
		firstBackoffInjected: false,
	};
	let notifyProviderStart: (() => void) | undefined;
	const providerStarted = new Promise<void>(resolve => {
		notifyProviderStart = resolve;
	});
	metrics.onStart = () => notifyProviderStart?.();
	let notifyProviderIdle: (() => void) | undefined;
	const providerIdle = new Promise<void>(resolve => {
		notifyProviderIdle = resolve;
	});
	metrics.onIdle = () => notifyProviderIdle?.();
	let notifyProviderAbort: (() => void) | undefined;
	const providerAbortObserved = new Promise<void>(resolve => {
		notifyProviderAbort = resolve;
	});
	metrics.onAbort = () => notifyProviderAbort?.();
	let releaseProviderAfterAbort: (() => void) | undefined;
	metrics.releaseAfterAbort = new Promise<void>(resolve => {
		releaseProviderAfterAbort = resolve;
	});
	const cancellationExpectedAttemptIds = new Set<string>();
	const cancellationExpectedJobIds = new Set<string>();
	const cancellationSettledAttemptIds = new Set<string>();
	const cancellationReleasedJobIds = new Set<string>();
	let resolveCancellationCleanup: (() => void) | undefined;
	const cancellationCleanup = new Promise<void>(resolve => {
		resolveCancellationCleanup = resolve;
	});
	const maybeResolveCancellationCleanup = () => {
		if (
			cancellationExpectedAttemptIds.size > 0 &&
			[...cancellationExpectedAttemptIds].every(id => cancellationSettledAttemptIds.has(id)) &&
			[...cancellationExpectedJobIds].every(id => cancellationReleasedJobIds.has(id))
		) {
			resolveCancellationCleanup?.();
		}
	};
	const providerControlLeafChunk = providerBackoffLeafChunk(fixture, snapshot.value);
	let lastProjection: ContextProjection | undefined;
	let openedContext: LcmContext | undefined;
	const settings = Settings.isolated({
		"compaction.enabled": true,
		"compaction.thresholdTokens": capture.thresholdTokens,
		"context.engine": "lossless",
		"context.lossless.retrievalCues": false,
		modelRoles: { smol: MODEL_ID },
	});
	const systemPrompt = capture.runtimeEnvelope?.systemPrompt ?? ["system prompt"];
	const runtimeInputs = reproducibleRuntimeInputs(capture.runtimeEnvelope);
	const agent = new Agent({
		initialState: { systemPrompt, messages: [], tools: runtimeInputs?.tools ?? [] },
	});
	const expectedNonMessageTokens = computeNonMessageTokens({
		systemPrompt,
		agent,
		skills: runtimeInputs?.skills ?? [],
	});
	const dependencies = {
		resolveProject: async () => ({
			projectId: snapshot.value?.scope.projectId ?? capture.runtimeEnvelope?.projectId ?? "lcm-replay",
			rootPath: projectRoot,
			storePath,
		}),
		openContext: async (contextOptions: Parameters<typeof openLcmContext>[0]) => {
			const context = await openLcmContext({
				...contextOptions,
				...(providerControlLeafChunk ? { leafChunk: providerControlLeafChunk } : {}),
				...options.contextOptions,
			});
			openedContext = context;
			const reconcile = context.reconcile.bind(context);
			context.reconcile = (source, reconcileOptions) => {
				snapshot.value ??= structuredClone(source);
				return reconcile(snapshot.value, reconcileOptions);
			};
			const project = context.project.bind(context);
			context.project = request => {
				const projection = project(request);
				lastProjection = projection;
				return projection;
			};
			const settleSummaryAttempt = context.settleSummaryAttempt.bind(context);
			context.settleSummaryAttempt = (lease, attempt, outcome) => {
				const settled = settleSummaryAttempt(lease, attempt, outcome);
				if (settled && "usage" in attempt && attempt.usage) {
					cancellationSettledAttemptIds.add(attempt.attemptId);
					maybeResolveCancellationCleanup();
				}
				return settled;
			};
			const releaseSummaryJob = context.releaseSummaryJob.bind(context);
			context.releaseSummaryJob = lease => {
				const released = releaseSummaryJob(lease);
				if (released) {
					cancellationReleasedJobIds.add(lease.jobId);
					maybeResolveCancellationCleanup();
				}
				return released;
			};
			return context;
		},
	};
	const session = new AgentSession({
		agent,
		sessionManager: manager,
		settings,
		skills: runtimeInputs?.skills ?? [],
		modelRegistry: {
			getAvailable: () => [model],
			resolver: () => async () => "fixture-key",
			authStorage: { recordObservedUsage: () => {}, recordUsageCost: () => {} },
		} as never,
		sideStreamFn: providerStream(metrics, fixture),
		lcm: {
			agentDir: root,
			maxConcurrentSummaries: fixture === "provider-backoff" ? 1 : 4,
			dependencies,
		},
	});
	const replayNonMessageTokens = computeNonMessageTokens({
		systemPrompt: session.agent.state.systemPrompt,
		agent: session.agent,
		skills: runtimeInputs?.skills ?? [],
	});
	if (runtimeInputs && replayNonMessageTokens !== expectedNonMessageTokens) {
		throw new Error("AgentSession did not reproduce the captured non-message token envelope");
	}
	try {
		session.agent.setModel(model);
		const input = manager.buildSessionContext().messages;
		const localRequestTokens = input.reduce((total, message) => total + estimateTokens(message), 0);
		const controller = new AbortController();
		const cpuStart = process.cpuUsage();
		const started = Bun.nanoseconds();
		const maintenance =
			capture.requestTokensFloor === undefined
				? undefined
				: {
						localRequestTokens,
						requestTokensFloor: capture.requestTokensFloor,
						...(await publicOwnershipDecision(session, input, controller.signal, capture.requestTokensFloor)),
					};
		const projectionPromise = session.projectLcmContext(input, controller.signal);
		let projected: AgentMessage[];
		let cancellationStarted = false;
		let cancellationClaimedJobs = 0;
		let cancellationAbortLatencyMs = 0;
		let cancellationCleanupLatencyMs = 0;
		let projectionReturnedBeforeProviderRelease = false;
		let cancellationCleanupStarted = 0;
		let cancellationDispose: Promise<void> | undefined;
		let cancellationScope: SourceSnapshot["scope"] | undefined;
		let cancellationSummariesAtAbort = 0;
		if (fixture === "cancellation") {
			await Promise.race([
				providerStarted,
				Bun.sleep(2_000).then(() => {
					throw new Error("cancellation fixture did not start a provider attempt");
				}),
			]);
			cancellationStarted = true;
			const activeScope = snapshot.value?.scope;
			if (!activeScope) throw new Error("cancellation fixture did not establish an LCM scope");
			cancellationScope = activeScope;
			const inFlightStore = sqliteReport(
				storePath,
				activeScope.projectId,
				activeScope.sessionId,
				activeScope.branchId,
			);
			cancellationClaimedJobs = inFlightStore.relevantJobRows.filter(row => row.status === "leased").length;
			cancellationSummariesAtAbort = inFlightStore.summaryRows.length;
			for (const attempt of inFlightStore.relevantAttemptRows) {
				if (attempt.outcome === "in_flight") cancellationExpectedAttemptIds.add(attempt.attemptId);
			}
			for (const job of inFlightStore.relevantJobRows) {
				if (job.status === "leased" && typeof job.job_id === "string") cancellationExpectedJobIds.add(job.job_id);
			}
			maybeResolveCancellationCleanup();
			if (inFlightStore.relevantAttemptRows.every(attempt => attempt.outcome !== "in_flight")) {
				throw new Error("cancellation fixture did not persist an in-flight summary attempt");
			}
			if (cancellationClaimedJobs < 1) throw new Error("cancellation fixture did not claim a summary job");
			const abortStarted = Bun.nanoseconds();
			controller.abort(new Error("fixture cancellation"));
			projected = await projectionPromise;
			cancellationAbortLatencyMs = (Bun.nanoseconds() - abortStarted) / 1e6;
			projectionReturnedBeforeProviderRelease = metrics.active > 0 && metrics.successes === 0;
			if (!projectionReturnedBeforeProviderRelease) {
				throw new Error("cancellation projection waited for provider completion instead of returning on abort");
			}
			cancellationCleanupStarted = Bun.nanoseconds();
			cancellationDispose = session.dispose();
			await Promise.race([
				providerAbortObserved,
				Bun.sleep(2_000).then(() => {
					throw new Error("cancellation lifecycle fence did not abort the in-flight provider");
				}),
			]);
			if (!cancellationScope) throw new Error("cancellation lifecycle fence lost its scope");
			const fencedStore = sqliteReport(
				storePath,
				cancellationScope.projectId,
				cancellationScope.sessionId,
				cancellationScope.branchId,
			);
			for (const attempt of fencedStore.relevantAttemptRows) {
				if (attempt.outcome === "in_flight") cancellationExpectedAttemptIds.add(attempt.attemptId);
			}
			for (const job of fencedStore.relevantJobRows) {
				if (job.status === "leased" && typeof job.job_id === "string") cancellationExpectedJobIds.add(job.job_id);
			}
			maybeResolveCancellationCleanup();
			if (metrics.successes !== 0) {
				throw new Error("cancellation provider completed before the explicit late-settlement release");
			}
			releaseProviderAfterAbort?.();
		} else {
			projected = await projectionPromise;
		}
		const latencyMs = (Bun.nanoseconds() - started) / 1e6;
		const cpu = process.cpuUsage(cpuStart);
		const cpuMs = (cpu.user + cpu.system) / 1_000;
		if (fixture === "cancellation") {
			await Promise.all([
				cancellationDispose,
				providerIdle,
				Promise.race([
					cancellationCleanup,
					Bun.sleep(2_000).then(() => {
						throw new Error("cancellation fixture did not settle every attempt and release every claimed job");
					}),
				]),
			]);
			cancellationCleanupLatencyMs = (Bun.nanoseconds() - cancellationCleanupStarted) / 1e6;
			openedContext = undefined;
		}
		if (fixture !== "cancellation") session.beginPrimaryProviderRequest(projected, controller.signal);
		const status = fixture === "cancellation" ? undefined : await session.lcmStatus();
		const routeMetrics = status?.runtime.lastRequestRoute?.metrics;
		const routeCandidateTokens = routeMetrics?.candidateTokens;
		const messageTokenBudget = routeMetrics?.messageTokenBudget;
		const wrappedProjected = wrapSteeringForModel(projected);
		const productionCandidateTokens = wrappedProjected.reduce(
			(total, message) => total + estimateLcmProjectionMessageTokens(message),
			0,
		);
		const providerVisibleProjected = convertToLlm(wrappedProjected);
		const promptInputTokens = productionCandidateTokens;
		const renderedHandles = renderedSummaryHandles(providerVisibleProjected);
		const branch = status?.runtime.currentBranch;
		const projection = lastProjection;
		const projectedSpans = projection?.historical ?? [];
		const runtimeEvidence = normalizedRuntimeStatus(status);
		const fallbackCategory = runtimeEvidence.failureCategory;
		const schemaVersion = status?.store?.schemaVersion ?? 0;
		const resolvedContext = openedContext;
		const contextStatus = resolvedContext?.status();
		const sourceScope = snapshot.value?.scope;
		const handlesAllResolved =
			renderedHandles.valid &&
			resolvedContext !== undefined &&
			sourceScope !== undefined &&
			renderedHandles.handles.every(
				handle =>
					handle.projectId === sourceScope.projectId &&
					handle.sessionId === sourceScope.sessionId &&
					handle.branchId === sourceScope.branchId &&
					Boolean(
						resolvedContext.expandSummary({ ...sourceScope, summaryHandle: handle.summaryHandle, limit: 1 }),
					),
			);
		await session.dispose();
		openedContext = undefined;
		const scope = snapshot.value?.scope;
		const sqlite = sqliteReport(
			storePath,
			scope?.projectId ?? branch?.projectId,
			scope?.sessionId ?? branch?.sessionId,
			scope?.branchId ?? branch?.branchId,
		);
		const selectedSpans = projectedSpans.map(item => ({
			summaryId: item.summaryId,
			summaryHandle: item.summaryHandle,
			level: item.level,
			tokenCount: item.tokenCount,
			sourceCount: item.sourceIds.length,
			summaryRow: sqlite.summaryRows.find(row => row.summary_id === item.summaryId) ?? null,
			sourceIds: [...item.sourceIds],
			lineageRows: sqlite.summaryLineageRows.filter(row => row.summaryId === item.summaryId),
		}));
		const attempts = summaryAttemptReport(sqlite.relevantAttemptRows);
		const staleSummaries = Math.max(0, sqlite.summaryRows.length - cancellationSummariesAtAbort);
		const summaryHandlesById = new Map(
			sqlite.summaryRows.flatMap(row =>
				typeof row.summary_id === "string" && typeof row.stable_handle === "string"
					? [[row.summary_id, row.stable_handle] as const]
					: [],
			),
		);
		const summaryPairsMatchStore =
			renderedHandles.valid &&
			renderedHandles.handles.length === selectedSpans.length &&
			renderedHandles.handles.every((handle, index) => {
				const span = selectedSpans[index];
				return Boolean(
					scope &&
						span &&
						handle.projectId === scope.projectId &&
						handle.sessionId === scope.sessionId &&
						handle.branchId === scope.branchId &&
						handle.summaryHandle === span.summaryHandle &&
						summaryHandlesById.get(span.summaryId) === handle.summaryHandle,
				);
			});
		const sourceRows = initialSqlite?.sourceRows ?? sqlite.sourceRows;
		const activeSourceKeys = sourceRows.map(row => row.sourceKey);
		const sourceKeyById = new Map(sourceRows.map(row => [row.entryId, row.sourceKey]));
		const historicalSourceKeys =
			projection?.historical.flatMap(item =>
				item.sourceIds.map(entryId => sourceKeyById.get(entryId)).filter((key): key is string => key !== undefined),
			) ?? [];
		const freshSourceKeys =
			projection?.freshTailSourceIds
				.map(entryId => sourceKeyById.get(entryId))
				.filter((key): key is string => key !== undefined) ?? [];
		const projectedSourceKeys = [...historicalSourceKeys, ...freshSourceKeys];
		const uncovered = projection?.uncoveredSourceIds.length ?? activeSourceKeys.length;
		const covered = historicalSourceKeys.length;
		const fresh = freshSourceKeys.length;
		const active = activeSourceKeys.length;
		const complete =
			projection?.ready === true &&
			uncovered === 0 &&
			canonicalJson(projectedSourceKeys) === canonicalJson(activeSourceKeys);
		logicalSnapshot.bytes ??= sqlite.snapshotBytes;
		const retries = [...metrics.promptCounts.values()].reduce((total, count) => total + Math.max(0, count - 1), 0);
		const storeRowsChanged =
			initialSqlite && initialSqlite.serializedStoreHash === sqlite.serializedStoreHash
				? 0
				: Math.max(1, Math.abs(sqlite.rows - (initialSqlite?.rows ?? 0)));
		const counters = {
			...(contextStatus?.performance ?? {}),
			projectionReads: contextStatus?.performance?.projectionCalls ?? 0,
			lineageReads: contextStatus?.performance?.projectionLineageRowsRead ?? 0,
			rowsChanged: contextStatus?.performance?.reconcileRowsChanged ?? 0,
		};
		let controlEvidence: ReplayControlEvidence | undefined;
		switch (options.recordControlEvidence === false ? "real" : fixture) {
			case "oversized-tool-output": {
				let inputBytes = 0;
				for (const message of input) {
					if (message.role !== "toolResult") continue;
					for (const content of message.content) {
						if (content.type === "text") inputBytes += Buffer.byteLength(content.text, "utf8");
					}
				}
				controlEvidence = {
					kind: "oversized-tool-output",
					inputBytes,
					projected: projected !== input,
					sourceCoverageComplete: complete,
				};
				break;
			}
			case "single-child-frontier":
				controlEvidence = await singleChildControl(root);
				break;
			case "minimal-marker-budget": {
				const fallbackReason = runtimeEvidence.failureReason;
				if (fallbackReason !== "minimum_representation" && fallbackReason !== "irreducible_input") {
					throw new Error("minimal-marker control did not report a representation fallback");
				}
				const candidateTokens = status?.runtime.lastRequestRoute?.metrics.candidateTokens;
				const budget = status?.runtime.lastRequestRoute?.metrics.messageTokenBudget;
				if (typeof candidateTokens !== "number" || typeof budget !== "number") {
					throw new Error("minimal-marker control did not expose measured route tokens");
				}
				controlEvidence = { kind: "minimal-marker-budget", fallbackReason, candidateTokens, budget };
				break;
			}
			case "provider-backoff":
				controlEvidence = {
					kind: "provider-backoff",
					providerAttempts: metrics.calls,
					retries: metrics.calls - metrics.successes,
					recovered: metrics.firstBackoffInjected && metrics.successes > 0 && projected !== input && complete,
				};
				break;
			case "model-change":
				controlEvidence = await modelChangeControl(root);
				break;
			case "stale-lease":
				controlEvidence = await staleLeaseControl(root);
				break;
			case "cancellation": {
				const attemptOutcomes = attempts.rows.map(attempt => attempt.outcome).sort();
				controlEvidence = {
					kind: "cancellation",
					started: cancellationStarted,
					claimedJobs: cancellationClaimedJobs,
					aborted: controller.signal.aborted,
					projectionReturnedBeforeProviderRelease,
					abortLatencyMs: cancellationAbortLatencyMs,
					cleanupLatencyMs: cancellationCleanupLatencyMs,
					providerAttempts: metrics.calls,
					providerWrites: staleSummaries,
					inFlightAttempts: attempts.inFlight,
					billedAttempts: attempts.billed,
					missingUsage: attempts.missingUsage,
					attemptOutcomes,
					staleSummaries,
					activeProviders: metrics.active,
					cleanupComplete:
						metrics.active === 0 &&
						sqlite.relevantJobRows.every(row => row.status !== "leased") &&
						attempts.inFlight === 0 &&
						attempts.billed > 0 &&
						attemptOutcomes.every(outcome => ABANDONED_ATTEMPT_OUTCOMES.has(outcome)) &&
						staleSummaries === 0,
					storeRowsChanged,
				};
				break;
			}
		}
		return {
			sample,
			latencyMs,
			cpuMs,
			peakProviderConcurrency: metrics.peak,
			providerUsage: {
				input: metrics.input,
				output: metrics.output,
				reasoning: metrics.reasoning,
				cacheRead: metrics.cacheRead,
				cacheWrite: metrics.cacheWrite,
				cost: metrics.cost,
			},
			retries,
			cacheReuse: metrics.cacheRead,
			...(controlEvidence ? { controlEvidence } : {}),
			fitProof: {
				owned: projected !== input,
				ready: projection?.ready === true,
				complete,
				revision: projection?.revision ?? null,
				uncoveredSources: uncovered,
			},
			fallbackCategory,
			selectedSpans,
			sourceCoverage: {
				active,
				covered,
				fresh,
				uncovered,
				complete,
				activeSourceKeys,
				historicalSourceKeys,
				freshSourceKeys,
				projectedSourceKeys,
			},
			promptInputTokens,
			underfillRatio:
				typeof messageTokenBudget === "number" ? Math.max(0, 1 - promptInputTokens / messageTokenBudget) : 0,
			storeRowsChanged,
			sqliteQuickCheck: sqlite.quickCheck,
			serializedStoreHash: initialSqlite?.serializedStoreHash ?? sqlite.serializedStoreHash,
			postStoreHash: sqlite.serializedStoreHash,
			postSnapshotHash: sqlite.snapshotHash,
			schemaVersion: schemaVersion || sqlite.schemaVersion,
			sourceRows,
			status: runtimeEvidence,
			...(typeof routeCandidateTokens === "number" && typeof messageTokenBudget === "number"
				? {
						tokens: {
							request:
								typeof status?.runtime.pressure?.requestTokens === "number"
									? status.runtime.pressure.requestTokens
									: localRequestTokens,
							nonMessage: replayNonMessageTokens,
							candidate: promptInputTokens,
							routeCandidate: routeCandidateTokens,
							budget: messageTokenBudget,
						},
					}
				: {}),
			handles: {
				count: renderedHandles.handles.length,
				unique: new Set(renderedHandles.handles.map(handle => handle.token)).size,
				allPresent: renderedHandles.valid && renderedHandles.handles.length === selectedSpans.length,
				allResolved: handlesAllResolved,
				allMatchStore: summaryPairsMatchStore,
				providerVisible: true,
				tokens: renderedHandles.handles.map(handle => handle.token),
			},
			attempts,
			counters,
			providerAttempts: metrics.calls,
			jobs: jobEvidence(
				sqlite.relevantJobRows,
				Math.max(0, (projection?.pendingJobs ?? 0) - sqlite.relevantJobRows.length),
			),
			...(maintenance ? { maintenance } : {}),
		};
	} finally {
		if (openedContext) openedContext.close();
		await session.dispose();
		await fs.rm(root, { recursive: true, force: true });
	}
}

function averageUsage(samples: readonly SampleReport[]): SampleReport["providerUsage"] {
	const totals = samples.reduce(
		(result, sample) => ({
			input: result.input + sample.providerUsage.input,
			output: result.output + sample.providerUsage.output,
			reasoning: result.reasoning + sample.providerUsage.reasoning,
			cacheRead: result.cacheRead + sample.providerUsage.cacheRead,
			cacheWrite: result.cacheWrite + sample.providerUsage.cacheWrite,
			cost: result.cost + sample.providerUsage.cost,
		}),
		{ input: 0, output: 0, reasoning: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
	);
	return Object.fromEntries(
		Object.entries(totals).map(([key, value]) => [key, value / samples.length]),
	) as SampleReport["providerUsage"];
}

function aggregateMetrics(
	samples: readonly SampleReport[],
	sqliteQuickCheck: string,
	serializedStoreHash: string,
): ReplayReport["metrics"] {
	const representative = samples[0];
	if (!representative) throw new Error("replay report has no samples");
	const latencies = samples.map(sample => sample.latencyMs);
	const cpuSamples = samples.map(sample => sample.cpuMs);
	return {
		latencyMs: {
			median: median(latencies),
			mad: medianAbsoluteDeviation(latencies),
			p95: percentile(latencies, 0.95),
		},
		cpuMs: {
			median: median(cpuSamples),
			mad: medianAbsoluteDeviation(cpuSamples),
			p95: percentile(cpuSamples, 0.95),
		},
		peakProviderConcurrency: Math.max(...samples.map(sample => sample.peakProviderConcurrency)),
		providerUsage: averageUsage(samples),
		retries: samples.reduce((total, sample) => total + sample.retries, 0) / samples.length,
		cacheReuse: samples.reduce((total, sample) => total + sample.cacheReuse, 0) / samples.length,
		fitProof: representative.fitProof,
		fallbackCategory: representative.fallbackCategory,
		selectedSpans: representative.selectedSpans,
		sourceCoverage: representative.sourceCoverage,
		promptInputTokens: median(samples.map(sample => sample.promptInputTokens)),
		underfillRatio: median(samples.map(sample => sample.underfillRatio)),
		storeRowsChanged: median(samples.map(sample => sample.storeRowsChanged)),
		sqliteQuickCheck,
		serializedStoreHash,
	};
}

async function loadRuntimeEnvelope(filePath: string | undefined): Promise<CapturedRuntimeEnvelope | undefined> {
	if (!filePath) return undefined;
	const value = (await Bun.file(filePath).json()) as CapturedRuntimeEnvelope;
	if (
		!value ||
		typeof value !== "object" ||
		!value.projectId ||
		!value.sessionId ||
		!value.branchId ||
		!Array.isArray(value.orderedSourceKeys) ||
		!value.orderedSourceKeys.every(key => typeof key === "string") ||
		!Number.isSafeInteger(value.contextWindow) ||
		value.contextWindow < 1 ||
		!Array.isArray(value.systemPrompt) ||
		!value.systemPrompt.every(part => typeof part === "string") ||
		!Array.isArray(value.toolSchemas) ||
		!Array.isArray(value.skills) ||
		!Number.isSafeInteger(value.nonMessageTokens) ||
		value.nonMessageTokens < 0 ||
		!value.settings
	) {
		throw new Error("runtime envelope is incomplete or invalid");
	}
	return value;
}

function logicalTableHash(table: LogicalTableEvidence): string {
	return hashText(canonicalJson(table));
}

function logicalTableHashes(tables: readonly LogicalTableEvidence[]): LogicalTableHash[] {
	return tables.map(table => ({ name: table.name, hash: logicalTableHash(table) }));
}

function assertLogicalTableHashes(evidence: PreparedTemplateEvidence, label: string): void {
	if (!evidence.logicalTables || !evidence.logicalTableHashes) {
		throw new Error(`${label} complete persisted-table snapshots and hashes are missing`);
	}
	const names = evidence.logicalTables.map(table => table.name);
	if (new Set(names).size !== names.length) throw new Error(`${label} contains duplicate persisted-table snapshots`);
	if (canonicalJson(evidence.logicalTableHashes) !== canonicalJson(logicalTableHashes(evidence.logicalTables))) {
		throw new Error(`${label} persisted-table row/order hashes contradict their canonical snapshots`);
	}
}

export function preparedTemplateEvidence(store: SqliteReport): PreparedTemplateEvidence {
	return {
		reserialized: true,
		quickCheck: store.quickCheck,
		byteHash: store.snapshotHash,
		logicalHash: store.serializedStoreHash,
		schemaRows: store.schemaRows,
		logicalTables: store.logicalTables,
		logicalTableHashes: logicalTableHashes(store.logicalTables),
		sourceRows: store.sourceRows,
		summaryRows: store.summaryRows,
		summaryLineageRows: store.summaryLineageRows,
		jobRows: store.jobRows,
		policyRows: store.policyRows,
		projectIds: store.projectIds,
		retryEpochs: store.retryEpochs,
	};
}

function jobIdentity(row: Record<string, unknown>): string | undefined {
	const value = row.job_id ?? row.id;
	return typeof value === "string" ? value : undefined;
}

function jobsByIdentity(rows: readonly Record<string, unknown>[]): Map<string, Record<string, unknown>> {
	const result = new Map<string, Record<string, unknown>>();
	for (const row of rows) {
		const identity = jobIdentity(row);
		if (!identity || result.has(identity)) throw new Error("prepared job rows have missing or duplicate identity");
		result.set(identity, row);
	}
	return result;
}

function replaySummaryInputHash(projectId: string, level: number, inputs: readonly Record<string, unknown>[]): string {
	const hasher = new Bun.CryptoHasher("sha256");
	for (const part of [
		"lcm-summary-input-v1",
		projectId,
		String(level),
		...inputs.flatMap(row => [String(row.input_kind), String(row.ref_id)]),
	]) {
		hasher.update(`${Buffer.byteLength(part, "utf8")}:`);
		hasher.update(part);
	}
	return hasher.digest("hex");
}

function withoutRetryAuthorization(row: Record<string, unknown>): Record<string, unknown> {
	const normalized = { ...row };
	delete normalized.retry_epoch;
	delete normalized.lease_policy_token;
	delete normalized.lease_mutation_nonce;
	return normalized;
}

function assertMigratedJobs(
	before: readonly Record<string, unknown>[],
	after: readonly Record<string, unknown>[],
	label: string,
): void {
	const candidates = jobsByIdentity(after);
	if (candidates.size !== before.length) throw new Error(`${label} added or removed pre-existing jobs`);
	for (const row of before) {
		const identity = jobIdentity(row);
		const candidate = identity ? candidates.get(identity) : undefined;
		if (!candidate) throw new Error(`${label} removed pre-existing job ${identity ?? "<missing>"}`);
		const expected = { ...row };
		const normalized = withoutRetryAuthorization(candidate);
		if (row.status === "leased") {
			expected.status = "pending";
			expected.worker_id = null;
			expected.lease_token = null;
			expected.lease_expires_at = null;
			expected.lease_input_tokens = null;
			expected.lease_output_budget = null;
			expected.updated_at = normalized.updated_at;
		}
		if (
			candidate.retry_epoch !== 0 ||
			candidate.lease_policy_token !== null ||
			candidate.lease_mutation_nonce !== null ||
			canonicalJson(expected) !== canonicalJson(normalized)
		) {
			throw new Error(`${label} mutated pre-existing job ${identity} outside schema-10 lease revocation`);
		}
		candidates.delete(identity!);
	}
}

function blockedRetryPolicies(projectIds: readonly string[]): Record<string, unknown>[] {
	return projectIds.map(projectId => ({
		project_id: projectId,
		retry_key: null,
		epoch: 0,
		claim_token: null,
		updated_at: 0,
	}));
}

function normalizedSql(sql: string | null): string {
	return (sql ?? "").toLowerCase().replaceAll(/\s+/g, "").replaceAll('"', "");
}

function assertRetryMigrationSchema(before: SqliteReport, after: SqliteReport, label: string): void {
	const beforeByName = new Map(before.schemaRows.map(row => [row.name, row]));
	const afterByName = new Map(after.schemaRows.map(row => [row.name, row]));
	for (const row of before.schemaRows) {
		const candidate = afterByName.get(row.name);
		if (!candidate) throw new Error(`${label} removed schema object ${row.name}`);
		if (row.name !== "summary_jobs" && canonicalJson(row) !== canonicalJson(candidate)) {
			throw new Error(`${label} mutated unrelated schema object ${row.name}`);
		}
	}
	const additions = after.schemaRows.filter(row => !beforeByName.has(row.name));
	const policyTable = additions.find(row => row.type === "table" && row.name === "summary_retry_policies");
	const triggers = additions.filter(row => row.type === "trigger");
	if (!policyTable || triggers.length !== 3 || additions.length !== 4) {
		throw new Error(`${label} did not add exactly one retry-policy table and three authorization triggers`);
	}
	const policySql = normalizedSql(policyTable.sql);
	if (
		!policySql.includes("project_idtextprimarykey") ||
		!policySql.includes("retry_keytext") ||
		!policySql.includes("epochintegernotnull") ||
		!policySql.includes("claim_tokentext") ||
		!policySql.includes("updated_atintegernotnull") ||
		!policySql.includes("retry_keyisnullandepoch=0andclaim_tokenisnull") ||
		!policySql.includes("retry_keyisnotnullandepoch>=1andclaim_tokenisnotnull")
	) {
		throw new Error(`${label} retry-policy table lacks the blocked/active invariant`);
	}
	const jobSql = normalizedSql(afterByName.get("summary_jobs")?.sql ?? null);
	if (
		!jobSql.includes("retry_epochintegernotnulldefault0check(retry_epoch>=0)") ||
		!jobSql.includes("lease_policy_tokentext") ||
		!jobSql.includes("lease_mutation_noncetext")
	) {
		throw new Error(`${label} summary_jobs lacks schema-10 retry authority columns`);
	}
	const triggerSql = triggers.map(row => normalizedSql(row.sql));
	const insertTriggers = triggerSql.filter(sql => sql.includes("beforeinsertonsummary_jobs"));
	const updateTriggers = triggerSql.filter(sql => sql.includes("beforeupdateonsummary_jobs"));
	if (
		insertTriggers.length !== 1 ||
		updateTriggers.length !== 2 ||
		triggerSql.some(sql => !sql.includes("raise(abort"))
	) {
		throw new Error(`${label} requires one claim and two update authorization triggers on summary_jobs`);
	}
}

const PROJECTION_APPEND_TABLES = ["branch_summary_spans", "job_inputs", "job_lineage"] as const;

function logicalTablesByName(report: SqliteReport, label: string): Map<string, LogicalTableEvidence> {
	const result = new Map<string, LogicalTableEvidence>();
	for (const table of report.logicalTables) {
		if (result.has(table.name)) throw new Error(`${label} contains duplicate persisted table ${table.name}`);
		result.set(table.name, table);
	}
	return result;
}

function canonicalValueMatches(left: unknown, right: unknown): boolean {
	const leftCanonical = canonicalJson(left);
	const rightCanonical = canonicalJson(right);
	return leftCanonical === rightCanonical && hashText(leftCanonical) === hashText(rightCanonical);
}

function assertLogicalTablesUnchanged(
	before: SqliteReport,
	after: SqliteReport,
	excluded: ReadonlySet<string>,
	allowedAdditions: ReadonlySet<string>,
	label: string,
): void {
	if (!before.logicalTablesComplete || !after.logicalTablesComplete) return;
	const beforeByName = logicalTablesByName(before, label);
	const afterByName = logicalTablesByName(after, label);
	for (const [name, table] of beforeByName) {
		const candidate = afterByName.get(name);
		if (!candidate) throw new Error(`${label} deleted pre-existing persisted table ${name}`);
		if (!excluded.has(name) && !canonicalValueMatches(table, candidate)) {
			throw new Error(`${label} mutated pre-existing persisted table ${name} row order or row hash`);
		}
	}
	for (const name of afterByName.keys()) {
		if (!beforeByName.has(name) && !allowedAdditions.has(name)) {
			throw new Error(`${label} added unrelated persisted table ${name}`);
		}
	}
}

function assertPreExistingRowsPreserved(
	before: SqliteReport,
	after: SqliteReport,
	tableName: string,
	label: string,
): void {
	if (!before.logicalTablesComplete || !after.logicalTablesComplete) return;
	const original = logicalTablesByName(before, label).get(tableName);
	const candidate = logicalTablesByName(after, label).get(tableName);
	if (!original || !candidate) throw new Error(`${label} deleted persisted scheduling table ${tableName}`);
	if (!canonicalValueMatches(original.columns, candidate.columns)) {
		throw new Error(`${label} mutated persisted scheduling table ${tableName} columns`);
	}
	let cursor = 0;
	for (const row of original.rows) {
		while (cursor < candidate.rows.length && !canonicalValueMatches(row, candidate.rows[cursor])) cursor++;
		if (cursor >= candidate.rows.length) {
			throw new Error(`${label} mutated or deleted a pre-existing ${tableName} row or its order/hash`);
		}
		cursor++;
	}
}

interface PreparedBranchPolicy {
	scope: { projectId: string; sessionId: string; branchId: string };
	tokenBudget: number;
	maxSources: number;
	maxTokens: number;
}

function assertBranchPolicyPreparation(
	before: SqliteReport,
	after: SqliteReport,
	policy: PreparedBranchPolicy,
	label: string,
): void {
	if (!before.logicalTablesComplete || !after.logicalTablesComplete) return;
	const original = logicalTablesByName(before, label).get("branches");
	const candidate = logicalTablesByName(after, label).get("branches");
	if (!original || !candidate || !canonicalValueMatches(original.columns, candidate.columns)) {
		throw new Error(`${label} mutated or deleted the persisted branches table`);
	}
	if (original.rows.length !== candidate.rows.length) throw new Error(`${label} added or deleted a branch row`);
	let found = false;
	for (const [index, row] of original.rows.entries()) {
		const replacement = candidate.rows[index];
		if (!replacement || row.id !== replacement.id) throw new Error(`${label} changed persisted branch row order`);
		const target =
			row.project_id === policy.scope.projectId &&
			row.session_id === policy.scope.sessionId &&
			row.branch_id === policy.scope.branchId;
		if (!target) {
			if (!canonicalValueMatches(row, replacement)) throw new Error(`${label} mutated an unrelated branch row`);
			continue;
		}
		found = true;
		const stableBefore = { ...row };
		const stableAfter = { ...replacement };
		for (const field of ["summary_token_budget", "fresh_tail_max_sources", "fresh_tail_max_tokens"]) {
			delete stableBefore[field];
			delete stableAfter[field];
		}
		if (
			!canonicalValueMatches(stableBefore, stableAfter) ||
			replacement.summary_token_budget !== policy.tokenBudget ||
			replacement.fresh_tail_max_sources !== policy.maxSources ||
			replacement.fresh_tail_max_tokens !== policy.maxTokens
		) {
			throw new Error(`${label} changed the target branch outside the declared projection policy`);
		}
	}
	if (!found) throw new Error(`${label} omitted the declared projection-policy branch`);
}

function appendedLogicalRows(
	before: SqliteReport,
	after: SqliteReport,
	tableName: string,
	label: string,
): Record<string, unknown>[] {
	const original = logicalTablesByName(before, label).get(tableName);
	const candidate = logicalTablesByName(after, label).get(tableName);
	if (!original || !candidate) throw new Error(`${label} deleted persisted scheduling table ${tableName}`);
	const remaining = new Map<string, number>();
	for (const row of original.rows) {
		const key = canonicalJson(row);
		remaining.set(key, (remaining.get(key) ?? 0) + 1);
	}
	const additions: Record<string, unknown>[] = [];
	for (const row of candidate.rows) {
		const key = canonicalJson(row);
		const count = remaining.get(key) ?? 0;
		if (count > 0) {
			remaining.set(key, count - 1);
		} else {
			additions.push(row);
		}
	}
	if ([...remaining.values()].some(count => count !== 0)) {
		throw new Error(`${label} mutated or deleted a pre-existing ${tableName} row or its order/hash`);
	}
	return additions;
}

function assertProjectionAppendRows(
	before: SqliteReport,
	after: SqliteReport,
	policy: PreparedBranchPolicy,
	label: string,
): void {
	if (!before.logicalTablesComplete || !after.logicalTablesComplete) return;
	const branches = logicalTablesByName(after, label).get("branches");
	if (!branches) throw new Error(`${label} omitted persisted branches while validating projection rows`);
	const branch = branches.rows.find(
		row =>
			row.project_id === policy.scope.projectId &&
			row.session_id === policy.scope.sessionId &&
			row.branch_id === policy.scope.branchId,
	);
	if (!branch || typeof branch.id !== "number" || typeof branch.revision !== "number") {
		throw new Error(`${label} omitted the target branch for appended projection rows`);
	}

	const beforeJobs = jobsByIdentity(before.jobRows);
	const afterJobs = jobsByIdentity(after.jobRows);
	const addedJobs = new Map([...afterJobs].filter(([jobId]) => !beforeJobs.has(jobId)));
	const targetSourceKeys = after.sourceRows.map((row, index) => {
		if (row.position !== index) throw new Error(`${label} target branch source positions are not contiguous`);
		return row.sourceKey;
	});
	const summaryProjects = new Map(
		after.summaryRows.flatMap(row =>
			typeof row.summary_id === "string" && typeof row.project_id === "string"
				? [[row.summary_id, row.project_id] as const]
				: [],
		),
	);
	const targetSpans = logicalTablesByName(after, label)
		.get("branch_summary_spans")!
		.rows.filter(row => row.branch_row_id === branch.id && row.revision === branch.revision);
	const addedSpans = appendedLogicalRows(before, after, "branch_summary_spans", label);
	const addedInputs = appendedLogicalRows(before, after, "job_inputs", label);
	const addedLineage = appendedLogicalRows(before, after, "job_lineage", label);

	for (const row of addedSpans) {
		const linkedJob = [...addedJobs.values()].find(
			job =>
				job.input_hash === row.input_hash &&
				job.project_id === policy.scope.projectId &&
				job.origin_branch_row_id === branch.id &&
				job.origin_revision === branch.revision,
		);
		const linkedSummary =
			typeof row.summary_id === "string" && summaryProjects.get(row.summary_id) === policy.scope.projectId;
		if (row.branch_row_id !== branch.id || row.revision !== branch.revision || (!linkedJob && !linkedSummary)) {
			throw new Error(`${label} added branch_summary_spans outside the declared projection scope or linkage`);
		}
	}

	const payloadByJob = (
		rows: readonly Record<string, unknown>[],
		tableName: "job_inputs" | "job_lineage",
	): Map<string, Record<string, unknown>[]> => {
		const result = new Map<string, Record<string, unknown>[]>();
		for (const row of rows) {
			const jobId = typeof row.job_id === "string" ? row.job_id : undefined;
			if (!jobId || !addedJobs.has(jobId)) {
				throw new Error(`${label} added ${tableName} outside a newly scheduled target job`);
			}
			const jobRows = result.get(jobId) ?? [];
			jobRows.push(row);
			result.set(jobId, jobRows);
		}
		return result;
	};
	const inputsByJob = payloadByJob(addedInputs, "job_inputs");
	const lineageByJob = payloadByJob(addedLineage, "job_lineage");
	const orderedPayload = (
		rows: readonly Record<string, unknown>[] | undefined,
		tableName: "job_inputs" | "job_lineage",
		jobId: string,
	): Record<string, unknown>[] => {
		const ordered = [...(rows ?? [])].sort((left, right) => Number(left.ordinal) - Number(right.ordinal));
		if (ordered.length === 0) throw new Error(`${label} added job ${jobId} without ${tableName}`);
		if (ordered.some((row, index) => row.ordinal !== index)) {
			throw new Error(`${label} added non-contiguous ${tableName} ordinals for ${jobId}`);
		}
		return ordered;
	};

	for (const [jobId, job] of addedJobs) {
		const level = job.level;
		const inputHash = job.input_hash;
		if (
			job.project_id !== policy.scope.projectId ||
			job.origin_branch_row_id !== branch.id ||
			job.origin_revision !== branch.revision ||
			typeof level !== "number" ||
			!Number.isSafeInteger(level) ||
			level < 0 ||
			typeof inputHash !== "string"
		) {
			throw new Error(`${label} added job ${jobId} outside the declared projection scope or linkage`);
		}
		const linkedSpans = addedSpans.filter(
			row =>
				row.branch_row_id === branch.id &&
				row.revision === branch.revision &&
				row.level === level &&
				row.input_hash === inputHash,
		);
		if (linkedSpans.length === 0) throw new Error(`${label} added job ${jobId} without a linked target span`);
		const inputs = orderedPayload(inputsByJob.get(jobId), "job_inputs", jobId);
		const lineage = orderedPayload(lineageByJob.get(jobId), "job_lineage", jobId);
		const computedInputHash = replaySummaryInputHash(policy.scope.projectId, level, inputs);
		if (inputHash !== computedInputHash || jobId !== `job_${computedInputHash}`) {
			throw new Error(`${label} added job ${jobId} payload does not match its content-addressed input hash`);
		}
		const lineageKeys = lineage.map(row => row.source_key);
		if (lineageKeys.some(value => typeof value !== "string")) {
			throw new Error(`${label} added job ${jobId} has invalid lineage source keys`);
		}

		for (const span of linkedSpans) {
			const start = span.start_position;
			const end = span.end_position;
			if (
				typeof start !== "number" ||
				!Number.isSafeInteger(start) ||
				start < 0 ||
				typeof end !== "number" ||
				!Number.isSafeInteger(end) ||
				end <= start ||
				end > targetSourceKeys.length ||
				canonicalJson(lineageKeys) !== canonicalJson(targetSourceKeys.slice(start, end))
			) {
				throw new Error(`${label} added job ${jobId} lineage does not cover its exact target span`);
			}
			if (level === 0) {
				const sourceInputs = inputs.map(row => (row.input_kind === "source" ? row.ref_id : undefined));
				if (canonicalJson(sourceInputs) !== canonicalJson(lineageKeys)) {
					throw new Error(`${label} added leaf job ${jobId} inputs do not match its exact target lineage`);
				}
				continue;
			}

			let cursor = start;
			for (const input of inputs) {
				if (input.input_kind !== "summary" || typeof input.ref_id !== "string") {
					throw new Error(`${label} added parent job ${jobId} has a non-summary input`);
				}
				const placements = targetSpans.filter(
					row =>
						row.summary_id === input.ref_id &&
						row.level === level - 1 &&
						row.start_position === cursor &&
						typeof row.end_position === "number" &&
						row.end_position <= end,
				);
				if (placements.length !== 1) {
					throw new Error(`${label} added parent job ${jobId} input is not bound to one direct child span`);
				}
				cursor = placements[0]!.end_position as number;
			}
			if (cursor !== end) {
				throw new Error(`${label} added parent job ${jobId} inputs do not cover its exact target span`);
			}
		}
	}
}

function assertMigrationLogicalTables(before: SqliteReport, after: SqliteReport, label: string): void {
	assertLogicalTablesUnchanged(before, after, new Set(["summary_jobs"]), new Set(["summary_retry_policies"]), label);
	if (!before.logicalTablesComplete || !after.logicalTablesComplete) return;
	const beforeJobs = logicalTablesByName(before, label).get("summary_jobs");
	const afterJobs = logicalTablesByName(after, label).get("summary_jobs");
	if (!beforeJobs || !afterJobs) throw new Error(`${label} omitted the persisted summary_jobs table`);
	const additions = afterJobs.columns.filter(column => !beforeJobs.columns.includes(column));
	const stableColumns = afterJobs.columns.filter(column => !additions.includes(column));
	if (
		!canonicalValueMatches(stableColumns, beforeJobs.columns) ||
		!canonicalValueMatches(additions, ["retry_epoch", "lease_policy_token", "lease_mutation_nonce"])
	) {
		throw new Error(`${label} changed summary_jobs columns outside exact schema-10 additions`);
	}
}

function assertPreparedTransition(before: SqliteReport, after: SqliteReport, label: string): void {
	if (canonicalJson(before.sourceRows) !== canonicalJson(after.sourceRows)) {
		throw new Error(`${label} mutated pre-existing source rows or branch scope`);
	}
	if (canonicalJson(before.summaryRows) !== canonicalJson(after.summaryRows)) {
		throw new Error(`${label} mutated pre-existing summary rows`);
	}
	if (canonicalJson(before.summaryLineageRows) !== canonicalJson(after.summaryLineageRows)) {
		throw new Error(`${label} mutated persisted summary lineage or branch placement`);
	}
	if (canonicalJson(before.projectIds) !== canonicalJson(after.projectIds)) {
		throw new Error(`${label} mutated project scope`);
	}
	if (before.schemaVersion === after.schemaVersion) {
		assertLogicalTablesUnchanged(before, after, new Set(), new Set(), label);
		if (
			canonicalJson(before.schemaRows) !== canonicalJson(after.schemaRows) ||
			canonicalJson(before.jobRows) !== canonicalJson(after.jobRows) ||
			canonicalJson(before.policyRows) !== canonicalJson(after.policyRows) ||
			canonicalJson(before.retryEpochs) !== canonicalJson(after.retryEpochs)
		) {
			throw new Error(`${label} changed job, retry policy, or epoch without a schema migration`);
		}
		return;
	}
	if (before.schemaVersion !== 9 || after.schemaVersion !== 10) {
		throw new Error(`${label} changed schema outside the exact 9 -> 10 migration`);
	}
	assertMigrationLogicalTables(before, after, label);
	assertRetryMigrationSchema(before, after, label);
	assertMigratedJobs(before.jobRows, after.jobRows, label);
	if (canonicalJson(after.policyRows) !== canonicalJson(blockedRetryPolicies(before.projectIds))) {
		throw new Error(`${label} did not create the exact blocked schema-10 retry policies`);
	}
	const expectedEpochs = before.projectIds.length > 0 || before.jobRows.length > 0 ? [0] : [];
	if (canonicalJson(after.retryEpochs) !== canonicalJson(expectedEpochs)) {
		throw new Error(`${label} did not initialize the exact blocked retry epoch`);
	}
}

function assertPolicyPreparation(
	before: SqliteReport,
	after: SqliteReport,
	projectId: string,
	retryKey: string,
	label: string,
	appendOnlyTables: readonly string[] = [],
	branchPolicy?: PreparedBranchPolicy,
): void {
	if (before.schemaVersion < 10) {
		assertPreparedTransition(before, after, label);
		return;
	}
	const excludedTables = new Set([
		"summary_jobs",
		"summary_retry_policies",
		...appendOnlyTables,
		...(branchPolicy ? ["branches"] : []),
	]);
	assertLogicalTablesUnchanged(before, after, excludedTables, new Set(), label);
	for (const table of appendOnlyTables) assertPreExistingRowsPreserved(before, after, table, label);
	if (branchPolicy && appendOnlyTables.length > 0) {
		assertProjectionAppendRows(before, after, branchPolicy, label);
	}
	if (branchPolicy) assertBranchPolicyPreparation(before, after, branchPolicy, label);
	if (
		canonicalJson(before.schemaRows) !== canonicalJson(after.schemaRows) ||
		canonicalJson(before.sourceRows) !== canonicalJson(after.sourceRows) ||
		canonicalJson(before.summaryRows) !== canonicalJson(after.summaryRows) ||
		canonicalJson(before.summaryLineageRows) !== canonicalJson(after.summaryLineageRows) ||
		canonicalJson(before.projectIds) !== canonicalJson(after.projectIds)
	) {
		throw new Error(`${label} mutated schema, source, summary, or project scope`);
	}
	const policies = new Map(after.policyRows.map(row => [row.project_id, row]));
	const targetExisted = before.policyRows.some(row => row.project_id === projectId);
	if (policies.size !== before.policyRows.length + (targetExisted ? 0 : 1)) {
		throw new Error(`${label} added or removed unrelated retry policies`);
	}
	for (const row of before.policyRows) {
		const candidate = typeof row.project_id === "string" ? policies.get(row.project_id) : undefined;
		if (!candidate) throw new Error(`${label} removed retry policy`);
		if (row.project_id !== projectId && canonicalJson(row) !== canonicalJson(candidate)) {
			throw new Error(`${label} mutated an unrelated retry policy`);
		}
	}
	const target = policies.get(projectId);
	if (
		!target ||
		target.retry_key !== retryKey ||
		target.epoch !== 1 ||
		typeof target.claim_token !== "string" ||
		target.claim_token.length === 0 ||
		typeof target.updated_at !== "number"
	) {
		throw new Error(`${label} did not initialize the captured retry policy`);
	}
	const candidates = jobsByIdentity(after.jobRows);
	if (candidates.size < before.jobRows.length) throw new Error(`${label} removed jobs`);
	for (const row of before.jobRows) {
		const identity = jobIdentity(row);
		const candidate = identity ? candidates.get(identity) : undefined;
		if (!candidate) throw new Error(`${label} removed pre-existing job ${identity ?? "<missing>"}`);
		if (row.project_id !== projectId) {
			if (canonicalJson(row) !== canonicalJson(candidate)) throw new Error(`${label} mutated an unrelated job`);
			candidates.delete(identity!);
			continue;
		}
		const stableBefore = { ...row };
		const stableAfter = { ...candidate };
		for (const field of [
			"retry_epoch",
			"status",
			"worker_id",
			"lease_token",
			"lease_expires_at",
			"lease_input_tokens",
			"lease_output_budget",
			"lease_policy_token",
			"lease_mutation_nonce",
			"transport_retry_count",
			"available_at",
			"last_error",
			"updated_at",
		]) {
			delete stableBefore[field];
			delete stableAfter[field];
		}
		const matchingRetryKey = row.last_resolved_model === retryKey;
		const expectedStatus = !matchingRetryKey && row.status === "failed" ? "pending" : row.status;
		const expectedRetryCount = matchingRetryKey ? row.transport_retry_count : 0;
		if (
			canonicalJson(stableBefore) !== canonicalJson(stableAfter) ||
			candidate.retry_epoch !== 1 ||
			candidate.status !== expectedStatus ||
			candidate.transport_retry_count !== expectedRetryCount ||
			(matchingRetryKey &&
				(candidate.available_at !== row.available_at || candidate.last_error !== row.last_error)) ||
			candidate.worker_id !== null ||
			candidate.lease_token !== null ||
			candidate.lease_expires_at !== null ||
			candidate.lease_input_tokens !== null ||
			candidate.lease_output_budget !== null ||
			candidate.lease_policy_token !== null ||
			candidate.lease_mutation_nonce !== null
		) {
			throw new Error(`${label} mutated a current-project job outside exact policy initialization`);
		}
		candidates.delete(identity!);
	}
	for (const candidate of candidates.values()) {
		if (
			candidate.project_id !== projectId ||
			candidate.status !== "pending" ||
			candidate.retry_epoch !== 1 ||
			candidate.transport_retry_count !== 0 ||
			candidate.last_resolved_model !== null ||
			candidate.last_error !== null ||
			candidate.worker_id !== null ||
			candidate.lease_token !== null ||
			candidate.lease_expires_at !== null ||
			candidate.lease_input_tokens !== null ||
			candidate.lease_output_budget !== null ||
			candidate.lease_policy_token !== null ||
			candidate.lease_mutation_nonce !== null ||
			typeof candidate.available_at !== "number" ||
			typeof candidate.created_at !== "number" ||
			typeof candidate.updated_at !== "number"
		) {
			throw new Error(`${label} added a job outside exact prepared projection scheduling`);
		}
	}
	if (!after.retryEpochs.includes(1)) throw new Error(`${label} omitted initialized retry epoch 1`);
}

export function assertProjectionPreparation(
	before: SqliteReport,
	after: SqliteReport,
	projectId: string,
	retryKey: string,
	branchPolicy: PreparedBranchPolicy,
	label: string,
	projectionScheduled = true,
): void {
	assertPolicyPreparation(
		before,
		after,
		projectId,
		retryKey,
		label,
		projectionScheduled ? PROJECTION_APPEND_TABLES : [],
		projectionScheduled ? branchPolicy : undefined,
	);
	if (canonicalJson(before.policyRows) !== canonicalJson(after.policyRows)) {
		throw new Error(`${label} mutated the resolved retry policy while scheduling prepared jobs`);
	}
}

function declaredPreparedBranchPolicy(report: ReplayReport, label: string): PreparedBranchPolicy {
	const branchTable = report.storeEvidence?.preparedTemplate.logicalTables?.find(table => table.name === "branches");
	const row = branchTable?.rows.find(
		candidate =>
			candidate.project_id === report.fixture.projectId &&
			candidate.session_id === report.fixture.sessionId &&
			candidate.branch_id === report.fixture.branchId,
	);
	const tokenBudget = row?.summary_token_budget;
	const maxSources = row?.fresh_tail_max_sources;
	const maxTokens = row?.fresh_tail_max_tokens;
	if (
		typeof tokenBudget !== "number" ||
		!Number.isSafeInteger(tokenBudget) ||
		tokenBudget < 0 ||
		typeof maxSources !== "number" ||
		!Number.isSafeInteger(maxSources) ||
		maxSources < 0 ||
		typeof maxTokens !== "number" ||
		!Number.isSafeInteger(maxTokens) ||
		maxTokens < 0
	) {
		throw new Error(`${label} does not declare one valid prepared branch projection policy`);
	}
	const sampleBudgets = new Set(
		report.samples.map(sample => sample.tokens?.budget).filter((value): value is number => value !== undefined),
	);
	if (sampleBudgets.size > 1 || (sampleBudgets.size === 1 && !sampleBudgets.has(tokenBudget))) {
		throw new Error(`${label} sample route budget contradicts the prepared branch projection policy`);
	}
	return {
		scope: {
			projectId: report.fixture.projectId,
			sessionId: report.fixture.sessionId,
			branchId: report.fixture.branchId,
		},
		tokenBudget,
		maxSources,
		maxTokens,
	};
}

interface CapturedRetryPolicy {
	retryKey: string;
	retryEpoch: number;
}

async function configureCapturedRetryPolicy(
	context: LcmContext,
	schemaVersion: number,
	projectId: string,
	retryKey: string,
): Promise<CapturedRetryPolicy> {
	if (schemaVersion < 10) return { retryKey, retryEpoch: 0 };
	const policyContext = context as unknown as {
		configureSummaryRetryPolicy?: (projectId: string, retryKey: string) => unknown | Promise<unknown>;
	};
	if (!policyContext.configureSummaryRetryPolicy)
		throw new Error("schema-10 context lacks configureSummaryRetryPolicy");
	const result = await policyContext.configureSummaryRetryPolicy(projectId, retryKey);
	if (
		!result ||
		typeof result !== "object" ||
		!("kind" in result) ||
		result.kind !== "ready" ||
		!("retryKey" in result) ||
		typeof result.retryKey !== "string" ||
		!("retryEpoch" in result) ||
		typeof result.retryEpoch !== "number"
	) {
		throw new Error("captured summary retry policy did not become ready");
	}
	return { retryKey: result.retryKey, retryEpoch: result.retryEpoch };
}

interface ContentFreeBoundary {
	thresholdTokens: number;
	freshTail: { maxSources: number; maxTokens: number };
}

function contentFreeBoundary(snapshot: SourceSnapshot): ContentFreeBoundary {
	const maxSources = snapshot.entries.length > 32 ? 32 : Math.max(1, snapshot.entries.length - 1);
	const freshTail = { maxSources, maxTokens: CONTENT_FREE_FRESH_TOKENS };
	let freshSources = 0;
	let freshTokens = 0;
	for (let index = snapshot.entries.length - 1; index >= 0 && freshSources < maxSources; index--) {
		const tokens = canonicalTokens(snapshot.entries[index]!.redactedText);
		if (freshSources > 0 && freshTokens + tokens > freshTail.maxTokens) break;
		freshSources++;
		freshTokens += tokens;
	}
	const firstSourceTokens =
		snapshot.entries.length > freshSources ? canonicalTokens(snapshot.entries[0]!.redactedText) : 0;
	const leafCount = Math.ceil(Math.max(0, snapshot.entries.length - freshSources) / CONTENT_FREE_LEAF_SOURCES);
	const thresholdTokens = Math.min(
		DEFAULT_THRESHOLD_TOKENS,
		Math.max(1_000, freshTokens + firstSourceTokens + leafCount * 500 + 1_000),
	);
	return { thresholdTokens, freshTail };
}

function contentFreeContextOptions(): {
	leafChunk: { maxSources: number; maxTokens: number };
	condenseFanIn: number;
} {
	return {
		leafChunk: { maxSources: CONTENT_FREE_LEAF_SOURCES, maxTokens: CONTENT_FREE_LEAF_TOKENS },
		condenseFanIn: CONTENT_FREE_CONDENSE_FAN_IN,
	};
}

function contentFreeFixtureHash(sourceCount: number, boundary: ContentFreeBoundary): string {
	return hashText(
		canonicalJson({
			kind: "content-free-shape",
			sourceCount,
			boundary,
			contextOptions: contentFreeContextOptions(),
			deterministicWork: CONTENT_FREE_EXPECTED_WORK,
			schema: HARNESS_SCHEMA,
		}),
	);
}

export function replayCancellationControlFixtureHash(sourceCount: number, boundary: ContentFreeBoundary): string {
	return hashText(
		canonicalJson({
			kind: "cancellation-control",
			sourceCount,
			boundary,
			contextOptions: contentFreeContextOptions(),
			schema: HARNESS_SCHEMA,
		}),
	);
}

function legacyContentFreeBoundary(sourceCount: number): ContentFreeBoundary {
	return {
		thresholdTokens: Math.max(10_000, Math.max(1, sourceCount) * 6_000 + 2_000),
		freshTail: { maxSources: 32, maxTokens: CONTENT_FREE_FRESH_TOKENS },
	};
}

function legacyContentFreeFixtureHash(sourceCount: number): string {
	return hashText(canonicalJson({ kind: "content-free-shape", sourceCount, schema: HARNESS_SCHEMA }));
}

function runtimeEnvelopeIsAuthoritative(
	envelope: CapturedRuntimeEnvelope | undefined,
	snapshot: SourceSnapshot,
	store: SqliteReport,
	settings: CapturedRuntimeEnvelope["settings"],
): boolean {
	const runtimeInputs = reproducibleRuntimeInputs(envelope);
	return Boolean(
		envelope &&
			runtimeInputs &&
			envelope.projectId === snapshot.scope.projectId &&
			envelope.sessionId === snapshot.scope.sessionId &&
			envelope.branchId === snapshot.scope.branchId &&
			canonicalJson(envelope.orderedSourceKeys) === canonicalJson(store.sourceRows.map(row => row.sourceKey)) &&
			envelope.summaryModelSelector === MODEL_SELECTOR &&
			canonicalJson(envelope.settings) === canonicalJson(settings),
	);
}

async function executeContentFreePair(
	options: CaptureOptions,
	sourceCount: number,
	root: string,
): Promise<NonNullable<ReplayReport["syntheticPair"]>> {
	await assertArtifactPath(root, "content-free pair directory", "write");
	await createOwnedPrivateDirectory(root);
	const manager = await createContentFreeManager(root, sourceCount);
	const snapshot: { value: SourceSnapshot } = {
		value: normalizeLcmBranch(manager, "lcm-replay-content-free", String),
	};
	const boundary = contentFreeBoundary(snapshot.value);
	const templatePath = path.join(root, "template.sqlite");
	const contextOptions = contentFreeContextOptions();
	const context = await openLcmContext({ dbPath: templatePath, ...contextOptions });
	let configured: SqliteReport;
	try {
		context.reconcile(snapshot.value, { summarize: false });
		const seeded = sqliteReport(
			templatePath,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
		await configureCapturedRetryPolicy(
			context,
			seeded.schemaVersion,
			snapshot.value.scope.projectId,
			`lcm-replay/${MODEL_ID}`,
		);
		configured = sqliteReport(
			templatePath,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
	} finally {
		context.close();
	}
	await Promise.all([
		fs.rm(templatePath, { force: true }),
		fs.rm(`${templatePath}-wal`, { force: true }),
		fs.rm(`${templatePath}-shm`, { force: true }),
	]);
	await writePrivateArtifact(templatePath, configured.snapshotBytes);
	const pairOptions: CaptureOptions = {
		...options,
		requestTokensFloor: undefined,
		runtimeEnvelope: undefined,
		snapshotIn: templatePath,
		thresholdTokens: boundary.thresholdTokens,
		workRoot: path.join(root, "work"),
	};
	const preparationSnapshot: { bytes?: Uint8Array } = {};
	await runSample(manager, "boundary-summary", pairOptions, 0, snapshot, preparationSnapshot, { contextOptions });
	if (!preparationSnapshot.bytes) throw new Error("content-free fixture preparation produced no SQLite snapshot");
	await writePrivateArtifact(templatePath, preparationSnapshot.bytes);
	const prepared = sqliteReport(
		templatePath,
		snapshot.value.scope.projectId,
		snapshot.value.scope.sessionId,
		snapshot.value.scope.branchId,
	);
	if (prepared.quickCheck !== "ok") throw new Error("content-free prepared template failed SQLite quick_check");
	const samples: SampleReport[] = [];
	const logicalSnapshot: { bytes?: Uint8Array } = {};
	for (let sample = 1; sample <= 5; sample++) {
		samples.push(
			await runSample(manager, "minimal-marker-budget", pairOptions, sample, snapshot, logicalSnapshot, {
				contextOptions,
				recordControlEvidence: false,
			}),
		);
	}
	return {
		kind: "content-free-shape",
		sourceCount: snapshot.value.entries.length,
		boundary,
		scope: snapshot.value.scope,
		templatePath,
		templateEvidence: preparedTemplateEvidence(prepared),
		fixtureHash: contentFreeFixtureHash(snapshot.value.entries.length, boundary),
		samples,
	};
}

async function executeCancellationControl(
	options: CaptureOptions,
	root: string,
): Promise<NonNullable<ReplayReport["cancellationControl"]>> {
	await assertArtifactPath(root, "cancellation control directory", "write");
	await createOwnedPrivateDirectory(root);
	const manager = await createContentFreeManager(root, CONTENT_FREE_MIN_SOURCES);
	const snapshot: { value: SourceSnapshot } = {
		value: normalizeLcmBranch(manager, "lcm-replay-cancellation", String),
	};
	const boundary = contentFreeBoundary(snapshot.value);
	const contextOptions = contentFreeContextOptions();
	const templatePath = path.join(root, "template.sqlite");
	const context = await openLcmContext({ dbPath: templatePath, ...contextOptions });
	let configured: SqliteReport;
	try {
		context.reconcile(snapshot.value, { summarize: false });
		const seeded = sqliteReport(
			templatePath,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
		await configureCapturedRetryPolicy(
			context,
			seeded.schemaVersion,
			snapshot.value.scope.projectId,
			`lcm-replay/${MODEL_ID}`,
		);
		configured = sqliteReport(
			templatePath,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
	} finally {
		context.close();
	}
	await Promise.all([
		fs.rm(templatePath, { force: true }),
		fs.rm(`${templatePath}-wal`, { force: true }),
		fs.rm(`${templatePath}-shm`, { force: true }),
	]);
	await writePrivateArtifact(templatePath, configured.snapshotBytes);
	const sample = await runSample(
		manager,
		"cancellation",
		{
			...options,
			requestTokensFloor: undefined,
			runtimeEnvelope: undefined,
			snapshotIn: templatePath,
			thresholdTokens: boundary.thresholdTokens,
			workRoot: path.join(root, "work"),
		},
		0,
		snapshot,
		{},
		{ contextOptions },
	);
	return {
		kind: "cancellation",
		sourceCount: snapshot.value.entries.length,
		boundary,
		scope: snapshot.value.scope,
		templatePath,
		templateEvidence: preparedTemplateEvidence(configured),
		fixtureHash: replayCancellationControlFixtureHash(snapshot.value.entries.length, boundary),
		sample,
	};
}
async function capture(inputOptions: CaptureOptions): Promise<ReplayReport> {
	const runtimeEnvelope = await loadRuntimeEnvelope(inputOptions.runtimeEnvelopeIn);
	const options: CaptureOptions = { ...inputOptions, runtimeEnvelope };
	const realLoaded =
		options.fixture === "real"
			? await loadRealManager(options.replayPath!, options.markerId, options.agentDir)
			: undefined;
	const suppliedSnapshot = options.sourceIn
		? ((await Bun.file(options.sourceIn).json()) as SourceSnapshot)
		: undefined;
	const realNormalizedSnapshot = realLoaded
		? normalizeLcmBranch(realLoaded.manager, runtimeEnvelope?.projectId ?? "lcm-replay", String)
		: undefined;
	if (
		suppliedSnapshot &&
		realNormalizedSnapshot &&
		canonicalJson(suppliedSnapshot) !== canonicalJson(realNormalizedSnapshot)
	) {
		throw new Error("supplied source snapshot does not exactly match the normalized selected journal");
	}
	const artifactBase = path.join(path.dirname(options.out), path.basename(options.out, path.extname(options.out)));
	const rawPrefixPath = `${artifactBase}.raw-prefix.jsonl`;
	const resolvedPrefixPath = `${artifactBase}.resolved-prefix.jsonl`;
	await fs.mkdir(path.dirname(options.out), { recursive: true });
	const privateRunRoot = await fs.mkdtemp(`${artifactBase}.run-`);
	await fs.chmod(privateRunRoot, 0o700);
	const harnessSourcePath = path.join(privateRunRoot, "harness-source.ts");
	const treatmentTemplatePath = path.join(privateRunRoot, "treatment.sqlite");
	const migratedTemplatePath = path.join(privateRunRoot, "migrated.sqlite");
	const contentFreeRoot = path.join(privateRunRoot, "content-free");
	const failureControlRoot = path.join(privateRunRoot, "failure-controls");
	const cancellationControlRoot = path.join(privateRunRoot, "cancellation-control");
	const workRoot = path.join(privateRunRoot, "work");
	const syntheticRoot = path.join(privateRunRoot, "synthetic-session");
	const seedPath = path.join(privateRunRoot, "pristine-seed.sqlite");
	await Promise.all([
		assertArtifactPath(privateRunRoot, "private replay run directory", "write"),
		assertArtifactPath(harnessSourcePath, "harness source output", "write"),
		assertArtifactPath(rawPrefixPath, "raw prefix output", "write"),
		assertArtifactPath(resolvedPrefixPath, "resolved prefix output", "write"),
		assertArtifactPath(treatmentTemplatePath, "treatment template output", "write"),
		assertArtifactPath(migratedTemplatePath, "migrated template output", "write"),
		assertArtifactPath(contentFreeRoot, "content-free pair directory", "write"),
		assertArtifactPath(failureControlRoot, "failure control directory", "write"),
		assertArtifactPath(cancellationControlRoot, "cancellation control directory", "write"),
		assertArtifactPath(workRoot, "sample work directory", "write"),
		assertArtifactPath(syntheticRoot, "synthetic session directory", "write"),
	]);
	const harnessSourceText = await Bun.file(import.meta.path).text();
	await writePrivateArtifact(harnessSourcePath, harnessSourceText);
	const loaded =
		realLoaded ??
		(await (async () => {
			const manager = await createSyntheticManager(syntheticRoot, options.fixture);
			const replication = manager.snapshotForReplication();
			const prefix = `${[replication.header, ...replication.entries].map(entry => JSON.stringify(entry)).join("\n")}\n`;
			return {
				manager,
				identity: syntheticIdentity(manager, options.fixture),
				rawPrefix: prefix,
				resolvedPrefix: prefix,
				missingBlobRefs: [] as string[],
				blobEvidence: { verifiedRefs: [], digestMismatchRefs: [], readFailureRefs: [] } satisfies BlobEvidence,
				prefixIdentity: { rawHash: hashText(prefix), resolvedHash: hashText(prefix) },
			};
		})());
	const messages = loaded.manager.buildSessionContext().messages;
	const sourceTokens = messages.reduce((total, message) => total + estimateTokens(message), 0);
	const normalizedSnapshot =
		realNormalizedSnapshot ?? normalizeLcmBranch(loaded.manager, runtimeEnvelope?.projectId ?? "lcm-replay", String);
	if (suppliedSnapshot && canonicalJson(suppliedSnapshot) !== canonicalJson(normalizedSnapshot)) {
		throw new Error("supplied source snapshot does not exactly match the normalized selected journal");
	}
	const snapshot: { value: SourceSnapshot } = { value: suppliedSnapshot ?? normalizedSnapshot };
	const sourceSnapshotText = canonicalJson(snapshot.value);
	const seededStore = await sourceSnapshotStoreReport(seedPath, normalizedSnapshot);
	if (seededStore.quickCheck !== "ok") throw new Error("seeded SQLite quick_check failed");
	const sqliteInput = options.snapshotIn ?? options.storePath;
	let snapshotBytes = seededStore.snapshotBytes;
	if (sqliteInput) {
		const sourceStore = sqliteReport(
			sqliteInput,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
		if (sourceStore.quickCheck !== "ok") throw new Error("captured SQLite quick_check failed");
		if (canonicalJson(sourceStore.sourceRows) !== canonicalJson(seededStore.sourceRows)) {
			throw new Error("captured store source rows or source keys do not match the normalized selected journal");
		}
		snapshotBytes = sourceStore.snapshotBytes;
	}
	await Promise.all(
		[options.out, options.snapshotOut, options.sourceOut].map(output =>
			fs.mkdir(path.dirname(output), { recursive: true }),
		),
	);
	await Promise.all([
		writePrivateArtifact(rawPrefixPath, loaded.rawPrefix),
		writePrivateArtifact(resolvedPrefixPath, loaded.resolvedPrefix),
		writePrivateArtifact(options.sourceOut, `${sourceSnapshotText}\n`),
		writePrivateArtifact(options.snapshotOut, snapshotBytes),
	]);
	const pristineStore = sqliteReport(
		options.snapshotOut,
		snapshot.value.scope.projectId,
		snapshot.value.scope.sessionId,
		snapshot.value.scope.branchId,
	);
	if (pristineStore.quickCheck !== "ok") throw new Error("serialized SQLite clone quick_check failed");
	await writePrivateArtifact(treatmentTemplatePath, pristineStore.snapshotBytes);
	const retryKey = `lcm-replay/${MODEL_ID}`;
	const projectionPolicy = capturedProjectionPolicy(options);
	const providerControlLeafChunk = providerBackoffLeafChunk(options.fixture, snapshot.value);
	const treatmentContext = await openLcmContext({
		dbPath: treatmentTemplatePath,
		...(providerControlLeafChunk ? { leafChunk: providerControlLeafChunk } : {}),
	});
	let migratedTreatment: SqliteReport;
	let preparedTreatment: SqliteReport;
	try {
		migratedTreatment = sqliteReport(
			treatmentTemplatePath,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
		assertPreparedTransition(pristineStore, migratedTreatment, "schema treatment");
		await writePrivateArtifact(migratedTemplatePath, migratedTreatment.snapshotBytes);
		await configureCapturedRetryPolicy(
			treatmentContext,
			migratedTreatment.schemaVersion,
			snapshot.value.scope.projectId,
			retryKey,
		);
		const configuredTreatment = sqliteReport(
			treatmentTemplatePath,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
		assertPolicyPreparation(
			migratedTreatment,
			configuredTreatment,
			snapshot.value.scope.projectId,
			retryKey,
			"retry policy preparation",
		);
		if (options.fixture !== "cancellation" && projectionPolicy.tokenBudget >= 1) {
			const initialized = treatmentContext.reconcile(snapshot.value, { summarize: projectionPolicy });
			if (initialized.changed) throw new Error("projection policy preparation changed the captured source frontier");
			const initializedPerformance = treatmentContext.status().performance;
			if (!initializedPerformance) throw new Error("prepared context omitted performance counters");
			const initializedRowsChanged = initializedPerformance.reconcileRowsChanged;
			treatmentContext.reconcile(snapshot.value, { summarize: projectionPolicy });
			const matchingPerformance = treatmentContext.status().performance;
			if (!matchingPerformance || matchingPerformance.reconcileRowsChanged !== initializedRowsChanged) {
				throw new Error("prepared projection policy did not make matching reconcile write-free");
			}
		}
		preparedTreatment = sqliteReport(
			treatmentTemplatePath,
			snapshot.value.scope.projectId,
			snapshot.value.scope.sessionId,
			snapshot.value.scope.branchId,
		);
		assertProjectionPreparation(
			configuredTreatment,
			preparedTreatment,
			snapshot.value.scope.projectId,
			retryKey,
			{
				scope: snapshot.value.scope,
				tokenBudget: projectionPolicy.tokenBudget,
				maxSources: projectionPolicy.freshTail.maxSources,
				maxTokens: projectionPolicy.freshTail.maxTokens,
			},
			"projection policy preparation",
			options.fixture !== "cancellation",
		);
	} finally {
		treatmentContext.close();
	}
	await Promise.all([
		fs.rm(treatmentTemplatePath, { force: true }),
		fs.rm(`${treatmentTemplatePath}-wal`, { force: true }),
		fs.rm(`${treatmentTemplatePath}-shm`, { force: true }),
	]);
	await writePrivateArtifact(treatmentTemplatePath, preparedTreatment.snapshotBytes);
	const treatmentStore = sqliteReport(
		treatmentTemplatePath,
		snapshot.value.scope.projectId,
		snapshot.value.scope.sessionId,
		snapshot.value.scope.branchId,
	);
	if (treatmentStore.serializedStoreHash !== preparedTreatment.serializedStoreHash) {
		throw new Error("treatment template logical reserialization drifted");
	}
	const runOptions: CaptureOptions = { ...options, snapshotIn: treatmentTemplatePath, workRoot };
	if (treatmentStore.quickCheck !== "ok") throw new Error("treatment template SQLite quick_check failed");
	if (canonicalJson(treatmentStore.sourceRows) !== canonicalJson(pristineStore.sourceRows)) {
		throw new Error("prepared treatment template mutated pre-existing source rows or branch scope");
	}
	const logicalSnapshot: { bytes?: Uint8Array } = {};
	const samples: SampleReport[] = [];
	for (let sampleOrdinal = 1; sampleOrdinal <= options.samples; sampleOrdinal++) {
		samples.push(
			await runSample(loaded.manager, options.fixture, runOptions, sampleOrdinal, snapshot, logicalSnapshot),
		);
	}
	const preparedSourceRows = canonicalJson(treatmentStore.sourceRows);
	if (samples.some(sample => canonicalJson(sample.sourceRows) !== preparedSourceRows)) {
		throw new Error("sample pre-existing source rows or branch scope drifted");
	}
	const sourceSnapshotHash = hashText(sourceSnapshotText);
	const expectedMarker = loaded.rawPrefix
		.trimEnd()
		.split("\n")
		.map(line => JSON.parse(line) as Record<string, unknown>)
		.findLast(entry => entry.id === loaded.identity.markerId);
	const expectedFallback = typeof expectedMarker?.lcmFallback === "string" ? expectedMarker.lcmFallback : undefined;
	const originalRedactionIdentityAvailable =
		loaded.missingBlobRefs.length === 0 &&
		pristineStore.sourceRows.length === snapshot.value.entries.length &&
		pristineStore.sourceRows.every((row, index) => row.entryId === snapshot.value.entries[index]?.entryId);
	const maintenanceAuthoritative =
		options.requestTokensFloor === undefined || samples.every(sample => sample.maintenance?.authoritative === true);
	const preChangeContractReproduced =
		options.fixture !== "real" ||
		(maintenanceAuthoritative &&
			expectedFallback !== undefined &&
			samples.every(
				sample =>
					!sample.fitProof.owned &&
					sample.status?.route === "native_fallback" &&
					sample.fallbackCategory === expectedFallback,
			));
	const capturedSettings = {
		thresholdTokens: options.thresholdTokens,
		freshTailMaxSources: 32,
		freshTailMaxTokens: 16_000,
		maxConcurrentSummaries: 4,
	};
	const runtimeEnvelopeAuthoritative =
		options.fixture !== "real" ||
		runtimeEnvelopeIsAuthoritative(runtimeEnvelope, snapshot.value, pristineStore, capturedSettings);
	const capturedStoreAuthoritative = options.fixture !== "real" || sqliteInput !== undefined;
	const readyStore = samplesReady(samples);
	const qualification: ReconstructionQualification = {
		projectId: snapshot.value.scope.projectId,
		sessionId: snapshot.value.scope.sessionId,
		branchId: snapshot.value.scope.branchId,
		orderedSourceKeys: pristineStore.sourceRows.map(row => row.sourceKey),
		runtimeEnvelopeAuthoritative,
		capturedStoreAuthoritative,
		readyStore,
	};
	const classification =
		originalRedactionIdentityAvailable &&
		capturedStoreAuthoritative &&
		runtimeEnvelopeAuthoritative &&
		preChangeContractReproduced &&
		readyStore
			? "exact-historical-replay"
			: "historical-reconstruction-impossible";
	const baselineEligibility: BaselineEligibility = {
		classification,
		preChangeContractReproduced,
	};
	const reconstruction: ReconstructionStatus = {
		classification,
		preChangeContractReproduced,
		originalRedactionIdentityAvailable,
		missingBlobRefs: loaded.missingBlobRefs,
		syntheticFixture:
			classification === "historical-reconstruction-impossible"
				? {
						kind: "content-free-shape",
						sourceCount: Math.max(CONTENT_FREE_MIN_SOURCES, snapshot.value.entries.length),
					}
				: null,
		baselineEligibility,
		qualification,
	};
	const harnessSourceHash = hashText(harnessSourceText);
	const fixtureIdentity: ReplayReport["fixture"] = {
		name: options.fixture,
		markerId: loaded.identity.markerId,
		parentId: loaded.identity.parentId,
		markerOrdinal: loaded.identity.markerOrdinal,
		sessionId: snapshot.value.scope.sessionId,
		projectId: snapshot.value.scope.projectId,
		branchId: snapshot.value.scope.branchId,
		sessionTimestamp: loaded.identity.sessionTimestamp,
		selectedEntriesHash: loaded.identity.selectedEntriesHash,
		selectedEntries: loaded.identity.selectedEntries,
		journalFileBytesAtCapture: loaded.identity.journalFileBytesAtCapture,
		journalSuffixHash: loaded.identity.journalSuffixHash,
		...(options.requestTokensFloor === undefined ? {} : { requestTokensFloor: options.requestTokensFloor }),
		activeSources: snapshot.value.entries.length,
		sourceTokens,
		summaryModelSelector: runtimeEnvelope?.summaryModelSelector ?? MODEL_SELECTOR,
		contextWindow: runtimeEnvelope?.contextWindow ?? CONTEXT_WINDOW,
		settings: capturedSettings,
		estimatorSchema: ESTIMATOR_SCHEMA,
		harnessSourceHash,
		harnessIdentityHash: replayHarnessIdentityHash(harnessSourceHash),
		...(runtimeEnvelope ? { nonMessageTokens: runtimeEnvelope.nonMessageTokens } : {}),
		systemPromptHash: hashText(canonicalJson(runtimeEnvelope?.systemPrompt ?? ["system prompt"])),
		toolSchemaHash: hashText(canonicalJson(runtimeEnvelope?.toolSchemas ?? [])),
		skillHash: hashText(canonicalJson(runtimeEnvelope?.skills ?? [])),
		sqliteSnapshotHash: pristineStore.snapshotHash,
		logicalStoreHash: pristineStore.serializedStoreHash,
		reconstruction,
		prefixIdentity: loaded.prefixIdentity,
		blobEvidence: loaded.blobEvidence,
	};
	const representative = samples[0]!;
	const migratedEvidence = preparedTemplateEvidence(migratedTreatment);
	const preparedEvidence = preparedTemplateEvidence(treatmentStore);
	const [failureControls, cancellationControl, syntheticPair] = await Promise.all([
		options.fixture === "real" ? executeFailureControls(failureControlRoot) : Promise.resolve(undefined),
		executeCancellationControl(options, cancellationControlRoot),
		classification === "historical-reconstruction-impossible"
			? executeContentFreePair(options, snapshot.value.entries.length, contentFreeRoot)
			: Promise.resolve(undefined),
	]);
	const report: ReplayReport = {
		harnessSchema: HARNESS_SCHEMA,
		workloadFingerprint: "",
		sourceSnapshotHash,
		artifacts: {
			root: privateRunRoot,
			harnessSource: harnessSourcePath,
			rawPrefix: rawPrefixPath,
			resolvedPrefix: resolvedPrefixPath,
			sourceSnapshot: options.sourceOut,
			sqliteSnapshot: options.snapshotOut,
			treatmentTemplate: treatmentTemplatePath,
			migratedTemplate: migratedTemplatePath,
		},
		fixture: fixtureIdentity,
		treatment: {
			label: options.treatment,
			hardProjectionWaitMs: null,
			schemaVersion: representative.schemaVersion,
			retryKey,
		},
		storeEvidence: {
			pristine: {
				path: options.snapshotOut,
				byteHash: pristineStore.snapshotHash,
				logicalHash: pristineStore.serializedStoreHash,
				quickCheck: pristineStore.quickCheck,
			},
			migratedTemplate: migratedEvidence,
			preparedTemplate: preparedEvidence,
		},
		...(syntheticPair ? { syntheticPair } : {}),
		cancellationControl,
		...(failureControls ? { failureControls } : {}),
		candidateOutcome: {
			route: representative.status?.route ?? (representative.fitProof.owned ? "lossless" : "native"),
			reproducedOldFallback: preChangeContractReproduced,
		},
		samples,
		metrics: aggregateMetrics(samples, pristineStore.quickCheck, pristineStore.serializedStoreHash),
	};
	report.workloadFingerprint = replayWorkloadFingerprint(report);
	return report;
}

function qualificationSourceRows(report: ReplayReport): SourceRow[] {
	return report.storeEvidence?.preparedTemplate.sourceRows ?? report.samples[0]?.sourceRows ?? [];
}

function samplesReady(samples: readonly SampleReport[]): boolean {
	return samples.every(
		sample =>
			sample.providerAttempts === 0 &&
			sample.storeRowsChanged === 0 &&
			sample.counters?.rowsChanged === 0 &&
			sample.jobs.pending === 0 &&
			sample.jobs.leased === 0 &&
			sample.jobs.backoff === 0 &&
			sample.jobs.exhausted === 0 &&
			sample.jobs.missing === 0,
	);
}

function validateReconstruction(report: ReplayReport, label: string): void {
	const reconstruction = report.fixture.reconstruction;
	if (!reconstruction) throw new Error(`${label} historical reconstruction metadata is missing`);
	const qualification = reconstruction.qualification;
	if (report.fixture.harnessSourceHash && !qualification) {
		throw new Error(`${label} authoritative scope and runtime qualification is missing`);
	}
	if (qualification) {
		if (qualification.projectId !== report.fixture.projectId)
			throw new Error(`${label} project qualification mismatch`);
		if (qualification.sessionId !== report.fixture.sessionId)
			throw new Error(`${label} session qualification mismatch`);
		if (qualification.branchId !== report.fixture.branchId) throw new Error(`${label} branch qualification mismatch`);
		if (
			canonicalJson(qualification.orderedSourceKeys) !==
			canonicalJson(qualificationSourceRows(report).map(row => row.sourceKey))
		) {
			throw new Error(`${label} ordered sourceKey qualification mismatch`);
		}
	}
	const inputAvailable =
		reconstruction.originalRedactionIdentityAvailable && reconstruction.missingBlobRefs.length === 0;
	if (qualification) {
		const derivedReadyStore = samplesReady(report.samples);
		if (qualification.readyStore !== derivedReadyStore) {
			throw new Error(`${label} captured-store readiness contradicts executed samples`);
		}
		if (reconstruction.classification === "exact-historical-replay") {
			if (!inputAvailable) throw new Error(`${label} exact history requires original redaction identity`);
			if (!qualification.runtimeEnvelopeAuthoritative) {
				throw new Error(`${label} exact history requires an authoritative runtime envelope`);
			}
			if (qualification.capturedStoreAuthoritative !== true) {
				throw new Error(`${label} exact history requires an authoritative captured store`);
			}
			if (!derivedReadyStore) throw new Error(`${label} exact history requires a ready captured store`);
		}
		const derivedClassification =
			inputAvailable &&
			qualification.runtimeEnvelopeAuthoritative &&
			qualification.capturedStoreAuthoritative === true &&
			reconstruction.preChangeContractReproduced &&
			derivedReadyStore
				? "exact-historical-replay"
				: "historical-reconstruction-impossible";
		if (reconstruction.classification !== derivedClassification) {
			throw new Error(`${label} reconstruction classification contradicts captured evidence`);
		}
		const frozen = reconstruction.baselineEligibility;
		if (
			!frozen ||
			frozen.classification !== derivedClassification ||
			frozen.preChangeContractReproduced !== reconstruction.preChangeContractReproduced
		) {
			throw new Error(`${label} frozen baseline eligibility contradicts captured evidence`);
		}
	}
	if (reconstruction.classification === "exact-historical-replay") {
		if (reconstruction.syntheticFixture !== null)
			throw new Error(`${label} exact replay cannot carry a synthetic pair`);
	} else {
		const synthetic = reconstruction.syntheticFixture;
		if (synthetic?.kind !== "content-free-shape" || !Number.isSafeInteger(synthetic.sourceCount)) {
			throw new Error(`${label} historical-reconstruction-impossible case requires a content-free synthetic pair`);
		}
		if (
			!report.syntheticPair ||
			!/^[a-f0-9]{64}$/.test(report.syntheticPair.fixtureHash) ||
			report.syntheticPair.samples.length !== 5
		) {
			throw new Error(`${label} synthetic content-free pair must contain five executed samples`);
		}
	}
}

function workloadIdentity(report: ReplayReport, includeHarnessSourceHash = true): unknown {
	const reconstruction = report.fixture.reconstruction;
	const qualification = reconstruction.qualification;
	const { harnessSourceHash, harnessIdentityHash, ...fixture } = report.fixture;
	const identityQualification = qualification
		? {
				projectId: qualification.projectId,
				sessionId: qualification.sessionId,
				branchId: qualification.branchId,
				orderedSourceKeys: qualification.orderedSourceKeys,
				runtimeEnvelopeAuthoritative: qualification.runtimeEnvelopeAuthoritative,
				capturedStoreAuthoritative: qualification.capturedStoreAuthoritative,
			}
		: undefined;
	return {
		harnessSchema: report.harnessSchema,
		sourceSnapshotHash: report.sourceSnapshotHash,
		fixture: {
			...fixture,
			...(!includeHarnessSourceHash
				? {
						logicalStoreHash: undefined,
						reconstruction: undefined,
					}
				: {}),
			...(includeHarnessSourceHash ? { harnessSourceHash, harnessIdentityHash } : {}),
			reconstruction: {
				originalRedactionIdentityAvailable: reconstruction.originalRedactionIdentityAvailable,
				missingBlobRefs: reconstruction.missingBlobRefs,
				qualification: identityQualification,
			},
		},
		pristineByteHash: report.storeEvidence?.pristine.byteHash ?? report.fixture.sqliteSnapshotHash ?? null,
		pristineLogicalHash: includeHarnessSourceHash
			? (report.storeEvidence?.pristine.logicalHash ??
				report.fixture.logicalStoreHash ??
				report.metrics.serializedStoreHash)
			: null,
		preExistingSourceRows: qualificationSourceRows(report),
	};
}

export function replayWorkloadFingerprint(report: ReplayReport): string {
	return hashText(canonicalJson(workloadIdentity(report)));
}

function preparedEvidence(report: ReplayReport): PreparedTemplateEvidence | undefined {
	return report.storeEvidence?.preparedTemplate;
}
function evidenceReport(evidence: PreparedTemplateEvidence, schemaVersion: number): SqliteReport {
	return {
		quickCheck: evidence.quickCheck,
		serializedStoreHash: evidence.logicalHash,
		snapshotHash: evidence.byteHash,
		snapshotBytes: new Uint8Array(),
		rows: 0,
		schemaVersion,
		schemaRows: evidence.schemaRows ?? [],
		logicalTables: evidence.logicalTables ?? [],
		logicalTablesComplete: evidence.logicalTables !== undefined && evidence.logicalTableHashes !== undefined,
		sourceRows: evidence.sourceRows,
		summaryRows: evidence.summaryRows,
		summaryLineageRows: evidence.summaryLineageRows ?? [],
		jobRows: evidence.jobRows,
		relevantJobRows: [],
		attemptRows: [],
		relevantAttemptRows: [],
		policyRows: evidence.policyRows,
		projectIds: evidence.projectIds ?? [],
		retryEpochs: evidence.retryEpochs,
	};
}

function comparableTreatment(
	treatment: ReplayReport["treatment"],
): Omit<ReplayReport["treatment"], "hardProjectionWaitMs" | "schemaVersion"> {
	const { hardProjectionWaitMs: _hardProjectionWaitMs, schemaVersion: _schemaVersion, ...comparable } = treatment;
	return comparable;
}
function assertReportLogicalEvidence(report: ReplayReport, label: string): void {
	const legacy = Boolean(
		report.fixture.harnessSourceHash && HISTORICAL_BASELINE_SOURCE_HASHES.has(report.fixture.harnessSourceHash),
	);
	for (const [name, evidence] of [
		["migrated", report.storeEvidence?.migratedTemplate],
		["prepared", report.storeEvidence?.preparedTemplate],
	] as const) {
		if (!evidence) continue;
		if (evidence.logicalTables === undefined || evidence.logicalTableHashes === undefined) {
			if (report.fixture.harnessSourceHash && !legacy) {
				throw new Error(`${label} ${name} template lacks complete persisted-table snapshots and hashes`);
			}
			continue;
		}
		assertLogicalTableHashes(evidence, `${label} ${name} template`);
	}
}

function assertPreparedStoreCompatibility(
	baseline: ReplayReport,
	candidate: ReplayReport,
	before: SqliteReport,
	after: SqliteReport,
	baselineMigrated: SqliteReport | undefined,
	candidateMigrated: SqliteReport | undefined,
	label: string,
): void {
	const migrating = baseline.treatment.schemaVersion === 9 && candidate.treatment.schemaVersion === 10;
	if (migrating) {
		const retryKey = candidate.treatment.retryKey;
		if (!candidateMigrated || !retryKey)
			throw new Error(`${label} schema-10 migrated template or retry key is missing`);
		assertPreparedTransition(before, candidateMigrated, `${label} schema migration`);
		assertPolicyPreparation(
			candidateMigrated,
			after,
			candidate.fixture.projectId,
			retryKey,
			`${label} retry policy`,
			PROJECTION_APPEND_TABLES,
			declaredPreparedBranchPolicy(candidate, `${label} candidate`),
		);
		return;
	}
	if (baseline.treatment.schemaVersion >= 10) {
		const retryKey = candidate.treatment.retryKey;
		if (!baselineMigrated || !candidateMigrated || !retryKey) {
			throw new Error(`${label} schema-10 migrated templates or retry key are missing`);
		}
		assertPreparedTransition(baselineMigrated, candidateMigrated, `${label} pre-policy template`);
		assertPolicyPreparation(
			baselineMigrated,
			before,
			baseline.fixture.projectId,
			retryKey,
			`${label} baseline retry policy`,
			PROJECTION_APPEND_TABLES,
			declaredPreparedBranchPolicy(baseline, `${label} baseline`),
		);
		assertPolicyPreparation(
			candidateMigrated,
			after,
			candidate.fixture.projectId,
			retryKey,
			`${label} candidate retry policy`,
			PROJECTION_APPEND_TABLES,
			declaredPreparedBranchPolicy(candidate, `${label} candidate`),
		);
		return;
	}
	assertPreparedTransition(before, after, `${label} template`);
}

function assertComparable(baseline: ReplayReport, candidate: ReplayReport): void {
	validateReconstruction(baseline, "baseline");
	validateReconstruction(candidate, "candidate");
	assertReportLogicalEvidence(baseline, "baseline");
	assertReportLogicalEvidence(candidate, "candidate");
	const frozenClassification =
		baseline.fixture.reconstruction.baselineEligibility?.classification ??
		baseline.fixture.reconstruction.classification;

	if (baseline.harnessSchema !== candidate.harnessSchema) throw new Error("harnessSchema mismatch");
	if (baseline.sourceSnapshotHash !== candidate.sourceSnapshotHash) throw new Error("sourceSnapshotHash mismatch");
	for (const [label, report] of [
		["baseline", baseline],
		["candidate", candidate],
	] as const) {
		if (report.fixture.harnessSourceHash && report.workloadFingerprint !== replayWorkloadFingerprint(report)) {
			throw new Error(`${label} workload fingerprint does not match immutable identity`);
		}
		if (
			report.fixture.harnessSourceHash &&
			report.fixture.harnessIdentityHash &&
			report.fixture.harnessIdentityHash !== replayHarnessIdentityHash(report.fixture.harnessSourceHash)
		) {
			throw new Error(`${label} harness compatibility identity does not match the current manifest`);
		}
	}
	const fingerprintsMatch = baseline.workloadFingerprint === candidate.workloadFingerprint;
	const compatibilityKey = `${baseline.fixture.harnessSourceHash ?? ""}:${candidate.fixture.harnessSourceHash ?? ""}`;
	const harnessCompatibility = HARNESS_SOURCE_COMPATIBILITY.get(compatibilityKey);
	if (!fingerprintsMatch && !harnessCompatibility) {
		throw new Error("workloadFingerprint mismatch");
	}
	if (
		canonicalJson(workloadIdentity(baseline, fingerprintsMatch)) !==
		canonicalJson(workloadIdentity(candidate, fingerprintsMatch))
	) {
		throw new Error("workload identity, prefix/suffix, scope, sourceKey, or SQLite snapshot drifted");
	}
	for (const [label, report] of [
		["baseline", baseline],
		["candidate", candidate],
	] as const) {
		if (report.metrics.sqliteQuickCheck !== "ok" || report.samples.some(sample => sample.sqliteQuickCheck !== "ok")) {
			throw new Error(`${label} SQLite quick_check failed`);
		}
		const rows = canonicalJson(report.samples[0]?.sourceRows ?? []);
		if (report.samples.some(sample => canonicalJson(sample.sourceRows) !== rows)) {
			throw new Error(`${label} source rows or branch scope drifted between samples`);
		}
	}
	if (frozenClassification === "historical-reconstruction-impossible") {
		const beforePair = baseline.syntheticPair;
		const afterPair = candidate.syntheticPair;
		if (!beforePair || !afterPair) throw new Error("content-free replay pair is missing");
		const beforeSourceCount =
			beforePair.sourceCount ?? baseline.fixture.reconstruction.syntheticFixture?.sourceCount ?? 0;
		const afterSourceCount =
			afterPair.sourceCount ?? candidate.fixture.reconstruction.syntheticFixture?.sourceCount ?? 0;
		if (
			!Number.isSafeInteger(beforeSourceCount) ||
			!Number.isSafeInteger(afterSourceCount) ||
			(beforeSourceCount ?? 0) < 1 ||
			(afterSourceCount ?? 0) < 1
		) {
			throw new Error("content-free replay source count is invalid");
		}
		const shapesMatch =
			beforeSourceCount === afterSourceCount &&
			beforePair.fixtureHash === afterPair.fixtureHash &&
			canonicalJson(beforePair.boundary) === canonicalJson(afterPair.boundary);
		if (!shapesMatch) {
			const migration = harnessCompatibility?.syntheticWorkloadMigration;
			if (!migration?.justification.trim()) throw new Error("content-free replay shape or boundary drifted");
			const baselineBoundary = legacyContentFreeBoundary(beforeSourceCount);
			const candidateBoundary = afterPair.boundary;
			if (
				migration.baseline.sourceCount !== beforeSourceCount ||
				migration.baseline.fixtureHash !== beforePair.fixtureHash ||
				migration.baseline.fixtureHash !== legacyContentFreeFixtureHash(beforeSourceCount) ||
				canonicalJson(migration.baseline.boundary) !== canonicalJson(baselineBoundary) ||
				(beforePair.boundary !== undefined &&
					canonicalJson(beforePair.boundary) !== canonicalJson(migration.baseline.boundary)) ||
				migration.candidate.sourceCount !== afterSourceCount ||
				migration.candidate.fixtureHash !== afterPair.fixtureHash ||
				canonicalJson(migration.candidate.boundary) !== canonicalJson(candidateBoundary) ||
				afterPair.fixtureHash !== contentFreeFixtureHash(afterSourceCount, candidateBoundary)
			) {
				throw new Error("content-free replay workload migration evidence is invalid");
			}
		}
		if (baseline.fixture.reconstruction.syntheticFixture?.sourceCount !== beforeSourceCount) {
			throw new Error("baseline content-free source count contradicts reconstruction metadata");
		}
		if (candidate.fixture.reconstruction.syntheticFixture?.sourceCount !== afterSourceCount) {
			throw new Error("candidate content-free source count contradicts reconstruction metadata");
		}
	}
	const baselineTreatment = comparableTreatment(baseline.treatment);
	const candidateTreatment = comparableTreatment(candidate.treatment);
	if (canonicalJson(baselineTreatment) !== canonicalJson(candidateTreatment))
		throw new Error("treatment metadata drifted");
	const waitUnchanged = baseline.treatment.hardProjectionWaitMs === candidate.treatment.hardProjectionWaitMs;
	const historicalWaitRemoved =
		harnessCompatibility !== undefined &&
		HISTORICAL_BASELINE_SOURCE_HASHES.has(baseline.fixture.harnessSourceHash ?? "") &&
		baseline.treatment.hardProjectionWaitMs === HISTORICAL_HARD_PROJECTION_WAIT_MS &&
		candidate.treatment.hardProjectionWaitMs === null;
	if (!waitUnchanged && !historicalWaitRemoved) {
		throw new Error(
			`hardProjectionWaitMs removal requires a manifest-declared historical baseline with ${HISTORICAL_HARD_PROJECTION_WAIT_MS}ms`,
		);
	}
	const migrating = baseline.treatment.schemaVersion === 9 && candidate.treatment.schemaVersion === 10;
	if (baseline.treatment.schemaVersion !== candidate.treatment.schemaVersion && !migrating) {
		throw new Error("schemaVersion changed outside exact 9 -> 10 migration");
	}
	if (candidate.samples.some(sample => sample.schemaVersion !== candidate.treatment.schemaVersion)) {
		throw new Error("candidate sample schemaVersion does not match treatment migration");
	}
	const before = preparedEvidence(baseline);
	const after = preparedEvidence(candidate);
	if (!before || !after || !before.reserialized || !after.reserialized) {
		throw new Error("prepared template evidence missing");
	}
	const beforeReport = evidenceReport(before, baseline.treatment.schemaVersion);
	const afterReport = evidenceReport(after, candidate.treatment.schemaVersion);
	const baselineMigratedEvidence = baseline.storeEvidence?.migratedTemplate;
	const candidateMigratedEvidence = candidate.storeEvidence?.migratedTemplate;
	if (migrating && (!candidateMigratedEvidence?.reserialized || !candidate.treatment.retryKey)) {
		throw new Error("schema-10 migrated template or retry key evidence missing");
	}
	if (
		baseline.treatment.schemaVersion >= 10 &&
		(!baselineMigratedEvidence?.reserialized ||
			!candidateMigratedEvidence?.reserialized ||
			!candidate.treatment.retryKey)
	) {
		throw new Error("schema-10 migrated templates or retry key evidence missing");
	}
	assertPreparedStoreCompatibility(
		baseline,
		candidate,
		beforeReport,
		afterReport,
		baselineMigratedEvidence ? evidenceReport(baselineMigratedEvidence, baseline.treatment.schemaVersion) : undefined,
		candidateMigratedEvidence
			? evidenceReport(candidateMigratedEvidence, candidate.treatment.schemaVersion)
			: undefined,
		"prepared",
	);
}

function validateReportedAggregate(report: ReplayReport, label: string): void {
	const pristine = report.storeEvidence?.pristine;
	if (!pristine) throw new Error(`${label} pristine store evidence is missing`);
	const expected = aggregateMetrics(report.samples, pristine.quickCheck, pristine.logicalHash);
	if (canonicalJson(report.metrics) !== canonicalJson(expected)) {
		throw new Error(`${label} aggregate metrics contradict sample evidence`);
	}
	const representative = report.samples[0];
	const expectedRoute = representative?.status?.route ?? (representative?.fitProof.owned ? "lossless" : "native");
	if (
		!report.candidateOutcome ||
		report.candidateOutcome.route !== expectedRoute ||
		report.candidateOutcome.reproducedOldFallback !== report.fixture.reconstruction.preChangeContractReproduced
	) {
		throw new Error(`${label} recorded outcome contradicts sample or reconstruction evidence`);
	}
}

function lineagePlacementKey(row: SummaryLineageEvidenceRow): string {
	return canonicalJson({
		summaryId: row.summaryId,
		summaryHandle: row.summaryHandle,
		projectId: row.projectId,
		branchRevision: row.branchRevision,
		level: row.level,
		startPosition: row.startPosition,
		endPosition: row.endPosition,
		frontier: row.frontier,
	});
}

function lineagePlacements(rows: readonly SummaryLineageEvidenceRow[]): SummaryLineageEvidenceRow[][] {
	const placements = new Map<string, SummaryLineageEvidenceRow[]>();
	for (const row of rows) {
		const key = lineagePlacementKey(row);
		const placement = placements.get(key);
		if (placement) placement.push(row);
		else placements.set(key, [row]);
	}
	return [...placements.values()];
}

function placementMatchesSpan(
	placement: readonly SummaryLineageEvidenceRow[],
	span: SelectedSpan,
	sample: SampleReport,
	expectedProjectId: string,
): boolean {
	const first = placement[0];
	if (
		!first ||
		placement.length !== span.sourceIds.length ||
		span.sourceCount !== span.sourceIds.length ||
		first.summaryId !== span.summaryId ||
		first.summaryHandle !== span.summaryHandle ||
		first.projectId !== expectedProjectId ||
		first.branchRevision !== sample.fitProof.revision ||
		first.level !== span.level ||
		first.endPosition - first.startPosition !== span.sourceIds.length
	) {
		return false;
	}
	return placement.every((row, ordinal) => {
		const position = first.startPosition + ordinal;
		const active = sample.sourceRows[position];
		return (
			row.ordinal === ordinal &&
			row.position === position &&
			row.entryId === span.sourceIds[ordinal] &&
			active?.position === position &&
			active.entryId === row.entryId &&
			active.sourceKey === row.sourceKey
		);
	});
}

function validateSummaryBindings(
	report: ReplayReport,
	samples: readonly SampleReport[],
	label: string,
	allowLegacyMissingBindings: boolean,
	frozenClassification: ReplayReport["fixture"]["reconstruction"]["classification"],
): void {
	const synthetic = frozenClassification === "historical-reconstruction-impossible";
	const template = synthetic ? report.syntheticPair?.templateEvidence : report.storeEvidence?.preparedTemplate;
	const handlesById = new Map(
		(template?.summaryRows ?? []).flatMap(row =>
			typeof row.summary_id === "string" && typeof row.stable_handle === "string"
				? [[row.summary_id, row.stable_handle] as const]
				: [],
		),
	);
	const templateLineageRows = template?.summaryLineageRows;
	const expectedScope = synthetic
		? report.syntheticPair?.scope
		: { projectId: report.fixture.projectId, sessionId: report.fixture.sessionId, branchId: report.fixture.branchId };
	if (!expectedScope) {
		if (allowLegacyMissingBindings) return;
		throw new Error(`${label} frozen content-free scope evidence is missing`);
	}
	if (!templateLineageRows && !allowLegacyMissingBindings) {
		throw new Error(`${label} persisted summary lineage and branch-placement evidence is missing`);
	}
	for (const sample of samples) {
		let historicalCursor = 0;
		const persistedHistoricalSourceKeys: string[] = [];
		for (const span of sample.selectedSpans) {
			if (typeof span.summaryHandle !== "string") {
				if (allowLegacyMissingBindings) continue;
				throw new Error(`${label} sample ${sample.sample} lacks a projected summary handle binding`);
			}
			const summaryRow = span.summaryRow;
			if (!summaryRow || typeof summaryRow !== "object") {
				if (allowLegacyMissingBindings && handlesById.get(span.summaryId) === span.summaryHandle) continue;
				throw new Error(`${label} sample ${sample.sample} lacks its reopened stable-handle summary row`);
			}
			if (
				summaryRow.summary_id !== span.summaryId ||
				summaryRow.stable_handle !== span.summaryHandle ||
				summaryRow.project_id !== expectedScope.projectId
			) {
				throw new Error(
					`${label} sample ${sample.sample} summary id/handle pair does not match its reopened store row`,
				);
			}
			const preparedSummaryRow = template?.summaryRows.find(row => row.summary_id === span.summaryId);
			if (preparedSummaryRow && canonicalJson(summaryRow) !== canonicalJson(preparedSummaryRow)) {
				throw new Error(`${label} sample ${sample.sample} summary row contradicts its prepared template row`);
			}
			if (!Array.isArray(span.sourceIds) || !Array.isArray(span.lineageRows)) {
				throw new Error(`${label} sample ${sample.sample} lacks persisted summary lineage binding evidence`);
			}
			const recordedRows = (templateLineageRows ?? []).filter(row => row.summaryId === span.summaryId);
			if (recordedRows.length > 0 && canonicalJson(span.lineageRows) !== canonicalJson(recordedRows)) {
				throw new Error(
					`${label} sample ${sample.sample} summary lineage contradicts its prepared template placement`,
				);
			}
			const matches = lineagePlacements(span.lineageRows).filter(placement =>
				placementMatchesSpan(placement, span, sample, expectedScope.projectId),
			);
			if (matches.length !== 1) {
				throw new Error(
					`${label} sample ${sample.sample} projected summary sourceIds do not match one persisted ordered lineage placement`,
				);
			}
			const placement = matches[0]!;
			const first = placement[0]!;
			if (first.startPosition !== historicalCursor) {
				throw new Error(
					`${label} sample ${sample.sample} persisted summary lineage leaves an active-branch coverage gap`,
				);
			}
			historicalCursor = first.endPosition;
			persistedHistoricalSourceKeys.push(...placement.map(row => row.sourceKey));
		}
		if (!allowLegacyMissingBindings) {
			const activeSourceKeys = sample.sourceRows.map(row => row.sourceKey);
			const persistedFreshSourceKeys = sample.sourceRows.slice(historicalCursor).map(row => row.sourceKey);
			if (
				canonicalJson(persistedHistoricalSourceKeys) !==
					canonicalJson(sample.sourceCoverage.historicalSourceKeys) ||
				canonicalJson(persistedFreshSourceKeys) !== canonicalJson(sample.sourceCoverage.freshSourceKeys) ||
				canonicalJson([...persistedHistoricalSourceKeys, ...persistedFreshSourceKeys]) !==
					canonicalJson(activeSourceKeys)
			) {
				throw new Error(
					`${label} sample ${sample.sample} persisted summary lineage contradicts active branch coverage`,
				);
			}
			const evidence = sample.handles;
			if (!evidence?.providerVisible || !Array.isArray(evidence.tokens)) {
				throw new Error(`${label} sample ${sample.sample} lacks provider-visible summary handle evidence`);
			}
			if (
				evidence.count !== evidence.tokens.length ||
				evidence.unique !== new Set(evidence.tokens).size ||
				evidence.tokens.length !== sample.selectedSpans.length
			) {
				throw new Error(`${label} sample ${sample.sample} has missing or duplicate rendered summary handles`);
			}
			for (const [index, token] of evidence.tokens.entries()) {
				let decoded: ReturnType<typeof decodeLcmHandle>;
				try {
					decoded = decodeLcmHandle(token);
				} catch {
					throw new Error(`${label} sample ${sample.sample} contains a corrupt rendered LCM handle`);
				}
				const span = sample.selectedSpans[index];
				if (
					decoded.kind !== "summary" ||
					encodeLcmHandle(decoded) !== token ||
					!span ||
					decoded.reference.projectId !== expectedScope.projectId ||
					decoded.reference.sessionId !== expectedScope.sessionId ||
					decoded.reference.branchId !== expectedScope.branchId ||
					decoded.reference.summaryHandle !== span.summaryHandle ||
					(span.summaryRow?.stable_handle ?? handlesById.get(span.summaryId)) !== decoded.reference.summaryHandle
				) {
					throw new Error(
						`${label} sample ${sample.sample} rendered summary handle does not bind to its ordered store row`,
					);
				}
			}
		}
		if (sample.selectedSpans.length > 0 && sample.handles?.allMatchStore !== true && !allowLegacyMissingBindings) {
			throw new Error(`${label} sample ${sample.sample} did not verify summary handle bindings against its store`);
		}
	}
}
function validateAttemptEvidence(
	attempts: SummaryAttemptReport | undefined,
	expectedProjectId: string,
	label: string,
): void {
	if (!attempts) throw new Error(`${label} lacks normalized summary attempt evidence`);
	const recomputed = summaryAttemptReport(attempts.rows);
	if (
		attempts.inFlight !== recomputed.inFlight ||
		attempts.billed !== recomputed.billed ||
		attempts.missingUsage !== recomputed.missingUsage
	) {
		throw new Error(`${label} summary attempt counts contradict its normalized rows`);
	}
	for (const attempt of attempts.rows) {
		if (attempt.projectId !== expectedProjectId) throw new Error(`${label} includes a cross-project summary attempt`);
		if (
			(attempt.outcome === "in_flight" && attempt.completedAt !== null) ||
			(attempt.outcome !== "in_flight" && attempt.completedAt === null)
		) {
			throw new Error(`${label} summary attempt outcome contradicts its terminal timestamp`);
		}
	}
}

function validateCancellationSample(sample: SampleReport, expectedProjectId: string, label: string): void {
	const evidence = sample.controlEvidence;
	validateAttemptEvidence(sample.attempts, expectedProjectId, label);
	if (evidence?.kind !== "cancellation") throw new Error(`${label} lacks cleanup evidence`);
	const outcomes = sample.attempts.rows.map(attempt => attempt.outcome).sort();
	if (
		!evidence.started ||
		evidence.claimedJobs < 1 ||
		!evidence.aborted ||
		!evidence.projectionReturnedBeforeProviderRelease ||
		!Number.isFinite(evidence.abortLatencyMs) ||
		evidence.abortLatencyMs < 0 ||
		evidence.abortLatencyMs >= 2_000 ||
		!Number.isFinite(evidence.cleanupLatencyMs) ||
		evidence.cleanupLatencyMs < 0 ||
		evidence.cleanupLatencyMs >= 2_000 ||
		evidence.providerAttempts < 1 ||
		sample.providerAttempts !== evidence.providerAttempts ||
		evidence.providerWrites !== 0 ||
		evidence.activeProviders !== 0 ||
		evidence.inFlightAttempts !== sample.attempts.inFlight ||
		evidence.billedAttempts !== sample.attempts.billed ||
		evidence.missingUsage !== sample.attempts.missingUsage ||
		canonicalJson(evidence.attemptOutcomes) !== canonicalJson(outcomes) ||
		evidence.staleSummaries !== 0 ||
		!evidence.cleanupComplete ||
		evidence.storeRowsChanged !== sample.storeRowsChanged ||
		sample.storeRowsChanged < 1 ||
		sample.jobs.leased !== 0 ||
		sample.attempts.inFlight !== 0 ||
		sample.attempts.billed < 1 ||
		sample.attempts.missingUsage !== 0 ||
		sample.attempts.rows.some(
			attempt =>
				!ABANDONED_ATTEMPT_OUTCOMES.has(attempt.outcome) ||
				attempt.usage === null ||
				(attempt.usage.totalTokens ?? 0) < 1 ||
				attempt.usage.costTotal === null,
		)
	) {
		throw new Error(`${label} has in-flight, missing-usage, stale-summary, or cleanup drift`);
	}
}

function validateCancellationControl(report: ReplayReport, label: string, required: boolean): void {
	const control = report.cancellationControl;
	if (!control) {
		if (required) throw new Error(`${label} mandatory cancellation control lane is missing`);
		return;
	}
	if (
		control.kind !== "cancellation" ||
		control.sourceCount < CONTENT_FREE_MIN_SOURCES ||
		control.sample.sourceRows.length !== control.sourceCount ||
		control.fixtureHash !== replayCancellationControlFixtureHash(control.sourceCount, control.boundary)
	) {
		throw new Error(`${label} cancellation control fixture identity is invalid`);
	}
	validateCancellationSample(control.sample, control.scope.projectId, `${label} cancellation control`);
}

function validateFailureControls(report: ReplayReport, label: string): void {
	if (report.fixture.name !== "real") return;
	const controls = report.failureControls;
	if (!controls || canonicalJson(controls.map(control => control.name)) !== canonicalJson(FAILURE_CONTROL_ORDER)) {
		throw new Error(`${label} deterministic replay failure controls are missing or out of order`);
	}
	for (const control of controls) {
		if (
			canonicalJson(control) !== canonicalJson({ name: control.name, ...FAILURE_CONTROL_EXPECTATIONS[control.name] })
		) {
			throw new Error(
				`${label} ${control.name} failure control contradicts its route, provider-call, or store-write evidence`,
			);
		}
	}
}

function validateBaselineSampleEvidence(
	report: ReplayReport,
	label: string,
	frozenClassification: ReplayReport["fixture"]["reconstruction"]["classification"],
): void {
	if (report.samples.length !== 5 || canonicalJson(report.samples.map(sample => sample.sample)) !== "[1,2,3,4,5]") {
		throw new Error(`${label} report must contain samples 1 through 5 exactly once`);
	}
	const synthetic = frozenClassification === "historical-reconstruction-impossible";
	const samples = synthetic ? report.syntheticPair?.samples : report.samples;
	if (samples?.length !== 5 || canonicalJson(samples.map(sample => sample.sample)) !== "[1,2,3,4,5]") {
		throw new Error(`${label} must contain five executed gating samples`);
	}
	if (
		!synthetic &&
		report.fixture.harnessSourceHash &&
		samples.some(
			sample =>
				sample.fitProof.owned ||
				sample.status?.route !== "native_fallback" ||
				typeof sample.fallbackCategory !== "string",
		)
	) {
		throw new Error(
			`${label} exact physical baseline must dispatch native_fallback with the selected category in every sample`,
		);
	}
	for (const sample of samples) {
		if (!Number.isFinite(sample.cpuMs) || !sample.jobs) throw new Error(`${label} sample lacks CPU or job evidence`);
		if (!sample.postStoreHash || !sample.postSnapshotHash || sample.sqliteQuickCheck !== "ok") {
			throw new Error(`${label} sample ${sample.sample} failed store integrity`);
		}
		if (synthetic) {
			if (sample.fitProof.owned) {
				if (
					sample.status?.route !== "lossless" ||
					sample.fallbackCategory !== null ||
					!sample.fitProof.complete ||
					!sample.sourceCoverage.complete ||
					!sample.tokens ||
					sample.promptInputTokens !== sample.tokens.candidate ||
					sample.tokens.candidate > sample.tokens.budget
				) {
					throw new Error(
						`${label} synthetic sample ${sample.sample} has inconsistent Lossless takeover evidence`,
					);
				}
			} else if (sample.status?.route === "native_fallback") {
				if (!sample.fallbackCategory) {
					throw new Error(`${label} synthetic sample ${sample.sample} has inconsistent native fallback evidence`);
				}
			} else if (sample.status?.route === "native_passthrough") {
				if (sample.fallbackCategory !== null) {
					throw new Error(
						`${label} synthetic sample ${sample.sample} has inconsistent native passthrough evidence`,
					);
				}
			} else {
				throw new Error(`${label} synthetic sample ${sample.sample} has no truthful baseline route evidence`);
			}
		}
	}
	validateSummaryBindings(
		report,
		samples,
		label,
		Boolean(
			report.fixture.harnessSourceHash && HISTORICAL_BASELINE_SOURCE_HASHES.has(report.fixture.harnessSourceHash),
		),
		frozenClassification,
	);
}
function validateSampleEvidence(
	report: ReplayReport,
	label: string,
	frozenClassification: ReplayReport["fixture"]["reconstruction"]["classification"],
): void {
	if (report.samples.length !== 5 || canonicalJson(report.samples.map(sample => sample.sample)) !== "[1,2,3,4,5]") {
		throw new Error(`${label} report must contain samples 1 through 5 exactly once`);
	}
	if (
		!report.metrics.cpuMs ||
		![report.metrics.cpuMs.median, report.metrics.cpuMs.mad, report.metrics.cpuMs.p95].every(Number.isFinite)
	) {
		throw new Error(`${label} CPU distribution evidence is missing`);
	}
	const synthetic = frozenClassification === "historical-reconstruction-impossible";
	const samples = synthetic ? report.syntheticPair?.samples : report.samples;
	if (samples?.length !== 5) throw new Error(`${label} must contain five executed gating samples`);
	if (canonicalJson(samples.map(sample => sample.sample)) !== "[1,2,3,4,5]") {
		throw new Error(`${label} gating sample ordinals must be exactly 1 through 5`);
	}
	const workFor = (sample: SampleReport) => {
		const counters = sample.counters;
		if (
			!counters ||
			!Number.isSafeInteger(counters.projectionCalls) ||
			counters.projectionCalls <= 0 ||
			counters.projectionReads !== counters.projectionCalls ||
			!Number.isSafeInteger(counters.projectionLineageRowsRead) ||
			counters.projectionLineageRowsRead < 0 ||
			counters.lineageReads !== counters.projectionLineageRowsRead ||
			!Number.isSafeInteger(counters.schedulerBranchPasses) ||
			counters.schedulerBranchPasses < 0 ||
			counters.reconcileRowsChanged !== 0 ||
			counters.rowsChanged !== counters.reconcileRowsChanged
		) {
			throw new Error(`${label} sample ${sample.sample} has invalid deterministic work counters`);
		}
		return {
			projectionReads: counters.projectionReads,
			lineageReads: counters.lineageReads,
			schedulerBranchPasses: counters.schedulerBranchPasses,
		};
	};
	const expectedWork = workFor(samples[0]!);
	if (synthetic && canonicalJson(expectedWork) !== canonicalJson(CONTENT_FREE_EXPECTED_WORK)) {
		throw new Error(`${label} synthetic replay performed unexpected projection, lineage, or scheduler work`);
	}
	for (const sample of samples) {
		if (!Number.isFinite(sample.cpuMs) || !sample.jobs) throw new Error(`${label} sample lacks CPU or job evidence`);
		if (canonicalJson(workFor(sample)) !== canonicalJson(expectedWork)) {
			throw new Error(
				`${label} sample ${sample.sample} deterministic work counters drifted between prepared clones`,
			);
		}
		const expectedKeys = sample.sourceRows.map(row => row.sourceKey);
		const expectedAttemptScope = synthetic ? report.syntheticPair?.scope.projectId : report.fixture.projectId;
		if (!expectedAttemptScope) throw new Error(`${label} frozen content-free attempt scope is missing`);
		validateAttemptEvidence(sample.attempts, expectedAttemptScope, `${label} sample ${sample.sample}`);
		if (
			!sample.fitProof.complete ||
			!sample.sourceCoverage.complete ||
			canonicalJson(sample.sourceCoverage.activeSourceKeys) !== canonicalJson(expectedKeys) ||
			canonicalJson(sample.sourceCoverage.projectedSourceKeys) !== canonicalJson(expectedKeys)
		) {
			throw new Error(
				`${label} sample ${sample.sample} failed exact ordered coverage (fit=${sample.fitProof.complete}, coverage=${sample.sourceCoverage.complete}, expected=${expectedKeys.length}:${hashText(canonicalJson(expectedKeys))}, active=${sample.sourceCoverage.activeSourceKeys.length}:${hashText(canonicalJson(sample.sourceCoverage.activeSourceKeys))}, projected=${sample.sourceCoverage.projectedSourceKeys.length}:${hashText(canonicalJson(sample.sourceCoverage.projectedSourceKeys))})`,
			);
		}
		if (
			!sample.fitProof.owned ||
			sample.status?.route !== "lossless" ||
			sample.status.committed !== true ||
			sample.status.hasMetrics !== true
		) {
			throw new Error(
				`${label} ${synthetic ? "synthetic " : ""}sample ${sample.sample} did not commit an explicit measured Lossless takeover`,
			);
		}
		if (
			!sample.tokens ||
			sample.promptInputTokens !== sample.tokens.candidate ||
			sample.tokens.routeCandidate !== sample.tokens.candidate ||
			sample.tokens.candidate > sample.tokens.budget ||
			sample.tokens.nonMessage < 0
		) {
			throw new Error(
				`${label} ${synthetic ? "synthetic " : ""}sample ${sample.sample} exceeded or contradicted the measured LCM message-token budget`,
			);
		}
		if (sample.providerAttempts !== 0) throw new Error(`${label} sample ${sample.sample} made provider attempts`);
		if (sample.storeRowsChanged !== 0) {
			throw new Error(`${label} sample ${sample.sample} mutated store rows`);
		}
		if (
			!sample.handles ||
			sample.handles.count < 1 ||
			!sample.handles.providerVisible ||
			!Array.isArray(sample.handles.tokens) ||
			!sample.handles.allPresent ||
			!sample.handles.allResolved ||
			!sample.handles.allMatchStore ||
			sample.handles.count !== sample.handles.unique
		) {
			throw new Error(
				`${label} sample ${sample.sample} has no exact provider-visible resolved historical handle binding`,
			);
		}
		if (
			!sample.postStoreHash ||
			sample.postStoreHash !== sample.serializedStoreHash ||
			!sample.postSnapshotHash ||
			sample.sqliteQuickCheck !== "ok"
		) {
			throw new Error(`${label} sample ${sample.sample} failed store integrity`);
		}
		if (
			sample.jobs.pending ||
			sample.jobs.leased ||
			sample.jobs.backoff ||
			sample.jobs.exhausted ||
			sample.jobs.missing
		) {
			throw new Error(`${label} sample ${sample.sample} has unresolved jobs`);
		}
	}
	validateSummaryBindings(report, samples, label, false, frozenClassification);
	validateFailureControls(report, label);
}

async function validateCaptureArtifacts(report: ReplayReport, label: string): Promise<void> {
	const rawPath = report.artifacts?.rawPrefix;
	const resolvedPath = report.artifacts?.resolvedPrefix;
	const sourcePath = report.artifacts?.sourceSnapshot;
	const identity = report.fixture.prefixIdentity;
	if (!rawPath || !resolvedPath || !sourcePath || !identity)
		throw new Error(`${label} immutable capture artifacts are missing`);
	const [raw, resolved, sourceText] = await Promise.all([
		Bun.file(await assertArtifactPath(rawPath, `${label} raw prefix`, "read")).text(),
		Bun.file(await assertArtifactPath(resolvedPath, `${label} resolved prefix`, "read")).text(),
		Bun.file(await assertArtifactPath(sourcePath, `${label} source snapshot`, "read")).text(),
	]);
	if (hashText(raw) !== identity.rawHash || report.fixture.selectedEntriesHash !== identity.rawHash) {
		throw new Error(`${label} raw prefix hash mismatch`);
	}
	if (hashText(resolved) !== identity.resolvedHash) throw new Error(`${label} resolved prefix hash mismatch`);
	let entries: Record<string, unknown>[];
	try {
		entries = raw
			.trimEnd()
			.split("\n")
			.map(line => JSON.parse(line) as Record<string, unknown>);
	} catch {
		throw new Error(`${label} raw prefix is not valid JSONL`);
	}
	const marker = entries.findLast(entry => entry.id === report.fixture.markerId);
	if (!marker || marker.parentId !== report.fixture.parentId)
		throw new Error(`${label} selected marker identity mismatch`);
	const expectedFallback = typeof marker.lcmFallback === "string" ? marker.lcmFallback : undefined;
	const maintenanceAuthoritative =
		report.fixture.requestTokensFloor === undefined ||
		report.samples.every(sample => sample.maintenance?.authoritative === true);
	const reproduced =
		report.fixture.name !== "real" ||
		(maintenanceAuthoritative &&
			expectedFallback !== undefined &&
			report.samples.every(
				sample =>
					!sample.fitProof.owned &&
					sample.status?.route === "native_fallback" &&
					sample.fallbackCategory === expectedFallback,
			));
	if (
		report.fixture.reconstruction.preChangeContractReproduced !== reproduced ||
		report.fixture.reconstruction.baselineEligibility?.preChangeContractReproduced !== reproduced
	) {
		throw new Error(`${label} pre-change fallback evidence contradicts the selected marker`);
	}
	let source: SourceSnapshot;
	try {
		source = JSON.parse(sourceText) as SourceSnapshot;
	} catch {
		throw new Error(`${label} source snapshot is not valid JSON`);
	}
	if (hashText(canonicalJson(source)) !== report.sourceSnapshotHash) {
		throw new Error(`${label} source snapshot hash mismatch`);
	}
	if (report.fixture.name === "real") {
		const storage = new MemorySessionStorage();
		const replayPath = path.join(path.dirname(sourcePath), `.replay-validation-${label}.jsonl`);
		storage.writeTextSync(replayPath, resolved);
		const manager = await SessionManager.open(replayPath, path.dirname(sourcePath), storage, {
			initialCwd: process.cwd(),
			suppressBreadcrumb: true,
		});
		manager.branch(report.fixture.parentId);
		const normalized = normalizeLcmBranch(manager, report.fixture.projectId, String);
		if (canonicalJson(source) !== canonicalJson(normalized)) {
			throw new Error(`${label} source snapshot does not match the normalized selected journal`);
		}
		if (
			normalized.scope.sessionId !== report.fixture.sessionId ||
			normalized.scope.branchId !== report.fixture.branchId ||
			normalized.entries.length !== report.fixture.activeSources
		) {
			throw new Error(`${label} normalized source scope or entry count contradicts fixture identity`);
		}
		const root = report.artifacts?.root;
		if (!root) throw new Error(`${label} private capture root is missing`);
		const probePath = path.join(root, `.source-validation-${Bun.randomUUIDv7()}.sqlite`);
		await assertArtifactPath(probePath, `${label} source validation store`, "write");
		try {
			const expectedStore = await sourceSnapshotStoreReport(probePath, normalized);
			if (canonicalJson(expectedStore.sourceRows) !== canonicalJson(qualificationSourceRows(report))) {
				throw new Error(`${label} captured store source rows or source keys do not match the selected journal`);
			}
		} finally {
			await Promise.all([
				fs.rm(probePath, { force: true }),
				fs.rm(`${probePath}-wal`, { force: true }),
				fs.rm(`${probePath}-shm`, { force: true }),
			]);
		}
	}
}

function expectSqliteRejection(operation: () => void, label: string): void {
	try {
		operation();
	} catch {
		return;
	}
	throw new Error(label);
}

function assertRetryTriggerBehavior(storePath: string, label: string): void {
	const db = new Database(storePath, { strict: true });
	try {
		db.run(
			"INSERT INTO summary_retry_policies(project_id, retry_key, epoch, claim_token, updated_at) VALUES ('__lcm_replay_probe__', NULL, 0, NULL, 0)",
		);
		const insertLeased = (policyToken: string): void => {
			db.run(`INSERT INTO summary_jobs(
				job_id, project_id, input_hash, level, origin_branch_row_id, origin_revision, status,
				worker_id, lease_token, lease_expires_at, attempt_count, available_at, created_at, updated_at,
				retry_epoch, lease_policy_token, lease_mutation_nonce
			) VALUES ('__lcm_replay_insert_probe__', '__lcm_replay_probe__', '__lcm_replay_insert_probe__',
				0, NULL, 0, 'leased', 'worker-insert', 'lease-insert', 100, 0, 0, 0, 0,
				1, '${policyToken}', 'nonce-insert')`);
		};
		expectSqliteRejection(
			() => insertLeased("wrong-token"),
			`${label} accepted a direct leased insert under a blocked retry policy`,
		);
		db.run(`INSERT INTO summary_jobs(
			job_id, project_id, input_hash, level, origin_branch_row_id, origin_revision, status,
			attempt_count, available_at, created_at, updated_at, retry_epoch
		) VALUES ('__lcm_replay_probe__', '__lcm_replay_probe__', '__lcm_replay_probe__', 0, NULL, 0,
			'pending', 0, 0, 0, 0, 0)`);
		const claim = (policyToken: string, nonce: string): void => {
			db.run(`UPDATE summary_jobs SET status = 'leased', worker_id = 'worker-1', lease_token = 'lease-1',
				lease_expires_at = 100, retry_epoch = 1, lease_policy_token = '${policyToken}',
				lease_mutation_nonce = '${nonce}' WHERE job_id = '__lcm_replay_probe__'`);
		};
		expectSqliteRejection(
			() => claim("wrong-token", "nonce-1"),
			`${label} accepted a claim under a blocked retry policy`,
		);
		db.run(`UPDATE summary_retry_policies SET retry_key = 'probe/model', epoch = 1,
			claim_token = 'claim-token', updated_at = 1 WHERE project_id = '__lcm_replay_probe__'`);
		expectSqliteRejection(
			() => insertLeased("wrong-token"),
			`${label} accepted a direct leased insert with the wrong policy token`,
		);
		insertLeased("claim-token");
		expectSqliteRejection(
			() => claim("wrong-token", "nonce-1"),
			`${label} accepted a claim with the wrong policy token`,
		);
		claim("claim-token", "nonce-1");
		expectSqliteRejection(
			() => db.run("UPDATE summary_jobs SET worker_id = 'worker-2' WHERE job_id = '__lcm_replay_probe__'"),
			`${label} accepted a worker mutation without rotating the nonce`,
		);
		expectSqliteRejection(
			() => db.run("UPDATE summary_jobs SET lease_token = 'lease-2' WHERE job_id = '__lcm_replay_probe__'"),
			`${label} accepted a lease-token mutation without rotating the nonce`,
		);
		expectSqliteRejection(
			() => db.run("UPDATE summary_jobs SET lease_expires_at = 200 WHERE job_id = '__lcm_replay_probe__'"),
			`${label} accepted a lease-expiry mutation without rotating the nonce`,
		);
		db.run(`UPDATE summary_jobs SET worker_id = 'worker-2', lease_token = 'lease-2', lease_expires_at = 200,
			lease_mutation_nonce = 'nonce-2' WHERE job_id = '__lcm_replay_probe__'`);
		expectSqliteRejection(
			() =>
				db.run(`UPDATE summary_jobs SET status = 'pending', worker_id = NULL, lease_token = NULL,
					lease_expires_at = NULL, lease_mutation_nonce = NULL WHERE job_id = '__lcm_replay_probe__'`),
			`${label} accepted cleanup with a lingering policy token`,
		);
		expectSqliteRejection(
			() =>
				db.run(`UPDATE summary_jobs SET status = 'pending', worker_id = NULL, lease_token = NULL,
					lease_expires_at = NULL, lease_policy_token = NULL WHERE job_id = '__lcm_replay_probe__'`),
			`${label} accepted cleanup with a lingering mutation nonce`,
		);
		db.run(`UPDATE summary_jobs SET status = 'pending', worker_id = NULL, lease_token = NULL,
			lease_expires_at = NULL, lease_policy_token = NULL, lease_mutation_nonce = NULL
			WHERE job_id = '__lcm_replay_probe__'`);
		expectSqliteRejection(() => claim("wrong-token", "nonce-3"), `${label} accepted an unauthorized reclaim`);
	} finally {
		db.close(false);
	}
}

function allowsLegacyLineageEvidence(report: ReplayReport): boolean {
	return Boolean(
		report.fixture.harnessSourceHash && HISTORICAL_BASELINE_SOURCE_HASHES.has(report.fixture.harnessSourceHash),
	);
}

function templateEvidenceMatchesStore(
	store: SqliteReport,
	evidence: PreparedTemplateEvidence,
	allowLegacyMissingEvidence: boolean,
): boolean {
	const actual = preparedTemplateEvidence(store);
	if (allowLegacyMissingEvidence && actual.logicalHash !== evidence.logicalHash) {
		actual.logicalHash = evidence.logicalHash;
	}
	if (evidence.summaryLineageRows === undefined) {
		if (!allowLegacyMissingEvidence) return false;
		delete actual.summaryLineageRows;
	}
	if (evidence.logicalTables === undefined || evidence.logicalTableHashes === undefined) {
		if (
			!allowLegacyMissingEvidence ||
			evidence.logicalTables !== undefined ||
			evidence.logicalTableHashes !== undefined
		) {
			return false;
		}
		delete actual.logicalTables;
		delete actual.logicalTableHashes;
	} else {
		try {
			assertLogicalTableHashes(evidence, "prepared template");
		} catch {
			return false;
		}
	}
	return canonicalJson(actual) === canonicalJson(evidence);
}

async function validateMigratedTemplateArtifact(report: ReplayReport, label: string): Promise<SqliteReport> {
	const migratedPath = report.artifacts?.migratedTemplate;
	const migrated = report.storeEvidence?.migratedTemplate;
	if (!migratedPath || !migrated) throw new Error(`${label} migrated template artifact is missing`);
	const canonicalPath = await assertArtifactPath(migratedPath, `${label} migrated template`, "read");
	const bytes = new Uint8Array(await Bun.file(canonicalPath).arrayBuffer());
	const store = sqliteReport(
		canonicalPath,
		report.fixture.projectId,
		report.fixture.sessionId,
		report.fixture.branchId,
	);
	if (!templateEvidenceMatchesStore(store, migrated, false)) {
		throw new Error(`${label} migrated template artifact contradicts recorded evidence`);
	}
	const probePath = `${canonicalPath}.probe-${Bun.randomUUIDv7()}`;
	await assertArtifactPath(probePath, `${label} migrated trigger probe`, "write");
	await writePrivateArtifact(probePath, bytes);
	try {
		assertRetryTriggerBehavior(probePath, label);
	} finally {
		await Promise.all([
			fs.rm(probePath, { force: true }),
			fs.rm(`${probePath}-wal`, { force: true }),
			fs.rm(`${probePath}-shm`, { force: true }),
		]);
	}
	return store;
}

async function validatePristineStoreArtifact(
	report: ReplayReport,
	label: string,
): Promise<{ pristine: SqliteReport; prepared: SqliteReport }> {
	const recordedPath = report.artifacts?.sqliteSnapshot;
	const pristine = report.storeEvidence?.pristine;
	if (!recordedPath || !pristine?.path || !pristine.byteHash || !pristine.logicalHash)
		throw new Error(`${label} pristine SQLite snapshot path/byte/logical evidence required`);
	if (path.resolve(recordedPath) !== path.resolve(pristine.path))
		throw new Error(`${label} pristine snapshot path mismatch`);
	const snapshotPath = await assertArtifactPath(recordedPath, `${label} SQLite snapshot`, "read");
	const bytes = new Uint8Array(await Bun.file(snapshotPath).arrayBuffer());
	if (hashBytes(bytes) !== pristine.byteHash || report.fixture.sqliteSnapshotHash !== pristine.byteHash)
		throw new Error(`${label} pristine snapshot byte hash mismatch`);
	const store = sqliteReport(
		snapshotPath,
		report.fixture.projectId,
		report.fixture.sessionId,
		report.fixture.branchId,
	);
	if (store.quickCheck !== "ok" || pristine.quickCheck !== "ok")
		throw new Error(`${label} pristine SQLite quick_check failed`);
	const legacyNormalizer = Boolean(
		report.fixture.harnessSourceHash && HISTORICAL_BASELINE_SOURCE_HASHES.has(report.fixture.harnessSourceHash),
	);
	if (
		(!legacyNormalizer && store.serializedStoreHash !== pristine.logicalHash) ||
		report.fixture.logicalStoreHash !== pristine.logicalHash
	) {
		throw new Error(`${label} pristine logical hash mismatch`);
	}
	const prepared = report.storeEvidence?.preparedTemplate;
	const templatePath = report.artifacts?.treatmentTemplate;
	if (!prepared || !templatePath) throw new Error(`${label} prepared template evidence missing`);
	const canonicalTemplate = await assertArtifactPath(templatePath, `${label} treatment template`, "read");
	const templateBytes = new Uint8Array(await Bun.file(canonicalTemplate).arrayBuffer());
	if (hashBytes(templateBytes) !== prepared.byteHash) throw new Error(`${label} prepared template byte hash mismatch`);
	const template = sqliteReport(
		canonicalTemplate,
		report.fixture.projectId,
		report.fixture.sessionId,
		report.fixture.branchId,
	);
	if (
		canonicalJson(template.projectIds) !== canonicalJson(prepared.projectIds) ||
		canonicalJson(template.retryEpochs) !== canonicalJson(prepared.retryEpochs)
	) {
		throw new Error(`${label} prepared template project or epoch evidence mismatch`);
	}
	if (
		template.schemaVersion !== report.treatment.schemaVersion ||
		template.quickCheck !== "ok" ||
		prepared.quickCheck !== "ok" ||
		!templateEvidenceMatchesStore(template, prepared, allowsLegacyLineageEvidence(report))
	) {
		throw new Error(`${label} prepared template schema, lineage, or logical evidence mismatch`);
	}
	for (const sample of report.samples) {
		if (
			sample.serializedStoreHash !== prepared.logicalHash ||
			canonicalJson(sample.sourceRows) !== canonicalJson(prepared.sourceRows)
		) {
			throw new Error(`${label} sample ${sample.sample} is not cloned from the prepared template`);
		}
	}
	return { pristine: store, prepared: template };
}

async function validateSyntheticPairArtifact(
	report: ReplayReport,
	label: string,
	approvedLegacy?: SyntheticWorkloadDescriptor,
): Promise<void> {
	const pair = report.syntheticPair;
	if (!pair) throw new Error(`${label} content-free replay pair is missing`);
	const sourceCount = pair.sourceCount ?? report.fixture.reconstruction.syntheticFixture?.sourceCount;
	if (sourceCount === undefined) throw new Error(`${label} content-free source count is missing`);
	if (approvedLegacy) {
		if (
			approvedLegacy.sourceCount !== sourceCount ||
			approvedLegacy.fixtureHash !== pair.fixtureHash ||
			approvedLegacy.fixtureHash !== legacyContentFreeFixtureHash(sourceCount) ||
			canonicalJson(approvedLegacy.boundary) !== canonicalJson(legacyContentFreeBoundary(sourceCount)) ||
			(pair.boundary !== undefined && canonicalJson(pair.boundary) !== canonicalJson(approvedLegacy.boundary))
		) {
			throw new Error(`${label} legacy content-free migration evidence is invalid`);
		}
	} else if (pair.fixtureHash !== contentFreeFixtureHash(sourceCount, pair.boundary)) {
		throw new Error(`${label} content-free fixture hash mismatch`);
	}
	if (sourceCount !== report.fixture.reconstruction.syntheticFixture?.sourceCount) {
		throw new Error(`${label} content-free source count contradicts reconstruction metadata`);
	}
	if (!pair.templatePath || !pair.templateEvidence || !pair.scope) {
		if (approvedLegacy) return;
		throw new Error(`${label} content-free template artifact evidence is missing`);
	}
	const templatePath = await assertArtifactPath(pair.templatePath, `${label} content-free template`, "read");
	const bytes = new Uint8Array(await Bun.file(templatePath).arrayBuffer());
	if (hashBytes(bytes) !== pair.templateEvidence.byteHash) {
		throw new Error(`${label} content-free template byte hash mismatch`);
	}
	const store = sqliteReport(templatePath, pair.scope.projectId, pair.scope.sessionId, pair.scope.branchId);
	if (!templateEvidenceMatchesStore(store, pair.templateEvidence, Boolean(approvedLegacy))) {
		throw new Error(`${label} content-free template artifact contradicts recorded evidence`);
	}
	for (const sample of pair.samples) {
		if (
			sample.serializedStoreHash !== pair.templateEvidence.logicalHash ||
			canonicalJson(sample.sourceRows) !== canonicalJson(pair.templateEvidence.sourceRows)
		) {
			throw new Error(`${label} content-free sample ${sample.sample} is not cloned from its template`);
		}
	}
}

async function validateCancellationControlArtifact(report: ReplayReport, label: string): Promise<void> {
	const control = report.cancellationControl;
	if (!control) return;
	const templatePath = await assertArtifactPath(
		control.templatePath,
		`${label} cancellation control template`,
		"read",
	);
	const bytes = new Uint8Array(await Bun.file(templatePath).arrayBuffer());
	const store = sqliteReport(templatePath, control.scope.projectId, control.scope.sessionId, control.scope.branchId);
	if (
		hashBytes(bytes) !== control.templateEvidence.byteHash ||
		!templateEvidenceMatchesStore(store, control.templateEvidence, false) ||
		control.sample.serializedStoreHash !== control.templateEvidence.logicalHash ||
		canonicalJson(control.sample.sourceRows) !== canonicalJson(control.templateEvidence.sourceRows)
	) {
		throw new Error(`${label} cancellation control template artifact contradicts its sample evidence`);
	}
}
function comparisonResult(
	baseline: ReplayReport,
	candidate: ReplayReport,
	harnessCompatibility: HarnessCompatibilityPair | undefined,
): Record<string, unknown> {
	const frozen =
		baseline.fixture.reconstruction.baselineEligibility?.classification ??
		baseline.fixture.reconstruction.classification;
	const syntheticMigration =
		frozen === "historical-reconstruction-impossible" ? harnessCompatibility?.syntheticWorkloadMigration : undefined;
	const qualificationCandidate =
		frozen === "historical-reconstruction-impossible" ? candidate.syntheticPair!.samples[0]! : candidate.samples[0]!;
	const result = {
		harnessSchema: candidate.harnessSchema,
		workloadFingerprint: candidate.workloadFingerprint,
		sourceSnapshotHash: candidate.sourceSnapshotHash,
		baselineEligibility: frozen,
		candidateRoute:
			qualificationCandidate.status?.route ?? (qualificationCandidate.fitProof.owned ? "lossless" : "native"),
	};
	if (syntheticMigration) {
		return {
			...result,
			metricLane: "not-comparable",
			syntheticQualification: {
				performanceComparable: false,
				justification: syntheticMigration.justification,
				baseline: syntheticMigration.baseline,
				candidate: syntheticMigration.candidate,
			},
			deltas: null,
			baseline: null,
			candidate: null,
		};
	}
	const baselineSamples =
		frozen === "historical-reconstruction-impossible" ? baseline.syntheticPair!.samples : baseline.samples;
	const candidateSamples =
		frozen === "historical-reconstruction-impossible" ? candidate.syntheticPair!.samples : candidate.samples;
	const baselineFirst = baselineSamples[0]!;
	const candidateFirst = candidateSamples[0]!;
	const baselineMetrics = aggregateMetrics(
		baselineSamples,
		baselineFirst.sqliteQuickCheck,
		baselineFirst.serializedStoreHash,
	);
	const candidateMetrics = aggregateMetrics(
		candidateSamples,
		candidateFirst.sqliteQuickCheck,
		candidateFirst.serializedStoreHash,
	);
	return {
		...result,
		metricLane: frozen === "historical-reconstruction-impossible" ? "synthetic-pair" : "physical-replay",
		deltas: {
			medianLatencyMs: candidateMetrics.latencyMs.median - baselineMetrics.latencyMs.median,
			p95LatencyMs: candidateMetrics.latencyMs.p95 - baselineMetrics.latencyMs.p95,
			medianCpuMs: candidateMetrics.cpuMs.median - baselineMetrics.cpuMs.median,
			p95CpuMs: candidateMetrics.cpuMs.p95 - baselineMetrics.cpuMs.p95,
			providerInputTokens: candidateMetrics.providerUsage.input - baselineMetrics.providerUsage.input,
			providerOutputTokens: candidateMetrics.providerUsage.output - baselineMetrics.providerUsage.output,
			providerCost: candidateMetrics.providerUsage.cost - baselineMetrics.providerUsage.cost,
			retries: candidateMetrics.retries - baselineMetrics.retries,
			promptInputTokens: candidateMetrics.promptInputTokens - baselineMetrics.promptInputTokens,
			underfillRatio: candidateMetrics.underfillRatio - baselineMetrics.underfillRatio,
			storeRowsChanged: candidateMetrics.storeRowsChanged - baselineMetrics.storeRowsChanged,
		},
		baseline: baselineMetrics,
		candidate: candidateMetrics,
	};
}

export async function compareReplayReports(
	baseline: ReplayReport,
	candidate: ReplayReport,
): Promise<Record<string, unknown>> {
	const frozen =
		baseline.fixture.reconstruction.baselineEligibility?.classification ??
		baseline.fixture.reconstruction.classification;
	validateBaselineSampleEvidence(baseline, "baseline", frozen);
	validateSampleEvidence(candidate, "candidate", frozen);
	validateCancellationControl(baseline, "baseline", false);
	validateCancellationControl(candidate, "candidate", true);
	assertComparable(baseline, candidate);
	const compatibilityKey = `${baseline.fixture.harnessSourceHash ?? ""}:${candidate.fixture.harnessSourceHash ?? ""}`;
	const harnessCompatibility = HARNESS_SOURCE_COMPATIBILITY.get(compatibilityKey);
	validateReportedAggregate(baseline, "baseline");
	validateReportedAggregate(candidate, "candidate");
	const [, , baselineStores, candidateStores] = await Promise.all([
		validateCaptureArtifacts(baseline, "baseline"),
		validateCaptureArtifacts(candidate, "candidate"),
		validatePristineStoreArtifact(baseline, "baseline"),
		validatePristineStoreArtifact(candidate, "candidate"),
		validateCancellationControlArtifact(baseline, "baseline"),
		validateCancellationControlArtifact(candidate, "candidate"),
	]);
	if (frozen === "historical-reconstruction-impossible") {
		const approvedLegacySynthetic =
			baseline.syntheticPair?.sourceCount === undefined
				? harnessCompatibility?.syntheticWorkloadMigration?.baseline
				: undefined;
		await Promise.all([
			validateSyntheticPairArtifact(baseline, "baseline", approvedLegacySynthetic),
			validateSyntheticPairArtifact(candidate, "candidate"),
		]);
	}
	const [baselineMigrated, candidateMigrated] = await Promise.all([
		baseline.treatment.schemaVersion >= 10
			? validateMigratedTemplateArtifact(baseline, "baseline")
			: Promise.resolve(undefined),
		candidate.treatment.schemaVersion >= 10
			? validateMigratedTemplateArtifact(candidate, "candidate")
			: Promise.resolve(undefined),
	]);
	assertPreparedStoreCompatibility(
		baseline,
		candidate,
		baselineStores.prepared,
		candidateStores.prepared,
		baselineMigrated,
		candidateMigrated,
		"artifact",
	);
	return comparisonResult(baseline, candidate, harnessCompatibility);
}

async function validateCandidateHarnessProvenance(candidate: ReplayReport, declaredSourceHash: string): Promise<void> {
	const sourceHash = candidate.fixture.harnessSourceHash;
	const identityHash = candidate.fixture.harnessIdentityHash;
	const sourcePath = candidate.artifacts?.harnessSource;
	if (!sourceHash || !/^[a-f0-9]{64}$/.test(sourceHash) || sourceHash !== declaredSourceHash) {
		throw new Error("candidate harness source hash does not match the manifest-declared candidate source");
	}
	if (!identityHash || identityHash !== replayHarnessIdentityHash(sourceHash)) {
		throw new Error("candidate harness compatibility identity does not match its captured source hash");
	}
	if (!sourcePath) throw new Error("candidate captured harness source artifact is missing");
	const canonicalSourcePath = await assertArtifactPath(sourcePath, "candidate harness source", "read");
	const sourceStat = await fs.lstat(canonicalSourcePath);
	if (sourceStat.isSymbolicLink() || !sourceStat.isFile()) {
		throw new Error("candidate harness source artifact must be a regular non-symlink file");
	}
	const [capturedSource, currentSource] = await Promise.all([
		Bun.file(canonicalSourcePath).text(),
		Bun.file(import.meta.path).text(),
	]);
	if (hashText(capturedSource) !== sourceHash) {
		throw new Error("candidate captured harness source digest does not match its report");
	}
	if (hashText(currentSource) !== sourceHash) {
		throw new Error("candidate harness source does not match the currently executing comparison harness");
	}
}

async function compare(options: CompareOptions): Promise<Record<string, unknown>> {
	const [baselineBytes, candidateBytes] = await Promise.all([
		Bun.file(options.baseline).bytes(),
		Bun.file(options.candidate).bytes(),
	]);
	const baseline = JSON.parse(new TextDecoder().decode(baselineBytes)) as ReplayReport;
	const candidate = JSON.parse(new TextDecoder().decode(candidateBytes)) as ReplayReport;
	const declaration = harnessCompatibilityManifest.pairs.find(
		pair =>
			pair.baseline === baseline.fixture.harnessSourceHash && pair.candidate === candidate.fixture.harnessSourceHash,
	);
	if (!declaration) throw new Error("comparison requires a manifest-declared historical baseline report");
	const declaredPath = lexicalArtifactPath(
		path.resolve(REPO_ROOT, declaration.baselineArtifact.path),
		"historical baseline",
	);
	const [actualPath, expectedPath] = await Promise.all([fs.realpath(options.baseline), fs.realpath(declaredPath)]);
	if (actualPath !== expectedPath || hashBytes(baselineBytes) !== declaration.baselineArtifact.sha256) {
		throw new Error("comparison requires the immutable manifest-declared historical baseline artifact");
	}
	await validateCandidateHarnessProvenance(candidate, declaration.candidate);
	return compareReplayReports(baseline, candidate);
}

async function main(): Promise<void> {
	const cli = parseCli(Bun.argv.slice(2));
	if (cli.mode === "baseline") {
		const harnessSourceHash = hashText(await Bun.file(import.meta.path).text());
		if (!harnessCompatibilityManifest.pairs.some(pair => pair.baseline === harnessSourceHash)) {
			throw new Error(
				`baseline capture requires a manifest-declared historical baseline harness source; ${harnessSourceHash} is not declared. Use capture for the current candidate and compare it with an immutable historical baseline report.`,
			);
		}
	}
	await validateCliArtifactPaths(cli);
	if (cli.mode === "capture" || cli.mode === "baseline") {
		const report = await capture(cli);
		await writePrivateArtifact(cli.out, `${JSON.stringify(report, null, 2)}\n`);
		process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
	} else if (cli.mode === "compare") {
		process.stdout.write(`${JSON.stringify(await compare(cli), null, 2)}\n`);
	}
}

if (import.meta.main) {
	try {
		await main();
	} catch (error) {
		const diagnostic = error instanceof Error ? (error.stack ?? error.message) : String(error);
		await Bun.write(Bun.stderr, `${diagnostic}\n`);
		process.exit(1);
	}
}
