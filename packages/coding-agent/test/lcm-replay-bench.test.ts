import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { openLcmContext, type SourceSnapshot } from "@oh-my-pi/lcm-context";
import type { AgentTool } from "@oh-my-pi/pi-agent-core";
import {
	assertProjectionPreparation,
	compareReplayReports,
	type PreparedTemplateEvidence,
	preparedTemplateEvidence,
	type ReplayReport,
	replayCancellationControlFixtureHash,
	replayHarnessIdentityHash,
	sqliteReport as replaySqliteReport,
	replayWorkloadFingerprint,
	type SummaryLineageEvidenceRow,
} from "../bench/lcm-replay.bench";
import type { Skill } from "../src/extensibility/skills";
import { encodeLcmHandle } from "../src/lcm/operations";
import { computeNonMessageTokens } from "../src/modes/utils/context-usage";

const repoRoot = path.resolve(import.meta.dir, "../../..");
const benchPath = path.join(repoRoot, "packages/coding-agent/bench/lcm-replay.bench.ts");
const artifactRoot = path.join(repoRoot, "compaction-results/lcm-replay");

interface CliResult {
	exitCode: number;
	stdout: string;
	stderr: string;
}

interface CapturedReplayReport {
	fixture: { projectId: string; sessionId: string; branchId: string };
	artifacts: { treatmentTemplate: string; migratedTemplate?: string };
	treatment: { hardProjectionWaitMs: number | null; schemaVersion: number; retryKey?: string };
	storeEvidence: {
		pristine: object;
		migratedTemplate: PreparedTemplateEvidence;
		preparedTemplate: PreparedTemplateEvidence;
	};
	samples: Array<{
		schemaVersion: number;
		serializedStoreHash: string;
		postStoreHash?: string;
		postSnapshotHash?: string;
	}>;
	failureControls?: ReplayFailureControl[];
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
	name:
		| "store"
		| "provider-exhausted"
		| "provider-key-mismatch"
		| "assembly-invalid"
		| "fit-invariant"
		| "irreducible-input"
		| "minimum-representation";
	route: "native_fallback";
	category: "store" | "provider" | "unfit";
	reason: string | null;
	providerCalls: number;
	storeRowsChanged: number;
}

type ReplayFallbackCategory = "deadline" | "provider" | "store" | "unfit" | null;
type ReplayFailureReason =
	| "coverage_gap"
	| "assembly_invalid"
	| "irreducible_input"
	| "minimum_representation"
	| "provider_key_mismatch"
	| "provider_exhausted"
	| "fit_invariant"
	| null;

interface ReplaySampleStatus {
	route: "lossless" | "native_passthrough" | "native_fallback" | "native";
	committed?: boolean;
	hasMetrics?: boolean;
	failureCategory: ReplayFallbackCategory;
	failureReason: ReplayFailureReason;
}

interface ReplaySelectedSpan {
	summaryId: string;
	summaryHandle: string;
	level: number;
	tokenCount: number;
	sourceCount: number;
	summaryRow: Record<string, unknown> | null;
	sourceIds: string[];
	lineageRows: SummaryLineageEvidenceRow[];
}

interface ReplayControlReport {
	fixture: { name: string; settings: { thresholdTokens: number } };
	samples: Array<{
		controlEvidence?: ReplayControlEvidence;
		fitProof: { owned: boolean };
		providerAttempts: number;
		retries: number;
		sourceCoverage: { complete: boolean };
		status?: { route: string };
		storeRowsChanged: number;
	}>;
}

interface MutableReplayEvidence {
	metrics: {
		latencyMs: { median: number };
		providerUsage: { cost: number };
	};
	candidateOutcome: { route: string };
}

let caseDir = "";
const cleanupPaths: string[] = [];

async function runHarness(args: readonly string[]): Promise<CliResult> {
	const child = Bun.spawn([process.execPath, benchPath, ...args], {
		cwd: repoRoot,
		env: { ...process.env, NO_COLOR: "1", PI_CODING_AGENT_DIR: path.join(caseDir, "agent") },
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
	});
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	return { exitCode, stdout, stderr };
}

function replayReport() {
	const fitProof = {
		owned: true,
		ready: true,
		complete: true,
		revision: 1,
		uncoveredSources: 0,
	};
	const sourceCoverage = { active: 1, covered: 1, fresh: 0, uncovered: 0, complete: true };
	const providerUsage = { input: 0, output: 0, reasoning: 0, cacheRead: 0, cacheWrite: 0, cost: 0 };
	const sourceRows = [{ position: 0, entryId: "source-1", sourceKey: "source-key-1" }];
	const fallbackCategory: ReplayFallbackCategory = null;
	const status: ReplaySampleStatus = {
		route: "lossless",
		committed: true,
		hasMetrics: true,
		failureCategory: null,
		failureReason: null,
	};
	const summaryLineageRows: SummaryLineageEvidenceRow[] = [
		{
			summaryId: "summary-1",
			summaryHandle: "handle-1",
			projectId: "project-a",
			branchRevision: 1,
			level: 0,
			startPosition: 0,
			endPosition: 1,
			frontier: 1,
			ordinal: 0,
			sourceKey: "source-key-1",
			entryId: "source-1",
			position: 0,
		},
	];
	const selectedSpans: ReplaySelectedSpan[] = [
		{
			summaryId: "summary-1",
			summaryHandle: "handle-1",
			level: 0,
			tokenCount: 1,
			sourceCount: 1,
			summaryRow: { summary_id: "summary-1", stable_handle: "handle-1", project_id: "project-a" },
			sourceIds: ["source-1"],
			lineageRows: structuredClone(summaryLineageRows),
		},
	];
	return {
		harnessSchema: "lcm-replay/v2",
		workloadFingerprint: "workload-fingerprint-a",
		sourceSnapshotHash: "source-snapshot-a",
		artifacts: { root: "/private/capture-a" },
		fixture: {
			name: "real",
			markerId: "marker-a",
			parentId: "source-1",
			markerOrdinal: 2,
			sessionId: "session-a",
			projectId: "project-a",
			branchId: "branch-a",
			sessionTimestamp: "2026-07-31T08:08:17.283Z",
			selectedEntriesHash: "raw-prefix-a",
			selectedEntries: 1,
			journalFileBytesAtCapture: 1_024,
			journalSuffixHash: "a".repeat(64),
			activeSources: 1,
			sourceTokens: 100,
			summaryModelSelector: "@smol",
			contextWindow: 400_000,
			settings: {
				thresholdTokens: 340_000,
				freshTailMaxSources: 32,
				freshTailMaxTokens: 16_000,
				maxConcurrentSummaries: 4,
			},
			estimatorSchema: "pi-estimate-tokens/v1",
			reconstruction: {
				classification: "exact-historical-replay",
				preChangeContractReproduced: true,
				originalRedactionIdentityAvailable: true,
				missingBlobRefs: [],
				syntheticFixture: null,
			},
		},
		treatment: { label: "baseline", hardProjectionWaitMs: 30_000, schemaVersion: 9 },
		samples: [
			{
				sample: 1,
				latencyMs: 1,
				peakProviderConcurrency: 0,
				providerUsage,
				retries: 0,
				cacheReuse: 0,
				fitProof,
				fallbackCategory,
				selectedSpans: [...selectedSpans],
				status,
				sourceCoverage,
				promptInputTokens: 100,
				underfillRatio: 0,
				storeRowsChanged: 0,
				sqliteQuickCheck: "ok",
				serializedStoreHash: "sqlite-snapshot-a",
				schemaVersion: 9,
				sourceRows,
			},
		],
		metrics: {
			latencyMs: { median: 1, mad: 0, p95: 1 },
			peakProviderConcurrency: 0,
			providerUsage,
			retries: 0,
			cacheReuse: 0,
			fitProof,
			fallbackCategory,
			selectedSpans: [...selectedSpans],
			sourceCoverage,
			promptInputTokens: 100,
			underfillRatio: 0,
			storeRowsChanged: 0,
			sqliteQuickCheck: "ok",
			serializedStoreHash: "sqlite-snapshot-a",
		},
	};
}

async function compareReports(baseline: object, candidate: object): Promise<CliResult> {
	try {
		return {
			exitCode: 0,
			stdout: `${JSON.stringify(await compareReplayReports(baseline as ReplayReport, candidate as ReplayReport))}\n`,
			stderr: "",
		};
	} catch (error) {
		return {
			exitCode: 1,
			stdout: "",
			stderr: error instanceof Error ? (error.stack ?? error.message) : String(error),
		};
	}
}

function sha256(value: string | Uint8Array): string {
	return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

async function unrelatedStateEvidence(value: string | null): Promise<PreparedTemplateEvidence> {
	const storePath = path.join(caseDir, `unrelated-state-${Bun.randomUUIDv7()}.sqlite`);
	const db = new Database(storePath, { strict: true });
	try {
		db.run("PRAGMA user_version = 9");
		db.run("CREATE TABLE unrelated_state (id TEXT PRIMARY KEY, value TEXT NOT NULL)");
		if (value !== null) db.run("INSERT INTO unrelated_state(id, value) VALUES ('state-1', ?)", [value]);
	} finally {
		db.close(false);
	}
	return preparedTemplateEvidence(replaySqliteReport(storePath, undefined, undefined, undefined));
}

async function permissionBits(target: string): Promise<number> {
	return (await fs.stat(target)).mode & 0o777;
}

function fiveSampleReport() {
	const report = replayReport();
	const seed = report.samples[0]!;
	const renderedHandle = encodeLcmHandle({
		kind: "summary",
		reference: { projectId: "project-a", sessionId: "session-a", branchId: "branch-a", summaryHandle: "handle-1" },
	});
	report.samples = Array.from({ length: 5 }, (_, index) => {
		const sample = structuredClone(seed);
		sample.sample = index + 1;
		Object.assign(sample.sourceCoverage, {
			activeSourceKeys: ["source-key-1"],
			historicalSourceKeys: ["source-key-1"],
			freshSourceKeys: [],
			projectedSourceKeys: ["source-key-1"],
		});
		Object.assign(sample, {
			cpuMs: 1,
			postStoreHash: sample.serializedStoreHash,
			postSnapshotHash: "sqlite-snapshot-bytes-a",
			providerAttempts: 0,
			counters: {
				projectionCalls: 1,
				projectionReads: 1,
				projectionLineageRowsRead: 0,
				lineageReads: 0,
				reconcileRowsChanged: 0,
				rowsChanged: 0,
				schedulerBranchPasses: 0,
			},
			handles: {
				count: 1,
				unique: 1,
				allPresent: true,
				allResolved: true,
				allMatchStore: true,
				providerVisible: true,
				tokens: [renderedHandle],
			},
			attempts: { rows: [], inFlight: 0, billed: 0, missingUsage: 0 },
			jobs: { relevant: 0, pending: 0, leased: 0, backoff: 0, exhausted: 0, missing: 0 },
			status: { route: "lossless", committed: true, hasMetrics: true, failureCategory: null, failureReason: null },
			tokens: { request: 100, nonMessage: 0, candidate: 100, routeCandidate: 100, budget: 100 },
		});
		return sample;
	});
	Object.assign(report.metrics, {
		latencyMs: { median: 1, mad: 0, p95: 1 },
		cpuMs: { median: 1, mad: 0, p95: 1 },
		sourceCoverage: structuredClone(report.samples[0]!.sourceCoverage),
	});
	report.metrics.selectedSpans = structuredClone(report.samples[0]!.selectedSpans);
	const preparedTemplate: PreparedTemplateEvidence = {
		reserialized: true,
		quickCheck: "ok",
		byteHash: "prepared-bytes-a",
		logicalHash: "prepared-logical-a",
		schemaRows: [],
		sourceRows: structuredClone(report.samples[0]!.sourceRows),
		summaryRows: [{ summary_id: "summary-1", stable_handle: "handle-1", project_id: "project-a" }],
		summaryLineageRows: structuredClone(report.samples[0]!.selectedSpans[0]!.lineageRows),
		jobRows: [],
		policyRows: [],
		projectIds: ["project-a"],
		retryEpochs: [],
	};
	const cancellationScope = {
		projectId: "cancellation-project",
		sessionId: "cancellation-session",
		branchId: "cancellation-branch",
	};
	const cancellationBoundary = { thresholdTokens: 5_000, freshTail: { maxSources: 32, maxTokens: 16_000 } };
	const cancellationSourceRows = Array.from({ length: 40 }, (_, index) => ({
		position: index,
		entryId: `cancellation-source-${index + 1}`,
		sourceKey: `cancellation-key-${index + 1}`,
	}));
	const cancellationAttempt = {
		attemptId: "cancellation-attempt",
		jobId: "cancellation-job",
		projectId: cancellationScope.projectId,
		outcome: "aborted",
		startedAt: 1,
		completedAt: 2,
		usage: {
			inputTokens: 10,
			outputTokens: 1,
			cacheReadTokens: 0,
			cacheWriteTokens: 0,
			totalTokens: 11,
			reasoningTokens: null,
			costTotal: 0.001,
		},
	};
	const cancellationSample = structuredClone(report.samples[0]!);
	Object.assign(cancellationSample, {
		sample: 0,
		sourceRows: cancellationSourceRows,
		serializedStoreHash: "cancellation-template-logical",
		postStoreHash: "cancellation-final-logical",
		providerAttempts: 1,
		storeRowsChanged: 1,
		attempts: { rows: [cancellationAttempt], inFlight: 0, billed: 1, missingUsage: 0 },
		jobs: { relevant: 1, pending: 1, leased: 0, backoff: 0, exhausted: 0, missing: 0 },
		controlEvidence: {
			kind: "cancellation",
			started: true,
			claimedJobs: 1,
			aborted: true,
			projectionReturnedBeforeProviderRelease: true,
			abortLatencyMs: 1,
			cleanupLatencyMs: 1,
			providerAttempts: 1,
			providerWrites: 0,
			inFlightAttempts: 0,
			billedAttempts: 1,
			missingUsage: 0,
			attemptOutcomes: ["aborted"],
			staleSummaries: 0,
			activeProviders: 0,
			cleanupComplete: true,
			storeRowsChanged: 1,
		} satisfies ReplayControlEvidence,
	});
	const cancellationTemplateEvidence: PreparedTemplateEvidence = {
		...structuredClone(preparedTemplate),
		byteHash: "cancellation-template-bytes",
		logicalHash: cancellationSample.serializedStoreHash,
		sourceRows: cancellationSourceRows,
		summaryRows: [],
		summaryLineageRows: [],
		jobRows: [],
		policyRows: [],
		projectIds: [cancellationScope.projectId],
		retryEpochs: [1],
	};
	const failureControls: ReplayFailureControl[] = [
		{
			name: "store",
			route: "native_fallback",
			category: "store",
			reason: null,
			providerCalls: 0,
			storeRowsChanged: 0,
		},
		{
			name: "provider-exhausted",
			route: "native_fallback",
			category: "provider",
			reason: "provider_exhausted",
			providerCalls: 1,
			storeRowsChanged: 1,
		},
		{
			name: "provider-key-mismatch",
			route: "native_fallback",
			category: "provider",
			reason: "provider_key_mismatch",
			providerCalls: 0,
			storeRowsChanged: 0,
		},
		{
			name: "assembly-invalid",
			route: "native_fallback",
			category: "unfit",
			reason: "assembly_invalid",
			providerCalls: 0,
			storeRowsChanged: 0,
		},
		{
			name: "fit-invariant",
			route: "native_fallback",
			category: "unfit",
			reason: "fit_invariant",
			providerCalls: 0,
			storeRowsChanged: 0,
		},
		{
			name: "irreducible-input",
			route: "native_fallback",
			category: "unfit",
			reason: "irreducible_input",
			providerCalls: 0,
			storeRowsChanged: 0,
		},
		{
			name: "minimum-representation",
			route: "native_fallback",
			category: "unfit",
			reason: "minimum_representation",
			providerCalls: 0,
			storeRowsChanged: 0,
		},
	];
	return Object.assign(report, {
		failureControls,
		cancellationControl: {
			kind: "cancellation",
			fixtureHash: replayCancellationControlFixtureHash(40, cancellationBoundary),
			sourceCount: 40,
			boundary: cancellationBoundary,
			scope: cancellationScope,
			templatePath: path.join(artifactRoot, "fixture-cancellation-template.sqlite"),
			templateEvidence: cancellationTemplateEvidence,
			sample: cancellationSample,
		},
		candidateOutcome: { route: "lossless", reproducedOldFallback: true },
		storeEvidence: {
			pristine: { quickCheck: "ok", logicalHash: "sqlite-snapshot-a" },
			migratedTemplate: structuredClone(preparedTemplate),
			preparedTemplate,
		},
	});
}

interface PhysicalReplayFixture {
	journalPath: string;
	agentDir: string;
	blobRef: string;
	blobPath: string;
	imageBase64: string;
	providerBlobRef: string;
	providerImageUrl: string;
	selectedMarkerId: string;
	latestMarkerId: string;
}

async function createPhysicalReplayFixture(additionalPrefixSources = 0): Promise<PhysicalReplayFixture> {
	const blobBytes = Buffer.from("physical replay blob", "utf8");
	const blobHash = sha256(blobBytes);
	const blobRef = `blob:sha256:${blobHash}`;
	const imageBase64 = blobBytes.toString("base64");
	const providerImageUrl = `data:image/png;base64,${Buffer.from("provider replay image", "utf8").toString("base64")}`;
	const providerBlobBytes = Buffer.from(providerImageUrl, "utf8");
	const providerBlobHash = sha256(providerBlobBytes);
	const providerBlobRef = `blob:sha256:${providerBlobHash}`;
	const agentDir = path.join(caseDir, "agent");
	const blobPath = path.join(agentDir, "blobs", "data", blobHash);
	const providerBlobPath = path.join(agentDir, "blobs", "data", providerBlobHash);
	await fs.mkdir(path.dirname(blobPath), { recursive: true });
	await Promise.all([Bun.write(blobPath, blobBytes), Bun.write(providerBlobPath, providerBlobBytes)]);
	const journalPath = path.join(caseDir, "physical-session.jsonl");
	const timestamp = "2026-07-31T08:08:17.283Z";
	const rows: Record<string, unknown>[] = [
		{ type: "session", version: 3, id: "physical-session", timestamp, cwd: caseDir },
		{
			type: "message",
			id: "source-1",
			parentId: null,
			timestamp,
			message: {
				role: "user",
				content: [
					{ type: "text", text: "opening physical replay source" },
					{ type: "image", data: blobRef, mimeType: "image/png" },
				],
				providerPayload: {
					type: "openaiResponsesHistory",
					provider: "openai-codex",
					items: [
						{
							type: "message",
							role: "user",
							content: [
								{ type: "input_text", text: "opening physical replay source" },
								{ type: "input_image", detail: "auto", image_url: providerBlobRef },
							],
						},
					],
				},
				timestamp: 1,
			},
		},
		{
			type: "message",
			id: "source-2",
			parentId: "source-1",
			timestamp,
			message: { role: "user", content: "selected marker source", timestamp: 2 },
		},
		{
			type: "compaction",
			id: "selected-marker",
			parentId: "source-2",
			timestamp,
			summary: "selected marker summary",
			firstKeptEntryId: "source-2",
			tokensBefore: 374_856,
			lcmFallback: "unfit",
		},
		{
			type: "message",
			id: "source-3",
			parentId: "selected-marker",
			timestamp,
			message: { role: "user", content: "newer source", timestamp: 3 },
		},
		{
			type: "compaction",
			id: "latest-marker",
			parentId: "source-3",
			timestamp,
			summary: "latest marker summary",
			firstKeptEntryId: "source-3",
			tokensBefore: 400_001,
			lcmFallback: "unfit",
		},
		{
			type: "message",
			id: "suffix-source",
			parentId: "latest-marker",
			timestamp,
			message: { role: "user", content: "captured suffix", timestamp: 4 },
		},
	];
	if (additionalPrefixSources > 0) {
		const markerIndex = rows.findIndex(row => row.id === "selected-marker");
		let parentId = "source-2";
		const extraRows = Array.from({ length: additionalPrefixSources }, (_, index) => {
			const id = `source-extra-${index}`;
			const row = {
				type: "message",
				id,
				parentId,
				timestamp,
				message: { role: "user", content: `additional source ${index}`, timestamp: index + 3 },
			};
			parentId = id;
			return row;
		});
		rows.splice(markerIndex, 0, ...extraRows);
		Object.assign(rows[markerIndex + extraRows.length]!, { parentId, firstKeptEntryId: parentId });
	}
	await Bun.write(journalPath, `${rows.map(row => JSON.stringify(row)).join("\n")}\n`);
	return {
		journalPath,
		agentDir,
		imageBase64,
		providerBlobRef,
		providerImageUrl,
		blobRef,
		blobPath,
		selectedMarkerId: "selected-marker",
		latestMarkerId: "latest-marker",
	};
}

async function capturePhysical(
	name: string,
	fixture: PhysicalReplayFixture,
	extra: readonly string[] = [],
	mode: "capture" | "baseline" = "capture",
	markerId = fixture.selectedMarkerId,
): Promise<{ result: CliResult; reportPath: string; snapshotPath: string; sourcePath: string }> {
	const reportPath = path.join(caseDir, `${name}.json`);
	const snapshotPath = path.join(caseDir, `${name}.sqlite`);
	const sourcePath = path.join(caseDir, `${name}.sources.json`);
	const result = await runHarness([
		mode,
		"--fixture",
		"real",
		"--replay",
		fixture.journalPath,
		"--marker",
		markerId,
		"--agent-dir",
		fixture.agentDir,
		"--samples",
		"5",
		"--threshold-tokens",
		"1000000",
		"--out",
		reportPath,
		"--snapshot-out",
		snapshotPath,
		"--source-out",
		sourcePath,
		...extra,
	]);
	return { result, reportPath, snapshotPath, sourcePath };
}

beforeEach(async () => {
	await fs.mkdir(artifactRoot, { recursive: true });
	caseDir = await fs.mkdtemp(path.join(artifactRoot, "contract-test-"));
});

afterEach(async () => {
	await Promise.all([
		fs.rm(caseDir, { recursive: true, force: true }),
		...cleanupPaths.splice(0).map(target => fs.rm(target, { recursive: true, force: true })),
	]);
});

describe("LCM replay CLI modes", () => {
	for (const mode of ["capture", "baseline", "compare"] as const) {
		it(`parses ${mode} before validating its required options`, async () => {
			const result = await runHarness([mode]);

			expect(result.exitCode).not.toBe(0);
			expect(result.stderr).toContain(`${mode} requires`);
		});
	}

	it("lists every supported mode for an unknown command", async () => {
		const result = await runHarness(["unknown"]);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("capture|baseline|compare");
	});

	it("rejects current-tree baseline capture before writing artifacts", async () => {
		const reportPath = path.join(caseDir, "current-baseline.json");
		const result = await runHarness(["baseline", "--fixture", "cancellation", "--samples", "1", "--out", reportPath]);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/manifest-declared historical baseline/i);
		expect(result.stderr).toMatch(/compare/i);
		expect(await Bun.file(reportPath).exists()).toBe(false);
		expect(await Bun.file(`${reportPath}.sqlite`).exists()).toBe(false);
		expect(await Bun.file(`${reportPath}.sources.json`).exists()).toBe(false);
		expect((await fs.readdir(caseDir)).some(name => name.startsWith("current-baseline."))).toBe(false);
	});

	it("rejects a current capture report in the immutable baseline slot", async () => {
		const fixture = await createPhysicalReplayFixture();
		const capture = await capturePhysical("current-baseline-slot", fixture);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);

		const comparison = await runHarness([
			"compare",
			"--baseline",
			capture.reportPath,
			"--candidate",
			capture.reportPath,
		]);

		expect(comparison.exitCode).not.toBe(0);
		expect(comparison.stderr).toMatch(/manifest-declared historical baseline/i);
	}, 60_000);
});

describe("LCM replay artifact confinement", () => {
	it("rejects capture outputs outside compaction-results/lcm-replay before writing them", async () => {
		const outsideDir = path.join(repoRoot, "compaction-results", `outside-lcm-replay-${path.basename(caseDir)}`);
		cleanupPaths.push(outsideDir);
		await fs.mkdir(outsideDir, { recursive: true });
		const reportPath = path.join(outsideDir, "report.json");
		const snapshotPath = path.join(outsideDir, "source-snapshot.json");

		const result = await runHarness([
			"capture",
			"--fixture",
			"minimal-marker-budget",
			"--samples",
			"1",
			"--threshold-tokens",
			"1000000",
			"--out",
			reportPath,
			"--snapshot-out",
			snapshotPath,
		]);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr.replaceAll("\\", "/")).toContain("compaction-results/lcm-replay");
		expect(await Bun.file(reportPath).exists()).toBe(false);
		expect(await Bun.file(snapshotPath).exists()).toBe(false);
	});

	it("rejects symlinked output parents before writing outside the artifact root", async () => {
		const outsideDir = path.join(repoRoot, "compaction-results", `outside-lcm-replay-${path.basename(caseDir)}`);
		cleanupPaths.push(outsideDir);
		await fs.mkdir(outsideDir, { recursive: true });
		const escapeLink = path.join(caseDir, "escape");
		await fs.symlink(outsideDir, escapeLink, "dir");
		const reportPath = path.join(escapeLink, "report.json");
		const snapshotPath = path.join(escapeLink, "source-snapshot.json");

		const result = await runHarness([
			"capture",
			"--fixture",
			"minimal-marker-budget",
			"--samples",
			"1",
			"--threshold-tokens",
			"1000000",
			"--out",
			reportPath,
			"--snapshot-out",
			snapshotPath,
		]);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr.replaceAll("\\", "/")).toContain("compaction-results/lcm-replay");
		expect(await Bun.file(path.join(outsideDir, "report.json")).exists()).toBe(false);
		expect(await Bun.file(path.join(outsideDir, "source-snapshot.json")).exists()).toBe(false);
	});
});

describe("LCM replay immutable identity", () => {
	it("does not treat private artifact roots as workload drift", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.artifacts.root = "/private/capture-b";

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/artifact/i);
		expect(result.stderr).not.toMatch(/workload.*drift|fingerprint mismatch/i);
	});

	it("accepts only the declared evidence-only harness migration pair", async () => {
		const fixture = await createPhysicalReplayFixture();
		const capture = await capturePhysical("compatibility", fixture);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);
		const baseline = (await Bun.file(capture.reportPath).json()) as ReplayReport;
		const candidate = structuredClone(baseline);
		const branchTable = candidate.storeEvidence?.preparedTemplate.logicalTables?.find(
			table => table.name === "branches",
		);
		const targetBranch = branchTable?.rows.find(
			row =>
				row.project_id === candidate.fixture.projectId &&
				row.session_id === candidate.fixture.sessionId &&
				row.branch_id === candidate.fixture.branchId,
		);
		if (!targetBranch || typeof targetBranch.summary_token_budget !== "number") {
			throw new Error("captured compatibility fixture omitted its prepared branch policy");
		}
		for (const report of [baseline, candidate]) {
			for (const sample of report.samples) {
				sample.tokens = {
					request: sample.promptInputTokens,
					nonMessage: 0,
					candidate: sample.promptInputTokens,
					routeCandidate: sample.promptInputTokens,
					budget: targetBranch.summary_token_budget,
				};
			}
		}
		baseline.fixture.harnessSourceHash = "da43b5239ee196a0f188aa3f0db7957aaf41ab42166153ba211a12526359e3b5";
		delete baseline.fixture.harnessIdentityHash;
		candidate.fixture.harnessSourceHash = "963640b7c2a07f4ceeba79be988e392529085b55e03a5542dd33384034ea4dd3";
		candidate.fixture.harnessIdentityHash = replayHarnessIdentityHash(candidate.fixture.harnessSourceHash);
		baseline.workloadFingerprint = replayWorkloadFingerprint(baseline);
		candidate.workloadFingerprint = replayWorkloadFingerprint(candidate);

		const compatible = await compareReports(baseline, candidate);
		expect(compatible.exitCode, compatible.stderr).toBe(0);
		const migrationResult = JSON.parse(compatible.stdout) as {
			metricLane?: string;
			deltas?: unknown;
			baseline?: unknown;
			candidate?: unknown;
		};
		expect(migrationResult).toMatchObject({
			metricLane: "not-comparable",
			deltas: null,
			baseline: null,
			candidate: null,
		});

		candidate.fixture.harnessSourceHash = "0000000000000000000000000000000000000000000000000000000000000000";
		candidate.fixture.harnessIdentityHash = replayHarnessIdentityHash(candidate.fixture.harnessSourceHash);
		candidate.workloadFingerprint = replayWorkloadFingerprint(candidate);
		const undeclared = await compareReports(baseline, candidate);
		expect(undeclared.exitCode).not.toBe(0);
		expect(undeclared.stderr).toMatch(/fingerprint mismatch/i);
	}, 60_000);

	it("rejects captured live-suffix drift", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.fixture.journalFileBytesAtCapture += 4_096;

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/identity|prefix|suffix|workload/i);
	});

	it("rejects a changed pre-marker prefix with a forged unchanged fingerprint", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.fixture.selectedEntriesHash = "raw-prefix-b";

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/identity|prefix|workload/i);
	});

	for (const [field, changed] of [
		["markerId", "marker-b"],
		["parentId", "source-2"],
		["sessionId", "session-b"],
		["branchId", "branch-b"],
	] as const) {
		it(`rejects forged ${field} scope drift`, async () => {
			const baseline = fiveSampleReport();
			const candidate = structuredClone(baseline);
			candidate.fixture[field] = changed;

			const result = await compareReports(baseline, candidate);

			expect(result.exitCode).not.toBe(0);
			expect(result.stderr).toMatch(/identity|marker|parent|scope|session|branch|workload|handle|bind/i);
		});
	}

	it("rejects supplied source snapshots that drift from the selected journal", async () => {
		const fixture = await createPhysicalReplayFixture();
		const seed = await capturePhysical("source-binding-seed", fixture, [], "capture", fixture.latestMarkerId);
		expect(seed.result.exitCode, seed.result.stderr).toBe(0);
		const seedStoreHash = sha256(await Bun.file(seed.snapshotPath).bytes());
		const source = (await Bun.file(seed.sourcePath).json()) as SourceSnapshot;
		const mutations: Array<[string, (value: SourceSnapshot) => void]> = [
			[
				"project",
				value => {
					value.scope.projectId = "forged-project";
					for (const entry of value.entries) entry.projectId = "forged-project";
				},
			],
			[
				"session",
				value => {
					value.scope.sessionId = "forged-session";
					for (const entry of value.entries) entry.sessionId = "forged-session";
				},
			],
			[
				"branch",
				value => {
					value.scope.branchId = "forged-branch";
					for (const entry of value.entries) entry.branchId = "forged-branch";
				},
			],
			["entry order", value => Object.assign(value, { entries: [...value.entries].reverse() })],
			[
				"entry content",
				value => {
					const entry = value.entries[0]!;
					entry.redactedText = `${entry.redactedText} forged`;
					entry.contentHash = sha256(entry.redactedText);
				},
			],
		];
		for (const [name, mutate] of mutations) {
			const forged = structuredClone(source);
			mutate(forged);
			const sourcePath = path.join(caseDir, `forged-${name.replaceAll(" ", "-")}-input.sources.json`);
			const sourceText = `${JSON.stringify(forged)}\n`;
			await Bun.write(sourcePath, sourceText);
			const sourceInputHash = sha256(sourceText);
			const rejectionStarted = Bun.nanoseconds();
			const capture = await capturePhysical(
				`forged-${name.replaceAll(" ", "-")}`,
				fixture,
				["--source-in", sourcePath, "--store", seed.snapshotPath],
				"capture",
				fixture.latestMarkerId,
			);
			const rejectionMs = (Bun.nanoseconds() - rejectionStarted) / 1e6;
			expect(capture.result.exitCode, `${name}: ${capture.result.stderr}`).not.toBe(0);
			expect(capture.result.stderr).toMatch(/source snapshot.*selected journal|normalized source/i);
			expect(await Bun.file(capture.reportPath).exists()).toBe(false);
			expect(await Bun.file(capture.snapshotPath).exists()).toBe(false);
			expect(await Bun.file(capture.sourcePath).exists()).toBe(false);
			expect(sha256(await Bun.file(sourcePath).bytes())).toBe(sourceInputHash);
			expect(sha256(await Bun.file(seed.snapshotPath).bytes())).toBe(seedStoreHash);
			expect(rejectionMs).toBeLessThan(2_000);
		}
	}, 60_000);

	it("rejects a supplied store whose ordered source keys do not match the selected journal", async () => {
		const fixture = await createPhysicalReplayFixture();
		const seed = await capturePhysical("source-key-seed", fixture, [], "capture", fixture.latestMarkerId);
		expect(seed.result.exitCode, seed.result.stderr).toBe(0);
		const seedSourceHash = sha256(await Bun.file(seed.sourcePath).bytes());
		const forgedStore = path.join(caseDir, "forged-source-key-input.sqlite");
		await fs.copyFile(seed.snapshotPath, forgedStore);
		const db = new Database(forgedStore, { strict: true });
		try {
			db.exec("PRAGMA foreign_keys=OFF");
			db.run("UPDATE branch_sources SET source_key = source_key || '-forged' WHERE position = 0");
		} finally {
			db.close(false);
		}
		const forgedStoreHash = sha256(await Bun.file(forgedStore).bytes());
		const rejectionStarted = Bun.nanoseconds();
		const capture = await capturePhysical(
			"forged-source-key",
			fixture,
			["--source-in", seed.sourcePath, "--store", forgedStore],
			"capture",
			fixture.latestMarkerId,
		);
		const rejectionMs = (Bun.nanoseconds() - rejectionStarted) / 1e6;
		expect(capture.result.exitCode).not.toBe(0);
		expect(capture.result.stderr).toMatch(/store.*source rows|source keys|selected journal/i);
		expect(await Bun.file(capture.reportPath).exists()).toBe(false);
		expect(await Bun.file(capture.snapshotPath).exists()).toBe(false);
		expect(await Bun.file(capture.sourcePath).exists()).toBe(false);
		expect(sha256(await Bun.file(forgedStore).bytes())).toBe(forgedStoreHash);
		expect(sha256(await Bun.file(seed.sourcePath).bytes())).toBe(seedSourceHash);
		expect(rejectionMs).toBeLessThan(2_000);
	}, 60_000);
});

describe("LCM replay SQLite integrity", () => {
	it("rejects a changed logical snapshot hash", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.metrics.serializedStoreHash = "sqlite-snapshot-b";
		candidate.samples[0]!.serializedStoreHash = "sqlite-snapshot-b";

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/SQLite|snapshot|store|integrity/i);
	});

	it("rejects a failed quick_check", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.metrics.sqliteQuickCheck = "database disk image is malformed";
		candidate.samples[0]!.sqliteQuickCheck = "database disk image is malformed";

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/quick_check|SQLite|integrity/i);
	});

	it("rejects pre-existing row or scope mutation across the declared schema migration", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		Object.assign(candidate.treatment, { hardProjectionWaitMs: null });
		candidate.treatment.schemaVersion = 10;
		candidate.samples[0]!.schemaVersion = 10;
		candidate.samples[0]!.sourceRows[0]!.entryId = "source-mutated";

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/row|scope|store|treatment|migration|lineage|placement/i);
	});

	for (const [name, candidateValue] of [
		["mutation", "changed"],
		["deletion", null],
	] as const) {
		it(`rejects unrelated persisted-table row ${name} across preparation`, async () => {
			const baseline = fiveSampleReport();
			baseline.storeEvidence.preparedTemplate = await unrelatedStateEvidence("original");
			const candidate = structuredClone(baseline);
			candidate.storeEvidence.preparedTemplate = await unrelatedStateEvidence(candidateValue);

			const result = await compareReports(baseline, candidate);

			expect(result.exitCode).not.toBe(0);
			expect(result.stderr).toMatch(
				/unrelated_state|pre-existing|persisted table|logical table|row order|row hash/i,
			);
		});
	}

	it("rejects unrelated append-only projection rows across preparation", async () => {
		const beforePath = path.join(caseDir, "append-only-before.sqlite");
		const afterPath = path.join(caseDir, "append-only-after.sqlite");
		for (const storePath of [beforePath, afterPath]) {
			const context = await openLcmContext({ dbPath: storePath });
			try {
				context.reconcile({
					scope: { projectId: "project-a", sessionId: "session-a", branchId: "branch-a" },
					entries: [
						{
							projectId: "project-a",
							sessionId: "session-a",
							branchId: "branch-a",
							entryId: "source-1",
							parentId: null,
							timestamp: 1,
							kind: "message",
							redactedText: "source text",
							contentHash: sha256("source text"),
							artifactRefs: [],
						},
					],
				});
			} finally {
				context.close();
			}
		}

		const afterDb = new Database(afterPath, { strict: true });
		try {
			afterDb.run(
				`INSERT INTO branches(project_id, session_id, branch_id, revision, reconciled_at)
				 VALUES ('unrelated-project', 'unrelated-session', 'unrelated-branch', 1, 0)`,
			);
			const branch = afterDb
				.query<{ id: number }, []>(
					"SELECT id FROM branches WHERE project_id = 'unrelated-project' AND session_id = 'unrelated-session' AND branch_id = 'unrelated-branch'",
				)
				.get();
			if (!branch) throw new Error("unrelated branch was not created");
			afterDb.run(
				`INSERT INTO branch_summary_spans(
					branch_row_id, revision, level, start_position, end_position, input_hash, summary_id, frontier
				 ) VALUES (?, 1, 0, 0, 1, 'unrelated-input', NULL, 1)`,
				[branch.id],
			);
		} finally {
			afterDb.close(false);
		}

		const before = replaySqliteReport(beforePath, "project-a", "session-a", "branch-a");
		const after = replaySqliteReport(afterPath, "project-a", "session-a", "branch-a");
		expect(() =>
			assertProjectionPreparation(
				before,
				after,
				"project-a",
				"lcm-replay/lcm-replay-summary",
				{
					scope: { projectId: "project-a", sessionId: "session-a", branchId: "branch-a" },
					tokenBudget: 100,
					maxSources: 32,
					maxTokens: 16_000,
				},
				"test projection preparation",
			),
		).toThrow(/branch_summary_spans|projection|scope|linkage/i);
	});

	it("binds every newly scheduled job to complete content-addressed payload and target lineage", async () => {
		const scope = { projectId: "project-a", sessionId: "session-a", branchId: "branch-a" };
		const source: SourceSnapshot = {
			scope,
			entries: Array.from({ length: 3 }, (_, index) => {
				const entryId = `source-${index + 1}`;
				const redactedText = `source text ${index + 1}`;
				return {
					...scope,
					entryId,
					parentId: index === 0 ? null : `source-${index}`,
					timestamp: index + 1,
					kind: "message",
					redactedText,
					contentHash: sha256(redactedText),
					artifactRefs: [],
				};
			}),
		};
		const retryKey = "lcm-replay/lcm-replay-summary";
		const branchPolicy = {
			scope,
			tokenBudget: 100,
			maxSources: 1,
			maxTokens: 16_000,
		};
		const basePath = path.join(caseDir, "projection-payload-base.sqlite");
		const base = await openLcmContext({ dbPath: basePath });
		try {
			base.reconcile(source);
			const policy = base.configureSummaryRetryPolicy(scope.projectId, retryKey);
			if (policy.kind !== "ready") throw new Error("projection payload retry policy did not become ready");
		} finally {
			base.close();
		}
		const baseDb = new Database(basePath, { strict: true });
		try {
			baseDb.run("DELETE FROM branch_summary_spans");
			baseDb.run("DELETE FROM summary_jobs");
		} finally {
			baseDb.close(false);
		}

		const beforePath = path.join(caseDir, "projection-payload-before.sqlite");
		const validPath = path.join(caseDir, "projection-payload-valid.sqlite");
		await Promise.all([fs.copyFile(basePath, beforePath), fs.copyFile(basePath, validPath)]);
		const prepared = await openLcmContext({ dbPath: validPath });
		try {
			prepared.reconcile(source, {
				summarize: {
					tokenBudget: branchPolicy.tokenBudget,
					freshTail: { maxSources: branchPolicy.maxSources, maxTokens: branchPolicy.maxTokens },
				},
			});
		} finally {
			prepared.close();
		}

		const before = replaySqliteReport(beforePath, scope.projectId, scope.sessionId, scope.branchId);
		const valid = replaySqliteReport(validPath, scope.projectId, scope.sessionId, scope.branchId);
		expect(() =>
			assertProjectionPreparation(
				before,
				valid,
				scope.projectId,
				retryKey,
				branchPolicy,
				"valid projection payload",
			),
		).not.toThrow();
		const beforeJobIds = new Set(before.jobRows.map(row => row.job_id));
		const addedJobId = valid.jobRows.find(row => !beforeJobIds.has(row.job_id))?.job_id;
		if (typeof addedJobId !== "string") throw new Error("valid projection payload did not add a job");

		for (const [name, mutate, pattern] of [
			[
				"missing-inputs",
				(db: Database) => db.run("DELETE FROM job_inputs WHERE job_id = ?", [addedJobId]),
				/job_inputs|without.*input/i,
			],
			[
				"missing-lineage",
				(db: Database) => db.run("DELETE FROM job_lineage WHERE job_id = ?", [addedJobId]),
				/job_lineage|without.*lineage/i,
			],
			[
				"forged-input-hash",
				(db: Database) => {
					db.run("UPDATE summary_jobs SET input_hash = 'forged-input-hash' WHERE job_id = ?", [addedJobId]);
					db.run("UPDATE branch_summary_spans SET input_hash = 'forged-input-hash' WHERE input_hash = ?", [
						addedJobId.slice("job_".length),
					]);
				},
				/content-addressed|input hash/i,
			],
			[
				"non-contiguous-inputs",
				(db: Database) =>
					db.run(
						"UPDATE job_inputs SET ordinal = 99 WHERE job_id = ? AND ordinal = (SELECT MAX(ordinal) FROM job_inputs WHERE job_id = ?)",
						[addedJobId, addedJobId],
					),
				/non-contiguous.*job_inputs/i,
			],
		] as const) {
			const mutatedPath = path.join(caseDir, `projection-payload-${name}.sqlite`);
			await fs.copyFile(validPath, mutatedPath);
			const db = new Database(mutatedPath, { strict: true });
			try {
				mutate(db);
			} finally {
				db.close(false);
			}
			const mutated = replaySqliteReport(mutatedPath, scope.projectId, scope.sessionId, scope.branchId);
			expect(() =>
				assertProjectionPreparation(
					before,
					mutated,
					scope.projectId,
					retryKey,
					branchPolicy,
					`mutated projection payload ${name}`,
				),
			).toThrow(pattern);
		}
	});

	it("scopes source and relevant-job evidence by exact project, session, and branch", async () => {
		const storePath = path.join(caseDir, "cross-project.sqlite");
		const context = await openLcmContext({
			dbPath: storePath,
			leafChunk: { maxSources: 1, maxTokens: 10_000 },
			condenseFanIn: 2,
		});
		try {
			for (const [projectId, ordinal] of [
				["project-a", 1],
				["project-b", 2],
			] as const) {
				const scope = { projectId, sessionId: "shared-session", branchId: "shared-branch" };
				const entryId = `${projectId}-source`;
				const redactedText = `${projectId} source long enough to summarize`;
				context.reconcile({
					scope,
					entries: [
						{
							...scope,
							entryId,
							parentId: null,
							timestamp: ordinal,
							kind: "message",
							redactedText,
							contentHash: sha256(redactedText),
							artifactRefs: [],
						},
					],
				});
				const policy = context.configureSummaryRetryPolicy(projectId, `${projectId}/model`);
				if (policy.kind !== "ready") throw new Error(`${projectId} retry policy was not ready`);
				const [job] = context.claimSummaryJobs({
					...policy,
					workerId: `${projectId}-worker`,
					leaseMs: 60_000,
					limit: 1,
					maxOutputTokens: 64,
					maxTransportRetries: 5,
					preferredScope: scope,
					allowFallback: false,
				});
				if (!job) throw new Error(`${projectId} summary job was not claimable`);
				const attempt = {
					attemptId: `${projectId}-attempt`,
					startedAt: ordinal,
					provider: "fixture",
					model: "fixture-model",
				};
				if (
					!context.beginSummaryAttempt(job, attempt, { promptHash: `${projectId}-prompt`, strategy: job.strategy })
				) {
					throw new Error(`${projectId} summary attempt did not start`);
				}
				context.settleSummaryAttempt(job, attempt, "aborted");
				context.releaseSummaryJob(job);
			}
		} finally {
			context.close();
		}

		for (const projectId of ["project-a", "project-b"] as const) {
			const report = replaySqliteReport(storePath, projectId, "shared-session", "shared-branch");
			expect(report.sourceRows.map(row => row.entryId)).toEqual([`${projectId}-source`]);
			expect(report.relevantJobRows).toHaveLength(1);
			expect(report.relevantJobRows.every(row => row.project_id === projectId)).toBe(true);
			expect(report.relevantAttemptRows).toHaveLength(1);
			expect(report.relevantAttemptRows.every(row => row.projectId === projectId)).toBe(true);
		}
	});
});

describe("LCM replay treatment boundary", () => {
	it("accepts only hard-wait removal and a proven schema 9 to 10 migration", async () => {
		const fixture = await createPhysicalReplayFixture();
		const seed = await capturePhysical("migration-seed", fixture, [], "capture", fixture.latestMarkerId);
		expect(seed.result.exitCode, seed.result.stderr).toBe(0);
		const seedReport = (await Bun.file(seed.reportPath).json()) as CapturedReplayReport;
		const storePath = path.join(caseDir, "migration-input.sqlite");
		await fs.copyFile(seed.snapshotPath, storePath);
		const input = new Database(storePath, { strict: true });
		try {
			for (const trigger of [
				"summary_jobs_authorized_insert",
				"summary_jobs_authorized_update",
				"summary_jobs_authorization_cleanup",
			]) {
				input.run(`DROP TRIGGER "${trigger}"`);
			}
			input.run("DROP TABLE summary_retry_policies");
			for (const column of ["lease_mutation_nonce", "lease_policy_token", "retry_epoch"]) {
				input.run(`ALTER TABLE summary_jobs DROP COLUMN "${column}"`);
			}
			input.run("PRAGMA user_version = 9");
			const branch = input
				.query<{ id: number; projectId: string; revision: number }, [string, string]>(
					"SELECT id, project_id AS projectId, revision FROM branches WHERE session_id = ? AND branch_id = ?",
				)
				.get(seedReport.fixture.sessionId, seedReport.fixture.branchId);
			if (!branch) throw new Error("migration fixture branch missing");
			input.run(
				`INSERT INTO summary_jobs(
					job_id, project_id, input_hash, level, origin_branch_row_id, origin_revision, status,
					worker_id, lease_token, lease_expires_at, attempt_count, available_at, created_at, updated_at,
					lease_input_tokens, lease_output_budget
				) VALUES (?, ?, ?, 0, ?, ?, 'leased', 'legacy-worker', 'legacy-lease', 9999999999999, 1, 0, 0, 0, 100, 50)`,
				["legacy-job", branch.projectId, "legacy-input", branch.id, branch.revision],
			);
		} finally {
			input.close(false);
		}
		const legacyStore = replaySqliteReport(
			storePath,
			seedReport.fixture.projectId,
			seedReport.fixture.sessionId,
			seedReport.fixture.branchId,
		);
		const baselineTemplatePath = path.join(caseDir, "migration-baseline-template.sqlite");
		await Bun.write(baselineTemplatePath, legacyStore.snapshotBytes);
		const canonicalLegacy = replaySqliteReport(
			baselineTemplatePath,
			seedReport.fixture.projectId,
			seedReport.fixture.sessionId,
			seedReport.fixture.branchId,
		);
		const capture = await capturePhysical(
			"migration-candidate",
			fixture,
			["--source-in", seed.sourcePath, "--store", baselineTemplatePath],
			"capture",
			fixture.latestMarkerId,
		);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);
		const candidate = (await Bun.file(capture.reportPath).json()) as ReplayReport & CapturedReplayReport;
		const relabeledCurrentBaseline = structuredClone(candidate);
		relabeledCurrentBaseline.treatment.hardProjectionWaitMs = 30_000;
		const relabeledCurrent = await compareReports(relabeledCurrentBaseline, candidate);
		expect(relabeledCurrent.exitCode).not.toBe(0);
		expect(relabeledCurrent.stderr).toMatch(/manifest-declared historical baseline/i);

		const baseline = structuredClone(candidate);
		const legacyEvidence = preparedTemplateEvidence(canonicalLegacy);
		Object.assign(baseline.treatment, { hardProjectionWaitMs: 30_000, schemaVersion: 9 });
		baseline.fixture.harnessSourceHash = "da43b5239ee196a0f188aa3f0db7957aaf41ab42166153ba211a12526359e3b5";
		delete baseline.fixture.harnessIdentityHash;
		baseline.artifacts.treatmentTemplate = baselineTemplatePath;
		baseline.artifacts.migratedTemplate = baselineTemplatePath;
		baseline.storeEvidence.migratedTemplate = structuredClone(legacyEvidence);
		baseline.storeEvidence.preparedTemplate = legacyEvidence;
		for (const sample of baseline.samples) {
			sample.schemaVersion = 9;
			sample.serializedStoreHash = legacyEvidence.logicalHash;
			sample.postStoreHash = legacyEvidence.logicalHash;
			sample.postSnapshotHash = legacyEvidence.byteHash;
		}
		baseline.workloadFingerprint = replayWorkloadFingerprint(baseline);
		const migratedTemplatePath = candidate.artifacts.migratedTemplate;
		if (!migratedTemplatePath) throw new Error("migration candidate artifact missing");
		const migratedStore = replaySqliteReport(
			migratedTemplatePath,
			baseline.fixture.projectId,
			baseline.fixture.sessionId,
			baseline.fixture.branchId,
		);
		const canonicalPrepared = replaySqliteReport(
			candidate.artifacts.treatmentTemplate,
			baseline.fixture.projectId,
			baseline.fixture.sessionId,
			baseline.fixture.branchId,
		);

		const wrongHistoricalWait = structuredClone(baseline);
		wrongHistoricalWait.treatment.hardProjectionWaitMs = 29_999;
		wrongHistoricalWait.workloadFingerprint = replayWorkloadFingerprint(wrongHistoricalWait);
		const wrongWait = await compareReports(wrongHistoricalWait, candidate);
		expect(wrongWait.exitCode).not.toBe(0);
		expect(wrongWait.stderr).toMatch(/30,?000/i);

		const allowed = await compareReports(baseline, candidate);
		expect(allowed.exitCode, allowed.stderr).toBe(0);
		const writeWeakenedTrigger = async (
			snapshot: Uint8Array,
			outputPath: string,
			triggerName: string,
			from: string,
			to: string,
		) => {
			await Bun.write(outputPath, snapshot);
			const db = new Database(outputPath, { strict: true });
			try {
				const triggerSql = db
					.query<{ sql: string }, [string]>("SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?")
					.get(triggerName)?.sql;
				if (!triggerSql?.includes(from)) throw new Error(`missing ${triggerName} negative-control predicate`);
				db.run(`DROP TRIGGER "${triggerName}"`);
				db.run(triggerSql.replace(from, to));
			} finally {
				db.close(false);
			}
			const report = replaySqliteReport(
				outputPath,
				baseline.fixture.projectId,
				baseline.fixture.sessionId,
				baseline.fixture.branchId,
			);
			await Promise.all([
				fs.rm(outputPath, { force: true }),
				fs.rm(`${outputPath}-wal`, { force: true }),
				fs.rm(`${outputPath}-shm`, { force: true }),
			]);
			await Bun.write(outputPath, report.snapshotBytes);
			return replaySqliteReport(
				outputPath,
				baseline.fixture.projectId,
				baseline.fixture.sessionId,
				baseline.fixture.branchId,
			);
		};
		for (const [triggerName, from, to, errorPattern] of [
			[
				"summary_jobs_authorized_insert",
				"AND NEW.lease_policy_token = p.claim_token",
				"AND NEW.lease_policy_token IS NOT NULL",
				/direct leased insert/i,
			],
			["summary_jobs_authorized_update", "NEW.worker_id IS NOT OLD.worker_id OR ", "", /worker mutation/i],
			[
				"summary_jobs_authorization_cleanup",
				"NEW.lease_policy_token IS NOT NULL OR NEW.lease_mutation_nonce IS NOT NULL",
				"NEW.lease_policy_token IS NOT NULL AND NEW.lease_mutation_nonce IS NOT NULL",
				/cleanup with a lingering/i,
			],
		] as const) {
			const weakenedMigratedPath = path.join(caseDir, `weakened-migrated-${triggerName}.sqlite`);
			const weakenedPreparedPath = path.join(caseDir, `weakened-prepared-${triggerName}.sqlite`);
			const weakenedMigrated = await writeWeakenedTrigger(
				migratedStore.snapshotBytes,
				weakenedMigratedPath,
				triggerName,
				from,
				to,
			);
			const weakenedPrepared = await writeWeakenedTrigger(
				canonicalPrepared.snapshotBytes,
				weakenedPreparedPath,
				triggerName,
				from,
				to,
			);
			const weakened = structuredClone(candidate);
			weakened.artifacts.migratedTemplate = weakenedMigratedPath;
			weakened.artifacts.treatmentTemplate = weakenedPreparedPath;
			weakened.storeEvidence.migratedTemplate = preparedTemplateEvidence(weakenedMigrated);
			weakened.storeEvidence.preparedTemplate = preparedTemplateEvidence(weakenedPrepared);
			for (const sample of weakened.samples) {
				sample.serializedStoreHash = weakenedPrepared.serializedStoreHash;
				sample.postStoreHash = weakenedPrepared.serializedStoreHash;
			}
			const result = await compareReports(baseline, weakened);
			expect(result.exitCode).not.toBe(0);
			expect(result.stderr).toMatch(errorPattern);
		}

		const rejected = structuredClone(candidate);
		rejected.treatment.retryKey = "different/model";
		const rejection = await compareReports(baseline, rejected);
		expect(rejection.exitCode).not.toBe(0);
		expect(rejection.stderr).toMatch(/retry|treatment|migration/i);
	}, 30_000);
	it("prepares the captured project without replacing existing retry policies", async () => {
		const fixture = await createPhysicalReplayFixture();
		const seed = await capturePhysical("project-policy-seed", fixture, [], "capture", fixture.latestMarkerId);
		expect(seed.result.exitCode, seed.result.stderr).toBe(0);

		const source = (await Bun.file(seed.sourcePath).json()) as SourceSnapshot;
		const capturedProjectId = source.scope.projectId;
		const input = new Database(seed.snapshotPath, { strict: true });
		try {
			input.run(
				"INSERT INTO summary_retry_policies(project_id, retry_key, epoch, claim_token, updated_at) VALUES (?, NULL, 0, NULL, 0)",
				["existing-project"],
			);
		} finally {
			input.close(false);
		}

		const capture = await capturePhysical(
			"project-policy-candidate",
			fixture,
			["--source-in", seed.sourcePath, "--store", seed.snapshotPath],
			"capture",
			fixture.latestMarkerId,
		);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);
		const report = (await Bun.file(capture.reportPath).json()) as CapturedReplayReport;
		expect(report.storeEvidence.preparedTemplate.policyRows).toContainEqual(
			expect.objectContaining({ project_id: "existing-project", retry_key: null, epoch: 0 }),
		);
		expect(report.storeEvidence.preparedTemplate.policyRows).toContainEqual(
			expect.objectContaining({
				project_id: capturedProjectId,
				retry_key: "lcm-replay/lcm-replay-summary",
				epoch: 1,
			}),
		);
	}, 30_000);
});

describe("LCM historical reconstruction eligibility", () => {
	it("rejects an exact-history claim when the pre-change contract was not reproduced", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.fixture.reconstruction.preChangeContractReproduced = false;
		candidate.fixture.reconstruction.originalRedactionIdentityAvailable = false;

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/historical-reconstruction-impossible|workload|identity/i);
	});

	it("rejects metadata-only synthetic pairing without executed content-free samples", async () => {
		const baseline = fiveSampleReport();
		Object.assign(baseline.fixture, {
			reconstruction: {
				classification: "historical-reconstruction-impossible",
				preChangeContractReproduced: false,
				originalRedactionIdentityAvailable: false,
				missingBlobRefs: ["blob:sha256:unavailable"],
				syntheticFixture: { kind: "content-free-shape", sourceCount: 1 },
			},
		});
		const candidate = structuredClone(baseline);

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/synthetic|content-free|sample|pair/i);
	});

	it("rejects a content-free gate that never exercises Lossless takeover", async () => {
		const baseline = fiveSampleReport();
		Object.assign(baseline.fixture, {
			reconstruction: {
				classification: "historical-reconstruction-impossible",
				preChangeContractReproduced: false,
				originalRedactionIdentityAvailable: false,
				missingBlobRefs: ["blob:sha256:unavailable"],
				syntheticFixture: { kind: "content-free-shape", sourceCount: 96 },
			},
		});
		const samples = structuredClone(baseline.samples);
		for (const sample of samples) {
			sample.fitProof.owned = false;
			Object.assign((sample as typeof sample & { counters: Record<string, number> }).counters, {
				projectionCalls: 3,
				projectionReads: 3,
				schedulerBranchPasses: 1,
			});
			Object.assign(sample, {
				status: {
					health: "healthy",
					coverageReadiness: "ready",
					route: "native_passthrough",
					failureCategory: null,
					failureReason: null,
				},
				tokens: { request: 1_000, nonMessage: 10, candidate: 100, budget: 100 },
			});
		}
		const syntheticBoundary = { thresholdTokens: 5_000, freshTail: { maxSources: 32, maxTokens: 16_000 } };
		Object.assign(baseline, {
			syntheticPair: {
				kind: "content-free-shape",
				fixtureHash: "a".repeat(64),
				sourceCount: 96,
				boundary: syntheticBoundary,
				scope: {
					projectId: baseline.fixture.projectId,
					sessionId: baseline.fixture.sessionId,
					branchId: baseline.fixture.branchId,
				},
				templatePath: path.join(artifactRoot, "fixture-synthetic-template.sqlite"),
				templateEvidence: structuredClone(baseline.storeEvidence.preparedTemplate),
				samples,
			},
		});
		const candidate = structuredClone(baseline);
		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/synthetic|lossless|takeover|owned/i);
	});
});

describe("LCM replay frozen reconstruction qualification", () => {
	it("does not recompute frozen eligibility from the candidate route", async () => {
		const baseline = fiveSampleReport();
		Object.assign(baseline.fixture.reconstruction, {
			baselineEligibility: {
				classification: "exact-historical-replay",
				preChangeContractReproduced: true,
			},
		});
		const candidate = structuredClone(baseline);

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/artifact/i);
		expect(result.stderr).not.toMatch(/eligibility|candidate route|workload.*drift/i);
	});

	it("uses frozen exact-history authority when the candidate self-classifies reconstruction as impossible", async () => {
		const baseline = fiveSampleReport();
		Object.assign(baseline.fixture.reconstruction, {
			baselineEligibility: {
				classification: "exact-historical-replay",
				preChangeContractReproduced: true,
			},
		});
		const candidate = structuredClone(baseline) as typeof baseline & {
			syntheticPair?: ReplayReport["syntheticPair"];
		};
		Object.assign(candidate.fixture.reconstruction, {
			classification: "historical-reconstruction-impossible",
			preChangeContractReproduced: false,
			syntheticFixture: { kind: "content-free-shape", sourceCount: 1 },
		});
		candidate.candidateOutcome.reproducedOldFallback = false;
		candidate.syntheticPair = {
			kind: "content-free-shape",
			fixtureHash: "f".repeat(64),
			sourceCount: 1,
			boundary: { thresholdTokens: 5_000, freshTail: { maxSources: 1, maxTokens: 1 } },
			scope: { projectId: "synthetic-project", sessionId: "synthetic-session", branchId: "synthetic-branch" },
			templatePath: path.join(artifactRoot, "candidate-self-classified-synthetic.sqlite"),
			templateEvidence: {
				...structuredClone(candidate.storeEvidence.preparedTemplate),
				summaryRows: [],
				summaryLineageRows: [],
			},
			samples: structuredClone(candidate.samples) as unknown as ReplayReport["samples"],
		};

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/artifact/i);
		expect(result.stderr).not.toMatch(/synthetic.*(?:binding|scope)|summary.*(?:binding|store row|scope)/i);
	});

	for (const [name, change] of [
		["project", { projectId: "wrong-project" }],
		["session", { sessionId: "wrong-session" }],
		["branch", { branchId: "wrong-branch" }],
		["ordered sourceKey", { orderedSourceKeys: ["wrong-source-key"] }],
	] as const) {
		it(`rejects exact history when ${name} qualification disagrees with captured rows`, async () => {
			const baseline = fiveSampleReport();
			const qualification = {
				projectId: baseline.fixture.projectId,
				sessionId: baseline.fixture.sessionId,
				branchId: baseline.fixture.branchId,
				orderedSourceKeys: baseline.samples[0]!.sourceRows.map(row => row.sourceKey),
				runtimeEnvelopeAuthoritative: true,
				...change,
			};
			Object.assign(baseline.fixture.reconstruction, { qualification });
			const candidate = structuredClone(baseline);

			const result = await compareReports(baseline, candidate);

			expect(result.exitCode).not.toBe(0);
			expect(result.stderr).toMatch(/project|session|branch|sourceKey|scope|qualification/i);
		});
	}

	it("rejects exact history without an authoritative captured runtime envelope", async () => {
		const baseline = fiveSampleReport();
		Object.assign(baseline.fixture.reconstruction, {
			qualification: {
				projectId: baseline.fixture.projectId,
				sessionId: baseline.fixture.sessionId,
				branchId: baseline.fixture.branchId,
				orderedSourceKeys: baseline.samples[0]!.sourceRows.map(row => row.sourceKey),
				runtimeEnvelopeAuthoritative: false,
				capturedStoreAuthoritative: true,
				readyStore: true,
			},
		});
		const candidate = structuredClone(baseline);

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/runtime|envelope|authoritative|qualification/i);
	});

	it("rejects stale failure diagnostics when an exact physical baseline did not dispatch native fallback", async () => {
		const baseline = fiveSampleReport();
		Object.assign(baseline.fixture, {
			harnessSourceHash: "da43b5239ee196a0f188aa3f0db7957aaf41ab42166153ba211a12526359e3b5",
		});
		for (const sample of baseline.samples) {
			sample.fitProof.owned = false;
			sample.status = {
				route: "native_passthrough",
				failureCategory: "unfit",
				failureReason: "assembly_invalid",
			};
			Object.assign(sample, { fallbackCategory: "unfit" satisfies ReplayFallbackCategory });
		}
		const candidate = structuredClone(baseline);

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/exact physical baseline|native_fallback|route/i);
	});
});

describe("LCM replay runtime envelope", () => {
	it("uses reproducible tool and skill bytes and disqualifies an unrenderable envelope", async () => {
		const fixture = await createPhysicalReplayFixture();
		const seed = await capturePhysical("runtime-envelope-seed", fixture, [], "capture", fixture.latestMarkerId);
		expect(seed.result.exitCode, seed.result.stderr).toBe(0);
		const source = (await Bun.file(seed.sourcePath).json()) as SourceSnapshot;
		const store = replaySqliteReport(
			seed.snapshotPath,
			source.scope.projectId,
			source.scope.sessionId,
			source.scope.branchId,
		);
		const toolSchemas: Array<Pick<AgentTool, "name" | "description" | "parameters">> = [
			{
				name: "read",
				description: "Read a captured file",
				parameters: {
					type: "object",
					properties: { path: { type: "string" } },
					required: ["path"],
				} as AgentTool["parameters"],
			},
		];
		const skills: Skill[] = [
			{
				name: "captured-skill",
				description: "Captured skill description",
				filePath: path.join(caseDir, "captured-skill", "SKILL.md"),
				baseDir: path.join(caseDir, "captured-skill"),
				source: "fixture",
			},
		];
		const systemPrompt = ["system prompt with captured skill bytes"];
		const promptOnlyTokens = computeNonMessageTokens({ systemPrompt, agent: { state: { tools: [] } } });
		const nonMessageTokens = computeNonMessageTokens({
			systemPrompt,
			agent: { state: { tools: toolSchemas } },
			skills,
		});
		expect(nonMessageTokens).toBeGreaterThan(promptOnlyTokens);
		const envelope = {
			...source.scope,
			orderedSourceKeys: store.sourceRows.map(row => row.sourceKey),
			contextWindow: 400_000,
			summaryModelSelector: "@smol",
			systemPrompt,
			toolSchemas,
			skills,
			nonMessageTokens,
			settings: {
				thresholdTokens: 1_000_000,
				freshTailMaxSources: 32,
				freshTailMaxTokens: 16_000,
				maxConcurrentSummaries: 4,
			},
		};
		const captureWithEnvelope = async (name: string, value: object) => {
			const envelopePath = path.join(caseDir, `${name}.runtime-envelope.json`);
			await Bun.write(envelopePath, `${JSON.stringify(value)}\n`);
			const capture = await capturePhysical(
				name,
				fixture,
				["--source-in", seed.sourcePath, "--store", seed.snapshotPath, "--runtime-envelope-in", envelopePath],
				"capture",
				fixture.latestMarkerId,
			);
			expect(capture.result.exitCode, capture.result.stderr).toBe(0);
			return (await Bun.file(capture.reportPath).json()) as ReplayReport;
		};

		const reproduced = await captureWithEnvelope("runtime-envelope-reproduced", envelope);
		expect(reproduced.fixture.reconstruction.qualification?.runtimeEnvelopeAuthoritative).toBe(true);
		expect(reproduced.fixture.nonMessageTokens).toBe(nonMessageTokens);
		expect(reproduced.samples.every(sample => !sample.tokens || sample.tokens.nonMessage === nonMessageTokens)).toBe(
			true,
		);

		const unrenderable = structuredClone(envelope) as typeof envelope & {
			toolSchemas: Array<Record<string, unknown>>;
		};
		unrenderable.toolSchemas[0]!.wireOnly = true;
		const rejected = await captureWithEnvelope("runtime-envelope-unrenderable", unrenderable);
		expect(rejected.fixture.reconstruction.qualification?.runtimeEnvelopeAuthoritative).toBe(false);
	}, 60_000);
});

describe("LCM replay selected marker and maintenance pressure", () => {
	it("selects the requested physical marker and records the ownership pressure floor", async () => {
		const fixture = await createPhysicalReplayFixture();
		const capture = await capturePhysical("selected-marker", fixture, ["--request-tokens-floor", "374856"]);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);
		const report = (await Bun.file(capture.reportPath).json()) as {
			fixture: {
				markerId: string;
				parentId: string;
				requestTokensFloor?: number;
				reconstruction: { classification: string };
			};
			samples: Array<{
				maintenance?: {
					localRequestTokens: number;
					requestTokensFloor: number;
					authoritative: boolean;
					decision: { kind: string };
				};
			}>;
		};

		expect(report.fixture.markerId).toBe(fixture.selectedMarkerId);
		expect(report.fixture.parentId).toBe("source-2");
		expect(report.fixture.requestTokensFloor).toBe(374_856);
		for (const sample of report.samples) {
			expect(sample.maintenance?.requestTokensFloor).toBe(374_856);
			expect(sample.maintenance?.localRequestTokens).toBeLessThan(374_856);
			expect(sample.maintenance?.decision.kind).toMatch(/owned|native|aborted/);
			expect(sample.maintenance?.authoritative).toBe(true);
		}
		expect(report.fixture.reconstruction.classification).toBe("historical-reconstruction-impossible");
	}, 30_000);
});

describe("LCM replay physical blob identity", () => {
	it("restores raw image base64 and provider image URL bytes while binding the resolved prefix hash", async () => {
		const fixture = await createPhysicalReplayFixture();
		const capture = await capturePhysical("resolved-blob", fixture);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);
		const report = (await Bun.file(capture.reportPath).json()) as {
			artifacts: { rawPrefix: string; resolvedPrefix: string };
			fixture: {
				prefixIdentity?: { rawHash: string; resolvedHash: string };
				blobEvidence?: { verifiedRefs: string[]; digestMismatchRefs: string[]; readFailureRefs: string[] };
			};
		};
		const [rawPrefix, resolvedPrefix] = await Promise.all([
			Bun.file(report.artifacts.rawPrefix).text(),
			Bun.file(report.artifacts.resolvedPrefix).text(),
		]);
		const resolvedEntries = resolvedPrefix
			.trimEnd()
			.split("\n")
			.map(line => JSON.parse(line) as Record<string, unknown>);
		const source = resolvedEntries.find(entry => entry.id === "source-1") as {
			message: {
				content: Array<{ type: string; data?: string }>;
				providerPayload: { items: Array<{ content: Array<{ image_url?: string }> }> };
			};
		};

		expect(rawPrefix).toContain(fixture.blobRef);
		expect(rawPrefix).toContain(fixture.providerBlobRef);
		expect(resolvedPrefix).not.toContain(fixture.blobRef);
		expect(resolvedPrefix).not.toContain(fixture.providerBlobRef);
		expect(source.message.content.find(item => item.type === "image")?.data).toBe(fixture.imageBase64);
		expect(source.message.providerPayload.items[0]!.content[1]!.image_url).toBe(fixture.providerImageUrl);
		expect(report.fixture.prefixIdentity).toEqual({
			rawHash: sha256(rawPrefix),
			resolvedHash: sha256(resolvedPrefix),
		});
		expect(report.fixture.blobEvidence).toEqual({
			verifiedRefs: [fixture.blobRef, fixture.providerBlobRef].sort(),
			digestMismatchRefs: [],
			readFailureRefs: [],
		});
	}, 30_000);

	it("marks a present blob with the wrong digest ineligible", async () => {
		const fixture = await createPhysicalReplayFixture();
		await Bun.write(fixture.blobPath, "corrupt blob bytes");
		const capture = await capturePhysical("corrupt-blob", fixture);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);
		const report = (await Bun.file(capture.reportPath).json()) as {
			fixture: {
				reconstruction: { classification: string };
				blobEvidence?: { digestMismatchRefs: string[] };
			};
		};

		expect(report.fixture.reconstruction.classification).toBe("historical-reconstruction-impossible");
		expect(report.fixture.blobEvidence?.digestMismatchRefs).toEqual([fixture.blobRef]);
	}, 30_000);
});

describe("LCM replay synthetic negative controls", () => {
	it("records discriminated behavioral evidence for all seven fixtures", async () => {
		const fixtures = [
			{ fixture: "oversized-tool-output", thresholdTokens: 5_000 },
			{ fixture: "single-child-frontier", thresholdTokens: 5_000 },
			{ fixture: "minimal-marker-budget", thresholdTokens: 64 },
			{ fixture: "provider-backoff", thresholdTokens: 5_000 },
			{ fixture: "model-change", thresholdTokens: 5_000 },
			{ fixture: "stale-lease", thresholdTokens: 5_000 },
			{ fixture: "cancellation", thresholdTokens: 5_000 },
		] as const;
		const reports: ReplayControlReport[] = [];
		for (const { fixture, thresholdTokens } of fixtures) {
			const reportPath = path.join(caseDir, `${fixture}.json`);
			const result = await runHarness([
				"capture",
				"--fixture",
				fixture,
				"--samples",
				"1",
				"--threshold-tokens",
				String(thresholdTokens),
				"--out",
				reportPath,
			]);
			expect(result.exitCode, result.stderr).toBe(0);
			reports.push((await Bun.file(reportPath).json()) as ReplayControlReport);
		}

		expect(reports.map(report => [report.fixture.name, report.fixture.settings.thresholdTokens])).toEqual(
			fixtures.map(({ fixture, thresholdTokens }) => [fixture, thresholdTokens]),
		);
		const samples = reports.map(report => {
			expect(report.samples).toHaveLength(1);
			return report.samples[0]!;
		});
		expect(samples.map(sample => sample.controlEvidence?.kind)).toEqual(fixtures.map(({ fixture }) => fixture));

		for (const sample of samples) {
			const evidence = sample.controlEvidence;
			if (!evidence) throw new Error("synthetic control evidence missing");
			switch (evidence.kind) {
				case "oversized-tool-output":
					expect(evidence).toEqual({
						kind: "oversized-tool-output",
						inputBytes: 264_000,
						projected: true,
						sourceCoverageComplete: true,
					});
					expect(sample.fitProof.owned).toBe(true);
					expect(sample.sourceCoverage.complete).toBe(true);
					break;
				case "single-child-frontier":
					expect(evidence).toEqual({
						kind: "single-child-frontier",
						childJobs: 1,
						parentJobs: 1,
						unresolvedJobs: 0,
					});
					break;
				case "minimal-marker-budget":
					expect(["minimum_representation", "irreducible_input"]).toContain(evidence.fallbackReason);
					expect(evidence.candidateTokens).toBeGreaterThan(evidence.budget);
					expect(sample.status?.route).toBe("native_fallback");
					break;
				case "provider-backoff":
					expect(evidence).toEqual({
						kind: "provider-backoff",
						providerAttempts: 2,
						retries: 1,
						recovered: true,
					});
					break;
				case "model-change":
					expect(evidence).toEqual({
						kind: "model-change",
						oldPolicyCompletionAccepted: false,
						newPolicyEpoch: 2,
					});
					break;
				case "stale-lease":
					expect(evidence).toEqual({
						kind: "stale-lease",
						staleCompletionAccepted: false,
						replacementLeaseTokenChanged: true,
					});
					break;
				case "cancellation":
					expect(evidence).toMatchObject({
						kind: "cancellation",
						started: true,
						aborted: true,
						providerWrites: 0,
						cleanupComplete: true,
						activeProviders: 0,
						inFlightAttempts: 0,
						missingUsage: 0,
						staleSummaries: 0,
					});
					expect(evidence.claimedJobs).toBeGreaterThan(0);
					expect(evidence.providerAttempts).toBeGreaterThan(0);
					expect(evidence.billedAttempts).toBeGreaterThan(0);
					expect(evidence.attemptOutcomes.length).toBe(evidence.billedAttempts);
					expect(
						evidence.attemptOutcomes.every(outcome => ["aborted", "lease_lost", "stale"].includes(outcome)),
					).toBe(true);
					expect(evidence.projectionReturnedBeforeProviderRelease).toBe(true);
					expect(evidence.abortLatencyMs).toBeLessThan(2_000);
					expect(evidence.cleanupLatencyMs).toBeLessThan(2_000);
					break;
			}
		}
	}, 60_000);
});

describe("LCM replay cancellation ledger integrity", () => {
	it("rejects a candidate with no cancellation control lane", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		Reflect.deleteProperty(candidate, "cancellationControl");

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/cancellation control/i);
	});

	for (const [name, mutate] of [
		[
			"forged in-flight cleanup",
			(sample: Record<string, unknown>) => {
				const attempts = sample.attempts as {
					rows: Array<{ outcome: string; completedAt: number | null }>;
					inFlight: number;
				};
				const evidence = sample.controlEvidence as Extract<ReplayControlEvidence, { kind: "cancellation" }>;
				attempts.rows[0]!.outcome = "in_flight";
				attempts.rows[0]!.completedAt = null;
				attempts.inFlight = 1;
				evidence.inFlightAttempts = 1;
				evidence.attemptOutcomes = ["in_flight"];
			},
		],
		[
			"forged missing billed usage",
			(sample: Record<string, unknown>) => {
				const attempts = sample.attempts as {
					rows: Array<{ usage: object | null }>;
					billed: number;
					missingUsage: number;
				};
				const evidence = sample.controlEvidence as Extract<ReplayControlEvidence, { kind: "cancellation" }>;
				attempts.rows[0]!.usage = null;
				attempts.billed = 0;
				attempts.missingUsage = 1;
				evidence.billedAttempts = 0;
				evidence.missingUsage = 1;
			},
		],
		[
			"forged waits for provider",
			(sample: Record<string, unknown>) => {
				const evidence = sample.controlEvidence as Extract<ReplayControlEvidence, { kind: "cancellation" }>;
				evidence.projectionReturnedBeforeProviderRelease = false;
			},
		],
	] as const) {
		it(`rejects ${name}`, async () => {
			const baseline = fiveSampleReport();
			const candidate = structuredClone(baseline);
			mutate(candidate.cancellationControl.sample as unknown as Record<string, unknown>);

			const result = await compareReports(baseline, candidate);

			expect(result.exitCode).not.toBe(0);
			expect(result.stderr).toMatch(/cancellation|in-flight|missing-usage|attempt|provider|cleanup/i);
		});
	}
});

describe("LCM replay executed synthetic pairing", () => {
	it("runs five deterministic store-backed adaptive-fit samples for an ineligible physical history", async () => {
		const fixture = await createPhysicalReplayFixture(38);
		await fs.rm(fixture.blobPath);
		await fs.chmod(caseDir, 0o750);
		const capture = await capturePhysical("synthetic-pair", fixture);
		expect(capture.result.exitCode, capture.result.stderr).toBe(0);
		const report = (await Bun.file(capture.reportPath).json()) as {
			fixture: {
				harnessSourceHash: string;
				harnessIdentityHash: string;
				reconstruction: { classification: string };
			};
			artifacts: {
				root: string;
				harnessSource: string;
				rawPrefix: string;
				resolvedPrefix: string;
				treatmentTemplate: string;
				migratedTemplate: string;
			};
			syntheticPair?: {
				kind: string;
				fixtureHash: string;
				sourceCount: number;
				boundary: object;
				scope: SourceSnapshot["scope"];
				templatePath: string;
				templateEvidence: PreparedTemplateEvidence;
				samples: Array<{
					sample: number;
					fitProof: { owned: boolean; complete: boolean };
					fallbackCategory: string | null;
					status?: { route: string; failureReason: string | null };
					sourceCoverage: { complete: boolean };
					promptInputTokens: number;
					tokens?: { candidate: number; budget: number };
					storeRowsChanged: number;
					serializedStoreHash: string;
					postStoreHash?: string;
					counters?: { rowsChanged?: number; reconcileRowsChanged?: number };
				}>;
			};
		};

		expect(report.fixture.reconstruction.classification).toBe("historical-reconstruction-impossible");
		expect(report.syntheticPair?.kind).toBe("content-free-shape");
		expect(report.syntheticPair?.fixtureHash).toMatch(/^[a-f0-9]{64}$/);
		if (!report.syntheticPair) throw new Error("synthetic pair missing");
		const templateBytes = new Uint8Array(await Bun.file(report.syntheticPair.templatePath).arrayBuffer());
		expect(sha256(templateBytes)).toBe(report.syntheticPair.templateEvidence.byteHash);
		const template = replaySqliteReport(
			report.syntheticPair.templatePath,
			report.syntheticPair.scope.projectId,
			report.syntheticPair.scope.sessionId,
			report.syntheticPair.scope.branchId,
		);
		expect(preparedTemplateEvidence(template)).toEqual(report.syntheticPair.templateEvidence);
		expect(await permissionBits(caseDir)).toBe(0o750);
		expect(await permissionBits(report.artifacts.root)).toBe(0o700);
		expect(await permissionBits(path.dirname(report.syntheticPair.templatePath))).toBe(0o700);
		const capturedHarnessSource = await Bun.file(report.artifacts.harnessSource).text();
		expect(sha256(capturedHarnessSource)).toBe(report.fixture.harnessSourceHash);
		expect(report.fixture.harnessIdentityHash).toBe(replayHarnessIdentityHash(report.fixture.harnessSourceHash));
		const privateFiles = [
			capture.reportPath,
			capture.snapshotPath,
			capture.sourcePath,
			report.artifacts.harnessSource,
			report.artifacts.rawPrefix,
			report.artifacts.resolvedPrefix,
			report.artifacts.treatmentTemplate,
			report.artifacts.migratedTemplate,
			report.syntheticPair.templatePath,
		];
		expect(await Promise.all(privateFiles.map(permissionBits))).toEqual(privateFiles.map(() => 0o600));
		expect(report.syntheticPair?.samples.map(sample => sample.sample)).toEqual([1, 2, 3, 4, 5]);
		expect(report.syntheticPair?.samples.every(sample => sample.sourceCoverage.complete)).toBe(true);
		expect(
			report.syntheticPair?.samples.map(sample => ({
				owned: sample.fitProof.owned,
				complete: sample.fitProof.complete,
				fallback: sample.fallbackCategory,
				route: sample.status?.route,
				reason: sample.status?.failureReason,
			})),
		).toEqual(
			Array.from({ length: 5 }, () => ({
				owned: true,
				complete: true,
				fallback: null,
				route: "lossless",
				reason: null,
			})),
		);
		expect(
			report.syntheticPair?.samples.every(
				sample =>
					sample.tokens !== undefined &&
					sample.promptInputTokens === sample.tokens.candidate &&
					sample.tokens.candidate <= sample.tokens.budget,
			),
		).toBe(true);
		expect(
			report.syntheticPair?.samples.map(sample => ({
				rows: sample.storeRowsChanged,
				counterRows: sample.counters?.rowsChanged,
				reconcileRows: sample.counters?.reconcileRowsChanged,
				unchanged: sample.postStoreHash === sample.serializedStoreHash,
			})),
		).toEqual(Array.from({ length: 5 }, () => ({ rows: 0, counterRows: 0, reconcileRows: 0, unchanged: true })));
		const sourceCountForgery = structuredClone(report);
		if (!sourceCountForgery.syntheticPair) throw new Error("synthetic pair clone missing");
		sourceCountForgery.syntheticPair.sourceCount += 1;
		const shapeResult = await compareReports(report, sourceCountForgery);
		expect(shapeResult.exitCode).not.toBe(0);
		expect(shapeResult.stderr).toMatch(/content-free.*shape|source count|fixture hash/i);

		const evidenceForgery = structuredClone(report);
		if (!evidenceForgery.syntheticPair) throw new Error("synthetic pair clone missing");
		evidenceForgery.syntheticPair.templateEvidence.logicalHash = "0".repeat(64);
		const evidenceResult = await compareReports(report, evidenceForgery);
		expect(evidenceResult.exitCode).not.toBe(0);
		expect(evidenceResult.stderr).toMatch(/content-free.*template.*evidence|artifact contradicts/i);
		expect(JSON.stringify(report.syntheticPair)).not.toContain("opening physical replay source");
	}, 20_000);
});

describe("LCM replay derived comparison evidence", () => {
	for (const [name, mutate] of [
		["baseline latency aggregate", (report: MutableReplayEvidence) => (report.metrics.latencyMs.median = 9)],
		["candidate cost aggregate", (report: MutableReplayEvidence) => (report.metrics.providerUsage.cost = 1)],
		[
			"candidate route outcome",
			(report: MutableReplayEvidence) => (report.candidateOutcome.route = "native_fallback"),
		],
	] as const) {
		it(`rejects edited ${name}`, async () => {
			const baseline = fiveSampleReport();
			const candidate = structuredClone(baseline);
			mutate(name.startsWith("baseline") ? baseline : candidate);

			const result = await compareReports(baseline, candidate);

			expect(result.exitCode).not.toBe(0);
			expect(result.stderr).toMatch(/aggregate|outcome.*contradict/i);
		});
	}
});

describe("LCM replay prepared template evidence", () => {
	it("rejects normalized source, summary, job, policy, or epoch drift", async () => {
		const prepared: PreparedTemplateEvidence = {
			reserialized: true,
			quickCheck: "ok",
			byteHash: "prepared-bytes-a",
			logicalHash: "prepared-logical-a",
			schemaRows: [],
			sourceRows: [{ entryId: "source-1", sourceKey: "source-key-1", position: 0 }],
			summaryRows: [{ id: "summary-1", inputHash: "input-a" }],
			jobRows: [{ id: "job-1", state: "pending", retryEpoch: 1 }],
			policyRows: [{ projectId: "project-a", retryKey: "fixture-key", epoch: 1 }],
			projectIds: ["project-a"],
			retryEpochs: [1],
		};
		for (const [name, mutate] of [
			["source", (value: typeof prepared) => (value.sourceRows[0]!.sourceKey = "changed")],
			["summary", (value: typeof prepared) => (value.summaryRows[0]!.inputHash = "changed")],
			["job", (value: typeof prepared) => (value.jobRows[0]!.state = "leased")],
			["policy", (value: typeof prepared) => (value.policyRows[0]!.retryKey = "changed")],
			["epoch", (value: typeof prepared) => (value.policyRows[0]!.epoch = Number(value.policyRows[0]!.epoch) + 1)],
		] as const) {
			const baseline = fiveSampleReport();
			baseline.storeEvidence.preparedTemplate = structuredClone(prepared);
			const candidate = structuredClone(baseline);
			const candidateEvidence = candidate.storeEvidence.preparedTemplate;
			mutate(candidateEvidence);

			const result = await compareReports(baseline, candidate);

			expect(result.exitCode, `${name}: ${result.stderr}`).not.toBe(0);
			expect(result.stderr).toMatch(/prepared|template|source|summary|job|policy|epoch|logical/i);
		}
	}, 15_000);
});

describe("LCM replay pristine snapshot proof", () => {
	it("requires pristine snapshot path, byte hash, and logical hash on both reports", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/artifact|pristine|snapshot|path|byte|logical|SQLite/i);
	});
});

describe("LCM replay five-sample deterministic gates", () => {
	it("rejects reports that do not contain exactly five samples", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.samples.pop();

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/five|5|sample/i);
	});

	for (const [name, mutate] of [
		[
			"ordered coverage",
			(sample: Record<string, unknown>) => {
				const coverage = sample.sourceCoverage as Record<string, unknown>;
				coverage.projectedSourceKeys = ["wrong-source-key"];
			},
		],
		["provider attempts", (sample: Record<string, unknown>) => (sample.providerAttempts = 1)],
		["store mutation", (sample: Record<string, unknown>) => (sample.storeRowsChanged = 1)],
		[
			"counter drift",
			(sample: Record<string, unknown>) => ((sample.counters as Record<string, unknown>).rowsChanged = 1),
		],
		[
			"projection read drift",
			(sample: Record<string, unknown>) => {
				const counters = sample.counters as Record<string, unknown>;
				counters.projectionCalls = 2;
				counters.projectionReads = 2;
			},
		],
		[
			"lineage read drift",
			(sample: Record<string, unknown>) => {
				const counters = sample.counters as Record<string, unknown>;
				counters.projectionLineageRowsRead = 1;
				counters.lineageReads = 1;
			},
		],
		[
			"scheduler pass drift",
			(sample: Record<string, unknown>) => ((sample.counters as Record<string, unknown>).schedulerBranchPasses = 1),
		],
		[
			"unresolvable handle",
			(sample: Record<string, unknown>) => ((sample.handles as Record<string, unknown>).allResolved = false),
		],
		[
			"stale low route metric",
			(sample: Record<string, unknown>) => ((sample.tokens as Record<string, unknown>).routeCandidate = 99),
		],
		[
			"oversized final provider array",
			(sample: Record<string, unknown>) => {
				sample.promptInputTokens = 101;
				Object.assign(sample.tokens as Record<string, unknown>, { candidate: 101, routeCandidate: 101 });
			},
		],
		[
			"missing committed route",
			(sample: Record<string, unknown>) => ((sample.status as Record<string, unknown>).committed = false),
		],
		[
			"missing rendered handle",
			(sample: Record<string, unknown>) => ((sample.handles as Record<string, unknown>).tokens = []),
		],
		[
			"corrupt rendered handle",
			(sample: Record<string, unknown>) =>
				((sample.handles as Record<string, unknown>).tokens = ["lcm-handle:v1:not-valid-json"]),
		],
		[
			"duplicate rendered handle",
			(sample: Record<string, unknown>) => {
				const handles = sample.handles as { count: number; tokens: string[] };
				handles.tokens.push(handles.tokens[0]!);
				handles.count = 2;
			},
		],
		["post-store hash drift", (sample: Record<string, unknown>) => (sample.postStoreHash = "changed")],
		["job-state drift", (sample: Record<string, unknown>) => ((sample.jobs as Record<string, unknown>).pending = 1)],
	] as const) {
		it(`rejects ${name} in sample five even when aggregate sample one stays green`, async () => {
			const baseline = fiveSampleReport();
			const candidate = structuredClone(baseline);
			mutate(candidate.samples[4] as unknown as Record<string, unknown>);

			const result = await compareReports(baseline, candidate);

			expect(result.exitCode, result.stderr).not.toBe(0);
			expect(result.stderr).toMatch(
				/sample|coverage|provider|route|takeover|budget|store|counter|rendered|summary|handle|job|integrity/i,
			);
		});
	}

	it("requires CPU distribution and normalized job evidence", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		for (const report of [baseline, candidate]) {
			delete (report.metrics as unknown as Record<string, unknown>).cpuMs;
			for (const sample of report.samples) {
				delete (sample as unknown as Record<string, unknown>).cpuMs;
				delete (sample as unknown as Record<string, unknown>).jobs;
			}
		}

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/CPU|job|sample|evidence/i);
	});

	it("rejects forged lineage with a valid summary, handle, and source count", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		for (const template of [candidate.storeEvidence.migratedTemplate, candidate.storeEvidence.preparedTemplate]) {
			template.summaryLineageRows![0]!.sourceKey = "forged-source-key";
		}
		for (const sample of candidate.samples) {
			sample.selectedSpans[0]!.lineageRows[0]!.sourceKey = "forged-source-key";
		}

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/lineage|active branch|sourceIds|coverage/i);
	});

	it("rejects a wrong but resolvable summary handle paired with another summary id", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.storeEvidence.preparedTemplate.summaryRows = [
			{ summary_id: "summary-a", stable_handle: "handle-a" },
			{ summary_id: "summary-b", stable_handle: "handle-b" },
		];
		candidate.samples[4]!.selectedSpans = [
			{
				...candidate.samples[4]!.selectedSpans[0]!,
				summaryId: "summary-a",
				summaryHandle: "handle-b",
				summaryRow: { summary_id: "summary-a", stable_handle: "handle-a", project_id: "project-a" },
				lineageRows: candidate.samples[4]!.selectedSpans[0]!.lineageRows.map(row => ({
					...row,
					summaryId: "summary-a",
					summaryHandle: "handle-a",
				})),
			},
		];

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/summary.*handle|binding|store row/i);
	});

	it("rejects swapped provider-visible summary handles", async () => {
		const baseline = fiveSampleReport();
		const rows = [
			{ summary_id: "summary-1", stable_handle: "handle-1" },
			{ summary_id: "summary-2", stable_handle: "handle-2" },
		];
		baseline.storeEvidence.preparedTemplate.summaryRows = structuredClone(rows);
		baseline.storeEvidence.migratedTemplate.summaryRows = structuredClone(rows);
		const candidate = structuredClone(baseline);
		const sample = candidate.samples[4]!;
		const seedSpan = sample.selectedSpans[0]!;
		const span = (summaryId: string, summaryHandle: string) => ({
			...seedSpan,
			summaryId,
			summaryHandle,
			summaryRow: { summary_id: summaryId, stable_handle: summaryHandle, project_id: "project-a" },
			lineageRows: seedSpan.lineageRows.map(row => ({
				...row,
				summaryId,
				summaryHandle,
			})),
		});
		sample.selectedSpans = [span("summary-1", "handle-1"), span("summary-2", "handle-2")];
		const first = encodeLcmHandle({
			kind: "summary",
			reference: { projectId: "project-a", sessionId: "session-a", branchId: "branch-a", summaryHandle: "handle-1" },
		});
		const second = encodeLcmHandle({
			kind: "summary",
			reference: { projectId: "project-a", sessionId: "session-a", branchId: "branch-a", summaryHandle: "handle-2" },
		});
		Object.assign(
			(sample as typeof sample & { handles: NonNullable<ReplayReport["samples"][number]["handles"]> }).handles,
			{ count: 2, unique: 2, tokens: [second, first] },
		);

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/ordered store row|rendered summary handle|binding/i);
	});
	it("rejects edited deterministic failure route, call, or write evidence", async () => {
		const baseline = fiveSampleReport();
		const candidate = structuredClone(baseline);
		candidate.failureControls[1]!.providerCalls = 0;

		const result = await compareReports(baseline, candidate);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toMatch(/failure control|provider-call|store-write/i);
	});
});

describe("LCM replay physical capture pipeline", () => {
	it("captures JSONL, blob, and WAL state through baseline and compare with complete evidence", async () => {
		const fixture = await createPhysicalReplayFixture();
		const seed = await capturePhysical("wal-seed", fixture, [], "capture", fixture.latestMarkerId);
		expect(seed.result.exitCode, seed.result.stderr).toBe(0);
		const walPath = path.join(caseDir, "physical-wal.sqlite");
		await fs.copyFile(seed.snapshotPath, walPath);
		const wal = new Database(walPath, { strict: true });
		try {
			wal.exec(
				"PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE capture_wal_probe(value TEXT NOT NULL); INSERT INTO capture_wal_probe VALUES ('wal-visible');",
			);
			const common = [
				"--source-in",
				seed.sourcePath,
				"--store",
				walPath,
				"--request-tokens-floor",
				"374856",
			] as const;
			const baseline = await capturePhysical("wal-baseline", fixture, common, "capture", fixture.latestMarkerId);
			const candidate = await capturePhysical("wal-candidate", fixture, common, "capture", fixture.latestMarkerId);
			expect(baseline.result.exitCode, baseline.result.stderr).toBe(0);
			expect(candidate.result.exitCode, candidate.result.stderr).toBe(0);
			const baselineReport = (await Bun.file(baseline.reportPath).json()) as ReplayReport;
			const candidateReport = (await Bun.file(candidate.reportPath).json()) as ReplayReport;
			const comparison = await compareReports(baselineReport, candidateReport);
			expect(comparison.exitCode, comparison.stderr).toBe(0);

			const captured = new Database(baseline.snapshotPath, { readonly: true, strict: true });
			try {
				expect(captured.query<{ value: string }, []>("SELECT value FROM capture_wal_probe").get()?.value).toBe(
					"wal-visible",
				);
			} finally {
				captured.close(false);
			}
			const report = (await Bun.file(candidate.reportPath).json()) as {
				artifacts: {
					rawPrefix: string;
					resolvedPrefix: string;
					sqliteSnapshot: string;
					treatmentTemplate?: string;
				};
				storeEvidence?: {
					pristine: { path: string; byteHash: string; logicalHash: string; quickCheck: string };
					preparedTemplate: {
						reserialized: boolean;
						quickCheck: string;
						sourceRows: unknown[];
						summaryRows: unknown[];
						jobRows: unknown[];
						policyRows: unknown[];
						retryEpochs: number[];
					};
				};
				samples: Array<{
					cpuMs?: number;
					jobs?: object;
					providerAttempts?: number;
					handles?: { allResolved: boolean; allMatchStore: boolean };
					counters?: { rowsChanged?: number; reconcileRowsChanged?: number };
				}>;
				metrics: { cpuMs?: { median: number; mad: number; p95: number } };
				failureControls?: ReplayFailureControl[];
			};
			expect(await Bun.file(report.artifacts.rawPrefix).exists()).toBe(true);
			expect(await Bun.file(report.artifacts.resolvedPrefix).exists()).toBe(true);
			expect(report.storeEvidence?.pristine).toMatchObject({
				path: report.artifacts.sqliteSnapshot,
				quickCheck: "ok",
			});
			expect(report.storeEvidence?.pristine.byteHash).toMatch(/^[a-f0-9]{64}$/);
			expect(report.storeEvidence?.pristine.logicalHash).toMatch(/^[a-f0-9]{64}$/);
			expect(report.storeEvidence?.preparedTemplate).toMatchObject({ reserialized: true, quickCheck: "ok" });
			expect(report.storeEvidence?.preparedTemplate.sourceRows).toBeArray();
			expect(report.storeEvidence?.preparedTemplate.summaryRows).toBeArray();
			expect(report.storeEvidence?.preparedTemplate.jobRows).toBeArray();
			expect(report.storeEvidence?.preparedTemplate.policyRows).toBeArray();
			expect(report.storeEvidence?.preparedTemplate.retryEpochs).toBeArray();
			expect(report.samples).toHaveLength(5);
			expect(report.samples.every(sample => typeof sample.cpuMs === "number" && sample.jobs !== undefined)).toBe(
				true,
			);
			expect(
				report.samples.every(
					sample =>
						sample.providerAttempts === 0 &&
						sample.handles?.allResolved === true &&
						sample.handles.allMatchStore === true &&
						sample.counters?.rowsChanged === 0 &&
						sample.counters.reconcileRowsChanged === 0,
				),
			).toBe(true);
			expect(report.failureControls).toEqual([
				{
					name: "store",
					route: "native_fallback",
					category: "store",
					reason: null,
					providerCalls: 0,
					storeRowsChanged: 0,
				},
				{
					name: "provider-exhausted",
					route: "native_fallback",
					category: "provider",
					reason: "provider_exhausted",
					providerCalls: 1,
					storeRowsChanged: 1,
				},
				{
					name: "provider-key-mismatch",
					route: "native_fallback",
					category: "provider",
					reason: "provider_key_mismatch",
					providerCalls: 0,
					storeRowsChanged: 0,
				},
				{
					name: "assembly-invalid",
					route: "native_fallback",
					category: "unfit",
					reason: "assembly_invalid",
					providerCalls: 0,
					storeRowsChanged: 0,
				},
				{
					name: "fit-invariant",
					route: "native_fallback",
					category: "unfit",
					reason: "fit_invariant",
					providerCalls: 0,
					storeRowsChanged: 0,
				},
				{
					name: "irreducible-input",
					route: "native_fallback",
					category: "unfit",
					reason: "irreducible_input",
					providerCalls: 0,
					storeRowsChanged: 0,
				},
				{
					name: "minimum-representation",
					route: "native_fallback",
					category: "unfit",
					reason: "minimum_representation",
					providerCalls: 0,
					storeRowsChanged: 0,
				},
			]);
			expect(report.metrics.cpuMs).toEqual(
				expect.objectContaining({ median: expect.any(Number), mad: expect.any(Number), p95: expect.any(Number) }),
			);
		} finally {
			wal.close(false);
		}
	}, 60_000);
});
