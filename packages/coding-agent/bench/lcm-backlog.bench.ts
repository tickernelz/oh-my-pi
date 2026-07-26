/**
 * Credential-free throughput canary for the production SessionLcm summary pool.
 *
 * Every measured run uses the same durable session history and a fresh temporary
 * SQLite store. The completion host performs no provider I/O: it waits for a
 * fixed delay and returns a deterministic one-token summary.
 *
 * Run: `bun run packages/coding-agent/bench/lcm-backlog.bench.ts`
 * Env: `LCM_BACKLOG_CONCURRENCY` selects the comparison width from 2 through 4 (default 2);
 *      `LCM_BACKLOG_DELAY_MS` controls completion latency (default 25);
 *      `LCM_BACKLOG_SAMPLES` controls samples per width (default 5).
 */
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import {
	type LcmCompletionRequest,
	type LcmPublicStatus,
	SessionLcm,
} from "../src/session/session-lcm";
import { SessionManager } from "../src/session/session-manager";

type Width = 1 | 2 | 3 | 4;
const COMPARISON_WIDTH = envInteger("LCM_BACKLOG_CONCURRENCY", 2, 2, 4) as Exclude<Width, 1>;
const WIDTHS: readonly Width[] = [1, COMPARISON_WIDTH];
type JobCounts = NonNullable<LcmPublicStatus["store"]>["jobs"];
type JobState = keyof JobCounts;

const JOB_STATES: readonly JobState[] = ["pending", "leased", "failed", "completed", "obsolete"];
const MIN_LEAF_JOBS = 16;
const SOURCE_COUNT = MIN_LEAF_JOBS * 24 + 1;
const DELAY_MS = envInteger("LCM_BACKLOG_DELAY_MS", 25, 0);
const SAMPLES = envInteger("LCM_BACKLOG_SAMPLES", 5, 1);
const HARD_WAIT_MS = Math.min(2_147_000_000, Math.max(30_000, (DELAY_MS + 1) * SOURCE_COUNT * 2));

interface Stats {
	medianMs: number;
	madMs: number;
}

interface RunResult {
	width: Width;
	sample: number;
	elapsedMs: number;
	peak: number;
	limitExceeded: boolean;
	activeAtEnd: number;
	completionCalls: number;
	uniqueCompletions: number;
	allCompletionsUnique: boolean;
	projectionReady: boolean;
	workerActiveAtEnd: number;
	workerLimit: number;
	leafJobs: number;
	condensedJobs: number;
	jobs: JobCounts;
}

type BenchmarkEnv = "LCM_BACKLOG_CONCURRENCY" | "LCM_BACKLOG_DELAY_MS" | "LCM_BACKLOG_SAMPLES";

function envInteger(
	name: BenchmarkEnv,
	fallback: number,
	minimum: number,
	maximum = Number.MAX_SAFE_INTEGER,
): number {
	const raw = Bun.env[name];
	if (raw === undefined) return fallback;
	if (!/^\d+$/.test(raw)) throw new Error(`${name} must be a base-10 integer`);
	const value = Number(raw);
	if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new Error(`${name} must be a safe integer from ${minimum} through ${maximum}`);
	}
	return value;
}

function median(values: readonly number[]): number {
	const sorted = [...values].sort((left, right) => left - right);
	const middle = sorted.length >> 1;
	return sorted.length % 2 === 1 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2;
}

function stats(samples: readonly number[]): Stats {
	const medianMs = median(samples);
	const madMs = median(samples.map(sample => Math.abs(sample - medianMs)));
	return { medianMs, madMs };
}

function createWorkload(projectRoot: string): SessionManager {
	const manager = SessionManager.inMemory(projectRoot);
	for (let index = 0; index < SOURCE_COUNT; index++) {
		manager.appendMessage({
			role: "user",
			content: [
				{
					type: "text",
					text: `Benchmark source ${index.toString().padStart(3, "0")}: alpha beta gamma delta epsilon zeta eta theta.`,
				},
			],
			timestamp: 1_700_000_000_000 + index,
		});
	}
	return manager;
}

async function runSample(
	manager: SessionManager,
	projectRoot: string,
	storePath: string,
	width: Width,
	sample: number,
): Promise<RunResult> {
	let active = 0;
	let peak = 0;
	let limitExceeded = false;
	let completionCalls = 0;
	const completions = new Map<string, number>();

	const complete = async (request: LcmCompletionRequest): Promise<string> => {
		const completionId = new Bun.CryptoHasher("sha256")
			.update(request.systemPrompt)
			.update("\0")
			.update(request.prompt)
			.digest("hex");
		completionCalls++;
		completions.set(completionId, (completions.get(completionId) ?? 0) + 1);
		active++;
		peak = Math.max(peak, active);
		limitExceeded ||= active > width;
		try {
			request.onResolvedModel?.("benchmark/fixed-delay");
			if (request.signal) await sleep(DELAY_MS, undefined, { signal: request.signal });
			else await sleep(DELAY_MS);
			return ".";
		} finally {
			active--;
		}
	};

	const lcm = new SessionLcm(
		{
			sessionManager: manager,
			projectionLimits: () => ({
				sourceTokens: SOURCE_COUNT * 100,
				softThresholdTokens: 1,
				hardThresholdTokens: 1,
				tokenBudget: 256,
				freshTail: { maxSources: 1, maxTokens: 128 },
			}),
			projectionFits: () => true,
			complete,
		},
		{
			summaryModel: "@smol",
			maxConcurrentSummaries: width,
			dependencies: {
				resolveProject: async () => ({
					projectId: "lcm-backlog-benchmark",
					rootPath: projectRoot,
					storePath,
				}),
				hardWaitMs: HARD_WAIT_MS,
			},
		},
	);

	try {
		const messages = manager.buildSessionContext().messages;
		const started = Bun.nanoseconds();
		const projected = await lcm.project(messages);
		const elapsedMs = (Bun.nanoseconds() - started) / 1e6;
		const status = await lcm.status();
		if (!status.store) throw new Error(`width ${width} sample ${sample} did not initialize its SQLite store`);
		const finalProjection = projected.projection;
		return {
			width,
			sample,
			elapsedMs,
			peak,
			limitExceeded,
			activeAtEnd: active,
			completionCalls,
			uniqueCompletions: completions.size,
			allCompletionsUnique: [...completions.values()].every(count => count === 1),
			projectionReady:
				projected.owned === true &&
				finalProjection?.ready === true &&
				finalProjection.pendingJobs === 0 &&
				finalProjection.uncoveredSourceIds.length === 0,
			workerActiveAtEnd: status.runtime.summaryWorkers.active,
			workerLimit: status.runtime.summaryWorkers.limit,
			leafJobs: status.store.leafSummaries,
			condensedJobs: status.store.condensedSummaries,
			jobs: { ...status.store.jobs },
		};
	} finally {
		await lcm.close();
	}
}

async function measure(): Promise<Map<Width, RunResult[]>> {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-lcm-backlog-"));
	try {
		const projectRoot = path.join(root, "project");
		await fs.mkdir(projectRoot);
		const manager = createWorkload(projectRoot);
		const results = new Map<Width, RunResult[]>(WIDTHS.map(width => [width, []]));
		for (let sample = 1; sample <= SAMPLES; sample++) {
			const order: readonly Width[] = sample % 2 === 1 ? WIDTHS : [COMPARISON_WIDTH, 1];
			for (const width of order) {
				const runDir = path.join(root, `sample-${sample}-width-${width}`);
				await fs.mkdir(runDir);
				results.get(width)!.push(
					await runSample(manager, projectRoot, path.join(runDir, "context.sqlite"), width, sample),
				);
			}
		}
		return results;
	} finally {
		await fs.rm(root, { recursive: true, force: true });
	}
}

const results = await measure();
const runsFor = (width: Width): RunResult[] => {
	const runs = results.get(width);
	if (!runs) throw new Error(`Missing benchmark runs for width ${width}`);
	return runs;
};
const widthStats = new Map<Width, Stats>(
	WIDTHS.map(width => [width, stats(runsFor(width).map(result => result.elapsedMs))]),
);
const statsFor = (width: Width): Stats => widthStats.get(width)!;
const speedup = statsFor(1).medianMs / statsFor(COMPARISON_WIDTH).medianMs;
const elapsedReduction = 1 - statsFor(COMPARISON_WIDTH).medianMs / statsFor(1).medianMs;
const baseline = runsFor(1)[0]!;
const failures: string[] = [];
const requireInvariant = (condition: boolean, message: string): void => {
	if (!condition) failures.push(message);
};

for (const width of WIDTHS) {
	for (const result of runsFor(width)) {
		const label = `width ${width} sample ${result.sample}`;
		requireInvariant(Number.isFinite(result.elapsedMs) && result.elapsedMs > 0, `${label}: elapsed time is invalid`);
		requireInvariant(result.peak === width, `${label}: peak ${result.peak}, expected ${width}`);
		requireInvariant(!result.limitExceeded && result.peak <= width, `${label}: exceeded worker limit ${width}`);
		requireInvariant(result.activeAtEnd === 0, `${label}: completion host still has ${result.activeAtEnd} active`);
		requireInvariant(result.workerActiveAtEnd === 0, `${label}: SessionLcm still has ${result.workerActiveAtEnd} active`);
		requireInvariant(result.workerLimit === width, `${label}: runtime limit ${result.workerLimit}, expected ${width}`);
		requireInvariant(result.projectionReady, `${label}: final projection is not ready`);
		requireInvariant(result.leafJobs >= MIN_LEAF_JOBS, `${label}: created only ${result.leafJobs} leaf jobs`);
		requireInvariant(result.allCompletionsUnique, `${label}: a completion request ran more than once`);
		requireInvariant(
			result.completionCalls === result.uniqueCompletions && result.completionCalls === result.jobs.completed,
			`${label}: ${result.completionCalls} calls, ${result.uniqueCompletions} unique, ${result.jobs.completed} completed jobs`,
		);
		requireInvariant(
			result.jobs.completed === result.leafJobs + result.condensedJobs,
			`${label}: completed jobs do not match persisted summaries`,
		);
		for (const state of ["pending", "leased", "failed", "obsolete"] as const) {
			requireInvariant(result.jobs[state] === 0, `${label}: ${result.jobs[state]} ${state} jobs remain`);
		}
		for (const state of JOB_STATES) {
			requireInvariant(
				result.jobs[state] === baseline.jobs[state],
				`${label}: ${state} count ${result.jobs[state]} differs from identical-store baseline ${baseline.jobs[state]}`,
			);
		}
		requireInvariant(result.leafJobs === baseline.leafJobs, `${label}: leaf-job count differs from identical-store baseline`);
		requireInvariant(
			result.condensedJobs === baseline.condensedJobs,
			`${label}: condensed-job count differs from identical-store baseline`,
		);
	}
}
requireInvariant(
	Number.isFinite(speedup) && elapsedReduction >= 0.25,
	`width ${COMPARISON_WIDTH} median ${statsFor(COMPARISON_WIDTH).medianMs.toFixed(3)} ms is not at least 25% below width 1 median ${statsFor(1).medianMs.toFixed(3)} ms`,
);

console.log(`\nBenchmark: lcm-backlog (delay=${DELAY_MS} ms, samples=${SAMPLES}, sources=${SOURCE_COUNT})\n`);
for (const width of WIDTHS) {
	const runs = runsFor(width);
	const peak = Math.max(...runs.map(result => result.peak));
	const final = runs[0]!;
	const widthStat = statsFor(width);
	console.log(
		`  width ${width}: peak ${peak}/${width}, median ${widthStat.medianMs.toFixed(3)} ms, MAD ${widthStat.madMs.toFixed(3)} ms`,
	);
	console.log(
		`    jobs: ${final.jobs.pending} pending, ${final.jobs.leased} leased, ${final.jobs.failed} failed, ${final.jobs.completed} completed, ${final.jobs.obsolete} obsolete`,
	);
}
console.log(`\n  speedup (width 1 / width ${COMPARISON_WIDTH}): ${speedup.toFixed(3)}x\n`);

console.log(`METRIC job_count=${baseline.jobs.completed}`);
console.log(`METRIC leaf_job_count=${baseline.leafJobs}`);
for (const width of WIDTHS) {
	const runs = runsFor(width);
	const peak = Math.max(...runs.map(result => result.peak));
	const final = runs[0]!;
	const widthStat = statsFor(width);
	console.log(`METRIC width_${width}_limit=${width}`);
	console.log(`METRIC width_${width}_peak=${peak}`);
	console.log(`METRIC width_${width}_median_ms=${widthStat.medianMs.toFixed(3)}`);
	console.log(`METRIC width_${width}_mad_ms=${widthStat.madMs.toFixed(3)}`);
	for (const state of JOB_STATES) console.log(`METRIC width_${width}_${state}=${final.jobs[state]}`);
}
console.log(`METRIC speedup=${speedup.toFixed(3)}`);

if (failures.length > 0) throw new Error(`LCM backlog canary failed:\n- ${failures.join("\n- ")}`);
