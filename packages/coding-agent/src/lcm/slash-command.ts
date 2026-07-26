import type { DoctorReport, PurgeResult, RebuildResult } from "@oh-my-pi/lcm-context";
import { replaceTabs, truncateToWidth } from "@oh-my-pi/pi-tui";
import type { LcmPublicStatus } from "../session/session-lcm";
import { commandConsumed, parseSubcommand, usage } from "../slash-commands/helpers/parse";
import type {
	ParsedSlashCommand,
	SlashCommandResult,
	SlashCommandRuntime,
	SubcommandDef,
} from "../slash-commands/types";
import { TRUNCATE_LENGTHS } from "../tools/render-utils";
import { decodeLcmHandle, renderLcmDescription, renderLcmSearchHits } from "./operations";
import { listLcmProjects } from "./project-catalog";
import { resolveLcmProject } from "./project-identity";
import { discoverLcmProjectJournals } from "./rebuild";

const LCM_USAGE =
	"Usage: /lcm <status|doctor|rebuild <current|project> --yes|gc|search <query>|describe <handle>|projects>";
const LCM_REBUILD_USAGE =
	"Usage: /lcm rebuild <current|project> --yes\nRebuild destructively replaces derived state for only the confirmed scope, can queue summary-model work and incur model cost, and never modifies JSONL.";

export const LCM_SUBCOMMANDS: SubcommandDef[] = [
	{ name: "status", description: "Show Lossless runtime, projection, and derived-store health" },
	{ name: "doctor", description: "Refresh the current projection and run derived-store integrity checks" },
	{
		name: "rebuild",
		description: "Rebuild from authoritative JSONL with explicit scope",
		usage: "<current|project> --yes",
	},
	{ name: "gc", description: "Run retention-aware garbage collection on eligible derived data" },
	{ name: "search", description: "Search current redacted derived history", usage: "<query>" },
	{ name: "describe", description: "Describe a current LCM handle", usage: "<handle>" },
	{ name: "projects", description: "List known explicitly selectable LCM projects" },
];

const SAFE_IDENTIFIER = /^[A-Za-z0-9@][A-Za-z0-9@._:/+-]{0,127}$/;
const DOCTOR_CHECK_LABELS: Record<string, string> = {
	"schema-version": "schema version",
	"sqlite-quick-check": "SQLite quick check",
	"foreign-keys": "foreign keys",
	"branch-sequences": "branch sequences",
	"fts-index": "search index",
	"search-documents": "search documents",
	quarantine: "quarantine state",
};

function safeIdentifier(value: string | undefined): string {
	if (!value) return "unresolved";
	const text = replaceTabs(value).trim();
	if (
		!SAFE_IDENTIFIER.test(text) ||
		text.includes("..") ||
		text.includes("://") ||
		/^[A-Za-z]:[\\/]/.test(text) ||
		/(?:token|key|secret|password)[=:]/i.test(text)
	) {
		return "[redacted]";
	}
	return truncateToWidth(text, TRUNCATE_LENGTHS.LONG);
}

function formatBackoff(retryAt: number | undefined): string {
	if (
		typeof retryAt !== "number" ||
		!Number.isSafeInteger(retryAt) ||
		retryAt < 0 ||
		retryAt > 8_640_000_000_000_000
	) {
		return "none";
	}
	return new Date(retryAt).toISOString();
}

export function formatLcmStatus(status: LcmPublicStatus): string {
	const { runtime, store } = status;
	const lines = [
		`LCM status: ${runtime.phase.toUpperCase()}`,
		"Authority: session JSONL is authoritative; LCM SQLite is redacted, derived, and rebuildable.",
	];
	if (runtime.summaryModelSelector) {
		lines.push(
			`Summary model: ${safeIdentifier(runtime.summaryModelSelector)} -> ${safeIdentifier(runtime.resolvedSummaryModel)}`,
		);
	}
	lines.push(`Workers: ${runtime.summaryWorkers.active}/${runtime.summaryWorkers.limit} active`);
	if (store) {
		lines.push(
			`SQLite WAL: ${store.journalMode.toLowerCase() === "wal" ? "enabled" : "not active"}; schema: ${store.schemaVersion}`,
		);
		lines.push(
			`Store: ${store.branches} branches, ${store.activeSources} active sources, ${store.tombstones} retained tombstones, ${store.leafSummaries + store.condensedSummaries} summary nodes`,
		);
		lines.push(
			`Project jobs: ${store.jobs.pending} pending, ${store.jobs.leased} running, ${store.jobs.failed} failed, ${store.jobs.completed} completed, ${store.jobs.obsolete} obsolete`,
		);
	} else {
		lines.push("SQLite WAL: not initialized");
		lines.push("Project jobs: not initialized");
	}
	lines.push(
		`Backoff: preferred until ${formatBackoff(runtime.summaryBackoff?.preferred)}; fallback until ${formatBackoff(runtime.summaryBackoff?.fallback)}`,
	);
	const projection = runtime.lastProjection;
	if (projection) {
		const selectedLevels = Object.entries(projection.selectedLevelCounts).filter(([, count]) => count > 0);
		const depth = selectedLevels.length === 0 ? 0 : Math.max(...selectedLevels.map(([level]) => Number(level))) + 1;
		const nodes = selectedLevels.reduce((total, [, count]) => total + count, 0);
		lines.push(
			`DAG: depth ${depth}, ${nodes} selected nodes, ${projection.coveredSourceCount} covered sources, ${projection.freshSourceCount} fresh sources`,
		);
		lines.push(`Estimated tokens: ${projection.sourceTokens} -> ${projection.estimatedTokens}`);
		lines.push(`Current branch: revision ${projection.revision}; ${projection.pendingJobs} relevant jobs pending`);
	} else {
		lines.push("DAG: no fitted projection yet");
	}
	if (runtime.lastFailureCategory) lines.push(`Last fallback: ${runtime.lastFailureCategory}`);
	if (store?.quarantined || runtime.phase === "quarantined") {
		lines.push("Derived store is quarantined; native compaction remains active.");
	} else if (runtime.phase === "degraded") {
		lines.push("Lossless projection is degraded; native compaction remains active.");
	}
	if (store?.recoveredFrom) lines.push("Derived store recovery: completed.");
	return lines.join("\n");
}

export function formatLcmDoctor(report: DoctorReport): string {
	const lines = [`LCM doctor: ${report.ok ? "healthy" : "DEGRADED"}`];
	for (const check of report.checks) {
		const label = DOCTOR_CHECK_LABELS[check.name] ?? "derived-store diagnostic";
		lines.push(`- ${check.ok ? "ok" : "FAIL"} ${label}${check.ok ? "" : ": attention required"}`);
	}
	return lines.join("\n");
}

export function formatLcmRebuild(result: RebuildResult, scope: "current" | "project", journalCount = 1): string {
	const counts = `${result.branches} branches, ${result.activeSources} active sources, ${result.queuedJobs} summary jobs queued`;
	const journalNoun = journalCount === 1 ? "journal" : "journals";
	return scope === "current"
		? `LCM current scope rebuilt from authoritative JSONL: ${counts}. Derived state now represents only the current session/branch; other sessions were not claimed restored. Summary jobs may incur model cost. Session JSONL was not modified.`
		: `LCM project derived state rebuilt from ${journalCount} authoritative JSONL ${journalNoun}: ${counts}. Summary jobs may incur model cost. Session JSONL was not modified.`;
}

export function formatLcmGc(result: PurgeResult): string {
	return `LCM retention-aware GC removed ${result.tombstones} eligible tombstones, ${result.jobs} eligible jobs, ${result.summaries} unreferenced summaries, ${result.sourceContents} unreferenced source contents, and ${result.files} unreferenced file records. Active lineage and authoritative session JSONL were not changed.`;
}

async function formatLcmProjects(agentDir: string): Promise<string> {
	const projects = await listLcmProjects(agentDir);
	if (projects.length === 0) return "No LCM projects are registered.";
	const lines = ["Known LCM projects (use the path-safe project ID as the explicit selector):"];
	for (const project of projects) {
		lines.push(`- ${safeIdentifier(project.projectId)}`);
		lines.push(`  authoritative journal directories known: ${project.journalDirs.length}`);
		lines.push(`  last seen: ${new Date(project.lastSeen).toISOString()}`);
	}
	return lines.join("\n");
}

export async function handleLcmCommand(
	command: ParsedSlashCommand,
	runtime: SlashCommandRuntime,
): Promise<SlashCommandResult> {
	const { verb, rest } = parseSubcommand(command.args);
	if (!verb) return usage(LCM_USAGE, runtime);
	try {
		switch (verb) {
			case "status": {
				if (rest) return usage(LCM_USAGE, runtime);
				await runtime.output(formatLcmStatus(await runtime.session.lcmStatus()));
				return commandConsumed();
			}
			case "doctor": {
				if (rest) return usage(LCM_USAGE, runtime);
				if (!runtime.session.lcmEnabled) {
					await runtime.output("LCM doctor unavailable: Lossless context is disabled; native context is active.");
					return commandConsumed();
				}
				const report = await runtime.session.lcmDoctor();
				await runtime.output(
					report
						? formatLcmDoctor(report)
						: "LCM doctor unavailable: derived state is uninitialized; session JSONL remains authoritative.",
				);
				return commandConsumed();
			}
			case "rebuild": {
				const { verb: scope, rest: confirmation } = parseSubcommand(rest);
				if ((scope !== "current" && scope !== "project") || confirmation !== "--yes") {
					return usage(LCM_REBUILD_USAGE, runtime);
				}
				if (!runtime.session.lcmEnabled) {
					await runtime.output(
						"LCM rebuild unavailable: Lossless context is disabled; session JSONL is unchanged.",
					);
					return commandConsumed();
				}
				if (scope === "current") {
					await runtime.output(
						"WARNING: Current rebuild destructively resets the entire LCM derived store, then restores only the current session/branch. Derived state for every other session will be removed. Queued summary work can incur model cost. Authoritative JSONL will not be modified.",
					);
					const result = await runtime.session.lcmRebuildCurrent();
					await runtime.output(
						result
							? formatLcmRebuild(result, "current")
							: "LCM current-scope rebuild could not initialize derived state; session JSONL was not modified.",
					);
					return commandConsumed();
				}
				await runtime.output(
					"LCM project rebuild confirmed for the selected project only. Scanning authoritative JSONL may be expensive; queued summary work can incur model cost. JSONL will not be modified.",
				);
				const agentDir = runtime.settings.getAgentDir();
				const project = await resolveLcmProject(runtime.sessionManager.getCwd(), agentDir);
				const discovery = await discoverLcmProjectJournals({
					project,
					sessionManager: runtime.sessionManager,
					agentDir,
				});
				if (discovery.issues.missing > 0 || discovery.issues.unreadable > 0) {
					await runtime.output(
						`LCM project rebuild stopped: ${discovery.issues.missing} authoritative journal paths were missing and ${discovery.issues.unreadable} were unreadable. Derived state and session JSONL were not changed.`,
					);
					return commandConsumed();
				}
				if (discovery.journals.length === 0) {
					await runtime.output(
						`LCM project rebuild found no readable authoritative JSONL journals for the selected project. ${discovery.issues.empty} empty and ${discovery.issues.outOfScope} out-of-scope journals were ignored; derived state was not changed.`,
					);
					return commandConsumed();
				}
				const result = await runtime.session.lcmRebuildProject(project.projectId, discovery.journals);
				await runtime.output(
					result
						? `${formatLcmRebuild(result, "project", discovery.filesLoaded)} ${discovery.issues.empty} empty and ${discovery.issues.outOfScope} out-of-scope journals were ignored.`
						: "LCM project rebuild could not initialize derived state. Derived state and session JSONL were not changed.",
				);
				return commandConsumed();
			}
			case "gc": {
				if (rest) return usage(LCM_USAGE, runtime);
				if (!runtime.session.lcmEnabled) {
					await runtime.output("LCM GC unavailable: Lossless context is disabled; session JSONL is unchanged.");
					return commandConsumed();
				}
				const result = await runtime.session.lcmGc();
				await runtime.output(
					result
						? formatLcmGc(result)
						: "LCM GC unavailable: derived state is uninitialized; session JSONL is unchanged.",
				);
				return commandConsumed();
			}
			case "search": {
				if (!rest) return usage("Usage: /lcm search <query>", runtime);
				if (!runtime.session.lcmEnabled) {
					await runtime.output("LCM search unavailable: Lossless context is disabled; native context is active.");
					return commandConsumed();
				}
				const hits = await runtime.session.lcmSearch(rest);
				if (hits.length > 0) {
					await runtime.output(await renderLcmSearchHits(hits));
					return commandConsumed();
				}
				const status = await runtime.session.lcmStatus();
				if (status.runtime.phase === "uninitialized") {
					await runtime.output(
						"LCM search unavailable: derived state is uninitialized; session JSONL remains authoritative.",
					);
				} else if (status.runtime.phase === "degraded" || status.runtime.phase === "quarantined") {
					await runtime.output(
						`LCM search unavailable while runtime is ${status.runtime.phase}; native context remains active.`,
					);
				} else {
					await runtime.output("No LCM matches found in the current project/session/branch derived state.");
				}
				return commandConsumed();
			}
			case "describe": {
				if (!rest) return usage("Usage: /lcm describe <handle>", runtime);
				if (!runtime.session.lcmEnabled) {
					await runtime.output(
						"LCM describe unavailable: Lossless context is disabled; native context is active.",
					);
					return commandConsumed();
				}
				const description = await runtime.session.lcmDescribe(decodeLcmHandle(rest));
				await runtime.output(
					description
						? await renderLcmDescription(description)
						: "LCM handle is valid but unavailable or outside the current session/branch scope.",
				);
				return commandConsumed();
			}
			case "projects":
				if (rest) return usage(LCM_USAGE, runtime);
				await runtime.output(await formatLcmProjects(runtime.settings.getAgentDir()));
				return commandConsumed();
			default:
				return usage(LCM_USAGE, runtime);
		}
	} catch {
		await runtime.output(
			"LCM operation failed without changing authoritative session JSONL. Native context remains available; retry or run /lcm status.",
		);
		return commandConsumed();
	}
}
