import * as os from "node:os";
import type { DoctorReport, PurgeResult, RebuildResult } from "@oh-my-pi/lcm-context";
import { replaceTabs, truncateToWidth } from "@oh-my-pi/pi-tui";
import type { LcmPublicStatus } from "../session/session-lcm";
import { commandConsumed, errorMessage, parseSubcommand, usage } from "../slash-commands/helpers/parse";
import type {
	ParsedSlashCommand,
	SlashCommandResult,
	SlashCommandRuntime,
	SubcommandDef,
} from "../slash-commands/types";
import { shortenPath, TRUNCATE_LENGTHS } from "../tools/render-utils";
import { decodeLcmCitation, renderLcmSearchHits, renderLcmSourceDescription } from "./operations";
import { listLcmProjects } from "./project-catalog";

const LCM_USAGE = "Usage: /lcm <status|doctor|rebuild|purge|search <query>|describe <citation>|projects>";

export const LCM_SUBCOMMANDS: SubcommandDef[] = [
	{ name: "status", description: "Show current derived-state health and counts" },
	{ name: "doctor", description: "Run read-only LCM integrity checks" },
	{ name: "rebuild", description: "Rebuild current derived state from the authoritative journal" },
	{ name: "purge", description: "Clear only current derived LCM state" },
	{ name: "search", description: "Search current redacted derived history", usage: "<query>" },
	{ name: "describe", description: "Describe a current source citation", usage: "<citation>" },
	{ name: "projects", description: "List known explicitly selectable LCM projects" },
];

function safeInline(value: unknown): string {
	return truncateToWidth(
		replaceTabs(String(value)).replaceAll(os.homedir(), "~").replace(/\s+/g, " ").trim(),
		TRUNCATE_LENGTHS.LONG,
	);
}

export function formatLcmStatus(status: LcmPublicStatus): string {
	const lines = [
		`LCM status: ${status.quarantined ? "DEGRADED (quarantined)" : "ready"}`,
		`Schema: ${status.schemaVersion}; journal: ${safeInline(status.journalMode)}`,
		`Branches: ${status.branches}; active sources: ${status.activeSources}; tombstones: ${status.tombstones}`,
		`Summaries: ${status.leafSummaries} leaf, ${status.condensedSummaries} condensed`,
		`Jobs: ${status.jobs.pending} pending, ${status.jobs.leased} leased, ${status.jobs.failed} failed, ${status.jobs.completed} completed, ${status.jobs.obsolete} obsolete`,
	];
	if (status.quarantineReason) lines.push(`Degraded reason: ${safeInline(status.quarantineReason)}`);
	if (status.recoveredFrom) lines.push(`Recovered derived store: ${replaceTabs(shortenPath(status.recoveredFrom))}`);
	return lines.join("\n");
}

export function formatLcmDoctor(report: DoctorReport): string {
	const lines = [`LCM doctor: ${report.ok ? "healthy" : "DEGRADED"}`];
	for (const check of report.checks) {
		lines.push(
			`- ${check.ok ? "ok" : "FAIL"} ${safeInline(check.name)}${check.detail ? `: ${safeInline(check.detail)}` : ""}`,
		);
	}
	return lines.join("\n");
}

export function formatLcmRebuild(result: RebuildResult): string {
	return `LCM derived state rebuilt: ${result.branches} branches, ${result.activeSources} active sources, ${result.queuedJobs} summary jobs queued.`;
}

export function formatLcmPurge(result: PurgeResult): string {
	return `LCM derived state purged: ${result.tombstones} tombstones, ${result.jobs} jobs, ${result.summaries} summaries, ${result.sourceContents} source contents cleared. The authoritative session journal was not changed.`;
}

async function formatLcmProjects(agentDir: string): Promise<string> {
	const projects = await listLcmProjects(agentDir);
	if (projects.length === 0) return "No LCM projects are registered.";
	const lines = ["Known LCM projects (cross-project access still requires an explicit selector):"];
	for (const project of projects) {
		lines.push(`- ${project.projectId}`);
		lines.push(`  root: ${replaceTabs(shortenPath(project.rootPath))}`);
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
				const status = await runtime.session.lcmStatus();
				await runtime.output(status ? formatLcmStatus(status) : "LCM is unavailable for this session.");
				return commandConsumed();
			}
			case "doctor": {
				if (rest) return usage(LCM_USAGE, runtime);
				const report = await runtime.session.lcmDoctor();
				await runtime.output(report ? formatLcmDoctor(report) : "LCM doctor is unavailable for this session.");
				return commandConsumed();
			}
			case "rebuild": {
				if (rest) return usage(LCM_USAGE, runtime);
				const result = await runtime.session.lcmRebuild();
				await runtime.output(result ? formatLcmRebuild(result) : "LCM rebuild is unavailable for this session.");
				return commandConsumed();
			}
			case "purge": {
				if (rest) return usage(LCM_USAGE, runtime);
				const result = await runtime.session.lcmPurge();
				await runtime.output(result ? formatLcmPurge(result) : "LCM purge is unavailable for this session.");
				return commandConsumed();
			}
			case "search": {
				if (!rest) return usage("Usage: /lcm search <query>", runtime);
				const hits = await runtime.session.lcmSearch(rest);
				await runtime.output(await renderLcmSearchHits(hits));
				return commandConsumed();
			}
			case "describe": {
				if (!rest) return usage("Usage: /lcm describe <citation>", runtime);
				const description = await runtime.session.lcmDescribe(decodeLcmCitation(rest));
				await runtime.output(
					description
						? await renderLcmSourceDescription(description)
						: "LCM citation is unavailable or outside the current session/branch scope.",
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
	} catch (error) {
		await runtime.output(`LCM ${verb} failed: ${safeInline(errorMessage(error))}`);
		return commandConsumed();
	}
}
