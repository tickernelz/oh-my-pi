import type { AgentTool, AgentToolResult } from "@oh-my-pi/pi-agent-core";
import { untilAborted } from "@oh-my-pi/pi-utils";
import { type } from "arktype";
import {
	decodeLcmCitation,
	normalizeLcmSearchLimit,
	renderLcmSearchHits,
	renderLcmSourceDescription,
	runLcmRecall,
	searchKnownLcmProject,
} from "../lcm/operations";
import { resolveLcmProjectSelector } from "../lcm/project-catalog";
import lcmCrossProjectSearchDescription from "../prompts/tools/lcm-cross-project-search.md" with { type: "text" };
import lcmDescribeDescription from "../prompts/tools/lcm-describe.md" with { type: "text" };
import lcmRecallDescription from "../prompts/tools/lcm-recall.md" with { type: "text" };
import lcmSearchDescription from "../prompts/tools/lcm-search.md" with { type: "text" };
import type { ToolSession } from ".";
import type { OutputMeta } from "./output-meta";
import { replaceTabs, shortenPath } from "./render-utils";
import { ToolError } from "./tool-errors";
import { toolResult } from "./tool-result";

interface LcmToolDetails {
	meta?: OutputMeta;
	matches?: number;
	citations?: number;
	citation?: string;
	available?: boolean;
	projectId?: string;
}

const lcmSearchSchema = type({
	query: type("string>0").describe("full-text query over redacted current-session LCM data"),
	"limit?": type("number").describe("maximum matches (1-20; default 8)"),
});

const lcmDescribeSchema = type({
	citation: type("string>0").describe("opaque lcm-citation:v1 token returned by lcm_search"),
});

const lcmRecallSchema = type({
	query: type("string>0").describe("question to answer from explicitly selected current-session LCM sources"),
});

const lcmCrossProjectSearchSchema = type({
	project: type("string>0").describe("exact known project ID or explicit absolute canonical project path"),
	query: type("string>0").describe("full-text query over that one project's redacted derived data"),
	"limit?": type("number").describe("maximum matches (1-20; default 8)"),
});

export type LcmSearchParams = typeof lcmSearchSchema.infer;
export type LcmDescribeParams = typeof lcmDescribeSchema.infer;
export type LcmRecallParams = typeof lcmRecallSchema.infer;
export type LcmCrossProjectSearchParams = typeof lcmCrossProjectSearchSchema.infer;

function runtimeFor(session: ToolSession) {
	const runtime = session.getLcmRuntime?.();
	if (!runtime) throw new ToolError("LCM is unavailable for this session.");
	return runtime;
}

function artifactExists(session: ToolSession): ((id: string) => Promise<boolean>) | undefined {
	const manager = session.getArtifactManager?.();
	return manager ? id => manager.exists(id) : undefined;
}

export class LcmSearchTool implements AgentTool<typeof lcmSearchSchema> {
	readonly name = "lcm_search";
	readonly approval = "read" as const;
	readonly label = "LCM Search";
	readonly description = lcmSearchDescription;
	readonly parameters = lcmSearchSchema;
	readonly strict = true;
	readonly loadMode = "discoverable";
	readonly summary = "Search current lossless history";

	constructor(private readonly session: ToolSession) {}

	static createIf(session: ToolSession): LcmSearchTool | null {
		return session.getLcmRuntime ? new LcmSearchTool(session) : null;
	}

	async execute(_id: string, params: LcmSearchParams, signal?: AbortSignal): Promise<AgentToolResult> {
		return untilAborted(signal, async () => {
			const hits = await runtimeFor(this.session).lcmSearch(params.query, normalizeLcmSearchLimit(params.limit));
			return toolResult<LcmToolDetails>({ matches: hits.length })
				.text(await renderLcmSearchHits(hits, { artifactExists: artifactExists(this.session) }))
				.useless(hits.length === 0)
				.done();
		});
	}
}

export class LcmDescribeTool implements AgentTool<typeof lcmDescribeSchema> {
	readonly name = "lcm_describe";
	readonly approval = "read" as const;
	readonly label = "LCM Describe";
	readonly description = lcmDescribeDescription;
	readonly parameters = lcmDescribeSchema;
	readonly strict = true;
	readonly loadMode = "discoverable";
	readonly summary = "Describe one cited lossless source";

	constructor(private readonly session: ToolSession) {}

	static createIf(session: ToolSession): LcmDescribeTool | null {
		return session.getLcmRuntime ? new LcmDescribeTool(session) : null;
	}

	async execute(_id: string, params: LcmDescribeParams, signal?: AbortSignal): Promise<AgentToolResult> {
		return untilAborted(signal, async () => {
			const citation = decodeLcmCitation(params.citation);
			const description = await runtimeFor(this.session).lcmDescribe(citation);
			if (!description) {
				return toolResult<LcmToolDetails>({ citation: params.citation, available: false })
					.text("LCM citation is unavailable or is outside the current session/branch scope.")
					.useless()
					.done();
			}
			return toolResult<LcmToolDetails>({ citation: params.citation, available: true })
				.text(await renderLcmSourceDescription(description, { artifactExists: artifactExists(this.session) }))
				.done();
		});
	}
}

export class LcmRecallTool implements AgentTool<typeof lcmRecallSchema> {
	readonly name = "lcm_recall";
	readonly approval = "read" as const;
	readonly label = "LCM Recall";
	readonly description = lcmRecallDescription;
	readonly parameters = lcmRecallSchema;
	readonly strict = true;
	readonly loadMode = "discoverable";
	readonly summary = "Recall from bounded lossless sources";

	constructor(private readonly session: ToolSession) {}

	static createIf(session: ToolSession): LcmRecallTool | null {
		return session.getLcmRuntime ? new LcmRecallTool(session) : null;
	}

	async execute(_id: string, params: LcmRecallParams, signal?: AbortSignal): Promise<AgentToolResult> {
		return untilAborted(signal, async () => {
			const result = await runLcmRecall(runtimeFor(this.session), params.query, signal);
			if (!result)
				return toolResult<LcmToolDetails>({ citations: 0 })
					.text("No cited LCM sources matched this recall query.")
					.useless()
					.done();
			return toolResult<LcmToolDetails>({ citations: result.citations.length }).text(result.text).done();
		});
	}
}

export class LcmCrossProjectSearchTool implements AgentTool<typeof lcmCrossProjectSearchSchema> {
	readonly name = "lcm_cross_project_search";
	readonly approval = "read" as const;
	readonly label = "LCM Cross-project Search";
	readonly description = lcmCrossProjectSearchDescription;
	readonly parameters = lcmCrossProjectSearchSchema;
	readonly strict = true;
	readonly loadMode = "discoverable";
	readonly summary = "Search one explicitly selected LCM project";

	constructor(private readonly session: ToolSession) {}

	async execute(_id: string, params: LcmCrossProjectSearchParams, signal?: AbortSignal): Promise<AgentToolResult> {
		return untilAborted(signal, async () => {
			const agentDir = this.session.settings.getAgentDir();
			const project = await resolveLcmProjectSelector(params.project, agentDir);
			const hits = await searchKnownLcmProject(
				project.projectId,
				params.query,
				normalizeLcmSearchLimit(params.limit),
				agentDir,
			);
			const rendered = await renderLcmSearchHits(hits);
			return toolResult<LcmToolDetails>({ projectId: project.projectId, matches: hits.length })
				.text(`Project: ${project.projectId}\nRoot: ${replaceTabs(shortenPath(project.rootPath))}\n\n${rendered}`)
				.useless(hits.length === 0)
				.done();
		});
	}
}
