import type { AgentTool, AgentToolResult } from "@oh-my-pi/pi-agent-core";
import { untilAborted } from "@oh-my-pi/pi-utils";
import { type } from "arktype";
import {
	decodeLcmHandle,
	normalizeLcmExpandOptions,
	normalizeLcmOffset,
	normalizeLcmSearchLimit,
	renderLcmDescription,
	renderLcmExpansionPage,
	renderLcmSearchHits,
	runLcmRecall,
	searchKnownLcmProject,
} from "../lcm/operations";
import { resolveLcmProjectSelector } from "../lcm/project-catalog";
import lcmCrossProjectSearchDescription from "../prompts/tools/lcm-cross-project-search.md" with { type: "text" };
import lcmDescribeDescription from "../prompts/tools/lcm-describe.md" with { type: "text" };
import lcmExpandDescription from "../prompts/tools/lcm-expand.md" with { type: "text" };
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
	handle?: string;
	kind?: "source" | "summary" | "file";
	available?: boolean;
	projectId?: string;
	offset?: number;
	nextOffset?: number;
	items?: number;
}

const lcmSearchSchema = type({
	query: type("string>0").describe("full-text query over redacted current-session LCM data"),
	"limit?": type("number").describe("maximum matches (1-20; default 8)"),
	"offset?": type("number").describe("zero-based result offset (0-1000; default 0)"),
	"summary?": type("string>0").describe("optional opaque summary handle restricting the search scope"),
});

const lcmDescribeSchema = type({
	handle: type("string>0").describe("opaque lcm-handle:v1 source, summary, or file handle"),
});

const lcmExpandSchema = type({
	handle: type("string>0").describe("opaque lcm-handle:v1 summary handle"),
	"depth?": type("number").describe("summary traversal depth (1-4; default 1)"),
	"offset?": type("number").describe("zero-based item offset (0-1000; default 0)"),
	"limit?": type("number").describe("maximum items (1-50; default 20)"),
	"max_tokens?": type("number").describe("approximate output-token ceiling (1024-8000; default 4000)"),
});

const lcmRecallSchema = type({
	query: type("string>0").describe("question to answer from explicitly selected current-session LCM sources"),
});

const lcmCrossProjectSearchSchema = type({
	project: type("string>0").describe("exact known project ID or explicit absolute canonical project path"),
	query: type("string>0").describe("full-text query over that one project's redacted derived data"),
	"limit?": type("number").describe("maximum matches (1-20; default 8)"),
	"offset?": type("number").describe("zero-based result offset (0-1000; default 0)"),
});

export type LcmSearchParams = typeof lcmSearchSchema.infer;
export type LcmDescribeParams = typeof lcmDescribeSchema.infer;
export type LcmExpandParams = typeof lcmExpandSchema.infer;
export type LcmRecallParams = typeof lcmRecallSchema.infer;
export type LcmCrossProjectSearchParams = typeof lcmCrossProjectSearchSchema.infer;

function runtimeFor(session: ToolSession) {
	const runtime = session.getLcmRuntime?.();
	if (!runtime) throw new ToolError("LCM is unavailable for this session.");
	return runtime;
}

function forwardedRuntimeFor(session: ToolSession) {
	const runtime = (session.taskDepth ?? 0) > 0 ? session.getForwardedLcmRuntime?.() : undefined;
	if (!runtime) {
		throw new ToolError("LCM expansion is available only inside an explicitly LCM-enabled child/task session.");
	}
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
			const limit = normalizeLcmSearchLimit(params.limit);
			const offset = normalizeLcmOffset(params.offset);
			const scoped = params.summary ? decodeLcmHandle(params.summary) : undefined;
			if (scoped && scoped.kind !== "summary") throw new ToolError("LCM search scope must be a summary handle.");
			const hits = await runtimeFor(this.session).lcmSearch(params.query, {
				limit,
				offset,
				...(scoped ? { summary: scoped.reference } : {}),
			});
			return toolResult<LcmToolDetails>({
				matches: hits.length,
				offset,
				...(hits.length >= limit ? { nextOffset: offset + hits.length } : {}),
			})
				.text(await renderLcmSearchHits(hits, { artifactExists: artifactExists(this.session), offset, limit }))
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
	readonly summary = "Describe one lossless handle";

	constructor(private readonly session: ToolSession) {}

	static createIf(session: ToolSession): LcmDescribeTool | null {
		return session.getLcmRuntime ? new LcmDescribeTool(session) : null;
	}

	async execute(_id: string, params: LcmDescribeParams, signal?: AbortSignal): Promise<AgentToolResult> {
		return untilAborted(signal, async () => {
			const handle = decodeLcmHandle(params.handle);
			const description = await runtimeFor(this.session).lcmDescribe(handle);
			if (!description) {
				return toolResult<LcmToolDetails>({ handle: params.handle, kind: handle.kind, available: false })
					.text("LCM handle is unavailable or is outside the current session/branch scope.")
					.useless()
					.done();
			}
			return toolResult<LcmToolDetails>({ handle: params.handle, kind: handle.kind, available: true })
				.text(await renderLcmDescription(description, { artifactExists: artifactExists(this.session) }))
				.done();
		});
	}
}

export class LcmExpandTool implements AgentTool<typeof lcmExpandSchema> {
	readonly name = "lcm_expand";
	readonly approval = "read" as const;
	readonly label = "LCM Expand";
	readonly description = lcmExpandDescription;
	readonly parameters = lcmExpandSchema;
	readonly strict = true;
	readonly loadMode = "discoverable";
	readonly summary = "Expand one scoped lossless summary";

	constructor(private readonly session: ToolSession) {}

	static createIf(session: ToolSession): LcmExpandTool | null {
		return (session.taskDepth ?? 0) > 0 && session.getForwardedLcmRuntime?.() ? new LcmExpandTool(session) : null;
	}

	async execute(_id: string, params: LcmExpandParams, signal?: AbortSignal): Promise<AgentToolResult> {
		return untilAborted(signal, async () => {
			const handle = decodeLcmHandle(params.handle);
			if (handle.kind !== "summary") throw new ToolError("LCM expand requires a summary handle.");
			const bounds = normalizeLcmExpandOptions({
				depth: params.depth,
				offset: params.offset,
				limit: params.limit,
				maxTokens: params.max_tokens,
			});
			const expansion = await forwardedRuntimeFor(this.session).lcmExpand({
				reference: handle.reference,
				...bounds,
			});
			if (!expansion) {
				return toolResult<LcmToolDetails>({ handle: params.handle, kind: "summary", available: false })
					.text("LCM summary is unavailable or is outside the current session/branch scope.")
					.useless()
					.done();
			}
			const rendered = await renderLcmExpansionPage(expansion, bounds.maxTokens, {
				artifactExists: artifactExists(this.session),
			});
			return toolResult<LcmToolDetails>({
				handle: params.handle,
				kind: "summary",
				available: true,
				items: rendered.renderedItems,
				offset: expansion.offset,
				nextOffset: rendered.nextOffset,
			})
				.text(rendered.text)
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
			const limit = normalizeLcmSearchLimit(params.limit);
			const offset = normalizeLcmOffset(params.offset);
			const hits = await searchKnownLcmProject(project.projectId, params.query, { limit, offset }, agentDir);
			const rendered = await renderLcmSearchHits(hits, { offset, limit, includeSummaryHandles: false });
			return toolResult<LcmToolDetails>({
				projectId: project.projectId,
				matches: hits.length,
				offset,
				...(hits.length >= limit ? { nextOffset: offset + hits.length } : {}),
			})
				.text(`Project: ${project.projectId}\nRoot: ${replaceTabs(shortenPath(project.rootPath))}\n\n${rendered}`)
				.useless(hits.length === 0)
				.done();
		});
	}
}
