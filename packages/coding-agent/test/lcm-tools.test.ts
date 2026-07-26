import { describe, expect, it } from "bun:test";
import type { SummaryDescription, SummaryReference } from "@oh-my-pi/lcm-context";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import {
	decodeLcmHandle,
	encodeLcmHandle,
	type LcmExpandOptions,
	type LcmRetrievalRuntime,
} from "@oh-my-pi/pi-coding-agent/lcm/operations";
import { BUILTIN_TOOL_NAMES } from "@oh-my-pi/pi-coding-agent/tools/builtin-names";
import { createTools, type Tool, type ToolSession } from "@oh-my-pi/pi-coding-agent/tools/index";

const TOP_LEVEL_LCM_TOOLS = ["lcm_search", "lcm_describe", "lcm_recall", "lcm_cross_project_search"] as const;
const LCM_TOOL_NAMES = [...TOP_LEVEL_LCM_TOOLS, "lcm_expand"] as const;
const summaryReference: SummaryReference = {
	projectId: "project",
	sessionId: "session",
	branchId: "branch",
	summaryHandle: "summary_stable",
};
const summary: SummaryDescription = {
	...summaryReference,
	kind: "leaf",
	level: 0,
	redactedText: "x".repeat(3_501),
	tokenCount: 2,
	sourceCount: 1,
	childCount: 0,
	parentHandles: [],
	files: [],
};

const runtime: LcmRetrievalRuntime = {
	lcmSearch: async () => [],
	lcmDescribe: async () => null,
	lcmExpand: async () => null,
	lcmComplete: async () => "answer",
};

function makeSession(engine: "native" | "lossless", extra: Partial<ToolSession> = {}): ToolSession {
	return {
		cwd: "/tmp/lcm-tool-gating",
		hasUI: false,
		skipPythonPreflight: true,
		enableLsp: false,
		enableMCP: false,
		getSessionFile: () => null,
		getSessionSpawns: () => "*",
		getLcmRuntime: () => runtime,
		settings: Settings.isolated({ "context.engine": engine, "tools.xdev": false }),
		...extra,
	};
}

function selectedLcmNames(tools: Tool[]): string[] {
	return tools.map(tool => tool.name).filter(name => name.startsWith("lcm_"));
}

describe("LCM tool registration and gating", () => {
	it("registers every LCM tool as a recognized CLI builtin", () => {
		for (const name of LCM_TOOL_NAMES) expect(BUILTIN_TOOL_NAMES).toContain(name);
	});

	it("keeps expand off the top level and enables it by default only for a child with a concrete forwarded runtime", async () => {
		const native = await createTools(makeSession("native"));
		const lossless = await createTools(makeSession("lossless"));
		const topLevelExplicit = await createTools(makeSession("lossless"), ["lcm_expand"]);
		const childWithoutForward = await createTools(makeSession("lossless", { taskDepth: 1 }));
		const forwardedChild = await createTools(
			makeSession("lossless", { taskDepth: 1, getForwardedLcmRuntime: () => runtime }),
		);

		expect(selectedLcmNames(native)).toEqual([]);
		expect(selectedLcmNames(lossless).sort()).toEqual([...TOP_LEVEL_LCM_TOOLS].sort());
		expect(selectedLcmNames(topLevelExplicit)).toEqual([]);
		expect(selectedLcmNames(childWithoutForward)).toEqual([]);
		expect(selectedLcmNames(forwardedChild)).toEqual(["lcm_expand"]);
	});

	it("allows explicit retrieval without granting expand to an unforwarded restricted child", async () => {
		const restrictedChild = makeSession("lossless", { taskDepth: 1, restrictToolNames: true });
		const implicit = await createTools(restrictedChild, ["read"]);
		const explicit = await createTools(restrictedChild, ["lcm_search", "lcm_cross_project_search", "lcm_expand"]);
		const nativeExplicit = await createTools(makeSession("native"), ["lcm_recall"]);

		expect(selectedLcmNames(implicit)).toEqual([]);
		expect(selectedLcmNames(explicit).sort()).toEqual(["lcm_cross_project_search", "lcm_search"]);
		expect(selectedLcmNames(nativeExplicit)).toEqual(["lcm_recall"]);
	});

	it("executes expansion on the forwarded runtime with capped depth, items, offset, tokens, and output", async () => {
		let observed: LcmExpandOptions | undefined;
		const forwarded: LcmRetrievalRuntime = {
			...runtime,
			lcmExpand: async options => {
				observed = options;
				return {
					root: summary,
					items: [
						{
							kind: "summary",
							depth: 1,
							summary: {
								...summary,
								redactedText: Array.from({ length: 100 }, () => "x".repeat(100)).join("\n"),
							},
						},
					],
					offset: options.offset,
					totalItems: 1,
					estimatedTokens: options.maxTokens,
					truncated: true,
				};
			},
		};
		const [tool] = await createTools(
			makeSession("lossless", {
				taskDepth: 1,
				restrictToolNames: true,
				getForwardedLcmRuntime: () => forwarded,
			}),
			["lcm_expand"],
		);
		const result = await tool!.execute("expand", {
			handle: encodeLcmHandle({ kind: "summary", reference: summaryReference }),
			depth: 99,
			offset: 99_999,
			limit: 999,
			max_tokens: 1_024,
		});

		expect(observed).toEqual({ reference: summaryReference, depth: 4, offset: 1_000, limit: 50, maxTokens: 1_024 });
		const text = result.content.find(block => block.type === "text")?.text ?? "";
		const tokens = text.match(/lcm-handle:v1:[A-Za-z0-9_-]+/g) ?? [];
		expect(text.length).toBeLessThanOrEqual(4_096);
		expect(tokens.length).toBeGreaterThan(0);
		for (const token of tokens) {
			expect(token.length).toBeLessThanOrEqual(4_096);
			expect(decodeLcmHandle(token)).toEqual({ kind: "summary", reference: summaryReference });
		}
		expect(text).toContain("Items: 0/1");
		expect(text).toContain("Next offset: 1000");
		expect(result.details).toMatchObject({ items: 0, offset: 1_000, nextOffset: 1_000 });
	});

	it("cannot construct current-session retrieval tools without their owning runtime", async () => {
		const childWithoutRuntime = makeSession("lossless", {
			taskDepth: 1,
			restrictToolNames: true,
			getLcmRuntime: undefined,
		});
		const tools = await createTools(childWithoutRuntime, ["lcm_search", "lcm_describe", "lcm_recall"]);
		expect(selectedLcmNames(tools)).toEqual([]);
	});
});
