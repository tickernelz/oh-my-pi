import { describe, expect, it } from "bun:test";
import type { Citation, SourceDescription } from "@oh-my-pi/lcm-context";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import type { LcmRetrievalRuntime } from "@oh-my-pi/pi-coding-agent/lcm/operations";
import { BUILTIN_TOOL_NAMES } from "@oh-my-pi/pi-coding-agent/tools/builtin-names";
import { createTools, type Tool, type ToolSession } from "@oh-my-pi/pi-coding-agent/tools/index";

const LCM_TOOL_NAMES = ["lcm_search", "lcm_describe", "lcm_recall", "lcm_cross_project_search"] as const;

const runtime: LcmRetrievalRuntime = {
	lcmSearch: async () => [],
	lcmDescribe: async (_citation: Citation): Promise<SourceDescription | null> => null,
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

	it("enables the complete read-only set by default only for a top-level Lossless session", async () => {
		const native = await createTools(makeSession("native"));
		const lossless = await createTools(makeSession("lossless"));
		const losslessChild = await createTools(makeSession("lossless", { taskDepth: 1 }));

		expect(selectedLcmNames(native)).toEqual([]);
		expect(selectedLcmNames(lossless).sort()).toEqual([...LCM_TOOL_NAMES].sort());
		expect(selectedLcmNames(losslessChild)).toEqual([]);
	});

	it("allows an explicit request without widening a restricted child automatically", async () => {
		const restrictedChild = makeSession("lossless", { taskDepth: 1, restrictToolNames: true });
		const implicit = await createTools(restrictedChild, ["read"]);
		const explicit = await createTools(restrictedChild, ["lcm_search", "lcm_cross_project_search"]);
		const nativeExplicit = await createTools(makeSession("native"), ["lcm_recall"]);

		expect(selectedLcmNames(implicit)).toEqual([]);
		expect(selectedLcmNames(explicit).sort()).toEqual(["lcm_cross_project_search", "lcm_search"]);
		expect(selectedLcmNames(nativeExplicit)).toEqual(["lcm_recall"]);
	});

	it("cannot construct current-session retrieval tools when the owning runtime port was not forwarded", async () => {
		const childWithoutRuntime = makeSession("lossless", {
			taskDepth: 1,
			restrictToolNames: true,
			getLcmRuntime: undefined,
		});
		const tools = await createTools(childWithoutRuntime, ["lcm_search", "lcm_describe", "lcm_recall"]);
		expect(selectedLcmNames(tools)).toEqual([]);
	});
});
