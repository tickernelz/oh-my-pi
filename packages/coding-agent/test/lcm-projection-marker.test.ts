import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { ContextProjection } from "@oh-my-pi/lcm-context";
import type { AssistantMessage } from "@oh-my-pi/pi-ai";
import { AssistantMessageComponent } from "@oh-my-pi/pi-coding-agent/modes/components/assistant-message";
import {
	LcmProjectionMarkerComponent,
	lcmProjectionFingerprint,
} from "@oh-my-pi/pi-coding-agent/modes/components/lcm-projection-marker";
import {
	getSymbolPresetOverride,
	initTheme,
	type SymbolPreset,
	setSymbolPreset,
} from "@oh-my-pi/pi-coding-agent/modes/theme/theme";

function projection(overrides: Partial<ContextProjection> = {}): ContextProjection {
	return {
		revision: 7,
		ready: true,
		historical: [
			{
				kind: "summary",
				summaryId: "leaf-a",
				summaryHandle: "handle-leaf-a",
				level: 0,
				redactedText: "summary a",
				tokenCount: 100,
				sourceIds: ["source-a"],
				citations: [],
			},
			{
				kind: "summary",
				summaryId: "leaf-b",
				summaryHandle: "handle-leaf-b",
				level: 0,
				redactedText: "summary b",
				tokenCount: 100,
				sourceIds: ["source-b"],
				citations: [],
			},
			{
				kind: "summary",
				summaryId: "condensed-c",
				summaryHandle: "handle-condensed-c",
				level: 2,
				redactedText: "summary c",
				tokenCount: 100,
				sourceIds: ["source-c"],
				citations: [],
			},
		],
		freshTailSourceIds: ["fresh-1", "fresh-2", "fresh-3", "fresh-4"],
		uncoveredSourceIds: [],
		estimatedTokens: 8_000,
		pendingJobs: 0,
		sourceTokens: 48_000,
		selectedLevelCounts: { 0: 2, 2: 1 },
		coveredSourceCount: 18,
		freshSourceCount: 4,
		...overrides,
	};
}

function assistantMessage(): AssistantMessage {
	return {
		role: "assistant",
		content: [{ type: "text", text: "Projected answer" }],
		api: "anthropic-messages",
		provider: "anthropic",
		model: "test-model",
		stopReason: "stop",
		usage: {
			input: 1,
			output: 1,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 2,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		timestamp: 1,
	};
}

let previousPreset: SymbolPreset | undefined;

beforeAll(async () => {
	await initTheme(false);
	previousPreset = getSymbolPresetOverride();
});

afterAll(async () => {
	await setSymbolPreset(previousPreset ?? "unicode");
});

describe("LcmProjectionMarkerComponent", () => {
	it("renders compact response evidence with one-based DAG depth and aggregate counts", () => {
		const lines = new LcmProjectionMarkerComponent(projection()).render(100);
		const text = Bun.stripANSI(lines.join("\n"));
		expect(lines).toHaveLength(3);
		expect(text).toContain("LCM context");
		expect(text).toContain("DAG depth 3");
		expect(text).toContain("3 summaries / 18 covered");
		expect(text).toContain("ctrl+o");
		expect(text).not.toContain("Journal unchanged");
	});

	it("expands once into a level tree and keeps the evidence open", () => {
		const component = new LcmProjectionMarkerComponent(projection());
		component.setExpanded(true);
		const expanded = Bun.stripANSI(component.render(100).join("\n"));
		expect(expanded).toContain("Depth 3: 1 summary");
		expect(expanded).toContain("Depth 1: 2 summaries");
		expect(expanded).toContain("Fresh tail: 4 sources");
		expect(expanded).toContain("Estimated tokens: 48K -> 8K");
		expect(expanded).toContain("Journal unchanged");
		expect(expanded).not.toContain("ctrl+o");

		component.setExpanded(false);
		expect(Bun.stripANSI(component.render(100).join("\n"))).toContain("Journal unchanged");
	});

	it("keeps text-first evidence within a narrow viewport", () => {
		const component = new LcmProjectionMarkerComponent(projection());
		const width = 14;
		const lines = component.render(width);
		expect(Bun.stripANSI(lines.join("\n"))).toContain("LCM context");
		for (const line of lines) expect(Bun.stringWidth(line)).toBeLessThanOrEqual(width);
		const concise = Bun.stripANSI(component.render(32).join("\n"));
		expect(concise).toContain("depth 3");
		expect(concise).toContain("3/18");

		component.setExpanded(true);
		const expanded = component.render(24);
		expect(Bun.stripANSI(expanded.join("\n"))).toContain("Journal unchanged");
		for (const line of expanded) expect(Bun.stringWidth(line)).toBeLessThanOrEqual(24);
	});

	it("uses only ASCII text and theme symbols under the ASCII preset", async () => {
		await setSymbolPreset("ascii");
		const component = new LcmProjectionMarkerComponent(projection());
		component.setExpanded(true);
		const text = Bun.stripANSI(component.render(100).join("\n"));
		expect(text).toMatch(/^[\x00-\x7f]*$/);
	});

	it("shares the assistant marker slot with cache evidence", () => {
		const component = new AssistantMessageComponent(assistantMessage());
		component.setLcmProjection(projection());
		component.setCacheInvalidation({ reprocessedTokens: 50_999 });
		const text = Bun.stripANSI(component.render(100).join("\n"));
		expect(text).toContain("LCM context");
		expect(text).toContain("cache miss");
		expect(text.match(/LCM context/g)).toHaveLength(1);
		expect(text.match(/cache miss/g)).toHaveLength(1);

		component.setExpanded(true);
		component.invalidate();
		const redrawn = Bun.stripANSI(component.render(100).join("\n"));
		expect(redrawn).toContain("Journal unchanged");
		expect(redrawn).toContain("cache miss");

		component.setLcmProjection(undefined);
		const replayed = Bun.stripANSI(component.render(100).join("\n"));
		expect(replayed).not.toContain("LCM context");
		expect(replayed).toContain("cache miss");
	});
});

describe("lcmProjectionFingerprint", () => {
	it("dedupes storage-only revisions but changes at a meaningful DAG boundary", () => {
		const first = projection();
		const storageOnly = projection({
			revision: 99,
			historical: first.historical.map(item => ({ ...item, summaryId: `rebuilt-${item.summaryId}` })),
		});
		const changedCounts = projection({ selectedLevelCounts: { 0: 3, 2: 1 } });
		const changedSummary = projection({
			historical: first.historical.map((item, index) =>
				index === 0 ? { ...item, summaryHandle: "handle-leaf-replacement" } : item,
			),
		});
		const changedFreshTail = projection({
			freshTailSourceIds: ["fresh-1", "fresh-2", "fresh-3", "fresh-replacement"],
		});
		expect(lcmProjectionFingerprint(storageOnly)).toBe(lcmProjectionFingerprint(first));
		expect(lcmProjectionFingerprint(changedCounts)).not.toBe(lcmProjectionFingerprint(first));
		expect(lcmProjectionFingerprint(changedSummary)).not.toBe(lcmProjectionFingerprint(first));
		expect(lcmProjectionFingerprint(changedFreshTail)).not.toBe(lcmProjectionFingerprint(first));
	});

	it("rejects unready, pending, uncovered, and summary-free projections", () => {
		expect(lcmProjectionFingerprint(projection({ ready: false }))).toBeUndefined();
		expect(lcmProjectionFingerprint(projection({ pendingJobs: 1 }))).toBeUndefined();
		expect(lcmProjectionFingerprint(projection({ uncoveredSourceIds: ["missing"] }))).toBeUndefined();
		expect(lcmProjectionFingerprint(projection({ historical: [] }))).toBeUndefined();
		expect(lcmProjectionFingerprint(projection({ selectedLevelCounts: {} }))).toBeUndefined();
	});
});
