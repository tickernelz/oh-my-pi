import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { activeSourceFingerprint, type ContextProjection } from "@oh-my-pi/lcm-context";
import type { Usage } from "@oh-my-pi/pi-ai";
import {
	LcmProjectionFooterComponent,
	lcmProjectionFingerprint,
} from "@oh-my-pi/pi-coding-agent/modes/components/lcm-projection-footer";
import { createResponseFooterBlock } from "@oh-my-pi/pi-coding-agent/modes/components/usage-row";
import {
	getSymbolPresetOverride,
	initTheme,
	type SymbolPreset,
	setSymbolPreset,
} from "@oh-my-pi/pi-coding-agent/modes/theme/theme";

function projection(overrides: Partial<ContextProjection> = {}): ContextProjection {
	return {
		revision: 7,
		activeSourceFingerprint: activeSourceFingerprint([
			"source-a",
			"source-b",
			"source-c",
			"fresh-1",
			"fresh-2",
			"fresh-3",
			"fresh-4",
		]),
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
				files: [],
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
				files: [],
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
				files: [],
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

function usage(): Usage {
	return {
		input: 12,
		output: 3,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 15,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
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

describe("LcmProjectionFooterComponent", () => {
	it("renders compact response evidence without a transcript divider", () => {
		const lines = new LcmProjectionFooterComponent(projection()).render(100);
		const text = Bun.stripANSI(lines.join("\n"));
		expect(lines).toHaveLength(1);
		expect(text.trimStart().startsWith("LCM context")).toBe(true);
		expect(text).toContain("3 summaries / 18 covered");
		expect(text).toContain("ctrl+o");
		expect(text).not.toContain("DAG depth");
		expect(text).not.toContain("Journal unchanged");
	});

	it("expands once into footer details and keeps the evidence open", () => {
		const component = new LcmProjectionFooterComponent(projection());
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

	it("keeps text-first evidence within narrow viewports", () => {
		const component = new LcmProjectionFooterComponent(projection());
		expect(Bun.stripANSI(component.render(14).join("\n"))).toContain("LCM");
		for (let width = 1; width <= 24; width++) {
			for (const line of component.render(width)) expect(Bun.stringWidth(line)).toBeLessThanOrEqual(width);
		}

		component.setExpanded(true);
		expect(Bun.stripANSI(component.render(24).join("\n"))).toContain("Journal unchanged");
		for (let width = 1; width <= 24; width++) {
			for (const line of component.render(width)) expect(Bun.stringWidth(line)).toBeLessThanOrEqual(width);
		}
	});

	it("uses only ASCII text and theme symbols under the ASCII preset", async () => {
		await setSymbolPreset("ascii");
		const component = new LcmProjectionFooterComponent(projection());
		component.setExpanded(true);
		const text = Bun.stripANSI(component.render(100).join("\n"));
		expect(text).toMatch(/^[\x00-\x7f]*$/);
	});

	it("shares one response footer with usage metrics", () => {
		const footer = createResponseFooterBlock({
			usage: usage(),
			durationMs: 1_000,
			timestamp: 1,
			lcmProjection: projection(),
		});
		const lines = footer.render(100).map(line => Bun.stripANSI(line));
		expect(lines).toHaveLength(3);
		expect(lines[0]).toBe("");
		expect(lines[1]).toContain("12");
		expect(lines[1]).not.toContain("LCM context");
		expect(lines[2]).toContain("LCM context");

		footer.setExpanded(true);
		expect(Bun.stripANSI(footer.render(100).join("\n"))).toContain("Journal unchanged");
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
			activeSourceFingerprint: activeSourceFingerprint([
				"source-a",
				"source-b",
				"source-c",
				"fresh-1",
				"fresh-2",
				"fresh-3",
				"fresh-replacement",
			]),
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
