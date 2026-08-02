/**
 * Contract: the compaction point renders as a slim horizontal divider —
 * `── 📷 compacted · ctrl+o ──` — instead of a full summary box, keeping the
 * transcript visually continuous. Expansion (ctrl+o) reveals the summary.
 * The render cache must honor the pi-tui same-reference contract: unchanged
 * components return the identical array so containers can memoize.
 */

import { beforeAll, describe, expect, it } from "bun:test";
import {
	type CompactionSummaryMessage,
	createCompactionSummaryMessage,
	defaultConvertToLlm,
	type LcmFallbackCategory,
} from "@oh-my-pi/pi-agent-core/compaction/messages";
import type { ImageContent } from "@oh-my-pi/pi-ai";
import { CompactionSummaryMessageComponent } from "@oh-my-pi/pi-coding-agent/modes/components/compaction-summary-message";
import { initTheme } from "@oh-my-pi/pi-coding-agent/modes/theme/theme";
import { convertToLlm } from "@oh-my-pi/pi-coding-agent/session/messages";

beforeAll(() => {
	initTheme();
});

const SUMMARY = "Earlier the user fixed the login TTL bug.";

function makeComponent(images?: ImageContent[], lcmFallback?: LcmFallbackCategory): CompactionSummaryMessageComponent {
	return new CompactionSummaryMessageComponent(
		createCompactionSummaryMessage(
			SUMMARY,
			84000,
			new Date().toISOString(),
			undefined,
			undefined,
			images,
			undefined,
			undefined,
			lcmFallback,
		),
	);
}

describe("CompactionSummaryMessageComponent", () => {
	it("collapsed: a single full-width divider carrying the expand affordance", () => {
		const lines = makeComponent().render(80);
		expect(lines.length).toBe(3); // breathing room above and below the rule
		const rule = Bun.stripANSI(lines[1]);
		expect(rule).toContain("compacted");
		expect(rule).toContain("ctrl+o");
		// The rule spans the full width and hides the summary body.
		expect(Bun.stringWidth(rule)).toBe(80);
		expect(rule).not.toContain(SUMMARY);
	});

	it("expanded: reveals the summary (and snapcompact frame count) below the divider", () => {
		const component = makeComponent([{ type: "image", data: "ZmFrZQ==", mimeType: "image/png" }]);
		component.setExpanded(true);
		const text = Bun.stripANSI(component.render(80).join("\n"));
		expect(text).toContain("compacted");
		expect(text).toContain(SUMMARY);
		expect(text).toContain("tokens");
		expect(text).toContain("1 snapcompact frame attached");
	});

	it("truncates the bare label when the viewport is too narrow for a framed rule", () => {
		const width = 10;
		const lines = makeComponent().render(width);
		const divider = Bun.stripANSI(lines[1]!);
		expect(divider.length).toBeGreaterThan(0);
		expect(Bun.stringWidth(divider)).toBeLessThanOrEqual(width);
	});

	it("honors the same-reference render cache and busts it on expansion toggle", () => {
		const component = makeComponent();
		const first = component.render(80);
		expect(component.render(80)).toBe(first);
		component.setExpanded(true);
		const expanded = component.render(80);
		expect(expanded).not.toBe(first);
		expect(component.render(80)).toBe(expanded);
	});

	it("annotates a persisted native LCM fallback with sanitized expanded detail", () => {
		const component = makeComponent(undefined, "deadline");
		const collapsed = Bun.stripANSI(component.render(100).join("\n"));
		expect(collapsed).toContain("LCM fallback: deadline");
		expect(collapsed).not.toContain("bounded LCM projection deadline elapsed");

		component.setExpanded(true);
		const expanded = Bun.stripANSI(component.render(100).join("\n")).replace(/\s+/g, " ");
		expect(expanded).toContain("LCM fallback (deadline)");
		expect(expanded).toContain("bounded LCM projection deadline elapsed");
		expect(expanded).toContain("failed open to native compaction");
	});

	it("truncates persisted LCM fallback labels at narrow widths", () => {
		const width = 20;
		const divider = Bun.stripANSI(makeComponent(undefined, "deadline").render(width)[1]!);
		expect(Bun.stringWidth(divider)).toBeLessThanOrEqual(width);
	});

	it("omits unrecognized persisted fallback text instead of rendering it", () => {
		const message = {
			...createCompactionSummaryMessage(SUMMARY, 84000, new Date().toISOString()),
			lcmFallback: "provider: raw secret detail",
		} as unknown as CompactionSummaryMessage;
		const component = new CompactionSummaryMessageComponent(message);
		component.setExpanded(true);
		const text = Bun.stripANSI(component.render(100).join("\n"));
		expect(text).not.toContain("LCM fallback");
		expect(text).not.toContain("raw secret detail");
		expect(JSON.stringify(convertToLlm([message]))).not.toContain("raw secret detail");
	});

	it("keeps fallback display metadata out of the compaction summary and provider content", () => {
		const message = createCompactionSummaryMessage(
			SUMMARY,
			84000,
			new Date().toISOString(),
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			"deadline",
		);
		expect(message.summary).toBe(SUMMARY);
		const providerPayloads = [defaultConvertToLlm([message]), convertToLlm([message])];
		for (const providerContent of providerPayloads.map(payload => JSON.stringify(payload))) {
			expect(providerContent).not.toContain("lcmFallback");
			expect(providerContent).not.toContain("LCM fallback");
			expect(providerContent).not.toContain("bounded LCM projection deadline");
		}
	});
});
