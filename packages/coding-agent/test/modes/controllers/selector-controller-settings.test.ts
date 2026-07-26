import { describe, expect, it, vi } from "bun:test";
import type { Model } from "@oh-my-pi/pi-ai";
import { buildModel } from "@oh-my-pi/pi-catalog/build";
import { resetSettingsForTest, Settings, settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import type { SettingsSelectorComponent } from "@oh-my-pi/pi-coding-agent/modes/components/settings-selector";
import { SelectorController } from "@oh-my-pi/pi-coding-agent/modes/controllers/selector-controller";
import { initTheme } from "@oh-my-pi/pi-coding-agent/modes/theme/theme";
import type { InteractiveModeContext } from "@oh-my-pi/pi-coding-agent/modes/types";

function model(provider: string, id: string): Model {
	return buildModel({
		id,
		name: id,
		api: "ollama-chat",
		provider,
		baseUrl: "https://example.com",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 2_048,
	});
}

describe("SelectorController prompt-affecting settings", () => {
	it("refreshes the active prompt when xdev docs mode changes", async () => {
		const refreshBaseSystemPrompt = vi.fn(async () => {});
		const ctx = {
			session: { refreshBaseSystemPrompt },
			showError: vi.fn(),
		} as unknown as InteractiveModeContext;
		const controller = new SelectorController(ctx);

		controller.handleSettingChange("tools.xdevDocs", "catalog");
		await Promise.resolve();

		expect(refreshBaseSystemPrompt).toHaveBeenCalledTimes(1);
		expect(ctx.showError).not.toHaveBeenCalled();
	});

	it("refreshes every live lossless setting while leaving the engine reload-bound", () => {
		const refreshLcmSettings = vi.fn();
		const ctx = {
			session: { refreshLcmSettings },
		} as unknown as InteractiveModeContext;
		const controller = new SelectorController(ctx);

		controller.handleSettingChange("context.lossless.summaryModel", "custom/summary");
		controller.handleSettingChange("context.lossless.maxConcurrentSummaries", 3);
		controller.handleSettingChange("context.lossless.futureSetting", true);
		controller.handleSettingChange("context.engine", "native");

		expect(refreshLcmSettings).toHaveBeenCalledTimes(3);
	});

	it("gives the summary picker the full authenticated inventory without widening the cycling scope", async () => {
		resetSettingsForTest();
		await Settings.init({ inMemory: true });
		await initTheme();
		settings.set("context.engine", "lossless");
		settings.set("context.lossless.summaryModel", "custom/summary-only");
		try {
			const cyclingModels = [model("anthropic", "cycle-model")];
			const summaryOnly = model("custom", "summary-only");
			const fullInventory = [...cyclingModels, summaryOnly];
			const shown = Promise.withResolvers<SettingsSelectorComponent>();
			const getAvailableModels = vi.fn(() => cyclingModels);
			const getSummaryModels = vi.fn(() => fullInventory);
			const ctx = {
				session: {
					getAvailableModels,
					getAvailableThinkingLevels: () => [],
					thinkingLevel: undefined,
					model: cyclingModels[0],
					modelRegistry: { getAvailable: getSummaryModels },
				},
				ui: {
					showOverlay: vi.fn(component => {
						shown.resolve(component as SettingsSelectorComponent);
						return { hide: vi.fn(), setHidden: vi.fn(), isHidden: () => false };
					}),
					setFocus: vi.fn(),
					requestRender: vi.fn(),
					terminal: { columns: 120 },
				},
			} as unknown as InteractiveModeContext;

			new SelectorController(ctx).showSettingsSelector();
			const selector = await shown.promise;
			for (const char of "lossless summary model") selector.handleInput(char);
			expect(Bun.stripANSI(selector.render(120).join("\n"))).not.toContain("summary-only (unavailable)");
			selector.handleInput("\n");
			selector.handleInput("\n");

			expect(Bun.stripANSI(selector.render(120).join("\n"))).toContain("custom/summary-only");
			expect(getAvailableModels).toHaveBeenCalledTimes(1);
			expect(getSummaryModels).toHaveBeenCalledTimes(1);
		} finally {
			resetSettingsForTest();
		}
	});
});
