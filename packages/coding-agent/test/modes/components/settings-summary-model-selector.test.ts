import { afterEach, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import type { Model } from "@oh-my-pi/pi-ai";
import { buildModel } from "@oh-my-pi/pi-catalog/build";
import { resetSettingsForTest, type SettingPath, Settings, settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import { SettingsSelectorComponent } from "@oh-my-pi/pi-coding-agent/modes/components/settings-selector";
import { initTheme, theme } from "@oh-my-pi/pi-coding-agent/modes/theme/theme";
import { visibleWidth } from "@oh-my-pi/pi-tui";

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

const MODELS = [model("anthropic", "claude-haiku-4-5"), model("custom", "summary-fast")];

function stubStdoutGeometry(cols: number): { restore(): void } {
	const rowsDesc = Object.getOwnPropertyDescriptor(process.stdout, "rows");
	const colsDesc = Object.getOwnPropertyDescriptor(process.stdout, "columns");
	Object.defineProperty(process.stdout, "rows", { configurable: true, get: () => 40, set: () => {} });
	Object.defineProperty(process.stdout, "columns", { configurable: true, get: () => cols, set: () => {} });
	return {
		restore() {
			if (rowsDesc) Object.defineProperty(process.stdout, "rows", rowsDesc);
			if (colsDesc) Object.defineProperty(process.stdout, "columns", colsDesc);
		},
	};
}

function createSelector(
	summaryModels: ReadonlyArray<Model> = MODELS,
	onChange: (path: SettingPath, value: unknown) => void = () => {},
	models: ReadonlyArray<Model> = summaryModels,
): SettingsSelectorComponent {
	return new SettingsSelectorComponent(
		{
			availableThinkingLevels: [],
			thinkingLevel: undefined,
			availableThemes: ["dark"],
			models,
			summaryModels,
			providers: [...new Set(models.map(item => item.provider))],
			cwd: process.cwd(),
		},
		{ onChange, onCancel: () => {} },
	);
}

function focusSummaryModel(selector: SettingsSelectorComponent): void {
	for (const char of "lossless summary model") selector.handleInput(char);
	expect(selector.render(120).join("\n")).toContain("Lossless Summary Model");
}
function focusConcurrentSummaries(selector: SettingsSelectorComponent): void {
	for (const char of "concurrent summaries") selector.handleInput(char);
	expect(selector.render(120).join("\n")).toContain("Concurrent Summaries");
}

function plainLines(selector: SettingsSelectorComponent, width: number): string[] {
	return selector.render(width).map(line => Bun.stripANSI(line));
}

/** SGR left-button press at a 1-based fullscreen row. */
function leftClick(row1Based: number, col1Based = 8): string {
	return `\x1b[<0;${col1Based};${row1Based}M`;
}

beforeAll(async () => {
	await initTheme(false, "unicode");
});

let geometry: { restore(): void } | undefined;

beforeEach(async () => {
	resetSettingsForTest();
	await Settings.init({ inMemory: true });
	settings.set("context.engine", "lossless");
	geometry = stubStdoutGeometry(120);
});

afterEach(() => {
	geometry?.restore();
	geometry = undefined;
	resetSettingsForTest();
});

describe("lossless summary model settings picker", () => {
	it("preselects the exact current model and clears the YAML leaf for the dynamic default", () => {
		const changes: Array<[SettingPath, unknown]> = [];
		settings.set("context.lossless.summaryModel", "custom/summary-fast");
		const selector = createSelector(MODELS, (path, value) => changes.push([path, value]));
		focusSummaryModel(selector);

		selector.handleInput("\n"); // Open Use default / Choose model.
		expect(plainLines(selector, 120).join("\n")).toContain("Choose model…");
		selector.handleInput("\n"); // Current explicit value preselects Choose model.
		expect(plainLines(selector, 120).join("\n")).toContain("custom/summary-fast");
		selector.handleInput("\n"); // Exact current browser row is preselected.
		expect(changes.at(-1)).toEqual(["context.lossless.summaryModel", "custom/summary-fast"]);

		selector.handleInput("\n"); // Re-open the action submenu.
		selector.handleInput("\x1b[A");
		selector.handleInput("\n"); // Use default (@smol).
		expect(settings.get("context.lossless.summaryModel")).toBeUndefined();
		expect(settings.getSettingProvenance("context.lossless.summaryModel")).toBe("default");
		expect(changes.at(-1)).toEqual(["context.lossless.summaryModel", undefined]);
		expect(plainLines(selector, 120).join("\n")).toContain("@smol (default)");
	});

	it("supports mouse selection of authenticated and custom model rows", () => {
		const selector = createSelector();
		focusSummaryModel(selector);
		selector.handleInput("\n");

		let lines = plainLines(selector, 100);
		const chooseRow = lines.findIndex(line => line.includes("Choose model…"));
		expect(chooseRow).toBeGreaterThanOrEqual(0);
		selector.handleInput(leftClick(chooseRow + 1));

		lines = plainLines(selector, 100);
		const modelRow = lines.findIndex(line => line.includes("custom/summary-fast"));
		expect(modelRow).toBeGreaterThanOrEqual(0);
		selector.handleInput(leftClick(modelRow + 1)); // Select.
		selector.handleInput(leftClick(modelRow + 1)); // Click again to activate.

		expect(settings.get("context.lossless.summaryModel")).toBe("custom/summary-fast");
	});

	it("uses the full authenticated inventory independently of the cycling scope", () => {
		settings.set("context.lossless.summaryModel", "custom/summary-fast");
		const selector = createSelector(MODELS, () => {}, [MODELS[0]!]);
		focusSummaryModel(selector);
		expect(plainLines(selector, 120).join("\n")).not.toContain("custom/summary-fast (unavailable)");

		selector.handleInput("\n");
		selector.handleInput("\n");
		expect(plainLines(selector, 120).join("\n")).toContain("custom/summary-fast");
	});

	it("uses the native cancel ladder without mutating the setting", () => {
		const selector = createSelector();
		focusSummaryModel(selector);
		selector.handleInput("\n");
		selector.handleInput("\x1b[B");
		selector.handleInput("\n");
		selector.handleInput("summary-fast");

		selector.handleInput("\x1b"); // Clear browser query.
		expect(plainLines(selector, 120).join("\n")).toContain("custom/summary-fast");
		selector.handleInput("\x1b"); // Return to Use default / Choose model.
		expect(plainLines(selector, 120).join("\n")).toContain("Use default (@smol)");
		selector.handleInput("\x1b"); // Close submenu.

		expect(settings.get("context.lossless.summaryModel")).toBeUndefined();
		expect(plainLines(selector, 120).join("\n")).not.toContain("Choose model…");
	});

	it("shows stale selectors and blocks values owned by a higher-precedence source", () => {
		settings.set("context.lossless.summaryModel", "deleted-provider/retired-zzzzzzz");
		const stale = createSelector();
		focusSummaryModel(stale);
		expect(plainLines(stale, 120).join("\n")).toContain("deleted-provider/retired-zzzzzzz (unavailable)");
		stale.handleInput("\n");
		expect(plainLines(stale, 120).join("\n")).toContain("Unavailable selector");

		settings.override("context.lossless.summaryModel", "runtime/owned-selector");
		const readOnly = createSelector();
		focusSummaryModel(readOnly);
		const row = plainLines(readOnly, 120).join("\n");
		expect(row).toContain("Read-only");
		expect(row).toContain("runtime override");
		readOnly.handleInput("\n");
		readOnly.handleInput("\n"); // Back is the only action.
		expect(settings.get("context.lossless.summaryModel")).toBe("runtime/owned-selector");
		expect(settings.getSettingProvenance("context.lossless.summaryModel")).toBe("runtime");
	});

	it("keeps the searchable browser bounded in a narrow ASCII-symbol terminal", async () => {
		await initTheme(false, "ascii");
		try {
			const selector = createSelector(Array.from({ length: 10 }, (_, index) => model("custom", `summary-${index}`)));
			focusSummaryModel(selector);
			selector.handleInput("\n");
			const actionLines = selector.render(36);
			const actionPlain = actionLines.map(line => Bun.stripANSI(line)).join("\n");
			expect(actionLines.every(line => visibleWidth(line) <= 36)).toBe(true);
			expect([...actionPlain].every(char => char.charCodeAt(0) <= 0x7f)).toBe(true);
			expect(actionPlain).toContain("Choose model...");
			selector.handleInput("\x1b[B");
			selector.handleInput("\n");

			const lines = selector.render(36);
			const plain = lines.map(line => Bun.stripANSI(line)).join("\n");
			expect(theme.getSymbolPreset()).toBe("ascii");
			expect(lines.every(line => visibleWidth(line) <= 36)).toBe(true);
			expect([...plain].every(char => char.charCodeAt(0) <= 0x7f)).toBe(true);
			expect(plain).toContain("Type to search |");
		} finally {
			await initTheme(false, "unicode");
		}
	});
	it("repairs a malformed string concurrency value through the schema-typed submenu", () => {
		const path = "context.lossless.maxConcurrentSummaries" as const;
		settings.set(path, "2" as never);
		const changes: Array<[SettingPath, unknown]> = [];
		const selector = createSelector(MODELS, (changedPath, value) => changes.push([changedPath, value]));
		focusConcurrentSummaries(selector);

		selector.handleInput("\n");
		selector.handleInput("\x1b[B");
		selector.handleInput("\x1b[B");
		selector.handleInput("\n");

		expect(settings.get(path)).toBe(4);
		expect(typeof settings.get(path)).toBe("number");
		expect(changes.at(-1)).toEqual([path, 4]);
	});
});
