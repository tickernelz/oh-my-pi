import { describe, expect, it, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import type { InteractiveModeContext } from "@oh-my-pi/pi-coding-agent/modes/types";
import {
	executeBuiltinSlashCommand,
	lookupBuiltinSlashCommand,
	type SlashCommandRuntime,
} from "@oh-my-pi/pi-coding-agent/slash-commands/builtin-registry";
import { getProjectDir, setProjectDir } from "@oh-my-pi/pi-utils";

function createRuntime() {
	const handleMoveCommand = vi.fn(async () => {});
	const showError = vi.fn();
	const setText = vi.fn();
	const addToHistory = vi.fn();
	return {
		handleMoveCommand,
		showError,
		setText,
		addToHistory,
		runtime: {
			ctx: {
				editor: { setText, addToHistory } as unknown as InteractiveModeContext["editor"],
				showError,
				handleMoveCommand,
			} as unknown as InteractiveModeContext,
		},
	};
}

describe("/move slash command", () => {
	it("routes the path through the move handler and saves the full command to history", async () => {
		const harness = createRuntime();

		const handled = await executeBuiltinSlashCommand("/move /tmp/project", harness.runtime);

		expect(handled).toBe(true);
		expect(harness.setText).toHaveBeenCalledWith("");
		expect(harness.handleMoveCommand).toHaveBeenCalledWith("/tmp/project");
	});

	it("routes a blank /move invocation to the interactive move handler", async () => {
		const harness = createRuntime();

		const handled = await executeBuiltinSlashCommand("/move   ", harness.runtime);

		expect(handled).toBe(true);
		expect(harness.showError).not.toHaveBeenCalled();
		expect(harness.setText).toHaveBeenCalledWith("");
		expect(harness.handleMoveCommand).toHaveBeenCalledWith(undefined);
	});

	it("reloads destination settings before the non-TUI LCM rebind", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-slash-move-"));
		const sourceDir = path.join(root, "source");
		const targetDir = path.join(root, "target");
		const agentDir = path.join(root, "agent");
		const originalProjectDir = getProjectDir();
		await fs.mkdir(path.join(sourceDir, ".omp"), { recursive: true });
		await fs.mkdir(path.join(targetDir, ".omp"), { recursive: true });
		await fs.mkdir(agentDir, { recursive: true });
		await fs.writeFile(
			path.join(sourceDir, ".omp", "config.yml"),
			"context:\n  engine: lossless\n  lossless:\n    summaryModel: source/model\n    maxConcurrentSummaries: 1\n",
		);
		await fs.writeFile(
			path.join(targetDir, ".omp", "config.yml"),
			"context:\n  engine: lossless\n  lossless:\n    summaryModel: target/model\n    maxConcurrentSummaries: 4\n",
		);

		try {
			const settings = await Settings.loadIsolated({ cwd: sourceDir, agentDir });
			const events: string[] = [];
			const state = { cwd: sourceDir };
			const reloadForCwd = settings.reloadForCwd.bind(settings);
			vi.spyOn(settings, "reloadForCwd").mockImplementation(async cwd => {
				await reloadForCwd(cwd);
				events.push(
					`reload:${settings.get("context.lossless.summaryModel")}:${settings.get("context.lossless.maxConcurrentSummaries")}`,
				);
			});
			const runtime = {
				session: {
					isStreaming: false,
					moveSession: async (cwd: string) => {
						state.cwd = cwd;
						events.push("manager-move");
					},
					refreshLcmSettingsAndRebind: async () => {
						events.push(
							`rebind-start:${settings.get("context.lossless.summaryModel")}:${settings.get("context.lossless.maxConcurrentSummaries")}`,
						);
						await Promise.resolve();
						events.push("rebind-finished");
					},
				},
				sessionManager: { getCwd: () => state.cwd },
				settings,
				cwd: sourceDir,
				output: vi.fn(async () => {}),
				refreshCommands: vi.fn(async () => {}),
				reloadPlugins: vi.fn(async () => {
					events.push("reload-plugins");
				}),
			} as unknown as SlashCommandRuntime;
			const move = lookupBuiltinSlashCommand("move");
			if (!move?.handle) throw new Error("Expected non-TUI /move handler");

			await move.handle({ name: "move", args: targetDir, text: `/move ${targetDir}` }, runtime);

			expect(events).toEqual([
				"manager-move",
				"reload:target/model:4",
				"rebind-start:target/model:4",
				"rebind-finished",
				"reload-plugins",
			]);
			expect(events.join("\n")).not.toContain("source/model");
		} finally {
			setProjectDir(originalProjectDir);
			await fs.rm(root, { recursive: true, force: true });
		}
	});
});
