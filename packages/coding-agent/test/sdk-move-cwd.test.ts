import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import { createAgentSession } from "@oh-my-pi/pi-coding-agent/sdk";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { removeSyncWithRetries, Snowflake } from "@oh-my-pi/pi-utils";

function textContent(result: { content?: Array<{ type: string; text?: string }> }): string {
	return (
		result.content
			?.filter(
				(block): block is { type: "text"; text: string } => block.type === "text" && typeof block.text === "string",
			)
			.map(block => block.text)
			.join("\n") ?? ""
	);
}

describe("createAgentSession cwd after /move", () => {
	const tempDirs: string[] = [];

	afterEach(() => {
		for (const tempDir of tempDirs.splice(0)) {
			removeSyncWithRetries(tempDir);
		}
	});

	it("uses the configured summary width and destination LCM settings after move", async () => {
		const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-sdk-move-cwd-${Snowflake.next()}-`));
		tempDirs.push(tempDir);
		const cwdA = path.join(tempDir, "cwd-a");
		const cwdB = path.join(tempDir, "cwd-b");
		fs.mkdirSync(cwdA, { recursive: true });
		fs.mkdirSync(cwdB, { recursive: true });
		fs.mkdirSync(path.join(cwdA, ".omp"), { recursive: true });
		fs.mkdirSync(path.join(cwdB, ".omp"), { recursive: true });
		fs.writeFileSync(
			path.join(cwdA, ".omp", "config.yml"),
			"context:\n  engine: lossless\n  lossless:\n    summaryModel: source/model\n    maxConcurrentSummaries: 1\n",
		);
		fs.writeFileSync(
			path.join(cwdB, ".omp", "config.yml"),
			"context:\n  engine: lossless\n  lossless:\n    summaryModel: destination/model\n    maxConcurrentSummaries: 4\n",
		);
		const settings = await Settings.loadIsolated({
			cwd: cwdA,
			agentDir: tempDir,
			overrides: {
				"async.enabled": false,
				"bash.autoBackground.enabled": false,
				"bashInterceptor.enabled": false,
			},
		});

		const sessionManager = SessionManager.create(cwdA, path.join(tempDir, "sessions"));
		const { session } = await createAgentSession({
			cwd: cwdA,
			agentDir: tempDir,
			sessionManager,
			settings,
			model: getBundledModel("openai", "gpt-4o-mini"),
			disableExtensionDiscovery: true,
			skills: [],
			contextFiles: [],
			promptTemplates: [],
			slashCommands: [],
			enableMCP: false,
			enableLsp: false,
			toolNames: ["bash"],
		});

		try {
			const sourceStatus = await session.lcmStatus();
			expect(sourceStatus.runtime).toMatchObject({
				summaryModelSelector: "source/model",
				summaryWorkers: { limit: 1 },
			});
			await session.moveSession(cwdB);
			await settings.reloadForCwd(cwdB);
			await session.refreshLcmSettingsAndRebind();
			const destinationStatus = await session.lcmStatus();
			expect(destinationStatus.runtime).toMatchObject({
				summaryModelSelector: "destination/model",
				summaryWorkers: { limit: 4 },
			});

			const bashTool = session.getToolByName("bash");
			if (!bashTool) throw new Error("Expected bash tool");
			const result = await bashTool.execute("pwd-after-move", { command: "pwd" });

			expect(textContent(result)).toContain(cwdB);
		} finally {
			await session.dispose();
		}
	});

	it("reports the normalized worker limit while lossless context is disabled", async () => {
		const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-sdk-native-lcm-${Snowflake.next()}-`));
		tempDirs.push(tempDir);
		const cwd = path.join(tempDir, "cwd");
		fs.mkdirSync(cwd, { recursive: true });
		const { session } = await createAgentSession({
			cwd,
			agentDir: tempDir,
			sessionManager: SessionManager.inMemory(cwd),
			settings: Settings.isolated({
				"async.enabled": false,
				"context.engine": "native",
				"context.lossless.maxConcurrentSummaries": 99,
			}),
			model: getBundledModel("openai", "gpt-4o-mini"),
			disableExtensionDiscovery: true,
			skills: [],
			contextFiles: [],
			promptTemplates: [],
			slashCommands: [],
			enableMCP: false,
			enableLsp: false,
			toolNames: [],
		});

		try {
			expect(await session.lcmStatus()).toMatchObject({
				runtime: {
					phase: "disabled",
					summaryWorkers: { active: 0, limit: 4 },
				},
			});
		} finally {
			await session.dispose();
		}
	});
});
