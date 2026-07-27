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

const OWN_SESSION_LCM_TOOLS = ["lcm_search", "lcm_describe", "lcm_recall", "lcm_cross_project_search"] as const;

describe("createAgentSession cwd after /move", () => {
	const tempDirs: string[] = [];

	afterEach(() => {
		for (const tempDir of tempDirs.splice(0)) {
			removeSyncWithRetries(tempDir);
		}
	});

	it("creates destination LCM runtime after moving from native to lossless", async () => {
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
			"context:\n  engine: native\n  lossless:\n    summaryModel: source/model\n    maxConcurrentSummaries: 1\n",
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
				"tools.xdev": false,
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
		});

		try {
			expect(session.lcmEnabled).toBe(false);
			const nativeToolInstances = new Map(OWN_SESSION_LCM_TOOLS.map(name => [name, session.getToolByName(name)]));
			for (const name of OWN_SESSION_LCM_TOOLS) {
				expect(nativeToolInstances.get(name)).toBeDefined();
				expect(session.getEnabledToolNames()).not.toContain(name);
			}
			expect(await session.lcmStatus()).toMatchObject({
				runtime: {
					phase: "disabled",
					summaryWorkers: { active: 0, limit: 1 },
				},
			});

			await session.moveSession(cwdB);
			await settings.reloadForCwd(cwdB);
			await session.refreshLcmSettingsAndRebind();

			expect(session.lcmEnabled).toBe(true);
			expect(await session.lcmStatus()).toMatchObject({
				runtime: {
					summaryModelSelector: "destination/model",
					summaryWorkers: { limit: 4 },
				},
			});
			for (const name of OWN_SESSION_LCM_TOOLS) {
				expect(session.getActiveToolNames()).toContain(name);
				expect(session.getToolByName(name)).toBe(nativeToolInstances.get(name));
			}

			const bashTool = session.getToolByName("bash");
			if (!bashTool) throw new Error("Expected bash tool");
			const result = await bashTool.execute("pwd-after-move", { command: "pwd" });

			expect(textContent(result)).toContain(cwdB);
		} finally {
			await session.dispose();
		}
	});

	it("disposes the source LCM runtime after moving from lossless to native", async () => {
		const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-sdk-move-native-${Snowflake.next()}-`));
		tempDirs.push(tempDir);
		const cwdA = path.join(tempDir, "cwd-a");
		const cwdB = path.join(tempDir, "cwd-b");
		fs.mkdirSync(path.join(cwdA, ".omp"), { recursive: true });
		fs.mkdirSync(path.join(cwdB, ".omp"), { recursive: true });
		fs.writeFileSync(
			path.join(cwdA, ".omp", "config.yml"),
			"context:\n  engine: lossless\n  lossless:\n    summaryModel: source/model\n    maxConcurrentSummaries: 2\n",
		);
		fs.writeFileSync(
			path.join(cwdB, ".omp", "config.yml"),
			"context:\n  engine: native\n  lossless:\n    summaryModel: destination/model\n    maxConcurrentSummaries: 99\n",
		);
		const settings = await Settings.loadIsolated({
			cwd: cwdA,
			agentDir: tempDir,
			overrides: { "async.enabled": false, "tools.xdev": true },
		});
		const { session } = await createAgentSession({
			cwd: cwdA,
			agentDir: tempDir,
			sessionManager: SessionManager.create(cwdA, path.join(tempDir, "sessions")),
			settings,
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
			expect(session.lcmEnabled).toBe(true);
			const losslessToolInstances = new Map(OWN_SESSION_LCM_TOOLS.map(name => [name, session.getToolByName(name)]));
			for (const name of OWN_SESSION_LCM_TOOLS) {
				expect(losslessToolInstances.get(name)).toBeDefined();
				expect(session.getEnabledToolNames()).toContain(name);
				expect(session.getActiveToolNames()).not.toContain(name);
				expect(session.getMountedXdevToolNames()).toContain(name);
			}
			expect(session.getActiveToolNames()).toContain("write");
			expect(await session.lcmStatus()).toMatchObject({
				runtime: {
					summaryModelSelector: "source/model",
					summaryWorkers: { limit: 2 },
				},
			});

			await session.moveSession(cwdB);
			await settings.reloadForCwd(cwdB);
			await session.refreshLcmSettingsAndRebind();

			expect(session.lcmEnabled).toBe(false);
			expect(await session.lcmStatus()).toMatchObject({
				runtime: {
					phase: "disabled",
					summaryWorkers: { active: 0, limit: 4 },
				},
			});
			for (const name of OWN_SESSION_LCM_TOOLS) {
				expect(session.getEnabledToolNames()).not.toContain(name);
				expect(session.getMountedXdevToolNames()).not.toContain(name);
				expect(session.getToolByName(name)).toBe(losslessToolInstances.get(name));
			}
		} finally {
			await session.dispose();
		}
	});
});
