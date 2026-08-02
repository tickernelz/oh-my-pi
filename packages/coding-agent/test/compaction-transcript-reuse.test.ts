import { beforeAll, describe, expect, it, vi } from "bun:test";
import { activeSourceFingerprint, type ContextProjection } from "@oh-my-pi/lcm-context";
import type { AgentMessage } from "@oh-my-pi/pi-agent-core";
import type { Message } from "@oh-my-pi/pi-ai";
import { AssistantMessageComponent } from "@oh-my-pi/pi-coding-agent/modes/components/assistant-message";
import { TranscriptContainer } from "@oh-my-pi/pi-coding-agent/modes/components/transcript-container";
import { createResponseFooterBlock } from "@oh-my-pi/pi-coding-agent/modes/components/usage-row";
import { initTheme } from "@oh-my-pi/pi-coding-agent/modes/theme/theme";
import type { InteractiveModeContext } from "@oh-my-pi/pi-coding-agent/modes/types";
import { UiHelpers } from "@oh-my-pi/pi-coding-agent/modes/utils/ui-helpers";

function buildContext(): InteractiveModeContext {
	const chatContainer = new TranscriptContainer();
	return {
		chatContainer,
		transcriptMessageComponents: new WeakMap(),
		pendingTools: new Map(),
		lastAssistantUsage: undefined,
		getUserMessageText: (message: Message) =>
			message.role === "user" && typeof message.content === "string" ? message.content : "",
		viewSession: {
			extensionRunner: undefined,
			sessionManager: { putBlobSync: () => "unused" },
			isStreaming: false,
			retryAttempt: 0,
		},
		ui: { requestRender: vi.fn(), imageBudget: undefined },
		settings: { get: vi.fn(() => false) },
		effectiveHideThinkingBlock: false,
		proseOnlyThinking: true,
		editor: { addToHistory: vi.fn() },
	} as unknown as InteractiveModeContext;
}

function projection(): ContextProjection {
	return {
		revision: 1,
		activeSourceFingerprint: activeSourceFingerprint(["source", "fresh"]),
		ready: true,
		historical: [],
		freshTailSourceIds: ["fresh"],
		uncoveredSourceIds: [],
		sourceTokens: 20_000,
		selectedLevelCounts: { 0: 1 },
		coveredSourceCount: 8,
		freshSourceCount: 1,
		estimatedTokens: 4_000,
		pendingJobs: 0,
	};
}

beforeAll(async () => {
	await initTheme(false);
});

describe("post-compaction transcript reuse", () => {
	it("retains settled user and assistant components across a rebuild", () => {
		const ctx = buildContext();
		const helpers = new UiHelpers(ctx);
		ctx.addMessageToChat = helpers.addMessageToChat.bind(helpers);
		const messages: AgentMessage[] = [
			{ role: "user", content: "large settled user turn", timestamp: Date.now() },
			{
				role: "assistant",
				content: [{ type: "text", text: "## Large settled assistant turn\n\n```ts\nconst retained = true;\n```" }],
				api: "anthropic-messages",
				provider: "anthropic",
				model: "test",
				usage: {
					input: 1,
					output: 1,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: 2,
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				},
				stopReason: "stop",
				timestamp: Date.now(),
			},
		];

		for (const message of messages) helpers.addMessageToChat(message);
		const settledComponents = [...ctx.chatContainer.children];
		const assistant = settledComponents.find(
			(component): component is AssistantMessageComponent => component instanceof AssistantMessageComponent,
		);
		if (!assistant) throw new Error("Expected a settled assistant component");
		assistant.setCacheInvalidation({ reprocessedTokens: 8_000 });
		ctx.chatContainer.addChild(createResponseFooterBlock({ lcmProjection: projection() }));
		expect(Bun.stripANSI(ctx.chatContainer.render(100).join("\n"))).toContain("LCM context");
		ctx.chatContainer.clear();
		for (const message of messages) {
			helpers.addMessageToChat(message, { reuseSettledComponent: true });
		}
		expect(ctx.chatContainer.children).toEqual(settledComponents);
		expect(Bun.stripANSI(ctx.chatContainer.render(100).join("\n"))).toContain("cache miss");
		expect(Bun.stripANSI(ctx.chatContainer.render(100).join("\n"))).not.toContain("LCM context");

		ctx.chatContainer.addChild(createResponseFooterBlock({ lcmProjection: projection() }));
		ctx.chatContainer.clear();
		helpers.renderSessionContext(
			{ messages: [messages[1]!], models: {}, injectedTtsrRules: [], mode: "none" },
			{ reuseSettledComponents: true },
		);
		expect(ctx.chatContainer.children).toEqual([assistant]);
		expect(Bun.stripANSI(ctx.chatContainer.render(100).join("\n"))).toContain("cache miss");
		expect(Bun.stripANSI(ctx.chatContainer.render(100).join("\n"))).not.toContain("LCM context");
	});
});
