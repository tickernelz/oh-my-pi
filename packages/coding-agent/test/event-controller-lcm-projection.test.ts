import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "bun:test";
import { activeSourceFingerprint, type ContextProjection } from "@oh-my-pi/lcm-context";
import type { AssistantMessage, Usage } from "@oh-my-pi/pi-ai";
import { resetSettingsForTest, Settings, settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import { AssistantMessageComponent } from "@oh-my-pi/pi-coding-agent/modes/components/assistant-message";
import { TranscriptContainer } from "@oh-my-pi/pi-coding-agent/modes/components/transcript-container";
import { EventController } from "@oh-my-pi/pi-coding-agent/modes/controllers/event-controller";
import { printableEvent } from "@oh-my-pi/pi-coding-agent/modes/print-mode";
import { initTheme } from "@oh-my-pi/pi-coding-agent/modes/theme/theme";
import type { InteractiveModeContext } from "@oh-my-pi/pi-coding-agent/modes/types";
import type { AgentSessionEvent } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { convertToLlm, SILENT_ABORT_MARKER } from "@oh-my-pi/pi-coding-agent/session/messages";
import type { TUI } from "@oh-my-pi/pi-tui";

function zeroUsage(): Usage {
	return {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 0,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

function assistantMessage(timestamp: number, stopReason: AssistantMessage["stopReason"] = "stop"): AssistantMessage {
	return {
		role: "assistant",
		content: [{ type: "text", text: `answer ${timestamp}` }],
		api: "anthropic-messages",
		provider: "anthropic",
		model: "test-model",
		stopReason,
		usage: zeroUsage(),
		timestamp,
	};
}

function projection(overrides: Partial<ContextProjection> = {}): ContextProjection {
	return {
		revision: 1,
		activeSourceFingerprint: activeSourceFingerprint(["source-a", "source-b", "fresh"]),
		ready: true,
		historical: [
			{
				kind: "summary",
				summaryId: "summary-a",
				summaryHandle: "handle-a",
				level: 0,
				redactedText: "summary a",
				tokenCount: 100,
				sourceIds: ["source-a"],
				citations: [],
				files: [],
			},
			{
				kind: "summary",
				summaryId: "summary-b",
				summaryHandle: "handle-b",
				level: 0,
				redactedText: "summary b",
				tokenCount: 100,
				sourceIds: ["source-b"],
				citations: [],
				files: [],
			},
		],
		freshTailSourceIds: ["fresh"],
		uncoveredSourceIds: [],
		estimatedTokens: 6_000,
		pendingJobs: 0,
		sourceTokens: 32_000,
		selectedLevelCounts: { 0: 2 },
		coveredSourceCount: 12,
		freshSourceCount: 1,
		...overrides,
	};
}

function count(text: string, needle: string): number {
	return text.split(needle).length - 1;
}

function createFixture() {
	const chatContainer = new TranscriptContainer();
	const appendMessage = vi.fn();
	const addMessageToChat = vi.fn();
	const ui = {
		requestRender: vi.fn(),
		requestComponentRender: vi.fn(),
		imageBudget: undefined,
	} as unknown as TUI;
	const viewSession = {
		extensionRunner: undefined,
		isStreaming: false,
		isRetrying: false,
		isTtsrAbortPending: false,
		retryAttempt: 0,
	};
	const ctx = {
		isInitialized: true,
		init: vi.fn(async () => {}),
		ui,
		settings,
		chatContainer,
		transcriptMessageComponents: new WeakMap(),
		pendingTools: new Map(),
		toolOutputExpanded: false,
		effectiveHideThinkingBlock: false,
		proseOnlyThinking: true,
		statusLine: { invalidate: vi.fn() },
		noteDisplayableThinkingContent: vi.fn(() => false),
		session: viewSession,
		viewSession,
		sessionManager: { appendMessage, getCwd: () => process.cwd() },
		addMessageToChat,
		lastAssistantUsage: zeroUsage(),
	} as unknown as InteractiveModeContext;
	return { controller: new EventController(ctx), chatContainer, appendMessage, addMessageToChat, viewSession };
}

async function send(controller: EventController, event: AgentSessionEvent): Promise<void> {
	await controller.handleEvent(event);
}

async function completeResponse(controller: EventController, message: AssistantMessage): Promise<void> {
	await send(controller, { type: "message_start", message });
	await send(controller, { type: "message_end", message });
}

function rendered(container: TranscriptContainer): string {
	return Bun.stripANSI(container.render(100).join("\n"));
}

beforeAll(async () => {
	await initTheme(false);
});

beforeEach(async () => {
	resetSettingsForTest();
	await Settings.init({
		inMemory: true,
		overrides: { "display.smoothStreaming": false },
	});
});

afterEach(() => {
	vi.restoreAllMocks();
	resetSettingsForTest();
});

describe("EventController LCM projection evidence", () => {
	it("renders projection evidence only after the response completes", async () => {
		settings.set("display.showTokenUsage", true);
		const { controller, chatContainer } = createFixture();
		const message = {
			...assistantMessage(1),
			usage: { ...zeroUsage(), input: 123, output: 7, totalTokens: 130 },
			duration: 1_000,
		} satisfies AssistantMessage;
		await send(controller, { type: "lcm_projection", projection: projection() });
		await send(controller, { type: "message_start", message });
		expect(rendered(chatContainer)).not.toContain("LCM context");

		await send(controller, { type: "message_end", message });
		const lines = rendered(chatContainer).split("\n");
		const usageIndex = lines.findIndex(line => line.includes("123"));
		const footerRows = lines.filter(line => line.includes("LCM context"));
		const footerIndex = lines.findIndex(line => line.includes("LCM context"));
		expect(footerRows).toHaveLength(1);
		expect(footerIndex).toBe(usageIndex + 1);
	});

	it("keeps LCM evidence visible when token usage is hidden", async () => {
		settings.set("display.showTokenUsage", false);
		const { controller, chatContainer } = createFixture();
		const message = {
			...assistantMessage(1),
			usage: { ...zeroUsage(), input: 321, output: 7, totalTokens: 328 },
		} satisfies AssistantMessage;
		await send(controller, { type: "lcm_projection", projection: projection() });
		await completeResponse(controller, message);

		const text = rendered(chatContainer);
		expect(text).toContain("LCM context");
		expect(text).not.toContain("321");
	});

	it("renders one footer per meaningful DAG boundary", async () => {
		const { controller, chatContainer } = createFixture();
		await send(controller, { type: "lcm_projection", projection: projection() });
		await completeResponse(controller, assistantMessage(1));

		await send(controller, { type: "lcm_projection", projection: projection({ revision: 2 }) });
		await completeResponse(controller, assistantMessage(2));

		await send(controller, {
			type: "lcm_projection",
			projection: projection({ revision: 3, selectedLevelCounts: { 0: 2, 1: 1 }, coveredSourceCount: 18 }),
		});
		await completeResponse(controller, assistantMessage(3));

		expect(count(rendered(chatContainer), "LCM context")).toBe(2);
	});

	it("does not render evidence without a complete fitted projection event", async () => {
		const { controller, chatContainer } = createFixture();
		await completeResponse(controller, assistantMessage(1));
		await send(controller, { type: "lcm_projection", projection: projection({ ready: false }) });
		await completeResponse(controller, assistantMessage(2));
		await send(controller, { type: "lcm_projection", projection: projection({ pendingJobs: 1 }) });
		await completeResponse(controller, assistantMessage(3));
		await send(controller, {
			type: "lcm_projection",
			projection: projection({ uncoveredSourceIds: ["missing"] }),
		});
		await completeResponse(controller, assistantMessage(4));
		expect(rendered(chatContainer)).not.toContain("LCM context");
	});

	it("dedupes the same projection across a failed response and its retry", async () => {
		const { controller, chatContainer } = createFixture();
		await send(controller, { type: "lcm_projection", projection: projection() });
		await completeResponse(controller, assistantMessage(1, "error"));
		await send(controller, { type: "lcm_projection", projection: projection({ revision: 2 }) });
		await completeResponse(controller, assistantMessage(2));
		expect(count(rendered(chatContainer), "LCM context")).toBe(1);
	});

	it("does not carry evidence from an incomplete response into its replacement", async () => {
		const { controller, chatContainer } = createFixture();
		await send(controller, { type: "lcm_projection", projection: projection() });
		await send(controller, { type: "message_start", message: assistantMessage(1, "aborted") });
		await completeResponse(controller, assistantMessage(2));
		expect(rendered(chatContainer)).not.toContain("LCM context");
	});

	it("reattaches the same boundary when an incomplete response is replaced", async () => {
		const { controller, chatContainer } = createFixture();
		await send(controller, { type: "lcm_projection", projection: projection() });
		await send(controller, { type: "message_start", message: assistantMessage(1, "aborted") });
		await send(controller, { type: "lcm_projection", projection: projection({ revision: 2 }) });
		await completeResponse(controller, assistantMessage(2));
		expect(count(rendered(chatContainer), "LCM context")).toBe(1);
	});

	it("drops an early duplicate after the active response completes", async () => {
		const { controller, chatContainer } = createFixture();
		const message = assistantMessage(1);
		await send(controller, { type: "lcm_projection", projection: projection() });
		await send(controller, { type: "message_start", message });
		await send(controller, { type: "lcm_projection", projection: projection({ revision: 2 }) });
		await send(controller, { type: "message_end", message });
		await completeResponse(controller, assistantMessage(2));
		expect(count(rendered(chatContainer), "LCM context")).toBe(1);
	});

	it("defers evidence from a silently aborted response to its replacement", async () => {
		const { controller, chatContainer } = createFixture();
		const aborted = { ...assistantMessage(1, "aborted"), errorMessage: SILENT_ABORT_MARKER };
		await send(controller, { type: "lcm_projection", projection: projection() });
		await completeResponse(controller, aborted);
		expect(rendered(chatContainer)).not.toContain("LCM context");

		await send(controller, { type: "lcm_projection", projection: projection({ revision: 2 }) });
		await completeResponse(controller, assistantMessage(2));
		expect(count(rendered(chatContainer), "LCM context")).toBe(1);
	});

	it("defers evidence from a TTSR-silenced response to its replacement", async () => {
		const { controller, chatContainer, viewSession } = createFixture();
		viewSession.isTtsrAbortPending = true;
		await send(controller, { type: "lcm_projection", projection: projection() });
		await completeResponse(controller, assistantMessage(1, "aborted"));
		expect(rendered(chatContainer)).not.toContain("LCM context");

		viewSession.isTtsrAbortPending = false;
		await send(controller, { type: "lcm_projection", projection: projection({ revision: 2 }) });
		await completeResponse(controller, assistantMessage(2));
		expect(count(rendered(chatContainer), "LCM context")).toBe(1);
	});

	it("does not let a duplicate old retry event erase a newer queued boundary", async () => {
		const { controller, chatContainer } = createFixture();
		const oldBoundary = projection();
		const newBoundary = projection({ selectedLevelCounts: { 0: 2, 1: 1 }, coveredSourceCount: 18 });
		await send(controller, { type: "lcm_projection", projection: oldBoundary });
		await completeResponse(controller, assistantMessage(1));
		await send(controller, { type: "lcm_projection", projection: newBoundary });
		await send(controller, { type: "lcm_projection", projection: oldBoundary });
		await completeResponse(controller, assistantMessage(2));
		expect(count(rendered(chatContainer), "LCM context")).toBe(2);
	});

	it("keeps the footer out of journal messages, provider content, and replay", async () => {
		const { controller, chatContainer, appendMessage, addMessageToChat } = createFixture();
		const message = assistantMessage(1);
		const event = { type: "lcm_projection", projection: projection() } satisfies AgentSessionEvent;
		expect(printableEvent(event)).toBeUndefined();
		await send(controller, event);
		expect(chatContainer.children).toHaveLength(0);
		expect(appendMessage).not.toHaveBeenCalled();
		expect(addMessageToChat).not.toHaveBeenCalled();

		await completeResponse(controller, message);
		expect(rendered(chatContainer)).toContain("LCM context");
		expect(JSON.stringify(message)).not.toContain("lcm_projection");
		expect(JSON.stringify(convertToLlm([message]))).not.toContain("LCM context");
		expect(Bun.stripANSI(new AssistantMessageComponent(message).render(100).join("\n"))).not.toContain("LCM context");
		expect(appendMessage).not.toHaveBeenCalled();
		expect(addMessageToChat).not.toHaveBeenCalled();
	});
});
