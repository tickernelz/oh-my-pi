import { afterEach, beforeEach, describe, expect, it, vi } from "bun:test";
import type { AssistantMessage } from "@oh-my-pi/pi-ai";
import { resetSettingsForTest, Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import { EventController } from "@oh-my-pi/pi-coding-agent/modes/controllers/event-controller";
import { initTheme } from "@oh-my-pi/pi-coding-agent/modes/theme/theme";
import type { InteractiveModeContext } from "@oh-my-pi/pi-coding-agent/modes/types";
import type { AgentSessionEvent } from "@oh-my-pi/pi-coding-agent/session/agent-session-events";

async function flushMicrotasks(): Promise<void> {
	for (let i = 0; i < 10; i++) {
		await Promise.resolve();
	}
}

function createAssistantMessage(): AssistantMessage {
	return {
		role: "assistant",
		content: [{ type: "text", text: "done" }],
		api: "anthropic-messages",
		provider: "anthropic",
		model: "claude-sonnet-4-5",
		usage: {
			input: 200,
			output: 10,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 210,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: Date.now(),
	};
}

function createContext(
	options: {
		editorText?: string;
		goalObjective?: string;
		isCompacting?: boolean;
		isStreaming?: boolean;
		runIdleCompaction?: (requestTokensFloor: number, signal?: AbortSignal) => void | Promise<void>;
		runEphemeralTurn?: (args: {
			promptText: string;
			signal?: AbortSignal;
		}) => Promise<{ replyText: string; assistantMessage: AssistantMessage }>;
		sessionName?: string;
		showStatus?: (message: string, options?: { dim?: boolean }) => void;
		todoPhases?: InteractiveModeContext["todoPhases"];
	} = {},
): InteractiveModeContext {
	const runIdleCompaction = options.runIdleCompaction ?? (() => {});
	const runEphemeralTurn =
		options.runEphemeralTurn ?? (async () => ({ replyText: "", assistantMessage: createAssistantMessage() }));
	const goalState = options.goalObjective
		? {
				enabled: true,
				mode: "active",
				goal: {
					id: "goal-test",
					objective: options.goalObjective,
					status: "active",
					tokensUsed: 0,
					timeUsedSeconds: 0,
					createdAt: 0,
					updatedAt: 0,
				},
			}
		: undefined;
	const context = {
		isInitialized: true,
		loadingAnimation: undefined,
		streamingComponent: undefined,
		streamingMessage: undefined,
		transcriptMessageComponents: new WeakMap(),
		pendingTools: new Map<string, unknown>(),
		flushPendingModelSwitch: async () => {},
		flushPendingCommandOutput: () => {},
		ui: { requestRender: vi.fn(), requestComponentRender: vi.fn() },
		chatContainer: { removeChild: vi.fn() },
		statusContainer: { clear: vi.fn(), disposeChildren: vi.fn(), addChild: vi.fn() },
		statusLine: { invalidate: vi.fn(), markActivityStart: vi.fn(), markActivityEnd: vi.fn() },
		updateEditorTopBorder: vi.fn(),
		editor: { getText: () => options.editorText ?? "" },
		sessionManager: { getSessionName: () => options.sessionName },
		todoPhases: options.todoPhases ?? [],
		showStatus: options.showStatus ?? (() => {}),
		session: {
			isCompacting: options.isCompacting ?? false,
			isStreaming: options.isStreaming ?? false,
			runIdleCompaction,
			runEphemeralTurn,
			model: { provider: "anthropic", id: "claude-sonnet-4-5" },
			messages: [createAssistantMessage()],
			getContextUsage: () => ({ tokens: 210 }),
			getGoalModeState: () => goalState,
			agent: { state: { messages: [createAssistantMessage()] } },
		},
		get viewSession() {
			return (this as typeof context).session;
		},
		clearTransientSessionUi: () => {},
		clearPinnedError: vi.fn(),
		ensureLoadingAnimation: vi.fn(),
		flushCompactionQueue: async () => {},
	} as unknown as InteractiveModeContext;
	return context;
}

function createPendingIdleCompaction() {
	const started = Promise.withResolvers<AbortSignal | undefined>();
	const settled = Promise.withResolvers<void>();
	let continuedAfterProbe = false;
	const runIdleCompaction = vi.fn(async (_requestTokensFloor: number, signal?: AbortSignal) => {
		started.resolve(signal);
		if (!signal) {
			settled.resolve();
			return;
		}
		const aborted = Promise.withResolvers<void>();
		if (signal.aborted) aborted.resolve();
		else signal.addEventListener("abort", () => aborted.resolve(), { once: true });
		await aborted.promise;
		if (!signal.aborted) continuedAfterProbe = true;
		settled.resolve();
	});
	return { runIdleCompaction, started, settled, continuedAfterProbe: () => continuedAfterProbe };
}

describe("EventController idle compaction teardown", () => {
	beforeEach(async () => {
		await initTheme();
		resetSettingsForTest();
		await Settings.init({
			inMemory: true,
			overrides: {
				"compaction.idleEnabled": true,
				"compaction.idleThresholdTokens": 100,
				"compaction.idleTimeoutSeconds": 60,
			},
		});
		vi.useFakeTimers();
	});

	afterEach(() => {
		vi.useRealTimers();
		vi.restoreAllMocks();
		resetSettingsForTest();
	});

	it("cancels scheduled idle compaction when disposed", async () => {
		const runIdleCompaction = vi.fn();
		const context = createContext({ runIdleCompaction });

		const controller = new EventController(context);
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		controller.dispose();
		vi.advanceTimersByTime(60_000);

		expect(runIdleCompaction).not.toHaveBeenCalled();
	});

	it("passes the exact firing pressure into idle maintenance", async () => {
		const runIdleCompaction = vi.fn((_requestTokensFloor: number, _signal?: AbortSignal) => {});
		const context = createContext({ runIdleCompaction });

		const controller = new EventController(context);
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(60_000);
		await flushMicrotasks();

		expect(runIdleCompaction).toHaveBeenCalledWith(210, expect.any(AbortSignal));
		controller.dispose();
	});

	it("keeps a fired idle compaction alive through its own lifecycle events", async () => {
		const started = Promise.withResolvers<AbortSignal | undefined>();
		const release = Promise.withResolvers<void>();
		const settled = Promise.withResolvers<void>();
		const runIdleCompaction = vi.fn(async (_requestTokensFloor: number, signal?: AbortSignal) => {
			started.resolve(signal);
			await release.promise;
			settled.resolve();
		});
		const controller = new EventController(createContext({ runIdleCompaction }));
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(60_000);
		const signal = await started.promise;

		try {
			await controller.handleEvent({ type: "auto_compaction_start", reason: "idle", action: "context-full" });
			expect(signal?.aborted).toBe(false);
			await controller.handleEvent({
				type: "auto_compaction_end",
				action: "context-full",
				result: undefined,
				aborted: false,
				willRetry: false,
				skipped: true,
			});
			expect(signal?.aborted).toBe(false);
			expect(runIdleCompaction).toHaveBeenCalledTimes(1);
		} finally {
			release.resolve();
			await settled.promise;
			controller.dispose();
		}
	});

	it("aborts a fired idle ownership probe when a new agent turn starts", async () => {
		const probe = createPendingIdleCompaction();
		const controller = new EventController(createContext({ runIdleCompaction: probe.runIdleCompaction }));
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(60_000);
		const signal = await probe.started.promise;

		expect(signal).toBeInstanceOf(AbortSignal);
		expect(signal?.aborted).toBe(false);
		await controller.handleEvent({ type: "agent_start" } as Extract<AgentSessionEvent, { type: "agent_start" }>);
		await probe.settled.promise;

		expect(signal?.aborted).toBe(true);
		expect(probe.continuedAfterProbe()).toBe(false);
		expect(probe.runIdleCompaction).toHaveBeenCalledTimes(1);
		controller.dispose();
	});

	it("aborts a fired idle ownership probe when the controller is disposed", async () => {
		const probe = createPendingIdleCompaction();
		const controller = new EventController(createContext({ runIdleCompaction: probe.runIdleCompaction }));
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(60_000);
		const signal = await probe.started.promise;

		expect(signal).toBeInstanceOf(AbortSignal);
		expect(signal?.aborted).toBe(false);
		controller.dispose();
		await probe.settled.promise;

		expect(signal?.aborted).toBe(true);
		expect(probe.continuedAfterProbe()).toBe(false);
		expect(probe.runIdleCompaction).toHaveBeenCalledTimes(1);
	});

	it("emits an LLM-generated recap after the default four-minute delay", async () => {
		resetSettingsForTest();
		await Settings.init({
			inMemory: true,
			overrides: {
				"compaction.idleEnabled": false,
				"completion.notify": "off",
			},
		});
		const showStatus = vi.fn((_: string, _options?: { dim?: boolean }) => {});
		let capturedPrompt = "";
		const runEphemeralTurn = vi.fn(async (args: { promptText: string; signal?: AbortSignal }) => {
			capturedPrompt = args.promptText;
			return {
				replyText: "Reworking the login flow; auth suite passes. Next: wire the focused token-refresh test.",
				assistantMessage: createAssistantMessage(),
			};
		});
		const context = createContext({
			sessionName: "Fix login flow",
			showStatus,
			runEphemeralTurn,
			todoPhases: [{ name: "Work", tasks: [{ content: "Wire focused tests", status: "pending" }] }],
		});

		const controller = new EventController(context);
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(239_999);
		expect(runEphemeralTurn).not.toHaveBeenCalled();

		vi.advanceTimersByTime(1);
		await flushMicrotasks();

		expect(runEphemeralTurn).toHaveBeenCalledTimes(1);
		// Live goal/title and the active todo task anchor the recap prompt the snapshot can't guarantee.
		expect(capturedPrompt).toContain("Fix login flow");
		expect(capturedPrompt).toContain("Wire focused tests");

		expect(showStatus).toHaveBeenCalledTimes(1);
		const [message, options] = showStatus.mock.calls[0] ?? [];
		expect(Bun.stripANSI(message ?? "")).toBe(
			"※ recap: Reworking the login flow; auth suite passes. Next: wire the focused token-refresh test.",
		);
		expect(options).toEqual({ dim: false });
		controller.dispose();
	});

	it("keeps the idle recap silent when disabled", async () => {
		resetSettingsForTest();
		await Settings.init({
			inMemory: true,
			overrides: {
				"compaction.idleEnabled": false,
				"completion.notify": "off",
				"recap.enabled": false,
				"recap.idleSeconds": 1,
			},
		});
		const showStatus = vi.fn((_: string, _options?: { dim?: boolean }) => {});
		const context = createContext({
			sessionName: "Fix login flow",
			showStatus,
			todoPhases: [{ name: "Work", tasks: [{ content: "Wire focused tests", status: "pending" }] }],
		});

		const controller = new EventController(context);
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(1_000);

		expect(showStatus).not.toHaveBeenCalled();
		controller.dispose();
	});

	it("keeps the idle recap silent while the editor has a draft", async () => {
		resetSettingsForTest();
		await Settings.init({
			inMemory: true,
			overrides: {
				"compaction.idleEnabled": false,
				"completion.notify": "off",
				"recap.idleSeconds": 1,
			},
		});
		const showStatus = vi.fn((_: string, _options?: { dim?: boolean }) => {});
		const context = createContext({
			editorText: "draft",
			sessionName: "Fix login flow",
			showStatus,
			todoPhases: [{ name: "Work", tasks: [{ content: "Wire focused tests", status: "pending" }] }],
		});

		const controller = new EventController(context);
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(1_000);

		expect(showStatus).not.toHaveBeenCalled();
		controller.dispose();
	});

	it("aborts the in-flight recap and drops its late reply when disposed", async () => {
		resetSettingsForTest();
		await Settings.init({
			inMemory: true,
			overrides: {
				"compaction.idleEnabled": false,
				"completion.notify": "off",
			},
		});
		const showStatus = vi.fn((_: string, _options?: { dim?: boolean }) => {});
		const { promise, resolve } = Promise.withResolvers<{ replyText: string; assistantMessage: AssistantMessage }>();
		let receivedSignal: AbortSignal | undefined;
		const runEphemeralTurn = vi.fn((args: { promptText: string; signal?: AbortSignal }) => {
			receivedSignal = args.signal;
			return promise;
		});
		const context = createContext({ sessionName: "Fix login flow", showStatus, runEphemeralTurn });

		const controller = new EventController(context);
		await controller.handleEvent({ type: "agent_end", messages: [createAssistantMessage()] });
		vi.advanceTimersByTime(240_000);
		await flushMicrotasks();

		expect(runEphemeralTurn).toHaveBeenCalledTimes(1);
		expect(receivedSignal?.aborted).toBe(false);

		controller.dispose();
		expect(receivedSignal?.aborted).toBe(true);

		// A reply that lands after cancellation must not paint a stale recap.
		resolve({ replyText: "stale recap", assistantMessage: createAssistantMessage() });
		await flushMicrotasks();
		expect(showStatus).not.toHaveBeenCalled();
	});
});
