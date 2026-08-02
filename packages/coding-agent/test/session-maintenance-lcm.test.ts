import { describe, expect, it, type Mock, vi } from "bun:test";
import { activeSourceFingerprint, type ContextProjection } from "@oh-my-pi/lcm-context";
import { Agent, type AgentMessage, type AgentTurnEndContext } from "@oh-my-pi/pi-agent-core";
import type { AssistantMessage, Model } from "@oh-my-pi/pi-ai";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import type { LcmOwnershipDecision } from "@oh-my-pi/pi-coding-agent/session/session-lcm";
import {
	COMPACTION_CHECK_NONE,
	SessionMaintenance,
	type SessionMaintenanceHost,
} from "@oh-my-pi/pi-coding-agent/session/session-maintenance";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { createAssistantMessage } from "./helpers/agent-session-setup";

const bundledModel = getBundledModel("anthropic", "claude-sonnet-4-5");
if (!bundledModel) throw new Error("Expected bundled Anthropic model");
const MODEL: Model = { ...bundledModel, contextWindow: 1_000, maxTokens: 100 };

const OWNED_PROJECTION: ContextProjection = {
	revision: 1,
	activeSourceFingerprint: activeSourceFingerprint([]),
	ready: true,
	historical: [],
	freshTailSourceIds: [],
	uncoveredSourceIds: [],
	sourceTokens: 0,
	selectedLevelCounts: {},
	coveredSourceCount: 0,
	freshSourceCount: 0,
	estimatedTokens: 0,
	pendingJobs: 0,
};
const OWNED_DECISION = { kind: "owned", projection: OWNED_PROJECTION } satisfies LcmOwnershipDecision;

function contextBreakdown(usedTokens: number, contextWindow = MODEL.contextWindow ?? 0) {
	return {
		contextWindow,
		anchored: false,
		usedTokens,
		systemPromptTokens: 0,
		systemToolsTokens: 0,
		systemContextTokens: 0,
		skillsTokens: 0,
		messagesTokens: usedTokens,
	};
}

function assistant(
	stopReason: AssistantMessage["stopReason"],
	tokens: number,
	errorMessage?: string,
): AssistantMessage {
	return {
		...createAssistantMessage("assistant result"),
		provider: MODEL.provider,
		model: MODEL.id,
		stopReason,
		errorMessage,
		usage: {
			input: tokens,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: tokens,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
	};
}

interface MaintenanceHarness {
	maintenance: SessionMaintenance;
	host: SessionMaintenanceHost;
	agent: Agent;
	manager: SessionManager;
	losslessOwnsRequest: Mock<SessionMaintenanceHost["losslessOwnsRequest"]>;
	rearmLosslessPrimaryIntent: Mock<SessionMaintenanceHost["rearmLosslessPrimaryIntent"]>;
	runRecoveryCompactionWithRollback: Mock<SessionMaintenanceHost["runRecoveryCompactionWithRollback"]>;
	scheduleAgentContinue: Mock<SessionMaintenanceHost["scheduleAgentContinue"]>;
	removeAssistantMessageFromActiveContext: Mock<SessionMaintenanceHost["removeAssistantMessageFromActiveContext"]>;
	dropPersistedAssistantTurn: Mock<SessionMaintenanceHost["dropPersistedAssistantTurn"]>;
}

function createHarness(
	options: {
		messages?: AgentMessage[];
		decision?: LcmOwnershipDecision | undefined;
		breakdownTokens?: number;
		settings?: Record<string, unknown>;
	} = {},
): MaintenanceHarness {
	const messages = options.messages ?? [{ role: "user", content: "hello", timestamp: 1 }];
	const agent = new Agent({
		initialState: {
			model: MODEL,
			systemPrompt: ["test"],
			tools: [],
			messages: [...messages],
		},
	});
	const manager = SessionManager.inMemory("/maintenance-lcm-test");
	const settings = Settings.isolated({
		"compaction.enabled": true,
		"compaction.strategy": "context-full",
		"compaction.thresholdTokens": 100,
		"compaction.thresholdPercent": -1,
		"compaction.autoContinue": false,
		"compaction.midTurnEnabled": true,
		"contextPromotion.enabled": false,
		...(options.settings ?? {}),
	});
	const losslessOwnsRequest = vi.fn<SessionMaintenanceHost["losslessOwnsRequest"]>(
		async () => options.decision ?? { kind: "native" },
	);
	const rearmLosslessPrimaryIntent = vi.fn();
	const runRecoveryCompactionWithRollback = vi.fn(async () => COMPACTION_CHECK_NONE);
	const scheduleAgentContinue = vi.fn();
	const removeAssistantMessageFromActiveContext = vi.fn((message: AssistantMessage) => {
		agent.replaceMessages(agent.state.messages.filter(candidate => candidate !== message));
	});
	const dropPersistedAssistantTurn = vi.fn(async () => {});
	const host = {
		agent,
		sessionManager: manager,
		settings,
		modelRegistry: {
			getAvailable: () => [MODEL],
			getApiKey: async () => "test-key",
		},
		extensionRunner: undefined,
		sideStreamFn: async () => {
			throw new Error("side stream must not run");
		},
		providerSessionState: new Map(),
		model: () => MODEL,
		thinkingLevel: () => undefined,
		isDisposed: () => false,
		isStreaming: () => false,
		isGeneratingHandoff: () => false,
		promptGeneration: () => 7,
		sessionId: () => manager.getSessionId(),
		messages: () => agent.state.messages,
		losslessOwnsRequest,
		rearmLosslessPrimaryIntent,
		requestUsedLossless: () => false,
		baseSystemPrompt: () => ["test"],
		goalModeState: () => undefined,
		planReferencePath: () => "local://PLAN.md",
		nonMessageTokenSource: () => ({ systemPrompt: ["test"], agent }),
		memoryBackendSession: () => ({}),
		emitSessionEvent: async () => {},
		emitNotice: () => {},
		schedulePostPromptTask: () => {},
		scheduleAgentContinue,
		scheduleCompactionContinuation: () => false,
		persistTurnMessagesForMidRunCompaction: async () => true,
		findLastAssistantMessage: () =>
			[...agent.state.messages]
				.reverse()
				.find((message): message is AssistantMessage => message.role === "assistant"),
		disconnectFromAgent: () => {},
		reconnectToAgent: () => {},
		drainStrandedQueuedMessages: () => {},
		buildDisplaySessionContext: () => ({ messages: agent.state.messages }),
		convertToLlmForSideRequest: () => [],
		obfuscateTextForProvider: (text: string | undefined) => text,
		obfuscatePreparationForProvider: (preparation: unknown) => preparation,
		closeCodexProviderSessionsForHistoryRewrite: () => {},
		resetCodexProviderAfterCompaction: () => {},
		resetPlanReference: () => {},
		syncTodoPhasesFromBranch: () => {},
		resetAdvisorRuntimes: () => {},
		rebaseAfterCompaction: () => {},
		getContextBreakdown: (breakdownOptions?: { contextWindow?: number; pendingMessages?: AgentMessage[] }) =>
			contextBreakdown(options.breakdownTokens ?? 150, breakdownOptions?.contextWindow),
		getContextUsage: () => ({ tokens: 0, contextWindow: MODEL.contextWindow }),
		shake: async () => ({ prunedCount: 0, tokensSaved: 0 }),
		dropImages: async () => ({ removed: 0 }),
		runHandoff: async () => undefined,
		removeAssistantMessageFromActiveContext,
		dropPersistedAssistantTurn,
		runRecoveryCompactionWithRollback,
		parseRetryAfterMsFromError: () => undefined,
		setModelTemporary: async () => {},
		abort: async () => {},
		abortHandoff: () => {},
	} as unknown as SessionMaintenanceHost;
	return {
		maintenance: new SessionMaintenance(host),
		host,
		agent,
		manager,
		losslessOwnsRequest,
		rearmLosslessPrimaryIntent,
		runRecoveryCompactionWithRollback,
		scheduleAgentContinue,
		removeAssistantMessageFromActiveContext,
		dropPersistedAssistantTurn,
	};
}

describe("SessionMaintenance Lossless ownership", () => {
	it("passes the exact pre-prompt trigger floor to the ownership decision", async () => {
		const pending: AgentMessage = { role: "user", content: "next prompt", timestamp: 2 };
		const harness = createHarness({ decision: OWNED_DECISION, breakdownTokens: 175 });

		await harness.maintenance.runPrePromptCompactionIfNeeded([pending]);

		expect(harness.losslessOwnsRequest).toHaveBeenCalledTimes(1);
		const [messages, signal, floor] = harness.losslessOwnsRequest.mock.calls[0]!;
		expect(messages).toEqual([...harness.agent.state.messages, pending]);
		expect(signal).toBeInstanceOf(AbortSignal);
		expect(floor).toBe(175);
	});

	it("passes the exact mid-run trigger floor to the ownership decision", async () => {
		const last = assistant("toolUse", 180);
		const active: AgentMessage[] = [{ role: "user", content: "work", timestamp: 1 }, last];
		const harness = createHarness({ messages: active, decision: OWNED_DECISION });
		const parent = new AbortController();

		await harness.maintenance.maintainContextMidRun(active, parent.signal, {
			willContinue: true,
		} as AgentTurnEndContext);

		const [messages, signal, floor] = harness.losslessOwnsRequest.mock.calls[0]!;
		expect(messages).toBe(active);
		expect(signal).not.toBe(parent.signal);
		expect(floor).toBe(180);
	});

	it("probes a context-full retry without the failed assistant and floors it above the window", async () => {
		const failed = assistant("error", 50, "prompt is too long: 1200 tokens > 1000 maximum");
		const user: AgentMessage = { role: "user", content: "oversized", timestamp: 1 };
		const harness = createHarness({ messages: [user, failed], decision: OWNED_DECISION });

		await harness.maintenance.checkCompaction(failed);

		const [messages, signal, floor] = harness.losslessOwnsRequest.mock.calls[0]!;
		expect(messages).toEqual([user]);
		expect(signal).toBeInstanceOf(AbortSignal);
		expect(floor).toBe(1_001);
		expect(harness.dropPersistedAssistantTurn).toHaveBeenCalledWith(failed);
		expect(harness.scheduleAgentContinue).toHaveBeenCalledTimes(1);
	});

	it("drops an owned overflow before a pre-prompt caller builds the retry payload", async () => {
		const failed = assistant("error", 50, "prompt is too long: 1200 tokens > 1000 maximum");
		const user: AgentMessage = { role: "user", content: "oversized", timestamp: 1 };
		const harness = createHarness({ messages: [user, failed], decision: OWNED_DECISION });

		await harness.maintenance.checkCompaction(failed, false, false, false);

		expect(harness.removeAssistantMessageFromActiveContext).toHaveBeenCalledWith(failed);
		expect(harness.dropPersistedAssistantTurn).toHaveBeenCalledWith(failed);
		expect(harness.scheduleAgentContinue).not.toHaveBeenCalled();
	});

	it("rearms an owned response.incomplete decision against the post-drop retry payload", async () => {
		const truncated = assistant("length", 180);
		const user: AgentMessage = { role: "user", content: "finish", timestamp: 1 };
		const harness = createHarness({
			messages: [user, truncated],
			decision: OWNED_DECISION,
		});

		await harness.maintenance.checkCompaction(truncated);

		expect(harness.losslessOwnsRequest).toHaveBeenCalledTimes(1);
		const [probedMessages, signal, floor] = harness.losslessOwnsRequest.mock.calls[0]!;
		expect(probedMessages).toEqual([user, truncated]);
		expect(signal).toBeInstanceOf(AbortSignal);
		expect(floor).toBe(180);
		expect(harness.removeAssistantMessageFromActiveContext).toHaveBeenCalledWith(truncated);
		expect(harness.removeAssistantMessageFromActiveContext).toHaveBeenCalledTimes(1);
		expect(harness.dropPersistedAssistantTurn).toHaveBeenCalledWith(truncated);
		expect(harness.rearmLosslessPrimaryIntent).toHaveBeenCalledWith([user], 180);
		expect(harness.scheduleAgentContinue).toHaveBeenCalledTimes(1);
		expect(harness.runRecoveryCompactionWithRollback).not.toHaveBeenCalled();
	});

	it("probes the current length payload and never re-probes an already Lossless-owned request", async () => {
		const user: AgentMessage = { role: "user", content: "finish", timestamp: 1 };
		const nativeTruncated = assistant("length", 200);
		nativeTruncated.usage = {
			...nativeTruncated.usage,
			input: 70,
			output: 100,
			cacheRead: 20,
			cacheWrite: 10,
		};
		const nativeHarness = createHarness({
			messages: [user, nativeTruncated],
			decision: { kind: "native" },
		});

		await nativeHarness.maintenance.checkCompaction(nativeTruncated);

		expect(nativeHarness.agent.state.messages).toEqual([user, nativeTruncated]);
		expect(nativeHarness.losslessOwnsRequest).toHaveBeenCalledTimes(1);
		const [retryMessages, retrySignal, retryFloor] = nativeHarness.losslessOwnsRequest.mock.calls[0]!;
		expect(retryMessages).toEqual([user, nativeTruncated]);
		expect(retrySignal).toBeInstanceOf(AbortSignal);
		expect(retryFloor).toBe(100);
		expect(nativeHarness.removeAssistantMessageFromActiveContext).not.toHaveBeenCalled();
		expect(nativeHarness.runRecoveryCompactionWithRollback).toHaveBeenCalledWith(
			"incomplete",
			nativeTruncated,
			true,
			{ autoContinue: true, triggerContextTokens: 100 },
		);

		const losslessTruncated = assistant("length", 200);
		losslessTruncated.usage = { ...nativeTruncated.usage };
		const losslessHarness = createHarness({
			messages: [user, losslessTruncated],
			decision: OWNED_DECISION,
		});
		losslessHarness.host.requestUsedLossless = message => message === losslessTruncated;

		await losslessHarness.maintenance.checkCompaction(losslessTruncated);

		expect(losslessHarness.agent.state.messages).toEqual([user, losslessTruncated]);
		expect(losslessHarness.losslessOwnsRequest).not.toHaveBeenCalled();
		expect(losslessHarness.rearmLosslessPrimaryIntent).not.toHaveBeenCalled();
		expect(losslessHarness.removeAssistantMessageFromActiveContext).not.toHaveBeenCalled();
		expect(losslessHarness.runRecoveryCompactionWithRollback).toHaveBeenCalledWith(
			"incomplete",
			losslessTruncated,
			true,
			{ autoContinue: true, triggerContextTokens: 100 },
		);
	});

	it("skips every ownership and native maintenance path when the post-prune pressure is below hard", async () => {
		const finished = assistant("stop", 90);
		const harness = createHarness({ messages: [finished], decision: { kind: "native" } });
		const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction");

		await harness.maintenance.checkCompaction(finished);

		expect(harness.losslessOwnsRequest).not.toHaveBeenCalled();
		expect(runAuto).not.toHaveBeenCalled();
	});

	it("routes idle pressure through ownership and keeps its fallback on that native call", async () => {
		const harness = createHarness({
			decision: { kind: "native", fallback: { category: "provider", reason: "provider_exhausted" } },
		});
		const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction").mockResolvedValue(COMPACTION_CHECK_NONE);

		await harness.maintenance.runIdleCompaction(250);

		expect(harness.losslessOwnsRequest).toHaveBeenCalledWith(
			harness.agent.state.messages,
			expect.any(AbortSignal),
			250,
		);
		expect(runAuto).toHaveBeenCalledWith("idle", false, true, true, { lcmFallback: "provider" });
	});

	it("aborts idle native compaction with its parent signal before it can rewrite history", async () => {
		const harness = createHarness({
			decision: { kind: "native" },
			settings: { "compaction.keepRecentTokens": 1 },
		});
		harness.manager.appendMessage({ role: "user", content: "old prompt ".repeat(100), timestamp: 1 });
		const oldAssistant = assistant("stop", 200);
		oldAssistant.timestamp = 2;
		harness.manager.appendMessage(oldAssistant);
		harness.manager.appendMessage({ role: "user", content: "latest prompt", timestamp: 3 });
		harness.agent.replaceMessages(harness.manager.buildSessionContext().messages);
		harness.host.buildDisplaySessionContext = () => harness.manager.buildSessionContext();
		const branchBefore = structuredClone(harness.manager.getBranch());
		const messagesBefore = structuredClone(harness.agent.state.messages);
		const compactionStarted = Promise.withResolvers<AbortSignal>();
		const compactionGate = Promise.withResolvers<void>();
		const compactionEnded = Promise.withResolvers<void>();
		harness.host.extensionRunner = {
			hasHandlers: (type: string) => type === "session_before_compact",
			emit: async (event: {
				type: string;
				signal?: AbortSignal;
				preparation?: { firstKeptEntryId: string; tokensBefore: number };
			}) => {
				if (event.type !== "session_before_compact") return undefined;
				if (!event.signal || !event.preparation) throw new Error("Expected compaction preparation");
				compactionStarted.resolve(event.signal);
				await compactionGate.promise;
				return {
					compaction: {
						summary: "idle compacted",
						firstKeptEntryId: event.preparation.firstKeptEntryId,
						tokensBefore: event.preparation.tokensBefore,
					},
				};
			},
		} as never;
		harness.host.emitSessionEvent = async event => {
			if (event.type === "auto_compaction_end") compactionEnded.resolve();
		};
		const parent = new AbortController();

		const operation = harness.maintenance.runIdleCompaction(250, parent.signal);
		const nativeSignal = await compactionStarted.promise;
		parent.abort();
		compactionGate.resolve();
		await Promise.all([operation, compactionEnded.promise]);

		expect(nativeSignal.aborted).toBe(true);
		expect(harness.manager.getBranch()).toEqual(branchBefore);
		expect(harness.agent.state.messages).toEqual(messagesBefore);
	});

	it("skips all successful post-turn maintenance after a Lossless-owned provider request", async () => {
		const harness = createHarness({ settings: { "compaction.dropUseless": true } });
		const staleAt = Date.now() - 120_000;
		harness.manager.appendMessage({ role: "user", content: "inspect", timestamp: staleAt });
		harness.manager.appendMessage({
			role: "toolResult",
			toolCallId: "stale-result",
			toolName: "read",
			content: [{ type: "text", text: "obsolete output ".repeat(200) }],
			isError: false,
			useless: true,
			timestamp: staleAt + 1,
		});
		harness.agent.replaceMessages(harness.manager.buildSessionContext().messages);
		const branchBefore = structuredClone(harness.manager.getBranch());
		const replaceMessages = vi.spyOn(harness.agent, "replaceMessages");
		harness.host.requestUsedLossless = () => true;
		const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction");

		await harness.maintenance.checkCompaction(assistant("stop", 250));

		expect(harness.losslessOwnsRequest).not.toHaveBeenCalled();
		expect(runAuto).not.toHaveBeenCalled();
		expect(replaceMessages).not.toHaveBeenCalled();
		expect(harness.manager.getBranch()).toEqual(branchBefore);
	});

	it("captures a fallback category in the exact deferred compaction closure", async () => {
		const harness = createHarness({ settings: { "compaction.strategy": "handoff" } });
		let scheduled: ((signal: AbortSignal) => Promise<void>) | undefined;
		harness.host.schedulePostPromptTask = vi.fn(task => {
			scheduled = task;
		});
		const maintenance = new SessionMaintenance(harness.host);
		const runAuto = vi.spyOn(maintenance, "runAutoCompaction");

		await maintenance.runAutoCompaction("threshold", false, false, true, { lcmFallback: "unfit" } as never);
		expect(scheduled).toBeDefined();
		runAuto.mockResolvedValue(COMPACTION_CHECK_NONE);
		await scheduled!(new AbortController().signal);
		await maintenance.runAutoCompaction("idle", false, true, true);

		expect(runAuto.mock.calls[1]?.[4]).toMatchObject({ lcmFallback: "unfit" });
		expect(runAuto.mock.calls[2]?.[4]).toBeUndefined();
	});

	for (const trigger of ["pre-prompt", "mid-run", "overflow", "response.incomplete", "threshold"] as const) {
		it(`stops ${trigger} before native work when cancellation lands during promotion`, async () => {
			const target =
				trigger === "overflow"
					? assistant("error", 50, "prompt is too long: 1200 tokens > 1000 maximum")
					: trigger === "response.incomplete"
						? assistant("length", 180)
						: trigger === "mid-run"
							? assistant("toolUse", 180)
							: trigger === "threshold"
								? assistant("stop", 250)
								: undefined;
			const user: AgentMessage = { role: "user", content: "work", timestamp: 1 };
			const harness = createHarness({
				messages: target ? [user, target] : [user],
				decision: { kind: "native" },
				breakdownTokens: 180,
				settings: { "contextPromotion.enabled": true },
			});
			const promotionStarted = Promise.withResolvers<AbortSignal | undefined>();
			const promotionGate = Promise.withResolvers<Model | undefined>();
			vi.spyOn(harness.maintenance, "resolveContextPromotionTarget").mockImplementation(
				async (_model, _contextWindow, signal) => {
					promotionStarted.resolve(signal);
					return await promotionGate.promise;
				},
			);
			const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction").mockResolvedValue(COMPACTION_CHECK_NONE);

			const operation =
				trigger === "pre-prompt"
					? harness.maintenance.runPrePromptCompactionIfNeeded([{ role: "user", content: "next", timestamp: 2 }])
					: trigger === "mid-run"
						? harness.maintenance.maintainContextMidRun(
								harness.agent.state.messages,
								new AbortController().signal,
								{ willContinue: true } as AgentTurnEndContext,
							)
						: harness.maintenance.checkCompaction(target!);
			const promotionSignal = await promotionStarted.promise;
			harness.maintenance.abortAutomaticCompaction();
			promotionGate.resolve(undefined);
			await operation;

			expect(promotionSignal).toBeInstanceOf(AbortSignal);
			expect(promotionSignal?.aborted).toBe(true);
			expect(runAuto).not.toHaveBeenCalled();
			expect(harness.runRecoveryCompactionWithRollback).not.toHaveBeenCalled();
			expect(harness.scheduleAgentContinue).not.toHaveBeenCalled();
		});
	}

	for (const trigger of ["overflow", "response.incomplete"] as const) {
		it(`completes committed owned ${trigger} recovery when cancellation lands during the persisted drop`, async () => {
			const target =
				trigger === "overflow"
					? assistant("error", 50, "prompt is too long: 1200 tokens > 1000 maximum")
					: assistant("length", 180);
			const user: AgentMessage = { role: "user", content: "work", timestamp: 1 };
			const harness = createHarness({
				messages: [user, target],
				decision: OWNED_DECISION,
			});
			const dropStarted = Promise.withResolvers<void>();
			const dropGate = Promise.withResolvers<void>();
			harness.dropPersistedAssistantTurn.mockImplementation(async () => {
				dropStarted.resolve();
				await dropGate.promise;
			});
			const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction").mockResolvedValue(COMPACTION_CHECK_NONE);

			const operation = harness.maintenance.checkCompaction(target);
			await dropStarted.promise;
			harness.maintenance.abortAutomaticCompaction();
			dropGate.resolve();
			await harness.maintenance.checkCompaction(assistant("aborted", 0));
			await operation;

			expect(harness.dropPersistedAssistantTurn).toHaveBeenCalledWith(target);
			expect(harness.losslessOwnsRequest).toHaveBeenCalledTimes(1);
			expect(harness.agent.state.messages).toEqual([user]);
			if (trigger === "response.incomplete") {
				expect(harness.rearmLosslessPrimaryIntent).toHaveBeenCalledWith([user], 180);
			} else {
				expect(harness.rearmLosslessPrimaryIntent).not.toHaveBeenCalled();
			}
			expect(harness.scheduleAgentContinue).toHaveBeenCalledTimes(1);
			expect(runAuto).not.toHaveBeenCalled();
			expect(harness.runRecoveryCompactionWithRollback).not.toHaveBeenCalled();
		});
	}

	it("drains a canceled promotion rollback before lifecycle teardown continues", async () => {
		const harness = createHarness({
			decision: { kind: "native" },
			breakdownTokens: 180,
			settings: { "contextPromotion.enabled": true },
		});
		const targetModel: Model = { ...MODEL, id: "promoted-model", contextWindow: 2_000 };
		vi.spyOn(harness.maintenance, "resolveContextPromotionTarget").mockResolvedValue(targetModel);
		const promotionCommitted = Promise.withResolvers<void>();
		const promotionGate = Promise.withResolvers<void>();
		const rollbackStarted = Promise.withResolvers<void>();
		const rollbackGate = Promise.withResolvers<void>();
		let activeModel = MODEL;
		harness.host.model = () => activeModel;
		let promotionCalls = 0;
		harness.host.setModelTemporary = vi.fn(async model => {
			if (promotionCalls++ === 0) {
				activeModel = model;
				promotionCommitted.resolve();
				await promotionGate.promise;
				return;
			}
			rollbackStarted.resolve();
			await rollbackGate.promise;
			activeModel = model;
		});
		const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction").mockResolvedValue(COMPACTION_CHECK_NONE);

		const operation = harness.maintenance.runPrePromptCompactionIfNeeded([
			{ role: "user", content: "next", timestamp: 2 },
		]);
		await promotionCommitted.promise;
		let drainSettled = false;
		const drain = harness.maintenance.abortAndDrainAutomaticCompaction().then(() => {
			drainSettled = true;
		});
		promotionGate.resolve();
		await rollbackStarted.promise;
		for (let i = 0; i < 8; i++) await Promise.resolve();
		expect(drainSettled).toBe(false);
		rollbackGate.resolve();
		await Promise.all([operation, drain]);

		expect(activeModel).toBe(MODEL);
		expect(harness.host.setModelTemporary).toHaveBeenNthCalledWith(1, targetModel, undefined, { ephemeral: true });
		expect(harness.host.setModelTemporary).toHaveBeenNthCalledWith(2, MODEL, undefined, {
			ephemeral: true,
			onlyIfCurrent: targetModel,
		});
		expect(runAuto).not.toHaveBeenCalled();
	});

	it("does not roll back a competing model selection during canceled-promotion restore", async () => {
		const harness = createHarness({
			decision: { kind: "native" },
			breakdownTokens: 180,
			settings: { "contextPromotion.enabled": true },
		});
		const targetModel: Model = { ...MODEL, id: "promoted-model", contextWindow: 2_000 };
		const competingModel: Model = { ...MODEL, id: "user-selected-model", contextWindow: 3_000 };
		vi.spyOn(harness.maintenance, "resolveContextPromotionTarget").mockResolvedValue(targetModel);
		const promotionCommitted = Promise.withResolvers<void>();
		const promotionGate = Promise.withResolvers<void>();
		const rollbackStarted = Promise.withResolvers<void>();
		const rollbackGate = Promise.withResolvers<void>();
		let activeModel = MODEL;
		let modelChangeCalls = 0;
		harness.host.model = () => activeModel;
		harness.host.setModelTemporary = vi.fn(async (model, _thinkingLevel, options) => {
			if (modelChangeCalls++ === 0) {
				activeModel = model;
				promotionCommitted.resolve();
				await promotionGate.promise;
				return;
			}
			rollbackStarted.resolve();
			await rollbackGate.promise;
			if (
				options?.onlyIfCurrent &&
				(activeModel.provider !== options.onlyIfCurrent.provider || activeModel.id !== options.onlyIfCurrent.id)
			) {
				return;
			}
			activeModel = model;
		});
		const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction").mockResolvedValue(COMPACTION_CHECK_NONE);

		const operation = harness.maintenance.runPrePromptCompactionIfNeeded([
			{ role: "user", content: "next", timestamp: 2 },
		]);
		await promotionCommitted.promise;
		harness.maintenance.abortAutomaticCompaction();
		promotionGate.resolve();
		await rollbackStarted.promise;
		activeModel = competingModel;
		rollbackGate.resolve();
		await operation;

		expect(activeModel).toBe(competingModel);
		expect(harness.host.setModelTemporary).toHaveBeenCalledTimes(2);
		expect(harness.host.setModelTemporary).toHaveBeenNthCalledWith(2, MODEL, undefined, {
			ephemeral: true,
			onlyIfCurrent: targetModel,
		});
		expect(runAuto).not.toHaveBeenCalled();
	});

	it("waits for canceled promotion rollback before a successor evaluates pressure", async () => {
		const harness = createHarness({
			decision: { kind: "native" },
			settings: {
				"compaction.thresholdTokens": -1,
				"compaction.thresholdPercent": 80,
				"contextPromotion.enabled": true,
			},
		});
		const promotedModel: Model = { ...MODEL, id: "promoted-model", contextWindow: 2_000 };
		let activeModel = MODEL;
		harness.host.model = () => activeModel;
		const observedWindows: number[] = [];
		harness.host.getContextBreakdown = options => {
			const contextWindow = options?.contextWindow ?? MODEL.contextWindow ?? 0;
			observedWindows.push(contextWindow);
			return contextBreakdown(900, contextWindow);
		};
		let promotionResolutions = 0;
		vi.spyOn(harness.maintenance, "resolveContextPromotionTarget").mockImplementation(async () =>
			promotionResolutions++ === 0 ? promotedModel : undefined,
		);
		const promotionCommitted = Promise.withResolvers<void>();
		const promotionGate = Promise.withResolvers<void>();
		const rollbackStarted = Promise.withResolvers<void>();
		const rollbackGate = Promise.withResolvers<void>();
		let modelChangeCalls = 0;
		harness.host.setModelTemporary = vi.fn(async (model, _thinkingLevel, options) => {
			if (modelChangeCalls++ === 0) {
				activeModel = model;
				promotionCommitted.resolve();
				await promotionGate.promise;
				return;
			}
			rollbackStarted.resolve();
			await rollbackGate.promise;
			if (
				options?.onlyIfCurrent &&
				(activeModel.provider !== options.onlyIfCurrent.provider || activeModel.id !== options.onlyIfCurrent.id)
			) {
				return;
			}
			activeModel = model;
		});
		const runAuto = vi.spyOn(harness.maintenance, "runAutoCompaction").mockResolvedValue(COMPACTION_CHECK_NONE);
		const pending: AgentMessage[] = [{ role: "user", content: "next", timestamp: 2 }];

		const oldProbe = harness.maintenance.runPrePromptCompactionIfNeeded(pending);
		await promotionCommitted.promise;
		let successorSettled = false;
		const successor = harness.maintenance.runPrePromptCompactionIfNeeded(pending).then(() => {
			successorSettled = true;
		});
		await oldProbe;
		const successorSettledBeforeRollback = successorSettled;

		promotionGate.resolve();
		await rollbackStarted.promise;
		for (let i = 0; i < 8; i++) await Promise.resolve();
		const successorSettledDuringRollback = successorSettled;
		let thirdSettled = false;
		const third = harness.maintenance.runPrePromptCompactionIfNeeded(pending).then(() => {
			thirdSettled = true;
		});
		for (let i = 0; i < 8; i++) await Promise.resolve();
		const thirdSettledDuringRollback = thirdSettled;
		const observedDuringRollback = [...observedWindows];
		rollbackGate.resolve();
		await Promise.all([oldProbe, successor, third]);

		expect(successorSettledBeforeRollback).toBe(false);
		expect(successorSettledDuringRollback).toBe(false);
		expect(thirdSettledDuringRollback).toBe(false);
		expect(observedDuringRollback).toEqual([1_000]);
		expect(observedWindows).toEqual([1_000, 1_000]);
		expect(harness.losslessOwnsRequest).toHaveBeenCalledTimes(2);
		expect(runAuto).toHaveBeenCalledTimes(1);
	});

	for (const trigger of ["pre-prompt", "overflow", "response.incomplete", "mid-run", "idle"] as const) {
		it(`cancels an in-flight ${trigger} ownership probe`, async () => {
			const target =
				trigger === "overflow"
					? assistant("error", 50, "prompt is too long: 1200 tokens > 1000 maximum")
					: trigger === "response.incomplete"
						? assistant("length", 180)
						: trigger === "mid-run"
							? assistant("toolUse", 180)
							: undefined;
			const user: AgentMessage = { role: "user", content: "work", timestamp: 1 };
			const harness = createHarness({
				messages: target ? [user, target] : [user],
				breakdownTokens: 180,
			});
			vi.spyOn(harness.maintenance, "runAutoCompaction").mockResolvedValue(COMPACTION_CHECK_NONE);
			let capturedSignal: AbortSignal | undefined;
			harness.losslessOwnsRequest.mockImplementation((_messages: AgentMessage[], signal?: AbortSignal) => {
				const pending = Promise.withResolvers<LcmOwnershipDecision>();
				capturedSignal = signal;
				signal?.addEventListener("abort", () => pending.resolve({ kind: "aborted" }), { once: true });
				return pending.promise;
			});

			const operation =
				trigger === "pre-prompt"
					? harness.maintenance.runPrePromptCompactionIfNeeded([{ role: "user", content: "next", timestamp: 2 }])
					: trigger === "mid-run"
						? harness.maintenance.maintainContextMidRun(
								harness.agent.state.messages,
								new AbortController().signal,
								{ willContinue: true } as AgentTurnEndContext,
							)
						: trigger === "idle"
							? harness.maintenance.runIdleCompaction(250)
							: harness.maintenance.checkCompaction(target!);
			await Promise.resolve();
			await Promise.resolve();
			harness.maintenance.abortAutomaticCompaction();
			const observed = capturedSignal;
			await operation;

			expect(observed).toBeInstanceOf(AbortSignal);
			expect(observed?.aborted).toBe(true);
			if (trigger === "overflow" || trigger === "response.incomplete") {
				expect(harness.removeAssistantMessageFromActiveContext).not.toHaveBeenCalled();
				expect(harness.agent.state.messages.at(-1)).toBe(target);
			}
		});
	}
});
