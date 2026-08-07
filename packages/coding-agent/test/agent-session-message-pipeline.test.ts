import { afterEach, describe, expect, it, vi } from "bun:test";
import { scheduler } from "node:timers/promises";
import {
	activeSourceFingerprint,
	type ContextProjection,
	type LcmContext,
	openLcmContext,
	type SourceSnapshot,
} from "@oh-my-pi/lcm-context";
import {
	Agent,
	type AgentMessage,
	type AgentTool,
	AppendOnlyContextManager,
	type StreamFn,
} from "@oh-my-pi/pi-agent-core";
import {
	type Api,
	type Context,
	clearCustomApis,
	type ImageContent,
	type Message,
	type Model,
	type ModelSpec,
	registerCustomApi,
	type SimpleStreamOptions,
	streamSimple,
	type TextContent,
} from "@oh-my-pi/pi-ai";
import * as AIError from "@oh-my-pi/pi-ai/error";
import { AssistantMessageEventStream } from "@oh-my-pi/pi-ai/utils/event-stream";
import { buildModel } from "@oh-my-pi/pi-catalog/build";
import { ModelRegistry } from "@oh-my-pi/pi-coding-agent/config/model-registry";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import * as memoryBackend from "@oh-my-pi/pi-coding-agent/memory-backend";
import type { MemoryBackend } from "@oh-my-pi/pi-coding-agent/memory-backend/types";
import { type MnemopiSessionState, setMnemopiSessionState } from "@oh-my-pi/pi-coding-agent/mnemopi/state";
import { createAgentSession, type ExtensionContext, type ExtensionFactory } from "@oh-my-pi/pi-coding-agent/sdk";
import { obfuscateProviderContext, SecretObfuscator } from "@oh-my-pi/pi-coding-agent/secrets";
import { AgentSession, type AgentSessionEvent, lcmCueTerms } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { AuthStorage } from "@oh-my-pi/pi-coding-agent/session/auth-storage";
import {
	convertToLlm,
	createHistoricalContextMessage,
	wrapSteeringForModel,
} from "@oh-my-pi/pi-coding-agent/session/messages";
import {
	estimateLcmProjectionMessageTokens,
	LcmCompletionError,
	type LcmPrimaryRouteKey,
	MAX_LCM_PRIMARY_TOKEN_MEASUREMENTS,
	SessionLcm,
	type SessionLcmProjectResult,
} from "@oh-my-pi/pi-coding-agent/session/session-lcm";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { createSettingsAwareStreamFn } from "@oh-my-pi/pi-coding-agent/session/settings-stream-fn";
import { TempDir } from "@oh-my-pi/pi-utils";
import { createAssistantMessage } from "./helpers/agent-session-setup";

function createAgent(): Agent {
	return new Agent({
		initialState: {
			systemPrompt: ["system prompt"],
			messages: [],
			tools: [],
		},
	});
}

function createModelRegistryStub(key = "key") {
	return {
		getApiKey: vi.fn(async () => key),
		resolver: vi.fn(() => async () => key),
	};
}

function getConvertedUserText(message: Message | undefined): string {
	if (message?.role !== "user") {
		throw new Error("Expected converted user message");
	}
	if (typeof message.content === "string") {
		return message.content;
	}
	const text = message.content.find((content): content is TextContent => content.type === "text");
	if (!text) {
		throw new Error("Expected converted text content");
	}
	return text.text;
}

async function withNativeDialectEnv<T>(fn: () => Promise<T>): Promise<T> {
	const previous = Bun.env.PI_DIALECT;
	delete Bun.env.PI_DIALECT;
	try {
		return await fn();
	} finally {
		if (previous === undefined) {
			delete Bun.env.PI_DIALECT;
		} else {
			Bun.env.PI_DIALECT = previous;
		}
	}
}

describe("AgentSession message pipeline", () => {
	const sessions: AgentSession[] = [];

	function createLcmCompletionSession(
		sideStreamFn: StreamFn,
		obfuscator?: SecretObfuscator,
		modelOverride?: Model<Api>,
	): AgentSession {
		const model =
			modelOverride ??
			(buildModel({
				id: "lcm-completion-model",
				name: "LCM Completion Model",
				api: "anthropic",
				provider: "test-lcm-provider",
				baseUrl: "",
				reasoning: false,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 4096,
				maxTokens: 1024,
			} as ModelSpec<Api>) as Model<Api>);
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory("/lcm-completion-test"),
			settings: Settings.isolated({
				"compaction.enabled": false,
				"context.engine": "lossless",
				modelRoles: { smol: "lcm-completion-model" },
			}),
			modelRegistry: {
				getAvailable: () => [model],
				resolver: () => async () => "key",
				authStorage: {
					recordObservedUsage: vi.fn(),
					recordUsageCost: vi.fn(),
					ingestUsageHeaders: vi.fn(),
				},
			} as never,
			sideStreamFn,
			obfuscator,
			lcm: { agentDir: "/lcm-completion-test" },
		});
		sessions.push(session);
		return session;
	}

	afterEach(async () => {
		vi.restoreAllMocks();
		clearCustomApis();
		for (const session of sessions.splice(0)) {
			await session.dispose();
		}
	});

	it("applies transformContext before convertToLlm", async () => {
		const inputMessages: AgentMessage[] = [{ role: "user", content: "hello", timestamp: Date.now() }];
		const transformedMessages: AgentMessage[] = [
			...inputMessages,
			{ role: "user", content: "injected context", timestamp: Date.now() },
		];
		const convertedMessages: Message[] = [
			{
				role: "user",
				content: [{ type: "text", text: "converted" }],
				attribution: "user",
				timestamp: Date.now(),
			},
		];
		const transformContext = vi.fn(async (messages: AgentMessage[], signal?: AbortSignal) => {
			expect(signal).toBe(abortController.signal);
			return [...messages, ...transformedMessages.slice(messages.length)];
		});
		const convertToLlm = vi.fn(async (_messages: AgentMessage[]) => {
			return convertedMessages;
		});
		const abortController = new AbortController();
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {} as never,
			sideTransformContext: transformContext,
			convertToLlm,
		});
		sessions.push(session);

		const result = await session.convertMessagesToLlm(inputMessages, abortController.signal);

		expect(transformContext).toHaveBeenCalledWith(inputMessages, abortController.signal);
		expect(convertToLlm).toHaveBeenCalledWith(transformedMessages);
		expect(result).toEqual(convertedMessages);
	});

	it("marks queued user steers without changing the public queue text", async () => {
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {} as never,
		});
		sessions.push(session);
		// #queueUserMessage schedules an idle-queue drain that would agent.continue()
		// and pop the steer before we can inspect it; stub it out to observe the queue.
		vi.spyOn(session.agent, "continue").mockResolvedValue(undefined);

		await session.sendUserMessage("raw <steer> &", { deliverAs: "steer" });

		expect(session.getQueuedMessages().steering).toEqual(["raw <steer> &"]);
		const queued = session.agent.popLastSteer();
		if (queued?.role !== "user") {
			throw new Error("Expected queued user steer");
		}
		expect(queued.steering).toBe(true);
		expect(queued.content).toEqual([{ type: "text", text: "raw <steer> &" }]);
		session.clearQueue();
	});

	it("resolves image attachments from submitted messages, not tool-result images", () => {
		const userImage: ImageContent = { type: "image", data: "user-image", mimeType: "image/png" };
		const toolImage: ImageContent = { type: "image", data: "tool-image", mimeType: "image/png" };
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {} as never,
		});
		sessions.push(session);

		session.agent.appendMessage({
			role: "user",
			content: [{ type: "text", text: "inspect this" }, userImage],
			timestamp: Date.now(),
		});
		session.agent.appendMessage({
			role: "toolResult",
			toolCallId: "eval-1",
			toolName: "eval",
			content: [{ type: "text", text: "plot output" }, toolImage],
			timestamp: Date.now(),
			isError: false,
		});

		expect(session.getImageAttachments()).toEqual([{ label: "Image #1", uri: "attachment://1", image: userImage }]);
	});

	it("keeps stored steering text raw while pre-LLM conversion wraps it", async () => {
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {} as never,
			sideTransformContext: wrapSteeringForModel,
			convertToLlm,
		});
		sessions.push(session);
		const raw: AgentMessage = {
			role: "user",
			content: [{ type: "text", text: "steer with <xml> & ampersand" }],
			steering: true,
			timestamp: 1,
		};
		session.agent.appendMessage(raw);

		const converted = await session.convertMessagesToLlm(session.messages);

		expect(session.messages[0]).toBe(raw);
		expect(raw.content).toEqual([{ type: "text", text: "steer with <xml> & ampersand" }]);
		const convertedText = getConvertedUserText(converted[0]);
		expect(convertedText).toContain("<system-notice>");
		expect(convertedText).not.toContain("<message>");
		expect(convertedText).toContain("steer with <xml> & ampersand");
		expect(convertedText).not.toContain("&lt;xml&gt;");
		expect(convertedText).not.toContain("&amp;");
	});

	it("composes session payload hooks into direct side-request options", async () => {
		const sessionOnPayload = vi.fn(async (payload: unknown) => ({
			...(payload as Record<string, unknown>),
			session: true,
		}));
		const requestOnPayload = vi.fn(async () => undefined);
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {} as never,
			onPayload: sessionOnPayload,
		});
		sessions.push(session);
		const options: SimpleStreamOptions = {
			apiKey: "key",
			onPayload: requestOnPayload,
		};

		const prepared = session.prepareSimpleStreamOptions(options);
		const result = await prepared.onPayload?.({ original: true });

		expect(sessionOnPayload).toHaveBeenCalledWith({ original: true }, undefined);
		expect(requestOnPayload).toHaveBeenCalledWith({ original: true, session: true }, undefined);
		expect(result).toEqual({ original: true, session: true });
	});
	it("keeps ephemeral side-channel cache key separate from provider routing while preserving websocket state", async () => {
		const api = "test-ephemeral-side-channel";
		let capturedOptions: SimpleStreamOptions | undefined;
		registerCustomApi(api, (_model, _context, options) => {
			capturedOptions = options;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("Answer");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "Answer", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});

		const model = buildModel({
			id: "side-model",
			name: "Side Model",
			api,
			provider: "test-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const promptCacheKey = "inherited-parent-cache";
		const session = new AgentSession({
			agent: new Agent({
				promptCacheKey,
				initialState: {
					model,
					systemPrompt: ["system prompt"],
					messages: [],
					tools: [],
				},
			}),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: createModelRegistryStub() as never,
			preferWebsockets: true,
		});
		sessions.push(session);
		const cacheSessionId = session.sessionId;

		const result = await session.runEphemeralTurn({ promptText: "Question?" });

		expect(result.replyText).toBe("Answer");
		expect(capturedOptions?.promptCacheKey).toBe(promptCacheKey);
		expect(capturedOptions?.sessionId).toStartWith(`${cacheSessionId}:side:`);
		expect(capturedOptions?.sessionId).not.toBe(cacheSessionId);
		expect(capturedOptions?.preferWebsockets).toBe(true);
		expect(capturedOptions?.providerSessionState).toBe(session.providerSessionState);
	});

	it("runs ephemeral side-channel requests through the configured side stream function", async () => {
		const model = buildModel({
			id: "side-stream-model",
			name: "Side Stream Model",
			api: "anthropic",
			provider: "test-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		let capturedOptions: SimpleStreamOptions | undefined;
		let capturedContext: Context | undefined;
		const sideStreamFn: StreamFn = (_model, context, options) => {
			capturedContext = context;
			capturedOptions = options;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("Side answer");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "Side answer", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		};
		const session = new AgentSession({
			agent: new Agent({
				initialState: {
					model,
					systemPrompt: ["system prompt"],
					messages: [],
					tools: [],
				},
			}),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: createModelRegistryStub() as never,
			sideStreamFn,
		});
		sessions.push(session);

		const result = await session.runEphemeralTurn({ promptText: "Question?" });

		expect(result.replyText).toBe("Side answer");
		expect(capturedContext?.messages.at(-1)?.content).toEqual([{ type: "text", text: "Question?" }]);
		expect(capturedOptions?.sessionId).toStartWith(`${session.sessionId}:side:`);
	});

	it("rotates ephemeral side-channel credentials on Google Resource exhausted", async () => {
		const api = "test-ephemeral-google-resource-exhausted";
		const googleErrorMessage = "Google API error (429): Resource exhausted. Please try again later.";
		const keys: unknown[] = [];
		let capturedOptions: SimpleStreamOptions | undefined;
		registerCustomApi(api, (_model, _context, options) => {
			capturedOptions = options;
			keys.push(options?.apiKey);
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				if (options?.apiKey === "next-key") {
					const message = createAssistantMessage("Recovered");
					stream.push({ type: "text_delta", contentIndex: 0, delta: "Recovered", partial: message });
					stream.push({ type: "done", reason: "stop", message });
					return;
				}

				const error = createAssistantMessage("");
				error.content = [];
				error.stopReason = "error";
				error.errorMessage = googleErrorMessage;
				error.errorStatus = 429;
				stream.push({ type: "start", partial: error });
				stream.push({ type: "error", reason: "error", error });
			});
			return stream;
		});

		const model = buildModel({
			id: "side-google-model",
			name: "Side Google Model",
			api,
			provider: "google",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const resolver = vi.fn(
			() => async (ctx: { error: unknown }) => (ctx.error === undefined ? "old-key" : "next-key"),
		);
		const session = new AgentSession({
			agent: new Agent({
				initialState: {
					model,
					systemPrompt: ["system prompt"],
					messages: [],
					tools: [],
				},
			}),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {
				getApiKey: vi.fn(async () => "old-key"),
				resolver,
			} as never,
		});
		sessions.push(session);
		const cacheSessionId = session.sessionId;

		const result = await session.runEphemeralTurn({ promptText: "Question?" });

		expect(result.replyText).toBe("Recovered");
		expect(keys).toEqual(["old-key", "next-key"]);
		expect(capturedOptions?.promptCacheKey).toBe(cacheSessionId);
		expect(capturedOptions?.sessionId).toStartWith(`${cacheSessionId}:side:`);
		expect(resolver).toHaveBeenCalledWith(model, cacheSessionId);
	});

	it("applies configured OpenRouter routing variant to ephemeral side-channel options", async () => {
		const api = "test-ephemeral-openrouter-variant";
		let capturedOptions: SimpleStreamOptions | undefined;
		registerCustomApi(api, (_model, _context, options) => {
			capturedOptions = options;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("Answer");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "Answer", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});

		const model = buildModel({
			id: "anthropic/claude-sonnet-4",
			name: "OpenRouter Model",
			api,
			provider: "openrouter",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const session = new AgentSession({
			agent: new Agent({
				initialState: {
					model,
					systemPrompt: ["system prompt"],
					messages: [],
					tools: [],
				},
			}),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({
				"compaction.enabled": false,
				"providers.openrouterVariant": "nitro",
			}),
			modelRegistry: createModelRegistryStub() as never,
		});
		sessions.push(session);

		const result = await session.runEphemeralTurn({ promptText: "Question?" });

		expect(result.replyText).toBe("Answer");
		expect(capturedOptions?.openrouterVariant).toBe("nitro");
	});

	it("obfuscates user messages on ephemeral side-channel requests", async () => {
		const api = "test-ephemeral-secret-redaction";
		const secret = "EPHEMERAL_SECRET_TOKEN_12345";
		let capturedContext: Context | undefined;
		registerCustomApi(api, (_model, context, _options) => {
			capturedContext = context;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("Answer");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "Answer", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});

		const model = buildModel({
			id: "side-model-secrets",
			name: "Side Model Secrets",
			api,
			provider: "test-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const session = new AgentSession({
			agent: new Agent({
				initialState: {
					model,
					systemPrompt: ["system prompt"],
					messages: [],
					tools: [],
				},
			}),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: createModelRegistryStub() as never,
			obfuscator: new SecretObfuscator([{ type: "plain", content: secret }]),
		});
		sessions.push(session);

		const result = await session.runEphemeralTurn({ promptText: `question about ${secret}` });

		expect(result.replyText).toBe("Answer");
		expect(capturedContext).toBeDefined();
		// The secret entered only via the user prompt, which the opt-in obfuscator redacts.
		expect(JSON.stringify(capturedContext)).not.toContain(secret);
	});

	it("keeps obfuscated side-channel stable prefix byte-identical to the main turn", async () => {
		await withNativeDialectEnv(async () => {
			const api = "test-ephemeral-obfuscated-prefix-parity";
			const secret = "PREFIX_SECRET_TOKEN_12345";
			let callCount = 0;
			let mainContext: Context | undefined;
			let sideContext: Context | undefined;
			registerCustomApi(api, (_model, context, _options) => {
				if (callCount === 0) {
					mainContext = context;
				} else {
					sideContext = context;
				}
				callCount += 1;
				const stream = new AssistantMessageEventStream();
				queueMicrotask(() => {
					const message = createAssistantMessage("Answer");
					stream.push({ type: "text_delta", contentIndex: 0, delta: "Answer", partial: message });
					stream.push({ type: "done", reason: "stop", message });
				});
				return stream;
			});

			const model = buildModel({
				id: "side-model-prefix-parity",
				name: "Side Model Prefix Parity",
				api,
				provider: "test-provider",
				baseUrl: "",
				reasoning: false,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 4096,
				maxTokens: 1024,
			} as ModelSpec<Api>) as Model<Api>;
			const obfuscator = new SecretObfuscator([{ type: "plain", content: secret }]);
			const tool: AgentTool = {
				name: "secret_probe",
				label: "Secret Probe",
				description: `Tool description ${secret}`,
				parameters: {
					type: "object",
					properties: {
						value: { type: "string", description: `Schema description ${secret}` },
					},
					required: ["value"],
				},
				execute: async () => ({ content: [], details: {} }),
			};
			const agent = new Agent({
				initialState: {
					model,
					systemPrompt: [`system prompt with ${secret}`],
					messages: [],
					tools: [tool],
				},
				transformProviderContext: context => obfuscateProviderContext(obfuscator, context),
			});
			const session = new AgentSession({
				agent,
				sessionManager: SessionManager.inMemory(),
				settings: Settings.isolated({ "compaction.enabled": false }),
				modelRegistry: createModelRegistryStub() as never,
				obfuscator,
			});
			sessions.push(session);

			await agent.prompt("Main Question?");
			await session.runEphemeralTurn({ promptText: `Side Question ${secret}?` });

			// The static prefix (system prompt + tools) is left untouched, so it stays byte-identical
			// between the main turn and the side turn and the prompt cache prefix survives.
			expect(JSON.stringify(mainContext?.systemPrompt)).toBe(JSON.stringify(sideContext?.systemPrompt));
			expect(JSON.stringify(mainContext?.tools)).toBe(JSON.stringify(sideContext?.tools));
			// The side turn's user prompt secret is redacted from the outbound messages.
			expect(JSON.stringify(sideContext?.messages)).not.toContain(secret);
		});
	});

	it("records raw SSE diagnostics into the session buffer before request hooks", async () => {
		const requestOnSseEvent = vi.fn();
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {} as never,
			onSseEvent: requestOnSseEvent,
		});
		sessions.push(session);

		const prepared = session.prepareSimpleStreamOptions({});
		prepared.onSseEvent?.({ event: "message", data: "{}", raw: ["event: message", "data: {}"] });

		expect(session.rawSseDebugBuffer.snapshot().totalEvents).toBe(1);
		expect(requestOnSseEvent).toHaveBeenCalledWith(
			{ event: "message", data: "{}", raw: ["event: message", "data: {}"] },
			undefined,
		);
	});

	it("emits message_update to session listeners before slow extension handlers finish", async () => {
		const { promise, resolve } = Promise.withResolvers<void>();
		const extensionEmit = vi.fn(async (event: { type: string }) => {
			if (event.type === "message_update") {
				await promise;
			}
		});
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: {} as never,
			extensionRunner: {
				hasHandlers: () => true,
				emit: extensionEmit,
			} as never,
		});
		sessions.push(session);

		const events: AgentSessionEvent[] = [];
		session.subscribe(event => {
			events.push(event);
		});

		const assistantMessage = {
			role: "assistant",
			content: [
				{
					type: "toolCall",
					id: "call_1",
					name: "edit",
					arguments: {},
					partialJson: '{"file":"preview.txt","steps":[{"kbd":["ggdGi"],"insert":"rep',
				},
			],
			api: "test",
			provider: "test",
			model: "test",
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			timestamp: Date.now(),
		} as const;

		session.agent.emitExternalEvent({
			type: "message_update",
			message: assistantMessage as never,
			assistantMessageEvent: {
				type: "toolcall_delta",
				contentIndex: 0,
				delta: "rep",
			},
		} as never);

		await Bun.sleep(0);

		expect(events.some(event => event.type === "message_update")).toBe(true);
		expect(extensionEmit).toHaveBeenCalledTimes(1);

		resolve();
		await Bun.sleep(0);
	});

	it("keeps first-turn memory in the stable prompt on the next turn", async () => {
		const api = "test-injected-memory-append-only-cache";
		const contexts: Context[] = [];
		let remembered = false;
		const injected = "<memories>remember blue</memories>";
		const fakeBackend: MemoryBackend = {
			id: "mnemopi",
			async start() {},
			async buildDeveloperInstructions() {
				return remembered ? `static memory instructions\n\n${injected}` : "static memory instructions";
			},
			async clear() {},
			async enqueue() {},
			async beforeAgentStartPrompt() {
				if (remembered) return undefined;
				remembered = true;
				return injected;
			},
		};
		vi.spyOn(memoryBackend, "resolveMemoryBackend").mockResolvedValue(fakeBackend);
		registerCustomApi(api, (_model, context) => {
			contexts.push(context);
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("ok");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "ok", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});
		const model = buildModel({
			id: "local-model",
			name: "Local Model",
			api,
			provider: "ollama",
			baseUrl: "http://127.0.0.1:11434",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const agent = new Agent({
			initialState: {
				model,
				systemPrompt: ["base", "static memory instructions"],
				messages: [],
				tools: [],
			},
		});
		agent.setAppendOnlyContext(new AppendOnlyContextManager());
		const session = new AgentSession({
			agent,
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false, "provider.appendOnlyContext": "on" }),
			modelRegistry: createModelRegistryStub() as never,
			rebuildSystemPrompt: async () => ({
				systemPrompt: remembered
					? ["base", `static memory instructions\n\n${injected}`]
					: ["base", "static memory instructions"],
			}),
		});
		sessions.push(session);

		await session.sendUserMessage("first");
		await session.sendUserMessage("second");

		expect(contexts).toHaveLength(2);
		const firstSystemPrompt = contexts[0]!.systemPrompt;
		expect(firstSystemPrompt).toBeDefined();
		expect(firstSystemPrompt!.join("\n")).toContain(injected);
		expect(contexts[1]!.systemPrompt).toEqual(firstSystemPrompt);
	});

	it("preserves append-only prefixes in subagent sessions when context handlers rewrite prior turns", async () => {
		using tempDir = TempDir.createSync("@pi-subagent-append-only-");
		const api = "test-subagent-append-only-cache";
		const contexts: Context[] = [];
		registerCustomApi(api, (_model, context) => {
			contexts.push(context);
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage(`ok-${contexts.length}`);
				stream.push({ type: "text_delta", contentIndex: 0, delta: "ok", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});
		const model = buildModel({
			id: "local-subagent-model",
			name: "Local Subagent Model",
			api,
			provider: "llama.cpp",
			baseUrl: "http://127.0.0.1:8080/v1",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const rewritePriorAssistant: ExtensionFactory = pi => {
			pi.on("context", async event => {
				const hasSecondTurn = event.messages.some(message => {
					if (message.role !== "user") return false;
					const content = message.content;
					if (typeof content === "string") return content.includes("second");
					return content.some(part => part.type === "text" && part.text.includes("second"));
				});
				if (!hasSecondTurn) return undefined;
				return {
					messages: event.messages.map(message =>
						message.role === "assistant"
							? { ...message, content: [{ type: "text" as const, text: "rewritten assistant" }] }
							: message,
					),
				};
			});
		};
		const authStorage = await AuthStorage.create(tempDir.join("auth.db"));
		const modelRegistry = new ModelRegistry(authStorage, tempDir.join("models.yml"));
		const { session } = await createAgentSession({
			cwd: tempDir.path(),
			agentDir: tempDir.path(),
			sessionManager: SessionManager.inMemory(tempDir.path()),
			authStorage,
			modelRegistry,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"provider.appendOnlyContext": "auto",
			}),
			model,
			disableExtensionDiscovery: true,
			extensions: [rewritePriorAssistant],
			skills: [],
			contextFiles: [],
			promptTemplates: [],
			slashCommands: [],
			enableMCP: false,
			enableLsp: false,
			skipPythonPreflight: true,
			taskDepth: 1,
			agentId: "SubAgent",
		});
		try {
			expect(session.agent.appendOnlyContext).toBeDefined();

			await session.sendUserMessage("first");
			await session.sendUserMessage("second");

			expect(contexts).toHaveLength(2);
			expect(contexts[0]!.messages).toHaveLength(1);
			expect(contexts[1]!.messages).toHaveLength(3);
			expect(contexts[1]!.messages[0]).toBe(contexts[0]!.messages[0]);
			expect((contexts[1]!.messages[1] as { content: unknown }).content).toEqual([
				{ type: "text", text: "rewritten assistant" },
			]);
		} finally {
			await session.dispose();
			authStorage.close();
		}
	});
	it("applies a tool_call input revision at arg-prep time across events, execution, and history", async () => {
		// End-to-end wiring for the loop-level tool_call emission (session
		// #beforeToolCall): the handler fires once per dispatch (the wrapper's
		// own emission is suppressed via the runner marker), the revision is what
		// tool_execution_start reports, what bash executes, and what the
		// assistant message persists.
		using tempDir = TempDir.createSync("@pi-tool-call-revision-");
		const api = "test-tool-call-revision";
		let requests = 0;
		registerCustomApi(api, () => {
			requests++;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				if (requests === 1) {
					const message = createAssistantMessage("");
					const toolCall = {
						type: "toolCall",
						id: "call-revise-1",
						name: "bash",
						arguments: { command: "echo original" },
					} as const;
					message.content = [toolCall];
					message.stopReason = "toolUse";
					stream.push({ type: "toolcall_start", contentIndex: 0, partial: message });
					stream.push({ type: "toolcall_end", contentIndex: 0, toolCall: toolCall as never, partial: message });
					stream.push({ type: "done", reason: "toolUse", message });
				} else {
					const message = createAssistantMessage("done");
					stream.push({ type: "done", reason: "stop", message });
				}
			});
			return stream;
		});
		const model = buildModel({
			id: "local-revision-model",
			name: "Local Revision Model",
			api,
			provider: "ollama",
			baseUrl: "http://127.0.0.1:11434",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		let handlerCalls = 0;
		const reviseBash: ExtensionFactory = pi => {
			pi.on("tool_call", async event => {
				if (event.toolName !== "bash") return undefined;
				handlerCalls++;
				return { input: { command: "echo revised" } };
			});
		};
		const authStorage = await AuthStorage.create(tempDir.join("auth.db"));
		const modelRegistry = new ModelRegistry(authStorage, tempDir.join("models.yml"));
		const { session } = await createAgentSession({
			cwd: tempDir.path(),
			agentDir: tempDir.path(),
			sessionManager: SessionManager.inMemory(tempDir.path()),
			authStorage,
			modelRegistry,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"bash.autoBackground.enabled": false,
				"bashInterceptor.enabled": false,
			}),
			model,
			disableExtensionDiscovery: true,
			extensions: [reviseBash],
			skills: [],
			contextFiles: [],
			promptTemplates: [],
			slashCommands: [],
			enableMCP: false,
			enableLsp: false,
			skipPythonPreflight: true,
			toolNames: ["bash"],
		});
		try {
			const startArgs: unknown[] = [];
			session.subscribe(event => {
				if (event.type === "tool_execution_start") startArgs.push(event.args);
			});

			await session.sendUserMessage("run it");

			expect(handlerCalls).toBe(1);
			expect(startArgs).toEqual([{ command: "echo revised" }]);
			const messages = session.agent.state.messages;
			const toolCallBlock = messages
				.filter(m => m.role === "assistant")
				.flatMap(m => (m as { content: Array<{ type: string }> }).content)
				.find(c => c.type === "toolCall") as { arguments?: unknown } | undefined;
			expect(toolCallBlock?.arguments).toEqual({ command: "echo revised" });
			const toolResult = messages.find(m => m.role === "toolResult") as
				| { content: Array<{ type: string; text?: string }> }
				| undefined;
			const text = toolResult?.content.find(block => block.type === "text")?.text ?? "";
			expect(text).toContain("revised");
			expect(text).not.toContain("original");
		} finally {
			await session.dispose();
			authStorage.close();
		}
	});
	it("exposes ctx.invokeTool to a re-registered built-in so it can delegate to the native tool", async () => {
		// End-to-end for the extension path: a tool that re-registers `bash` receives ctx.invokeTool
		// (bound to its own name), delegates to the native bash, and the native output flows back.
		using tempDir = TempDir.createSync("@pi-invoke-tool-");
		const api = "test-invoke-tool";
		let requests = 0;
		registerCustomApi(api, () => {
			requests++;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				if (requests === 1) {
					const message = createAssistantMessage("");
					const toolCall = {
						type: "toolCall",
						id: "call-invoke-1",
						name: "bash",
						arguments: { command: "echo from-model" },
					} as const;
					message.content = [toolCall];
					message.stopReason = "toolUse";
					stream.push({ type: "toolcall_start", contentIndex: 0, partial: message });
					stream.push({ type: "toolcall_end", contentIndex: 0, toolCall: toolCall as never, partial: message });
					stream.push({ type: "done", reason: "toolUse", message });
				} else {
					const message = createAssistantMessage("done");
					stream.push({ type: "done", reason: "stop", message });
				}
			});
			return stream;
		});
		const model = buildModel({
			id: "local-invoke-model",
			name: "Local Invoke Model",
			api,
			provider: "ollama",
			baseUrl: "http://127.0.0.1:11434",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		let invokeToolPresent = false;
		let delegatedText = "";
		// Re-register `bash`: the wrapper ignores the model's args, delegates to the native bash with
		// its own command via ctx.invokeTool, and returns the native result.
		const wrapBash: ExtensionFactory = pi => {
			pi.registerTool({
				name: "bash",
				label: "Bash",
				description: "wrapped bash",
				parameters: pi.arktype({ command: pi.arktype("string") }),
				async execute(
					_toolCallId: string,
					_params: unknown,
					_signal: unknown,
					_onUpdate: unknown,
					ctx: ExtensionContext,
				) {
					invokeToolPresent = typeof ctx.invokeTool === "function";
					const native = await ctx.invokeTool?.({ command: "echo from-wrapper" });
					const textBlock = native?.content.find(b => b.type === "text");
					delegatedText = textBlock?.type === "text" ? textBlock.text : "";
					return native ?? { content: [{ type: "text" as const, text: "no invokeTool" }], details: {} };
				},
			});
		};
		const authStorage = await AuthStorage.create(tempDir.join("auth.db"));
		const modelRegistry = new ModelRegistry(authStorage, tempDir.join("models.yml"));
		const { session } = await createAgentSession({
			cwd: tempDir.path(),
			agentDir: tempDir.path(),
			sessionManager: SessionManager.inMemory(tempDir.path()),
			authStorage,
			modelRegistry,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"bash.autoBackground.enabled": false,
				"bashInterceptor.enabled": false,
				"tools.xdev": false,
			}),
			model,
			disableExtensionDiscovery: true,
			extensions: [wrapBash],
			skills: [],
			contextFiles: [],
			promptTemplates: [],
			slashCommands: [],
			enableMCP: false,
			enableLsp: false,
			skipPythonPreflight: true,
			toolNames: ["bash"],
		});
		try {
			await session.sendUserMessage("run it");

			expect(invokeToolPresent).toBe(true);
			// The native bash actually ran the wrapper's command, not the model's.
			expect(delegatedText).toContain("from-wrapper");
			expect(delegatedText).not.toContain("from-model");
		} finally {
			await session.dispose();
			authStorage.close();
		}
	});

	it("clears promoted memory from the base prompt when switching sessions", async () => {
		using tempDir = TempDir.createSync("@pi-injected-memory-switch-");
		const sessionManager = SessionManager.create(tempDir.path(), tempDir.join("sessions"));
		const firstSessionFile = sessionManager.getSessionFile();
		expect(firstSessionFile).toBeString();
		await sessionManager.flush();
		const nextSessionManager = SessionManager.create(tempDir.path(), tempDir.join("sessions"));
		const nextSessionFile = nextSessionManager.getSessionFile();
		expect(nextSessionFile).toBeString();
		await nextSessionManager.flush();

		const api = "test-injected-memory-switch-cache";
		const contexts: Context[] = [];
		let remembered = false;
		let recallAvailable = true;
		const injected = "<memories>session A only</memories>";
		const fakeBackend: MemoryBackend = {
			id: "mnemopi",
			async start() {},
			async buildDeveloperInstructions() {
				return remembered ? `static memory instructions\n\n${injected}` : "static memory instructions";
			},
			async clear() {},
			async enqueue() {},
			async beforeAgentStartPrompt() {
				if (remembered || !recallAvailable) return undefined;
				remembered = true;
				return injected;
			},
		};
		vi.spyOn(memoryBackend, "resolveMemoryBackend").mockResolvedValue(fakeBackend);
		registerCustomApi(api, (_model, context) => {
			contexts.push(context);
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("ok");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "ok", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});
		const model = buildModel({
			id: "local-model",
			name: "Local Model",
			api,
			provider: "ollama",
			baseUrl: "http://127.0.0.1:11434",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const agent = new Agent({
			initialState: {
				model,
				systemPrompt: ["base", "static memory instructions"],
				messages: [],
				tools: [],
			},
		});
		agent.setAppendOnlyContext(new AppendOnlyContextManager());
		const session = new AgentSession({
			agent,
			sessionManager,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"memory.backend": "mnemopi",
				"provider.appendOnlyContext": "on",
			}),
			modelRegistry: createModelRegistryStub() as never,
			rebuildSystemPrompt: async () => ({
				systemPrompt: remembered
					? ["base", `static memory instructions\n\n${injected}`]
					: ["base", "static memory instructions"],
			}),
		});
		sessions.push(session);
		setMnemopiSessionState(session, {
			aliasOf: undefined,
			setSessionId(_sessionId: string) {},
			resetConversationTracking() {
				remembered = false;
			},
			async dispose() {},
		} as unknown as MnemopiSessionState);

		await session.sendUserMessage("first");
		expect(session.systemPrompt.join("\n")).toContain(injected);
		recallAvailable = false;

		await session.switchSession(nextSessionFile!);
		await session.sendUserMessage("second");

		expect(session.systemPrompt.join("\n")).not.toContain(injected);
		expect(contexts).toHaveLength(2);
		expect(contexts[1]!.systemPrompt?.join("\n")).not.toContain(injected);
	});

	it("clears promoted memory from the base prompt when starting a new session", async () => {
		const api = "test-injected-memory-new-session-cache";
		const contexts: Context[] = [];
		let remembered = false;
		let recallAvailable = true;
		const injected = "<memories>previous session only</memories>";
		const fakeBackend: MemoryBackend = {
			id: "mnemopi",
			async start() {},
			async buildDeveloperInstructions() {
				return remembered ? `static memory instructions\n\n${injected}` : "static memory instructions";
			},
			async clear() {},
			async enqueue() {},
			async beforeAgentStartPrompt() {
				if (remembered || !recallAvailable) return undefined;
				remembered = true;
				return injected;
			},
		};
		vi.spyOn(memoryBackend, "resolveMemoryBackend").mockResolvedValue(fakeBackend);
		registerCustomApi(api, (_model, context) => {
			contexts.push(context);
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("ok");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "ok", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});
		const model = buildModel({
			id: "local-model",
			name: "Local Model",
			api,
			provider: "ollama",
			baseUrl: "http://127.0.0.1:11434",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const agent = new Agent({
			initialState: {
				model,
				systemPrompt: ["base", "static memory instructions"],
				messages: [],
				tools: [],
			},
		});
		agent.setAppendOnlyContext(new AppendOnlyContextManager());
		const session = new AgentSession({
			agent,
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({
				"compaction.enabled": false,
				"memory.backend": "mnemopi",
				"provider.appendOnlyContext": "on",
			}),
			modelRegistry: createModelRegistryStub() as never,
			rebuildSystemPrompt: async () => ({
				systemPrompt: remembered
					? ["base", `static memory instructions\n\n${injected}`]
					: ["base", "static memory instructions"],
			}),
		});
		sessions.push(session);
		setMnemopiSessionState(session, {
			aliasOf: undefined,
			setSessionId(_sessionId: string) {},
			resetConversationTracking() {
				remembered = false;
			},
			async dispose() {},
		} as unknown as MnemopiSessionState);

		await session.sendUserMessage("first");
		expect(session.systemPrompt.join("\n")).toContain(injected);
		recallAvailable = false;

		await session.newSession();
		await session.sendUserMessage("second");

		expect(session.systemPrompt.join("\n")).not.toContain(injected);
		expect(contexts).toHaveLength(2);
		expect(contexts[1]!.systemPrompt?.join("\n")).not.toContain(injected);
	});

	it("does not duplicate promoted memory in the base prompt when forking", async () => {
		using tempDir = TempDir.createSync("@pi-injected-memory-fork-");
		const sessionManager = SessionManager.create(tempDir.path(), tempDir.join("sessions"));
		expect(sessionManager.getSessionFile()).toBeString();
		await sessionManager.flush();

		const api = "test-injected-memory-fork-cache";
		const contexts: Context[] = [];
		let remembered = false;
		const injected = "<memories>forked recall</memories>";
		const fakeBackend: MemoryBackend = {
			id: "mnemopi",
			async start() {},
			async buildDeveloperInstructions() {
				return remembered ? `static memory instructions\n\n${injected}` : "static memory instructions";
			},
			async clear() {},
			async enqueue() {},
			async beforeAgentStartPrompt() {
				if (remembered) return undefined;
				remembered = true;
				return injected;
			},
		};
		vi.spyOn(memoryBackend, "resolveMemoryBackend").mockResolvedValue(fakeBackend);
		registerCustomApi(api, (_model, context) => {
			contexts.push(context);
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("ok");
				stream.push({ type: "text_delta", contentIndex: 0, delta: "ok", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});
		const model = buildModel({
			id: "local-model",
			name: "Local Model",
			api,
			provider: "ollama",
			baseUrl: "http://127.0.0.1:11434",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const agent = new Agent({
			initialState: {
				model,
				systemPrompt: ["base", "static memory instructions"],
				messages: [],
				tools: [],
			},
		});
		agent.setAppendOnlyContext(new AppendOnlyContextManager());
		const session = new AgentSession({
			agent,
			sessionManager,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"memory.backend": "mnemopi",
				"provider.appendOnlyContext": "on",
			}),
			modelRegistry: createModelRegistryStub() as never,
			rebuildSystemPrompt: async () => ({
				systemPrompt: remembered
					? ["base", `static memory instructions\n\n${injected}`]
					: ["base", "static memory instructions"],
			}),
		});
		sessions.push(session);
		setMnemopiSessionState(session, {
			aliasOf: undefined,
			setSessionId(_sessionId: string) {},
			resetConversationTracking() {
				remembered = false;
			},
			async dispose() {},
		} as unknown as MnemopiSessionState);

		await session.sendUserMessage("first");
		expect(session.systemPrompt.join("\n")).toContain(injected);

		await session.fork();
		await session.sendUserMessage("second");

		const forkedPrompt = contexts[1]!.systemPrompt?.join("\n") ?? "";
		const occurrences = forkedPrompt.split(injected).length - 1;
		expect(occurrences).toBe(1);
	});

	it("ephemeral side-channel forwards native tools, injects developer reminder, leaves toolChoice auto", async () => {
		await withNativeDialectEnv(async () => {
			const api = "test-ephemeral-tools-warm-cache";
			let capturedContext: Context | undefined;
			let capturedOptions: SimpleStreamOptions | undefined;
			registerCustomApi(api, (_model, context, options) => {
				capturedContext = context;
				capturedOptions = options;
				const stream = new AssistantMessageEventStream();
				queueMicrotask(() => {
					const message = createAssistantMessage("Not using tools");
					stream.push({ type: "text_delta", contentIndex: 0, delta: "Not using tools", partial: message });
					stream.push({ type: "done", reason: "stop", message });
				});
				return stream;
			});

			const model = buildModel({
				id: "side-model-with-tools",
				name: "Side Model with Tools",
				api,
				provider: "test-provider",
				baseUrl: "",
				reasoning: false,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 4096,
				maxTokens: 1024,
			} as ModelSpec<Api>) as Model<Api>;

			const tool: AgentTool = {
				name: "side_tool",
				label: "Side Tool",
				description: "A tool in side channel",
				parameters: { type: "object", properties: {} },
				execute: async () => ({ content: [], details: {} }),
			};

			const session = new AgentSession({
				agent: new Agent({
					initialState: {
						model,
						systemPrompt: ["system prompt"],
						messages: [],
						tools: [tool],
					},
				}),
				sessionManager: SessionManager.inMemory(),
				settings: Settings.isolated({ "compaction.enabled": false }),
				modelRegistry: createModelRegistryStub() as never,
			});
			sessions.push(session);

			const result = await session.runEphemeralTurn({ promptText: "Side Question?" });

			expect(result.replyText).toBe("Not using tools");
			expect(capturedContext).toBeDefined();
			expect(capturedContext!.tools).toBeDefined();
			expect(capturedContext!.tools!.length).toBe(1);
			expect(capturedContext!.tools![0].name).toBe("side_tool");

			// Developer reminder injected immediately before user prompt
			const messages = capturedContext!.messages;
			expect(messages.length).toBeGreaterThanOrEqual(2);
			const lastMessage = messages.at(-1);
			const secondToLast = messages.at(-2);

			expect(lastMessage?.role).toBe("user");
			expect(getConvertedUserText(lastMessage)).toBe("Side Question?");

			expect(secondToLast?.role).toBe("developer");
			const textContent = secondToLast?.content as TextContent[];
			expect(textContent).toHaveLength(1);
			expect(textContent[0]?.type).toBe("text");
			expect(textContent[0]?.text).toMatch(/^<system-reminder>\n[\s\S]+\n<\/system-reminder>\n?$/);

			// Tool choice must be undefined (not "none") for cache hits
			expect(capturedOptions?.toolChoice).toBeUndefined();
		});
	});

	it("ephemeral side-channel discards any emitted tool calls", async () => {
		const api = "test-ephemeral-tools-discard";
		registerCustomApi(api, (_model, _context, _options) => {
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const message = createAssistantMessage("Here is text");
				message.content.push({
					type: "toolCall",
					id: "call_123",
					name: "side_tool",
					arguments: {},
				});
				stream.push({ type: "text_delta", contentIndex: 0, delta: "Here is text", partial: message });
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		});

		const model = buildModel({
			id: "side-model-discard",
			name: "Side Model Discard",
			api,
			provider: "test-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;

		const session = new AgentSession({
			agent: new Agent({
				initialState: {
					model,
					systemPrompt: ["system prompt"],
					messages: [],
					tools: [],
				},
			}),
			sessionManager: SessionManager.inMemory(),
			settings: Settings.isolated({ "compaction.enabled": false }),
			modelRegistry: createModelRegistryStub() as never,
		});
		sessions.push(session);

		const result = await session.runEphemeralTurn({ promptText: "Side Question?" });

		expect(result.replyText).toBe("Here is text");
		expect(result.assistantMessage.content.some(block => block.type === "toolCall")).toBe(false);
		expect(result.assistantMessage.content.every(block => block.type !== "toolCall")).toBe(true);
	});

	it("resolves LCM role fallback chains with configured provider preference and accounts usage off-journal", async () => {
		const other = buildModel({
			id: "summary-model",
			name: "Other Summary",
			api: "openai-completions",
			provider: "openai",
			baseUrl: "https://other.invalid",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const preferred = buildModel({
			...other,
			name: "Preferred Summary",
			provider: "opencode-go",
			baseUrl: "https://preferred.invalid",
		} as ModelSpec<Api>) as Model<Api>;
		const recordObservedUsage = vi.fn();
		const recordUsageCost = vi.fn();
		const resolver = vi.fn(() => async () => "key");
		const selected: Model<Api>[] = [];
		let capturedContext: Context | undefined;
		let capturedOptions: SimpleStreamOptions | undefined;
		const sideStreamFn: StreamFn = (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => {
			selected.push(model);
			capturedContext = context;
			capturedOptions = options;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const base = createAssistantMessage("summary text");
				const message = {
					...base,
					api: model.api,
					provider: model.provider,
					model: model.id,
					timestamp: 123,
					usage: {
						...base.usage,
						input: 11,
						output: 7,
						cacheRead: 3,
						cacheWrite: 2,
						cost: { ...base.usage.cost, total: 1.25 },
					},
				};
				stream.push({ type: "done", reason: "stop", message });
			});
			return stream;
		};
		const manager = SessionManager.inMemory("/lcm-role-test");
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: manager,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"context.engine": "lossless",
				modelProviderOrder: ["opencode-go", "openai"],
				modelRoles: { smol: "missing-summary-model,summary-model" },
			}),
			modelRegistry: {
				getAvailable: () => [other, preferred],
				resolver,
				authStorage: { recordObservedUsage, recordUsageCost },
				getProviderBaseUrl: () => preferred.baseUrl,
			} as never,
			sideStreamFn,
			lcm: { agentDir: "/lcm-role-test" },
		});
		sessions.push(session);
		const affinitySessionId = session.sessionId;

		const text = await session.lcmComplete({
			systemPrompt: "system",
			prompt: "prompt",
			oneshotKind: "lcm_summary",
			maxOutputTokens: 128,
			modelSelector: "@smol",
		});

		expect(text).toBe("summary text");
		expect(selected).toEqual([preferred]);
		expect(resolver).toHaveBeenCalledWith(preferred, affinitySessionId);
		expect(capturedContext?.systemPrompt).toEqual(["system"]);
		expect(capturedContext?.tools).toEqual([]);
		expect(capturedContext?.messages).toEqual([
			expect.objectContaining({ role: "user", content: [{ type: "text", text: "prompt" }] }),
		]);
		expect(capturedOptions?.statefulResponses).toBe(false);
		expect(capturedOptions?.providerSessionState).toBeInstanceOf(Map);
		expect(capturedOptions?.sessionId).toStartWith(`${affinitySessionId}:lcm:lcm_summary:`);
		expect(recordUsageCost).toHaveBeenCalledWith("opencode-go", 1.25, {
			sessionId: affinitySessionId,
			recordedAt: 123,
			baseUrl: preferred.baseUrl,
		});
		expect(recordObservedUsage).toHaveBeenCalledWith({
			provider: "opencode-go",
			model: "summary-model",
			at: 123,
			usage: { input: 11, output: 7, cacheRead: 3, cacheWrite: 2 },
			costUsd: 1.25,
		});
		// The status line reads this: a dispatched LCM request must move the session's LCM cost.
		expect(session.getLcmCost()).toBeCloseTo(1.25, 8);
		expect(manager.getBranch()).toHaveLength(0);
	});

	it("reuses durable summary transport state and retires superseded epoch, credential, and model state", async () => {
		const firstModel = buildModel({
			id: "summary-model-a",
			name: "Summary Model A",
			api: "anthropic",
			provider: "test-lcm-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const secondModel = buildModel({
			...firstModel,
			id: "summary-model-b",
			name: "Summary Model B",
		} as ModelSpec<Api>) as Model<Api>;
		const capturedOptions: SimpleStreamOptions[] = [];
		const closeProviderState = vi.fn();
		const closeCountsAtDispatch: number[] = [];
		let resolvedApiKey = "key-a";
		const sideStreamFn: StreamFn = (model, _context, options) => {
			if (!options?.providerSessionState || !options.sessionId) throw new Error("Missing provider session options");
			capturedOptions.push(options);
			const invocation = capturedOptions.length;
			closeCountsAtDispatch.push(closeProviderState.mock.calls.length);
			if (!options.providerSessionState.has("learned-capability")) {
				options.providerSessionState.set("learned-capability", { close: closeProviderState });
			}
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				if (invocation === 1 || invocation === 3 || invocation === 5 || invocation === 7) {
					stream.fail(new Error("retryable summary transport failure"));
					return;
				}
				const response = createAssistantMessage("summary text");
				stream.push({
					type: "done",
					reason: "stop",
					message: { ...response, api: model.api, provider: model.provider, model: model.id },
				});
			});
			return stream;
		};
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory("/lcm-provider-state-test"),
			settings: Settings.isolated({ "compaction.enabled": false, "context.engine": "lossless" }),
			modelRegistry: {
				getAvailable: () => [firstModel, secondModel],
				resolver: () => async () => resolvedApiKey,
				authStorage: { recordObservedUsage: vi.fn(), recordUsageCost: vi.fn() },
			} as never,
			sideStreamFn,
			lcm: { agentDir: "/lcm-provider-state-test" },
		});
		sessions.push(session);
		const request = {
			systemPrompt: "system",
			prompt: "prompt",
			oneshotKind: "lcm_summary",
			maxOutputTokens: 128,
			modelSelector: firstModel.id,
			providerSessionKey: "summary-job-a:1",
			providerSessionFamilyKey: "summary-job-a",
			retainProviderStateOnFailure: true,
		} as const;

		await expect(session.lcmComplete(request)).rejects.toThrow("LCM completion failed");
		await session.lcmComplete(request);
		await expect(session.lcmComplete(request)).rejects.toThrow("LCM completion failed");
		await session.lcmComplete({ ...request, providerSessionKey: "summary-job-a:2" });
		await expect(session.lcmComplete({ ...request, providerSessionKey: "summary-job-a:3" })).rejects.toThrow(
			"LCM completion failed",
		);
		resolvedApiKey = "key-b";
		await session.lcmComplete({ ...request, providerSessionKey: "summary-job-a:3" });
		await expect(session.lcmComplete({ ...request, providerSessionKey: "summary-job-a:3" })).rejects.toThrow(
			"LCM completion failed",
		);
		session.settings.set("context.lossless.summaryModel", secondModel.id);
		session.refreshLcmSettings();
		await session.lcmComplete({
			...request,
			modelSelector: secondModel.id,
			providerSessionKey: "summary-job-a:3",
		});

		expect(capturedOptions).toHaveLength(8);
		expect(capturedOptions[1]?.providerSessionState).toBe(capturedOptions[0]?.providerSessionState);
		expect(capturedOptions[1]?.sessionId).toBe(capturedOptions[0]?.sessionId);
		expect(capturedOptions[3]?.providerSessionState).not.toBe(capturedOptions[2]?.providerSessionState);
		expect(capturedOptions[3]?.sessionId).not.toBe(capturedOptions[2]?.sessionId);
		expect(capturedOptions[5]?.providerSessionState).not.toBe(capturedOptions[4]?.providerSessionState);
		expect(capturedOptions[5]?.sessionId).not.toBe(capturedOptions[4]?.sessionId);
		expect(capturedOptions[7]?.providerSessionState).not.toBe(capturedOptions[6]?.providerSessionState);
		expect(capturedOptions[7]?.sessionId).not.toBe(capturedOptions[6]?.sessionId);
		expect(capturedOptions[0]?.providerSessionState).not.toBe(session.providerSessionState);
		expect(closeCountsAtDispatch).toEqual([0, 0, 1, 2, 3, 4, 5, 6]);
		expect(closeProviderState).toHaveBeenCalledTimes(7);
		await session.dispose();
		expect(closeProviderState).toHaveBeenCalledTimes(7);
	});

	it("retires aborted summary state before a timeout-ignoring completion settles", async () => {
		const started = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
		const finish = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
		const closeProviderState = [vi.fn(), vi.fn()];
		const providerStates: Array<NonNullable<SimpleStreamOptions["providerSessionState"]>> = [];
		let invocation = 0;
		const sideStreamFn: StreamFn = (model, _context, options) => {
			const index = invocation++;
			if (!options?.providerSessionState) throw new Error("Missing provider session state");
			providerStates.push(options.providerSessionState);
			options.providerSessionState.set("transport", { close: closeProviderState[index]! });
			started[index]!.resolve();
			const stream = new AssistantMessageEventStream();
			void finish[index]!.promise.then(() => {
				const response = createAssistantMessage("summary text");
				stream.push({
					type: "done",
					reason: "stop",
					message: { ...response, api: model.api, provider: model.provider, model: model.id },
				});
			});
			return stream;
		};
		const session = createLcmCompletionSession(sideStreamFn);
		const request = {
			systemPrompt: "system",
			prompt: "prompt",
			oneshotKind: "lcm_summary",
			maxOutputTokens: 128,
			providerSessionKey: "summary-overlap:1",
			providerSessionFamilyKey: "summary-overlap",
			retainProviderStateOnFailure: true,
		} as const;
		const firstController = new AbortController();
		const first = session.lcmComplete({ ...request, signal: firstController.signal }).catch(error => error);
		await started[0]!.promise;

		firstController.abort("provider deadline elapsed");
		expect(closeProviderState[0]).toHaveBeenCalledTimes(1);
		const second = session.lcmComplete(request);
		await started[1]!.promise;
		expect(providerStates[1]).not.toBe(providerStates[0]);

		finish[0]!.resolve();
		await first;
		expect(closeProviderState[1]).not.toHaveBeenCalled();
		expect(providerStates[1]?.has("transport")).toBe(true);

		finish[1]!.resolve();
		await second;
		expect(closeProviderState[1]).toHaveBeenCalledTimes(1);
	});

	it("wraps rejected LCM provider errors with bounded scheduling metadata and no raw error object", async () => {
		const providerError = Object.assign(new Error("upstream body says try again in 45s; credential=raw-secret"), {
			headers: new Headers({ "retry-after-ms": "60000" }),
		});
		const sideStreamFn: StreamFn = () => {
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => stream.fail(providerError));
			return stream;
		};
		const session = createLcmCompletionSession(sideStreamFn);

		const caught = await session
			.lcmComplete({
				systemPrompt: "system",
				prompt: "prompt",
				oneshotKind: "lcm_summary",
				maxOutputTokens: 128,
			})
			.catch(error => error);

		expect(caught).toBeInstanceOf(LcmCompletionError);
		const error = caught as LcmCompletionError & { cause?: unknown; headers?: unknown };
		expect(error.provider).toBe("test-lcm-provider");
		expect(error.retryAfterMs).toBe(60_000);
		expect(error.message).toBe("LCM completion failed");
		expect(error.cause).toBeUndefined();
		expect(error.headers).toBeUndefined();
	});

	it("dispatches one provider transport call per durable LCM attempt", async () => {
		const model = buildModel({
			id: "lcm-completion-model",
			name: "LCM OpenAI Completion Model",
			api: "openai-responses",
			provider: "openai",
			baseUrl: "https://example.test/v1",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<"openai-responses">) as Model<Api>;
		const fetchMock = vi.fn(
			async () =>
				new Response(JSON.stringify({ error: { message: "busy", type: "server_error" } }), {
					status: 503,
					headers: { "content-type": "application/json", "retry-after-ms": "0" },
				}),
		);
		const session = createLcmCompletionSession(
			(requestModel, requestContext, options) =>
				streamSimple(requestModel, requestContext, {
					...options,
					fetch: fetchMock,
				}),
			undefined,
			model,
		);

		const caught = await session
			.lcmComplete({
				systemPrompt: "system",
				prompt: "prompt",
				oneshotKind: "lcm_summary",
				maxOutputTokens: 128,
			})
			.catch(error => error);

		expect(caught).toBeInstanceOf(LcmCompletionError);
		expect(fetchMock).toHaveBeenCalledTimes(1);
	});

	it("keeps normal provider retries for transient LCM recall failures", async () => {
		vi.spyOn(scheduler, "wait").mockResolvedValue(undefined);
		const model = buildModel({
			id: "lcm-completion-model",
			name: "LCM Recall Model",
			api: "openai-completions",
			provider: "openai",
			baseUrl: "https://example.test/v1",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<"openai-completions">) as Model<Api>;
		let wireCalls = 0;
		const fetchMock = vi.fn(async () => {
			wireCalls++;
			if (wireCalls === 1) {
				return new Response(JSON.stringify({ error: { message: "busy", type: "server_error" } }), {
					status: 503,
					headers: { "content-type": "application/json", "retry-after-ms": "0" },
				});
			}
			return new Response(
				`data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: "recalled answer" }, finish_reason: null }] })}\n\n` +
					`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n` +
					"data: [DONE]\n\n",
				{ status: 200, headers: { "content-type": "text/event-stream" } },
			);
		});
		const session = createLcmCompletionSession(
			(requestModel, requestContext, options) => {
				expect(options?.disableProviderRetries).toBeUndefined();
				return streamSimple(requestModel, requestContext, {
					...options,
					fetch: fetchMock,
				});
			},
			undefined,
			model,
		);

		const outcome = await session
			.lcmComplete({
				systemPrompt: "system",
				prompt: "recall this",
				oneshotKind: "lcm_recall",
				maxOutputTokens: 128,
			})
			.then(
				text => ({ text }),
				error => ({ error }),
			);
		expect(fetchMock).toHaveBeenCalledTimes(2);
		if ("error" in outcome) throw outcome.error;
		expect(outcome.text).toBe("recalled answer");
	});

	it("obfuscates returned LCM provider errors and preserves parsed retry metadata", async () => {
		const secret = "LCM_PROVIDER_SECRET_123456";
		const obfuscator = new SecretObfuscator([{ type: "plain", content: secret }]);
		const sideStreamFn: StreamFn = () => {
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const error = createAssistantMessage("");
				error.content = [];
				error.stopReason = "error";
				error.errorMessage = `Rate limited; wrapped retry-after-ms=45000; token=${secret}`;
				stream.push({ type: "error", reason: "error", error });
			});
			return stream;
		};
		const session = createLcmCompletionSession(sideStreamFn, obfuscator);

		const caught = await session
			.lcmComplete({
				systemPrompt: "system",
				prompt: "prompt",
				oneshotKind: "lcm_summary",
				maxOutputTokens: 128,
			})
			.catch(error => error);

		expect(caught).toBeInstanceOf(LcmCompletionError);
		const error = caught as LcmCompletionError;
		expect(error.provider).toBe("test-lcm-provider");
		expect(error.retryAfterMs).toBe(45_000);
		expect(error.message).toContain(obfuscator.obfuscate(secret));
		expect(error.message).not.toContain(secret);
	});

	it("preserves structural aborts from rejected and returned LCM streams", async () => {
		const rejectedAbort = new Error("wrapped provider cancellation", {
			cause: new AIError.AbortError("provider-local abort"),
		});
		const rejectedSession = createLcmCompletionSession(() => {
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => stream.fail(rejectedAbort));
			return stream;
		});
		const rejected = await rejectedSession
			.lcmComplete({
				systemPrompt: "system",
				prompt: "prompt",
				oneshotKind: "lcm_summary",
				maxOutputTokens: 128,
			})
			.catch(error => error);
		expect(rejected).toBe(rejectedAbort);

		const controller = new AbortController();
		const returnedAbort = new AIError.AbortError("caller cancelled summary");
		const returnedSession = createLcmCompletionSession(() => {
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => {
				const aborted = createAssistantMessage("");
				aborted.content = [];
				aborted.stopReason = "aborted";
				controller.abort(returnedAbort);
				stream.push({ type: "error", reason: "aborted", error: aborted });
			});
			return stream;
		});
		const returned = await returnedSession
			.lcmComplete({
				systemPrompt: "system",
				prompt: "prompt",
				oneshotKind: "lcm_summary",
				maxOutputTokens: 128,
				signal: controller.signal,
			})
			.catch(error => error);
		expect(returned).toBe(returnedAbort);
		expect(AIError.is(AIError.classify(returned), AIError.Flag.Abort)).toBe(true);
	});

	it("routes concurrent LCM completions through the shared provider in-flight limiter", async () => {
		const suffix = crypto.randomUUID();
		const api = `test-lcm-provider-limiter-${suffix}`;
		const provider = `test-lcm-provider-limiter-${suffix}`;
		let starts = 0;
		const firstStarted = Promise.withResolvers<void>();
		const releaseFirst = Promise.withResolvers<void>();
		registerCustomApi(api, () => {
			starts++;
			const stream = new AssistantMessageEventStream();
			if (starts === 1) {
				firstStarted.resolve();
				void releaseFirst.promise.then(() => {
					stream.push({ type: "done", reason: "stop", message: createAssistantMessage("summary") });
				});
			} else {
				queueMicrotask(() => {
					stream.push({ type: "done", reason: "stop", message: createAssistantMessage("unexpected") });
				});
			}
			return stream;
		});
		const model = buildModel({
			id: "limited-lcm-model",
			name: "Limited LCM Model",
			api,
			provider,
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const settings = Settings.isolated({
			"compaction.enabled": false,
			"context.engine": "lossless",
			"providers.maxInFlightRequests": { [provider]: 1 },
			modelRoles: { smol: "limited-lcm-model" },
		});
		const settingsAwareStreamFn = createSettingsAwareStreamFn(settings);
		const secondEnteredLimiter = Promise.withResolvers<void>();
		let sideCalls = 0;
		const sideStreamFn: StreamFn = (streamModel, context, options) => {
			const stream = settingsAwareStreamFn(streamModel, context, options);
			sideCalls++;
			if (sideCalls === 2) secondEnteredLimiter.resolve();
			return stream;
		};
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: SessionManager.inMemory("/lcm-limiter-test"),
			settings,
			modelRegistry: {
				getAvailable: () => [model],
				resolver: () => async () => "key",
				authStorage: { recordObservedUsage: vi.fn(), recordUsageCost: vi.fn() },
			} as never,
			sideStreamFn,
			lcm: { agentDir: "/lcm-limiter-test" },
		});
		sessions.push(session);
		const request = {
			systemPrompt: "system",
			prompt: "prompt",
			oneshotKind: "lcm_summary" as const,
			maxOutputTokens: 128,
		};
		const first = session.lcmComplete(request);
		await firstStarted.promise;
		const controller = new AbortController();
		const queuedAbort = new AIError.AbortError("cancel queued LCM completion");
		const second = session.lcmComplete({ ...request, signal: controller.signal });
		try {
			await secondEnteredLimiter.promise;
			controller.abort(queuedAbort);
			expect(await second.catch(error => error)).toBe(queuedAbort);
			expect(starts).toBe(1);
			releaseFirst.resolve();
			expect(await first).toBe("summary");
		} finally {
			releaseFirst.resolve();
			controller.abort(queuedAbort);
			await Promise.allSettled([first, second]);
		}
	});

	it("injects a cue into an owned projection and suppresses it when the setting is off", async () => {
		using tempDir = TempDir.createSync("@pi-lcm-cue-owned-");
		const model = buildModel({
			id: "lcm-cue-model",
			name: "LCM Cue Model",
			api: "anthropic",
			provider: "test-lcm-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 200_000,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const manager = SessionManager.inMemory(tempDir.path());
		// The first user entry is always kept raw, so the token mass must live in a middle entry
		// that the summary covers — otherwise the projection cannot fit under the threshold.
		manager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "opening question" }],
			steering: true,
			timestamp: 1,
		});
		manager.appendMessage({
			role: "user",
			content: [{ type: "text", text: `bulk ${"detail ".repeat(2000)}` }],
			timestamp: 2,
		});
		manager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "which deployment rollback checklist did we approve" }],
			timestamp: 3,
		});
		const settings = Settings.isolated({
			"compaction.enabled": true,
			// The threshold must leave a positive provider-message budget after system/tool inputs.
			"compaction.thresholdTokens": 2_000,
			"context.engine": "lossless",
			modelRoles: { smol: "lcm-cue-model" },
		});
		let openedContext: LcmContext | undefined;
		const projectionTokenMeasurements = vi.fn();
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: manager,
			settings,
			modelRegistry: {
				getAvailable: () => [model],
				resolver: () => async () => "key",
				authStorage: { recordObservedUsage: vi.fn(), recordUsageCost: vi.fn() },
			} as never,
			sideStreamFn: () => {
				throw new Error("primary stream must not run");
			},
			lcm: {
				agentDir: tempDir.path(),
				dependencies: {
					onProjectionTokenMeasurement: projectionTokenMeasurements,
					openContext: async options => {
						const context = await openLcmContext(options);
						openedContext = context;
						let captured: SourceSnapshot | undefined;
						const reconcile = context.reconcile.bind(context);
						vi.spyOn(context, "reconcile").mockImplementation((snapshot, reconcileOptions) => {
							captured = snapshot;
							return reconcile(snapshot, reconcileOptions);
						});
						vi.spyOn(context, "project").mockImplementation(() => {
							const snapshot = captured;
							const entries = snapshot?.entries ?? [];
							const historicalSources = entries.slice(0, -1);
							const fresh = entries.at(-1);
							const projection: ContextProjection = {
								revision: 1,
								activeSourceFingerprint: activeSourceFingerprint(entries.map(entry => entry.entryId)),
								ready: true,
								historical:
									historicalSources.length > 0 && snapshot
										? [
												{
													kind: "summary" as const,
													summaryId: "seen-summary",
													summaryHandle: "seen-handle",
													level: 0,
													redactedText: `FIRST\n${"historical detail ".repeat(2_000)}\nLAST`,
													tokenCount: 3,
													sourceIds: historicalSources.map(entry => entry.entryId),
													citations: historicalSources.map((entry, position) => ({
														...snapshot.scope,
														sourceId: entry.entryId,
														sourceKey: `covered-key-${position}`,
														contentHash: entry.contentHash,
														position,
													})),
													files: [],
												},
											]
										: [],
								freshTailSourceIds: fresh ? [fresh.entryId] : [],
								uncoveredSourceIds: [],
								sourceTokens: 5_000,
								selectedLevelCounts: historicalSources.length > 0 ? { 0: 1 } : {},
								coveredSourceCount: historicalSources.length,
								freshSourceCount: fresh ? 1 : 0,
								estimatedTokens: 20,
								pendingJobs: 0,
							};
							return projection;
						});
						vi.spyOn(context, "search").mockReturnValue([
							{
								kind: "summary",
								id: "unseen-summary",
								summaryHandle: "unseen-handle",
								redactedText: "earlier decision: roll back with the staged checklist",
								rank: 1,
								citations: [
									{
										projectId: "p",
										sessionId: "s",
										branchId: "b",
										sourceId: "src",
										sourceKey: "key",
										contentHash: "hash",
										position: 0,
									},
								],
							},
						]);
						return context;
					},
				},
			},
		});
		sessions.push(session);
		// Direct assignment to agent.state.model does not stick; this is the supported setter.
		session.agent.setModel(model);
		expect(session.model?.contextWindow).toBe(200_000);
		expect(session.settings.getGroup("compaction").enabled).toBe(true);
		expect(session.settings.getGroup("compaction").strategy).not.toBe("off");
		const input = manager.buildSessionContext().messages;
		const projected = await session.projectLcmContext(input);
		expect(projected).not.toBe(input);
		const rowsChangedAfterFirst = openedContext?.status().performance?.reconcileRowsChanged;
		expect(rowsChangedAfterFirst).toBeDefined();

		const cueIndex = projected.findIndex(
			message =>
				message.role === "developer" &&
				Array.isArray(message.content) &&
				message.content.some(part => part.type === "text" && part.text.includes("<lcm-cues>")),
		);
		expect(cueIndex).toBeGreaterThanOrEqual(0);
		expect(cueIndex).toBe(projected.findLastIndex(message => message.role === "user") - 1);
		const cueText = JSON.stringify(projected[cueIndex]);
		expect(cueText).toContain("lcm-handle:v1:");
		expect(cueText).not.toContain("older facts");

		expect((await session.lcmStatus()).runtime.lastRequestRoute).toBeUndefined();
		const rawTokens = projected.reduce((total, message) => total + estimateLcmProjectionMessageTokens(message), 0);
		const providerTokens = wrapSteeringForModel(projected).reduce(
			(total, message) => total + estimateLcmProjectionMessageTokens(message),
			0,
		);
		expect(providerTokens).toBeGreaterThan(rawTokens);
		session.beginPrimaryProviderRequest(projected);
		const committedRoute = (await session.lcmStatus()).runtime.lastRequestRoute;
		const measurementCount = projectionTokenMeasurements.mock.calls.length;
		expect(committedRoute).toMatchObject({
			kind: "lossless",
			metrics: { candidateTokens: providerTokens, projectionTokenMeasurements: measurementCount },
		});
		expect(measurementCount).toBeGreaterThan(0);
		expect(measurementCount).toBeLessThanOrEqual(MAX_LCM_PRIMARY_TOKEN_MEASUREMENTS);

		settings.set("context.lossless.retrievalCues", false);
		const withoutCues = await session.projectLcmContext(input);
		expect(openedContext?.status().performance?.reconcileRowsChanged).toBe(rowsChangedAfterFirst);
		expect(withoutCues.some(message => message.role === "developer")).toBe(false);
		const abortedDispatch = new AbortController();
		abortedDispatch.abort();
		session.beginPrimaryProviderRequest(withoutCues, abortedDispatch.signal);
		expect((await session.lcmStatus()).runtime.lastRequestRoute).toEqual(committedRoute);
	}, 30_000);

	it("drops a retrieval cue when the exact steering-aware final render exceeds its budget", async () => {
		const model = buildModel({
			id: "lcm-cue-fit-model",
			name: "LCM Cue Fit Model",
			api: "anthropic",
			provider: "test-lcm-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4_096,
			maxTokens: 256,
		} as ModelSpec<Api>) as Model<Api>;
		const projected: AgentMessage[] = [
			{
				role: "user",
				content: [{ type: "text", text: `near budget ${"x".repeat(1_200)}` }],
				steering: true,
				timestamp: 1,
			},
		];
		const projectedTokens = wrapSteeringForModel(projected).reduce(
			(total, message) => total + estimateLcmProjectionMessageTokens(message),
			0,
		);
		const manager = SessionManager.inMemory("/lcm-cue-fit");
		const settings = Settings.isolated({
			"compaction.enabled": true,
			"compaction.thresholdTokens": projectedTokens + 12,
			"context.engine": "lossless",
		});
		const session = new AgentSession({
			agent: createAgent(),
			sessionManager: manager,
			settings,
			modelRegistry: {
				getAvailable: () => [model],
				resolver: () => async () => "key",
				authStorage: { recordObservedUsage: vi.fn(), recordUsageCost: vi.fn() },
			} as never,
			sideStreamFn: () => {
				throw new Error("primary stream must not run");
			},
			lcm: { agentDir: "/lcm-cue-fit" },
		});
		sessions.push(session);
		session.agent.setModel(model);
		const routeKey = { generation: 1, projectionAttempt: 1, sessionId: manager.getSessionId() } as LcmPrimaryRouteKey;
		vi.spyOn(SessionLcm.prototype, "project").mockResolvedValue({
			messages: projected,
			owned: true,
			candidateTokens: projectedTokens,
			messageTokenBudget: projectedTokens + 12,
			projectionTokenMeasurements: 8,
			projection: {
				revision: 1,
				activeSourceFingerprint: activeSourceFingerprint([]),
				ready: true,
				historical: [],
				freshTailSourceIds: [],
				uncoveredSourceIds: [],
				sourceTokens: projectedTokens,
				selectedLevelCounts: {},
				coveredSourceCount: 0,
				freshSourceCount: 0,
				estimatedTokens: projectedTokens,
				pendingJobs: 0,
			},
			routeKey,
		});
		vi.spyOn(SessionLcm.prototype, "searchProjected").mockResolvedValue([
			{
				kind: "summary",
				id: "cue-summary",
				summaryHandle: "cue-handle",
				redactedText: "deployment rollback checklist ".repeat(20),
				rank: 1,
				citations: [
					{
						projectId: "project",
						sessionId: "session",
						branchId: "branch",
						sourceId: "source",
						sourceKey: "source-key",
						contentHash: "content-hash",
						position: 0,
					},
				],
			},
		]);

		const result = await session.projectLcmContext([
			{ role: "user", content: "which deployment rollback checklist", timestamp: 1 },
		]);

		expect(result).toBe(projected);
		expect(result.some(message => message.role === "developer")).toBe(false);
	});

	it("adds no cue when no lossless projection owns the request", async () => {
		const session = createLcmCompletionSession(() => {
			throw new Error("primary stream must not run");
		});
		const input: AgentMessage[] = [
			{ role: "user", content: [{ type: "text", text: "deployment rollback checklist question" }], timestamp: 1 },
		];
		// Cues are gated on ownership: compaction is disabled here, so LCM returns the native array
		// untouched and no `developer` block may appear even though the setting defaults to on.
		expect(session.settings.get("context.lossless.retrievalCues")).toBe(true);
		const projected = await session.projectLcmContext(input);
		expect(projected).toBe(input);
		expect(projected.some(message => message.role === "developer")).toBe(false);
	});

	it("emits projection evidence only while Lossless owns the transformed request", async () => {
		const session = createLcmCompletionSession(() => {
			throw new Error("primary stream must not run");
		});
		session.settings.set("context.lossless.retrievalCues", false);
		const projection: ContextProjection = {
			revision: 1,
			activeSourceFingerprint: activeSourceFingerprint([]),
			ready: true,
			historical: [],
			freshTailSourceIds: [],
			uncoveredSourceIds: [],
			sourceTokens: 1,
			selectedLevelCounts: {},
			coveredSourceCount: 0,
			freshSourceCount: 0,
			estimatedTokens: 1,
			pendingJobs: 0,
		};
		const projected: AgentMessage[] = [{ role: "user", content: "projected", timestamp: 1 }];
		const validResult: SessionLcmProjectResult = {
			messages: projected,
			owned: true,
			candidateTokens: 1,
			messageTokenBudget: 10,
			projectionTokenMeasurements: 1,
			projection,
			routeKey: { generation: 1, projectionAttempt: 1, sessionId: "test" },
		};
		vi.spyOn(SessionLcm.prototype, "project")
			.mockResolvedValueOnce(validResult)
			.mockResolvedValueOnce({ ...validResult, routeKey: undefined });
		const events: AgentSessionEvent[] = [];
		session.subscribe(event => events.push(event));

		const ownedInput: AgentMessage[] = [{ role: "user", content: "owned", timestamp: 1 }];
		expect(await session.projectLcmContext(ownedInput)).toBe(projected);
		expect(events.filter(event => event.type === "lcm_projection")).toEqual([{ type: "lcm_projection", projection }]);

		const nativeInput: AgentMessage[] = [{ role: "user", content: "native", timestamp: 2 }];
		expect(await session.projectLcmContext(nativeInput)).toBe(nativeInput);
		expect(events.filter(event => event.type === "lcm_projection")).toHaveLength(1);
	});

	it("binds overlapping primary transforms to their own provider dispatch", async () => {
		const session = createLcmCompletionSession(() => {
			throw new Error("primary stream must not run");
		});
		await session.lcmStatus();
		const olderInput: AgentMessage[] = [{ role: "user", content: "older input", timestamp: 1 }];
		const olderProjected: AgentMessage[] = [{ role: "user", content: "older projected", timestamp: 1 }];
		const newerInput: AgentMessage[] = [{ role: "user", content: "newer input", timestamp: 2 }];
		const newerProjected: AgentMessage[] = [{ role: "user", content: "newer projected", timestamp: 2 }];
		const olderKey = {
			generation: 1,
			projectionAttempt: 1,
			sessionId: "older",
		} as LcmPrimaryRouteKey;
		const newerKey = {
			generation: 1,
			projectionAttempt: 2,
			sessionId: "newer",
		} as LcmPrimaryRouteKey;
		const olderResult = Promise.withResolvers<SessionLcmProjectResult>();
		const newerReady = Promise.withResolvers<AgentMessage[]>();
		const releaseNewerDispatch = Promise.withResolvers<void>();
		const project = vi
			.spyOn(SessionLcm.prototype, "project")
			.mockImplementationOnce(async () => olderResult.promise)
			.mockResolvedValueOnce({
				messages: newerProjected,
				owned: true,
				candidateTokens: 1,
				messageTokenBudget: 10,
				projectionTokenMeasurements: 1,
				routeKey: newerKey,
			});
		const commit = vi.spyOn(SessionLcm.prototype, "commitPrimaryRequestRoute").mockReturnValue(true);
		const recordTokens = vi.spyOn(SessionLcm.prototype, "recordPendingPrimaryProviderTokens");

		const olderDispatch = (async () => {
			const messages = await session.projectLcmContext(olderInput);
			session.beginPrimaryProviderRequest(messages);
			return messages;
		})();
		expect(project).toHaveBeenCalledTimes(1);
		const newerDispatch = (async () => {
			const messages = await session.projectLcmContext(newerInput);
			newerReady.resolve(messages);
			await releaseNewerDispatch.promise;
			session.beginPrimaryProviderRequest(messages);
			return messages;
		})();
		expect(await newerReady.promise).toBe(newerProjected);

		olderResult.resolve({
			messages: olderProjected,
			owned: true,
			candidateTokens: 1,
			messageTokenBudget: 10,
			projectionTokenMeasurements: 1,
			routeKey: olderKey,
		});
		expect(await olderDispatch).toBe(olderInput);
		expect(commit).not.toHaveBeenCalled();

		releaseNewerDispatch.resolve();
		expect(await newerDispatch).toBe(newerProjected);
		expect(commit).toHaveBeenCalledTimes(1);
		expect(commit).toHaveBeenCalledWith(newerKey);
		expect(recordTokens).toHaveBeenCalledTimes(1);
		expect(recordTokens).toHaveBeenCalledWith(newerKey, 1, 1);

		const staleOwnedKey = { ...olderKey, projectionAttempt: 3, sessionId: "stale-owned" };
		const currentOwnedKey = { ...newerKey, projectionAttempt: 4, sessionId: "current-owned" };
		const sharedProjected: AgentMessage[] = [{ role: "user", content: "shared projected", timestamp: 3 }];
		project.mockResolvedValueOnce({
			messages: sharedProjected,
			owned: true,
			candidateTokens: 1,
			messageTokenBudget: 10,
			projectionTokenMeasurements: 1,
			routeKey: staleOwnedKey,
		});
		const staleOwned = await session.projectLcmContext(olderInput);
		project.mockResolvedValueOnce({
			messages: sharedProjected,
			owned: true,
			candidateTokens: 1,
			messageTokenBudget: 10,
			projectionTokenMeasurements: 1,
			routeKey: currentOwnedKey,
		});
		const currentOwned = await session.projectLcmContext(newerInput);
		expect(currentOwned).not.toBe(staleOwned);

		expect(() => session.beginPrimaryProviderRequest(staleOwned)).toThrow(AIError.AbortError);
		expect(commit).toHaveBeenCalledTimes(1);
		session.beginPrimaryProviderRequest(currentOwned);
		expect(commit).toHaveBeenCalledTimes(2);
		expect(commit).toHaveBeenLastCalledWith(currentOwnedKey);

		const supersedingKey = { ...newerKey, projectionAttempt: 5, sessionId: "superseding" };
		project.mockResolvedValueOnce({
			messages: newerProjected,
			owned: true,
			candidateTokens: 1,
			messageTokenBudget: 10,
			projectionTokenMeasurements: 1,
			routeKey: supersedingKey,
		});
		await session.projectLcmContext([{ role: "user", content: "superseding input", timestamp: 4 }]);
		expect(() => session.beginPrimaryProviderRequest(currentOwned)).toThrow(AIError.AbortError);
		expect(commit).toHaveBeenCalledTimes(2);
	});

	it("builds cue terms from user text only, across scripts, ignoring image parts", () => {
		// Image parts must never reach the query; stringifying one would send base64 to FTS.
		expect(
			lcmCueTerms([
				{
					role: "user",
					content: [
						{ type: "text", text: "which deployment rollback checklist did we approve" },
						{ type: "image", data: "iVBORw0KGgo=", mimeType: "image/png" },
					],
					timestamp: 1,
				},
			]),
		).toEqual(["deployment", "checklist", "rollback"]);

		// A Unicode-aware split is required: an ASCII-only one returns nothing for non-Latin text.
		expect(
			lcmCueTerms([
				{ role: "user", content: [{ type: "text", text: "kenapa proyeksi belum dievaluasi" }], timestamp: 1 },
			]),
		).toEqual(["dievaluasi", "proyeksi", "kenapa"]);
		expect(
			lcmCueTerms([{ role: "user", content: [{ type: "text", text: "配置文件 已经 更新" }], timestamp: 1 }]),
		).toEqual(["配置文件"]);

		// Only the newest user message matters; stopwords and short tokens are dropped.
		expect(
			lcmCueTerms([
				{ role: "user", content: [{ type: "text", text: "stale earlier question" }], timestamp: 1 },
				{ role: "user", content: [{ type: "text", text: "that with them" }], timestamp: 2 },
			]),
		).toEqual([]);
		expect(lcmCueTerms([{ ...createAssistantMessage("no user here"), timestamp: 1 }])).toEqual([]);
		expect(lcmCueTerms([{ role: "user", content: "plain string content works", timestamp: 1 }])).toEqual([
			"content",
			"string",
			"plain",
		]);
	});

	it("runs LCM before extensions only on the primary provider path", async () => {
		using tempDir = TempDir.createSync("@pi-lcm-transform-order-");
		const api = "test-lcm-transform-order";
		const providerContexts: Context[] = [];
		let providerSignal: AbortSignal | undefined;
		const beginPrimary = vi.spyOn(AgentSession.prototype, "beginPrimaryProviderRequest");
		registerCustomApi(api, (_model, context, options) => {
			providerContexts.push(context);
			providerSignal = options?.signal;
			const stream = new AssistantMessageEventStream();
			queueMicrotask(() => stream.push({ type: "done", reason: "stop", message: createAssistantMessage("ok") }));
			return stream;
		});
		const model = buildModel({
			id: "lcm-transform-model",
			name: "LCM Transform Model",
			api,
			provider: "test-provider",
			baseUrl: "",
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 4096,
			maxTokens: 1024,
		} as ModelSpec<Api>) as Model<Api>;
		const extensionInputs: AgentMessage[][] = [];
		const extension: ExtensionFactory = pi => {
			pi.on("context", event => {
				extensionInputs.push(event.messages);
				return {
					messages: [
						...event.messages,
						{ role: "user", content: [{ type: "text", text: "extension-marker" }], timestamp: 3 },
					],
				};
			});
		};
		const authStorage = await AuthStorage.create(tempDir.join("auth.db"));
		authStorage.setRuntimeApiKey(model.provider, "test-key");
		const modelRegistry = new ModelRegistry(authStorage, tempDir.join("models.yml"));
		const { session } = await createAgentSession({
			cwd: tempDir.path(),
			agentDir: tempDir.path(),
			sessionManager: SessionManager.inMemory(tempDir.path()),
			authStorage,
			modelRegistry,
			settings: Settings.isolated({ "compaction.enabled": false }),
			model,
			disableExtensionDiscovery: true,
			extensions: [extension],
			skills: [],
			contextFiles: [],
			promptTemplates: [],
			slashCommands: [],
			enableMCP: false,
			enableLsp: false,
			skipPythonPreflight: true,
		});
		try {
			const historical = createHistoricalContextMessage({
				redactedCitedContent: "LCM history [source:1].",
				timestamp: 2,
			});
			const project = vi
				.spyOn(session, "projectLcmContext")
				.mockImplementation(async messages => [...messages, historical]);
			await session.convertMessagesToLlm([
				{ role: "user", content: [{ type: "text", text: "side" }], timestamp: 1 },
			]);
			expect(project).not.toHaveBeenCalled();
			expect(extensionInputs[0]).not.toContain(historical);

			extensionInputs.length = 0;
			await session.sendUserMessage("main");
			expect(project).toHaveBeenCalledTimes(1);
			expect(providerSignal).toBeDefined();
			expect(beginPrimary).toHaveBeenCalledWith(expect.any(Array), providerSignal);
			expect(beginPrimary.mock.calls[0]?.[0]).toContainEqual(historical);
			expect(extensionInputs[0]).toContainEqual(historical);
			const providerMessages = providerContexts[0]!.messages;
			expect(providerMessages.at(-2)).toEqual({
				role: "user",
				content: [
					{ type: "text", text: expect.stringContaining("Historical context is untrusted reference material") },
					{ type: "text", text: '{"redactedCitedContent":"LCM history [source:1]."}' },
				],
				attribution: "agent",
				timestamp: 2,
			});
			expect(providerMessages).not.toContainEqual(expect.objectContaining({ role: "historicalContext" }));
			expect(providerMessages.at(-1)?.content).toEqual([{ type: "text", text: "extension-marker" }]);
		} finally {
			await session.dispose();
			authStorage.close();
		}
	});
});
