import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import { scheduler } from "node:timers/promises";
import type { Context } from "@oh-my-pi/pi-ai";
import {
	clearGitLabDuoDirectAccessCache,
	getGitLabDuoModels,
	streamGitLabDuo,
} from "@oh-my-pi/pi-ai/providers/gitlab-duo";
import * as registerBuiltins from "@oh-my-pi/pi-ai/providers/register-builtins";

const context: Context = {
	systemPrompt: ["You are helpful."],
	messages: [{ role: "user", content: "Reply OK", timestamp: 0 }],
	tools: [],
};

afterEach(() => {
	clearGitLabDuoDirectAccessCache();
	mock.restore();
});

describe("GitLab Duo prompt cache affinity", () => {
	it("forwards explicit cache affinity and disabled Responses chaining to the proxy", async () => {
		const model = getGitLabDuoModels().find(candidate => candidate.id === "duo-chat-gpt-5-codex");
		if (!model) throw new Error("GitLab Duo Responses model is missing");
		const cacheKey = "gitlab-duo-cache-key";
		let payload: Record<string, unknown> | undefined;
		const responsesSpy = spyOn(registerBuiltins, "streamOpenAIResponses");
		const stream = streamGitLabDuo(model, context, {
			apiKey: "gitlab-access-token",
			promptCacheKey: cacheKey,
			statefulResponses: false,
			fetch: async input => {
				if (String(input).includes("/direct_access")) {
					return new Response(JSON.stringify({ token: "direct-access-token", headers: {} }), {
						status: 200,
						headers: { "content-type": "application/json" },
					});
				}
				throw new Error("the payload hook should stop the proxy request before fetch");
			},
			onPayload: body => {
				payload = body as Record<string, unknown>;
				throw new Error("stop after payload capture");
			},
		});

		await stream.result();

		expect(responsesSpy).toHaveBeenCalledTimes(1);
		expect(responsesSpy.mock.calls[0]?.[2]).toMatchObject({
			promptCacheKey: cacheKey,
			statefulResponses: false,
		});
		expect(payload?.prompt_cache_key).toBe(cacheKey);
	});

	it.each([
		["Anthropic", "duo-chat-sonnet-4-6"],
		["OpenAI chat", "duo-chat-gpt-5-1"],
		["OpenAI Responses", "duo-chat-gpt-5-codex"],
	] as const)("forwards single-attempt requests through the %s proxy", async (_transport, modelId) => {
		spyOn(scheduler, "wait").mockResolvedValue(undefined);
		const model = getGitLabDuoModels().find(candidate => candidate.id === modelId);
		if (!model) throw new Error(`GitLab Duo model is missing: ${modelId}`);
		let providerWireCalls = 0;
		const stream = streamGitLabDuo(model, context, {
			apiKey: `gitlab-access-token-${modelId}`,
			disableProviderRetries: true,
			fetch: async input => {
				if (String(input).includes("/direct_access")) {
					return new Response(JSON.stringify({ token: "direct-access-token", headers: {} }), {
						status: 200,
						headers: { "content-type": "application/json" },
					});
				}
				providerWireCalls++;
				return new Response(JSON.stringify({ error: { type: "server_error", message: "busy" } }), {
					status: 503,
					headers: { "content-type": "application/json", "retry-after-ms": "0" },
				});
			},
		});

		const result = await stream.result();

		expect(providerWireCalls).toBe(1);
		expect(result.stopReason).toBe("error");
	});
});
