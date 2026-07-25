import { afterEach, describe, expect, it, vi } from "bun:test";
import { getLatestDownstreamRelease } from "./downstream-release";

type FetchInput = string | URL | Request;
type FetchInit = RequestInit | BunFetchRequestInit;

describe("downstream release metadata", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("queries the downstream releases list with a timeout signal and includes prereleases", async () => {
		let requestedUrl = "";
		let requestSignal: AbortSignal | undefined;
		let requestHeaders: Headers | undefined;
		const fetchStub = Object.assign(
			async (input: FetchInput, init?: FetchInit) => {
				requestedUrl = String(input);
				requestSignal = init?.signal ?? undefined;
				requestHeaders = new Headers(init?.headers);
				return Response.json([
					{ tag_name: "v17.1.3-lcm.8", draft: false, prerelease: true },
					{ tag_name: "v17.1.3", draft: false, prerelease: false },
				]);
			},
			{ preconnect: globalThis.fetch.preconnect },
		);
		vi.spyOn(globalThis, "fetch").mockImplementation(fetchStub);

		await expect(getLatestDownstreamRelease()).resolves.toEqual({
			tag: "v17.1.3-lcm.8",
			version: "17.1.3-lcm.8",
		});
		expect(requestedUrl).toBe("https://api.github.com/repos/tickernelz/oh-my-pi/releases?per_page=100");
		expect(requestSignal).toBeInstanceOf(AbortSignal);
		const userAgent = requestHeaders?.get("user-agent");
		expect(userAgent).toMatch(/^tickernelz-oh-my-pi\/\d+\.\d+\.\d+-lcm\.\d+ /);
		expect(userAgent).toMatch(/upstream\/(?:unknown|[0-9a-f]{40}|[0-9a-f]{64}) /i);
		expect(userAgent).toMatch(/downstream\/(?:unknown|[0-9a-f]{40}|[0-9a-f]{64})$/i);
	});
});
