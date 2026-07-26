import { describe, expect, it, vi } from "bun:test";
import { getLatestDownstreamRelease, resolveDownstreamGitHubToken } from "./downstream-release";
import { fetchDownstreamReleaseAsset } from "./update-cli";

type FetchInput = string | URL | Request;
type FetchInit = RequestInit | BunFetchRequestInit;

describe("downstream GitHub authentication", () => {
	it("prefers trimmed GH_TOKEN, then trimmed GITHUB_TOKEN, without invoking gh", async () => {
		const getGhToken = vi.fn(async () => "cli-token");
		await expect(
			resolveDownstreamGitHubToken({
				env: { GH_TOKEN: "  gh-token  ", GITHUB_TOKEN: "github-token" },
				getGhToken,
			}),
		).resolves.toBe("gh-token");
		await expect(
			resolveDownstreamGitHubToken({
				env: { GH_TOKEN: "  ", GITHUB_TOKEN: "  github-token  " },
				getGhToken,
			}),
		).resolves.toBe("github-token");
		expect(getGhToken).not.toHaveBeenCalled();
	});

	it("falls back to a trimmed gh auth token", async () => {
		const getGhToken = vi.fn(async () => "  cli-token  ");
		await expect(
			resolveDownstreamGitHubToken({
				env: { GH_TOKEN: "", GITHUB_TOKEN: "\t" },
				getGhToken,
			}),
		).resolves.toBe("cli-token");
		expect(getGhToken).toHaveBeenCalledTimes(1);
	});

	it("fails actionably without exposing gh errors", async () => {
		const sensitiveOutput = "credential-from-gh-stderr";
		const error = await resolveDownstreamGitHubToken({
			env: {},
			getGhToken: async () => {
				throw new Error(sensitiveOutput);
			},
		}).catch((err: unknown) => err);
		const message = String(error);
		expect(message).toContain("gh auth login");
		expect(message).toContain("GH_TOKEN/GITHUB_TOKEN");
		expect(message).not.toContain(sensitiveOutput);
	});
});

describe("downstream release metadata", () => {
	it("queries the authenticated downstream releases list with a timeout signal and includes prereleases", async () => {
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

		await expect(getLatestDownstreamRelease(30_000, "metadata-token", fetchStub)).resolves.toEqual({
			tag: "v17.1.3-lcm.8",
			version: "17.1.3-lcm.8",
		});
		expect(requestedUrl).toBe("https://api.github.com/repos/tickernelz/oh-my-pi/releases?per_page=100");
		expect(requestSignal).toBeInstanceOf(AbortSignal);
		expect(requestHeaders?.get("accept")).toBe("application/vnd.github+json");
		expect(requestHeaders?.get("authorization")).toBe("Bearer metadata-token");
		expect(requestHeaders?.get("x-github-api-version")).toBe("2022-11-28");
		const userAgent = requestHeaders?.get("user-agent");
		expect(userAgent).toMatch(/^tickernelz-oh-my-pi\/\d+\.\d+\.\d+-lcm\.\d+ /);
		expect(userAgent).toMatch(/upstream\/(?:unknown|[0-9a-f]{40}|[0-9a-f]{64}) /i);
		expect(userAgent).toMatch(/downstream\/(?:unknown|[0-9a-f]{40}|[0-9a-f]{64})$/i);
	});

	it("authenticates the initial github.com release asset request", async () => {
		let requestedUrl = "";
		let requestInit: FetchInit | undefined;
		const fetchStub = Object.assign(
			async (input: FetchInput, init?: FetchInit) => {
				requestedUrl = String(input);
				requestInit = init;
				return new Response("artifact");
			},
			{ preconnect: globalThis.fetch.preconnect },
		);

		await fetchDownstreamReleaseAsset(
			"https://github.com/tickernelz/oh-my-pi/releases/download/v17.1.3-lcm.8/omp-linux-x64",
			"omp-linux-x64",
			"asset-token",
			30_000,
			fetchStub,
		);
		expect(requestedUrl).toContain("github.com/tickernelz/oh-my-pi/releases/download/");
		expect(requestInit?.redirect).toBe("follow");
		expect(requestInit?.signal).toBeInstanceOf(AbortSignal);
		const headers = new Headers(requestInit?.headers);
		expect(headers.get("accept")).toBe("application/octet-stream");
		expect(headers.get("authorization")).toBe("Bearer asset-token");
		expect(headers.get("user-agent")).toMatch(/^tickernelz-oh-my-pi\//);
		expect(headers.get("x-github-api-version")).toBe("2022-11-28");
	});
});
