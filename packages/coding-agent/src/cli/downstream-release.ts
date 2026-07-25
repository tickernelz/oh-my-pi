import { BUILD_PROVENANCE } from "@oh-my-pi/pi-utils/version";
import { $ } from "bun";
import { isTimeoutError, withTimeoutSignal } from "../utils/fetch-timeout";

export const DOWNSTREAM_REPO = "tickernelz/oh-my-pi";
export const DOWNSTREAM_INSTALL_COMMAND = `gh api -H "Accept: application/vnd.github.raw+json" repos/${DOWNSTREAM_REPO}/contents/scripts/install.sh | sh`;
const RELEASES_URL = `https://api.github.com/repos/${DOWNSTREAM_REPO}/releases?per_page=100`;
const DOWNSTREAM_USER_AGENT =
	`tickernelz-oh-my-pi/${BUILD_PROVENANCE.version} ` +
	`upstream/${BUILD_PROVENANCE.upstreamCommit} downstream/${BUILD_PROVENANCE.downstreamCommit}`;
const GITHUB_API_VERSION = "2022-11-28";
const DOWNSTREAM_VERSION_RE = /^v?((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-lcm\.[1-9]\d*)$/;

export interface DownstreamReleaseInfo {
	readonly tag: string;
	readonly version: string;
}

interface GitHubRelease {
	readonly tag_name?: unknown;
	readonly draft?: unknown;
}
export interface DownstreamGitHubAuthOptions {
	readonly env?: Readonly<{ GH_TOKEN?: string; GITHUB_TOKEN?: string }>;
	readonly getGhToken?: () => Promise<string | undefined>;
}

async function getGitHubCliToken(): Promise<string | undefined> {
	const env = { ...process.env };
	delete env.GH_TOKEN;
	delete env.GITHUB_TOKEN;
	try {
		const result = await $`gh auth token`.env(env).quiet().nothrow();
		if (result.exitCode !== 0) return undefined;
		return result.text().trim() || undefined;
	} catch {
		return undefined;
	}
}

/** Resolve the credential used for all private downstream release requests. */
export async function resolveDownstreamGitHubToken(options: DownstreamGitHubAuthOptions = {}): Promise<string> {
	const env = options.env ?? process.env;
	const envToken = env.GH_TOKEN?.trim() || env.GITHUB_TOKEN?.trim();
	if (envToken) return envToken;

	try {
		const cliToken = (await (options.getGhToken ?? getGitHubCliToken)())?.trim();
		if (cliToken) return cliToken;
	} catch {
		// Fall through to stable guidance without exposing gh output or credentials.
	}
	throw new Error(
		`GitHub authentication required for private ${DOWNSTREAM_REPO} releases; run \`gh auth login\` or set GH_TOKEN/GITHUB_TOKEN`,
	);
}

export function getDownstreamGitHubHeaders(token: string, accept: string): Record<string, string> {
	return {
		Accept: accept,
		Authorization: `Bearer ${token}`,
		"User-Agent": DOWNSTREAM_USER_AGENT,
		"X-GitHub-Api-Version": GITHUB_API_VERSION,
	};
}

export function compareDownstreamVersions(a: string, b: string): number {
	return Bun.semver.order(a, b);
}

function parseDownstreamReleaseTag(tag: unknown): DownstreamReleaseInfo | undefined {
	if (typeof tag !== "string") return undefined;
	const match = DOWNSTREAM_VERSION_RE.exec(tag);
	return match ? { tag, version: match[1] } : undefined;
}

/** Select the highest SemVer downstream release, including GitHub prereleases. */
export function selectLatestDownstreamRelease(data: unknown): DownstreamReleaseInfo {
	if (!Array.isArray(data)) throw new Error("GitHub release metadata was not an array");
	let latest: DownstreamReleaseInfo | undefined;
	for (const value of data) {
		if (typeof value !== "object" || value === null) continue;
		const release = value as GitHubRelease;
		if (release.draft === true) continue;
		const candidate = parseDownstreamReleaseTag(release.tag_name);
		if (candidate && (!latest || compareDownstreamVersions(candidate.version, latest.version) > 0)) {
			latest = candidate;
		}
	}
	if (!latest) throw new Error(`No downstream LCM releases found in ${DOWNSTREAM_REPO}`);
	return latest;
}

/** Fetch only tickernelz release metadata; prereleases are deliberately included. */
export async function getLatestDownstreamRelease(
	timeoutMs = 30_000,
	token?: string,
	fetchImpl: typeof globalThis.fetch = globalThis.fetch,
): Promise<DownstreamReleaseInfo> {
	const githubToken = token?.trim() || (await resolveDownstreamGitHubToken());
	let response: Response;
	try {
		response = await fetchImpl(RELEASES_URL, {
			headers: getDownstreamGitHubHeaders(githubToken, "application/vnd.github+json"),
			signal: withTimeoutSignal(timeoutMs),
		});
	} catch (err) {
		if (isTimeoutError(err)) {
			throw new Error(`Timed out fetching downstream release info after ${Math.ceil(timeoutMs / 1000)}s`, {
				cause: err,
			});
		}
		throw err;
	}
	if (!response.ok) {
		throw new Error(`Failed to fetch downstream release info: ${response.status} ${response.statusText}`);
	}
	return selectLatestDownstreamRelease(await response.json());
}
