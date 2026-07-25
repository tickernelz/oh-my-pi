import { BUILD_PROVENANCE } from "@oh-my-pi/pi-utils/version";
import { isTimeoutError, withTimeoutSignal } from "../utils/fetch-timeout";

export const DOWNSTREAM_REPO = "tickernelz/oh-my-pi";
export const DOWNSTREAM_INSTALLER = `https://raw.githubusercontent.com/${DOWNSTREAM_REPO}/main/scripts/install.sh`;
const RELEASES_URL = `https://api.github.com/repos/${DOWNSTREAM_REPO}/releases?per_page=100`;
const DOWNSTREAM_USER_AGENT =
	`tickernelz-oh-my-pi/${BUILD_PROVENANCE.version} ` +
	`upstream/${BUILD_PROVENANCE.upstreamCommit} downstream/${BUILD_PROVENANCE.downstreamCommit}`;
const DOWNSTREAM_VERSION_RE = /^v?((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-lcm\.[1-9]\d*)$/;

export interface DownstreamReleaseInfo {
	readonly tag: string;
	readonly version: string;
}

interface GitHubRelease {
	readonly tag_name?: unknown;
	readonly draft?: unknown;
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
export async function getLatestDownstreamRelease(timeoutMs = 30_000): Promise<DownstreamReleaseInfo> {
	let response: Response;
	try {
		response = await fetch(RELEASES_URL, {
			headers: {
				Accept: "application/vnd.github+json",
				"User-Agent": DOWNSTREAM_USER_AGENT,
			},
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
