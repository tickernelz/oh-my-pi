import { version as upstreamVersion } from "../package.json" with { type: "json" };

const UPSTREAM_VERSION_RE = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$/;
const LCM_REVISION_RE = /^(?:0|[1-9]\d*)$/;
const GIT_COMMIT_RE = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/i;

export interface BuildProvenance {
	readonly version: string;
	readonly upstreamVersion: string;
	readonly lcmRevision: string;
	readonly upstreamCommit: string;
	readonly downstreamCommit: string;
}

export interface BuildProvenanceInput {
	readonly upstreamVersion: string;
	readonly lcmRevision?: string;
	readonly upstreamCommit?: string;
	readonly downstreamCommit?: string;
}

/** Build a SemVer-valid downstream version and retain both exact source commits. */
export function resolveBuildProvenance(input: BuildProvenanceInput, requireRelease = false): BuildProvenance {
	if (!UPSTREAM_VERSION_RE.test(input.upstreamVersion)) {
		throw new Error(`Invalid upstream version: ${input.upstreamVersion}`);
	}

	const lcmRevision = input.lcmRevision ?? "0";
	const upstreamCommit = input.upstreamCommit ?? "unknown";
	const downstreamCommit = input.downstreamCommit ?? "unknown";
	if (!LCM_REVISION_RE.test(lcmRevision)) {
		throw new Error(
			`OMP_LCM_REVISION must be a non-negative integer without leading zeroes (received ${lcmRevision})`,
		);
	}
	if (requireRelease && lcmRevision === "0") {
		throw new Error("OMP_LCM_REVISION must be greater than zero for release builds");
	}
	const requireExactCommits = requireRelease || lcmRevision !== "0";
	for (const [name, commit] of [
		["OMP_UPSTREAM_COMMIT", upstreamCommit],
		["OMP_DOWNSTREAM_COMMIT", downstreamCommit],
	] as const) {
		if (requireExactCommits && !GIT_COMMIT_RE.test(commit)) {
			throw new Error(
				`${name} must contain the full 40- or 64-character Git commit outside local revision 0 builds`,
			);
		}
		if (commit !== "unknown" && !GIT_COMMIT_RE.test(commit)) {
			throw new Error(`${name} must be a full Git commit or "unknown" (received ${commit})`);
		}
	}

	return Object.freeze({
		version: `${input.upstreamVersion}-lcm.${lcmRevision}`,
		upstreamVersion: input.upstreamVersion,
		lcmRevision,
		upstreamCommit,
		downstreamCommit,
	});
}

/** Resolve provenance from compile-time environment values; release mode rejects incomplete inputs. */
export function resolveBuildProvenanceFromEnvironment(requireRelease = false): BuildProvenance {
	return resolveBuildProvenance(
		{
			upstreamVersion,
			lcmRevision: process.env.OMP_LCM_REVISION,
			upstreamCommit: process.env.OMP_UPSTREAM_COMMIT,
			downstreamCommit: process.env.OMP_DOWNSTREAM_COMMIT,
		},
		requireRelease,
	);
}

export const BUILD_PROVENANCE = resolveBuildProvenanceFromEnvironment();

export const VERSION: string = BUILD_PROVENANCE.version;
export const UPSTREAM_VERSION: string = BUILD_PROVENANCE.upstreamVersion;
export const LCM_REVISION: string = BUILD_PROVENANCE.lcmRevision;
export const UPSTREAM_COMMIT: string = BUILD_PROVENANCE.upstreamCommit;
export const DOWNSTREAM_COMMIT: string = BUILD_PROVENANCE.downstreamCommit;

const DIGITS = /^\d+$/;

/**
 * Compare two version strings.
 *
 * Canonical comparator that supersedes the historical in-repo copies
 * (update-cli, hackage scraper, release scripts):
 * - inputs are trimmed and at most one leading `v`/`V` is stripped
 * - dot-separated segments are compared numerically, missing trailing
 *   segments count as 0, so `1.2` === `1.2.0` and any segment count works
 * - a SemVer-2.0 prerelease suffix sorts before the plain release
 *   (`1.0.0-beta` < `1.0.0`); prerelease identifiers follow SemVer order
 *   (numeric < alphanumeric, numeric compared by value, alphanumeric
 *   compared lexically, longer sets of equal fields win)
 * - SemVer build metadata begins at the first `+` and does not participate
 *   in precedence; it is stripped before core/prerelease parsing
 * - malformed numeric segments compare as 0 (`1.2.x` === `1.2.0`)
 * - never throws; returns only -1 | 0 | 1
 */
export function compareVersions(a: string, b: string): number {
	const pa = parseVersion(a);
	const pb = parseVersion(b);

	const core = compareNumericParts(pa.core, pb.core);
	if (core !== 0) return core;

	return comparePrerelease(pa.prerelease, pb.prerelease);
}

interface ParsedVersion {
	core: string[];
	prerelease: string[] | null;
}

function parseVersion(version: string): ParsedVersion {
	const trimmed = version.trim();
	const stripped = trimmed.startsWith("v") || trimmed.startsWith("V") ? trimmed.slice(1) : trimmed;
	const plusIndex = stripped.indexOf("+");
	const withoutBuild = plusIndex === -1 ? stripped : stripped.slice(0, plusIndex);
	const dashIndex = withoutBuild.indexOf("-");
	if (dashIndex === -1) {
		return { core: withoutBuild.split("."), prerelease: null };
	}
	return {
		core: withoutBuild.slice(0, dashIndex).split("."),
		prerelease: withoutBuild.slice(dashIndex + 1).split("."),
	};
}

/** Compare dot-separated numeric segments; missing/malformed segments count as 0. */
function compareNumericParts(a: string[], b: string[]): number {
	const length = Math.max(a.length, b.length);
	for (let i = 0; i < length; i++) {
		// Missing or malformed segments compare as 0.
		const sa = a[i];
		const sb = b[i];
		const result = compareDigits(
			sa !== undefined && DIGITS.test(sa) ? sa : "0",
			sb !== undefined && DIGITS.test(sb) ? sb : "0",
		);
		if (result !== 0) return result;
	}
	return 0;
}

/** Exact integer comparison of digit strings, avoiding float overflow. */
function compareDigits(a: string, b: string): number {
	const na = a.replace(/^0+/, "") || "0";
	const nb = b.replace(/^0+/, "") || "0";
	if (na.length !== nb.length) return na.length < nb.length ? -1 : 1;
	if (na < nb) return -1;
	if (na > nb) return 1;
	return 0;
}

/** SemVer-2.0 prerelease ordering; null means a plain release, which wins. */
function comparePrerelease(a: string[] | null, b: string[] | null): number {
	if (a === null || b === null) {
		return a === b ? 0 : a === null ? 1 : -1;
	}
	const length = Math.max(a.length, b.length);
	for (let i = 0; i < length; i++) {
		const ia = a[i];
		const ib = b[i];
		if (ia === undefined) return -1;
		if (ib === undefined) return 1;
		const aNumeric = DIGITS.test(ia);
		const bNumeric = DIGITS.test(ib);
		if (aNumeric && bNumeric) {
			const result = compareDigits(ia, ib);
			if (result !== 0) return result;
		} else if (aNumeric !== bNumeric) {
			return aNumeric ? -1 : 1;
		} else if (ia !== ib) {
			return ia < ib ? -1 : 1;
		}
	}
	return 0;
}
