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
