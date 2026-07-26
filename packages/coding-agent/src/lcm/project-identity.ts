import * as path from "node:path";
import { getLcmDir, normalizePathForComparison, resolveEquivalentPath } from "@oh-my-pi/pi-utils/dirs";
import * as git from "../utils/git";

const PROJECT_ID_VERSION = "v1";

export interface LcmProject {
	projectId: string;
	rootPath: string;
	storePath: string;
}

/** Resolve the stable, path-derived LCM identity and store for a project. */
export async function resolveLcmProject(cwd: string, agentDir?: string): Promise<LcmProject> {
	const rootPath = resolveEquivalentPath((await git.repo.primaryRoot(cwd)) ?? cwd);
	const identityPath = normalizePathForComparison(rootPath);
	const digest = new Bun.CryptoHasher("sha256")
		.update(`omp-lcm-project:${PROJECT_ID_VERSION}\0${identityPath}`)
		.digest("hex");
	const projectId = `${PROJECT_ID_VERSION}-${digest}`;
	return {
		projectId,
		rootPath,
		storePath: path.join(getLcmDir(agentDir), "projects", projectId, "context.sqlite"),
	};
}
