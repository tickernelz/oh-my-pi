import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { resolveLcmProject } from "../src/lcm/project-identity";

function initRepository(root: string): void {
	fs.mkdirSync(path.join(root, ".git"), { recursive: true });
	fs.writeFileSync(path.join(root, ".git", "HEAD"), "ref: refs/heads/main\n");
}

function linkWorktree(primaryRoot: string, worktreeRoot: string): void {
	const commonDir = path.join(primaryRoot, ".git");
	const gitDir = path.join(commonDir, "worktrees", path.basename(worktreeRoot));
	fs.mkdirSync(gitDir, { recursive: true });
	fs.mkdirSync(worktreeRoot, { recursive: true });
	fs.writeFileSync(path.join(gitDir, "HEAD"), "ref: refs/heads/feature\n");
	fs.writeFileSync(path.join(gitDir, "commondir"), `${path.relative(gitDir, commonDir)}\n`);
	fs.writeFileSync(path.join(worktreeRoot, ".git"), `gitdir: ${path.relative(worktreeRoot, gitDir)}\n`);
}

describe("resolveLcmProject", () => {
	let tempRoot: string;
	let agentDir: string;

	beforeEach(() => {
		tempRoot = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "omp-lcm-project-")));
		agentDir = path.join(tempRoot, "agent-data");
	});

	afterEach(() => {
		fs.rmSync(tempRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
	});

	it("shares one stable identity and store across linked worktrees", async () => {
		const primaryRoot = path.join(tempRoot, "project");
		const worktreeRoot = path.join(tempRoot, "worktrees", "feature");
		initRepository(primaryRoot);
		linkWorktree(primaryRoot, worktreeRoot);
		fs.mkdirSync(path.join(worktreeRoot, "nested"));

		const primary = await resolveLcmProject(primaryRoot, agentDir);
		const linked = await resolveLcmProject(path.join(worktreeRoot, "nested"), agentDir);

		expect(linked).toEqual(primary);
		expect(primary.rootPath).toBe(fs.realpathSync.native(primaryRoot));
		expect(primary.projectId).toMatch(/^v1-[0-9a-f]{64}$/);
		expect(primary.storePath).toBe(path.join(agentDir, "lcm", "projects", primary.projectId, "context.sqlite"));
	});

	it("keeps separate clones distinct even when their Git metadata matches", async () => {
		const firstRoot = path.join(tempRoot, "first", "project");
		const secondRoot = path.join(tempRoot, "second", "project");
		initRepository(firstRoot);
		initRepository(secondRoot);

		const first = await resolveLcmProject(firstRoot, agentDir);
		const second = await resolveLcmProject(secondRoot, agentDir);

		expect(second.projectId).not.toBe(first.projectId);
		expect(second.storePath).not.toBe(first.storePath);
	});

	it("canonicalizes equivalent cwd paths outside Git", async () => {
		const projectRoot = path.join(tempRoot, "loose-project");
		const aliasRoot = path.join(tempRoot, "loose-project-alias");
		fs.mkdirSync(projectRoot);
		fs.symlinkSync(projectRoot, aliasRoot, process.platform === "win32" ? "junction" : "dir");

		const direct = await resolveLcmProject(projectRoot, agentDir);
		const aliased = await resolveLcmProject(aliasRoot, agentDir);

		expect(aliased).toEqual(direct);
		expect(direct.rootPath).toBe(fs.realpathSync.native(projectRoot));
	});
});
