import { describe, expect, it } from "bun:test";
import * as path from "node:path";
import { $, Glob } from "bun";
import { refuseOfficialChannelPublishing } from "./official-publishing-disabled";

const repoRoot = path.join(import.meta.dir, "..");

type JsonRecord = Record<string, unknown>;

function record(value: unknown, label: string): JsonRecord {
	if (!value || typeof value !== "object" || Array.isArray(value)) {
		throw new Error(`${label} must be an object`);
	}
	return value as JsonRecord;
}

function namedStep(job: JsonRecord, name: string): JsonRecord {
	const steps = job.steps;
	if (!Array.isArray(steps)) throw new Error(`${name} job has no steps`);
	const step = steps.find(candidate => record(candidate, `${name} step`).name === name);
	if (!step) throw new Error(`Missing workflow step: ${name}`);
	return record(step, name);
}

function namedStepIndex(job: JsonRecord, name: string): number {
	const steps = job.steps;
	if (!Array.isArray(steps)) throw new Error(`${name} job has no steps`);
	const index = steps.findIndex(candidate => record(candidate, `${name} step`).name === name);
	if (index < 0) throw new Error(`Missing workflow step: ${name}`);
	return index;
}

describe("downstream release governance", () => {
	it("defines one tag-only authenticated Linux x64 prerelease publisher", async () => {
		const release = record(
			Bun.YAML.parse(await Bun.file(path.join(repoRoot, ".github/workflows/downstream-release.yml")).text()),
			"downstream-release.yml",
		);
		const triggers = record(release.on, "release triggers");
		expect(Object.keys(triggers)).toEqual(["push"]);
		expect(record(triggers.push, "push trigger").tags).toEqual(["v*-lcm.*"]);

		const jobs = record(release.jobs, "release jobs");
		expect(Object.keys(jobs)).toEqual(["release_linux_x64"]);
		const job = record(jobs.release_linux_x64, "release_linux_x64");
		expect(job.if).toBe("github.repository == 'tickernelz/oh-my-pi'");
		expect(record(job.permissions, "release permissions").contents).toBe("write");

		const native = namedStep(job, "Build glibc Linux x64 native addon");
		expect(native.uses).toBe("./.github/actions/build-native");
		expect(record(native.with, "native inputs")).toMatchObject({
			platform: "linux",
			arch: "x64",
			variant: "baseline",
		});
		expect(namedStepIndex(job, "Build glibc Linux x64 native addon")).toBeLessThan(
			namedStepIndex(job, "Run focused downstream contract gates"),
		);
		expect(namedStepIndex(job, "Run focused downstream contract gates")).toBeLessThan(
			namedStepIndex(job, "Build only the Linux x64 release binary"),
		);

		const binary = namedStep(job, "Build only the Linux x64 release binary");
		expect(record(binary.env, "binary provenance")).toMatchObject({
			RELEASE_TARGETS: "linux-x64",
			OMP_LCM_REVISION: `\${{ steps.provenance.outputs.lcm-revision }}`,
			OMP_UPSTREAM_COMMIT: `\${{ steps.provenance.outputs.upstream-commit }}`,
			OMP_DOWNSTREAM_COMMIT: `\${{ steps.provenance.outputs.downstream-commit }}`,
		});
		const versionCheck = namedStep(job, "Verify the embedded downstream version");
		expect(String(versionCheck.run)).toContain('expected="omp/$EXPECTED_VERSION"');

		const signing = namedStep(job, "Create and verify signed checksum manifest");
		expect(record(signing.env, "signing environment").LCM_RELEASE_SIGNING_KEY).toBe(
			`\${{ secrets.LCM_RELEASE_SIGNING_KEY }}`,
		);

		const publish = namedStep(job, "Create Linux x64 prerelease");
		const inputs = record(publish.with, "release inputs");
		expect(inputs.prerelease).toBe(true);
		expect(inputs.make_latest).toBe(false);
		expect(String(inputs.files).trim().split(/\s+/)).toEqual([
			"downstream-release/omp-linux-x64",
			"downstream-release/SHA256SUMS",
			"downstream-release/SHA256SUMS.sig",
		]);
	});

	it("keeps the inherited CI workflow read-only and free of official publishers", async () => {
		const ci = record(
			Bun.YAML.parse(await Bun.file(path.join(repoRoot, ".github/workflows/ci.yml")).text()),
			"ci.yml",
		);
		const jobs = record(ci.jobs, "CI jobs");
		for (const removed of [
			"release_binary",
			"release_github",
			"release_github_verify",
			"release_npm",
			"release_brew",
		]) {
			expect(jobs[removed]).toBeUndefined();
		}
		for (const [name, value] of Object.entries(jobs)) {
			const permissions = record(value, name).permissions;
			if (permissions === undefined) continue;
			const scoped = record(permissions, `${name} permissions`);
			expect(scoped.contents).not.toBe("write");
			expect(scoped["id-token"]).not.toBe("write");
		}
	});

	it("hard-fails every official-channel helper before side effects", async () => {
		expect(() => refuseOfficialChannelPublishing("npm publication")).toThrow(
			"npm publication is disabled in tickernelz/oh-my-pi",
		);
		const blockedCommands = [
			["bun", "scripts/release.ts", "patch"],
			["bun", "scripts/setup-npm-trust.ts"],
			["bun", "scripts/ci-update-brew-formula.ts"],
			["bun", "scripts/ci-release-publish.ts"],
			["bash", "scripts/ci-macos-sign.sh", "unused-binary"],
			["bash", "scripts/ci-macos-upload-secrets.sh", "--dry-run"],
			["bun", "run", "prepublishOnly"],
		] as const;

		for (const command of blockedCommands) {
			const result = await $`${command}`.cwd(repoRoot).quiet().nothrow();
			const output = `${result.stdout.toString()}${result.stderr.toString()}`;
			expect(result.exitCode, command.join(" ")).not.toBe(0);
			expect(output, command.join(" ")).toContain("is disabled in tickernelz/oh-my-pi");
		}
	});

	it("identifies the downstream as package distribution owner", async () => {
		const manifests = ["package.json", "python/robomp/web/package.json"];
		for await (const manifest of new Glob("packages/*/package.json").scan(repoRoot)) manifests.push(manifest);

		for (const manifestPath of manifests) {
			const manifest = record(await Bun.file(path.join(repoRoot, manifestPath)).json(), manifestPath);
			expect(manifest.homepage).toBe("https://github.com/tickernelz/oh-my-pi#readme");
			const repository = record(manifest.repository, `${manifestPath} repository`);
			expect(repository.url).toBe("git+https://github.com/tickernelz/oh-my-pi.git");
			expect(record(manifest.bugs, `${manifestPath} bugs`).url).toBe(
				"https://github.com/tickernelz/oh-my-pi/issues",
			);
			if (manifestPath.startsWith("packages/")) {
				expect(typeof manifest.author).toBe("string");
				expect(manifest.license).toBe("MIT");
			}
		}

		const cargo = record(Bun.TOML.parse(await Bun.file(path.join(repoRoot, "Cargo.toml")).text()), "Cargo.toml");
		const workspacePackage = record(record(cargo.workspace, "Cargo workspace").package, "Cargo workspace package");
		expect(workspacePackage.homepage).toBe("https://github.com/tickernelz/oh-my-pi");
		expect(workspacePackage.repository).toBe("https://github.com/tickernelz/oh-my-pi");

		for (const pyproject of ["python/omp-rpc/pyproject.toml", "python/robomp/pyproject.toml"]) {
			const parsed = record(Bun.TOML.parse(await Bun.file(path.join(repoRoot, pyproject)).text()), pyproject);
			const urls = record(record(parsed.project, `${pyproject} project`).urls, `${pyproject} URLs`);
			expect(urls.Homepage).toBe("https://github.com/tickernelz/oh-my-pi#readme");
			expect(urls.Repository).toBe("https://github.com/tickernelz/oh-my-pi");
			expect(urls.Issues).toBe("https://github.com/tickernelz/oh-my-pi/issues");
		}
	});
});
