import { afterEach, describe, expect, it, spyOn, vi } from "bun:test";
import { createHash } from "node:crypto";
import * as nodeFs from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import {
	compareDownstreamVersions,
	DOWNSTREAM_INSTALL_COMMAND,
	selectLatestDownstreamRelease,
} from "@oh-my-pi/pi-coding-agent/cli/downstream-release";
import * as pluginCli from "@oh-my-pi/pi-coding-agent/cli/plugin-cli";
import * as updateCli from "@oh-my-pi/pi-coding-agent/cli/update-cli";
import {
	assertDownstreamUpdateTarget,
	downloadVerifiedBinary,
	getDownstreamBinaryName,
	parseUpdateArgs,
	replaceBinaryForUpdate,
	resolveDownstreamReleaseBinaryAsset,
	resolveUpdateMethodForTest,
	sweepStaleBackups,
} from "@oh-my-pi/pi-coding-agent/cli/update-cli";
import Update from "@oh-my-pi/pi-coding-agent/commands/update";
import { removeWithRetries } from "@oh-my-pi/pi-utils";
import type { CliConfig } from "@oh-my-pi/pi-utils/cli";
import { resolveBuildProvenance } from "@oh-my-pi/pi-utils/version";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-update-test-"));
	tempDirs.push(dir);
	return dir;
}

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(tempDirs.splice(0).map(dir => removeWithRetries(dir)));
});

const TEST_CONFIG: CliConfig = {
	bin: "omp",
	version: "0.0.0-test",
	commands: new Map(),
};

describe("update command plugin dispatch", () => {
	it("routes -l to plugin upgrade instead of the app updater", async () => {
		const pluginSpy = spyOn(pluginCli, "runPluginCommand").mockResolvedValue(undefined);
		const updateSpy = spyOn(updateCli, "runUpdateCommand").mockResolvedValue(undefined);

		const command = new Update(["-l"], TEST_CONFIG);
		await command.run();

		expect(pluginSpy).toHaveBeenCalledWith({ action: "upgrade", args: [], flags: {} });
		expect(updateSpy).not.toHaveBeenCalled();
	});

	it("keeps normal update flags on the downstream app updater path", async () => {
		const pluginSpy = spyOn(pluginCli, "runPluginCommand").mockResolvedValue(undefined);
		const updateSpy = spyOn(updateCli, "runUpdateCommand").mockResolvedValue(undefined);

		const command = new Update(["--check", "--force"], TEST_CONFIG);
		await command.run();

		expect(updateSpy).toHaveBeenCalledWith({ force: true, check: true });
		expect(pluginSpy).not.toHaveBeenCalled();
	});
});

describe("downstream version provenance", () => {
	it("builds the required SemVer version while retaining both full commits", () => {
		const upstreamCommit = "1".repeat(40);
		const downstreamCommit = "a".repeat(40);
		expect(
			resolveBuildProvenance(
				{ upstreamVersion: "17.1.3", lcmRevision: "12", upstreamCommit, downstreamCommit },
				true,
			),
		).toEqual({
			version: "17.1.3-lcm.12",
			upstreamVersion: "17.1.3",
			lcmRevision: "12",
			upstreamCommit,
			downstreamCommit,
		});
	});

	it("uses revision 0 and explicit unknown commits only for local development", () => {
		expect(resolveBuildProvenance({ upstreamVersion: "17.1.3" })).toEqual({
			version: "17.1.3-lcm.0",
			upstreamVersion: "17.1.3",
			lcmRevision: "0",
			upstreamCommit: "unknown",
			downstreamCommit: "unknown",
		});
		expect(() => resolveBuildProvenance({ upstreamVersion: "17.1.3", lcmRevision: "1" })).toThrow(
			"outside local revision 0 builds",
		);
	});

	it("fails release provenance with a zero revision or missing full commits", () => {
		expect(() =>
			resolveBuildProvenance(
				{
					upstreamVersion: "17.1.3",
					lcmRevision: "0",
					upstreamCommit: "1".repeat(40),
					downstreamCommit: "2".repeat(40),
				},
				true,
			),
		).toThrow("greater than zero");
		expect(() => resolveBuildProvenance({ upstreamVersion: "17.1.3", lcmRevision: "2" }, true)).toThrow(
			"OMP_UPSTREAM_COMMIT",
		);
	});

	it("rejects revisions and upstream versions with SemVer-invalid leading zeroes", () => {
		expect(() => resolveBuildProvenance({ upstreamVersion: "17.1.3", lcmRevision: "02" })).toThrow(
			"without leading zeroes",
		);
		expect(() => resolveBuildProvenance({ upstreamVersion: "017.1.3" })).toThrow("Invalid upstream version");
	});
});

describe("downstream release selection", () => {
	it("orders numeric LCM revisions and upstream versions with SemVer precedence", () => {
		expect(compareDownstreamVersions("17.1.3-lcm.10", "17.1.3-lcm.9")).toBeGreaterThan(0);
		expect(compareDownstreamVersions("17.2.0-lcm.1", "17.1.99-lcm.999")).toBeGreaterThan(0);
		expect(compareDownstreamVersions("17.1.3-lcm.7", "17.1.3-lcm.7")).toBe(0);
	});

	it("selects the highest downstream prerelease and ignores drafts, development, or upstream-shaped tags", () => {
		expect(
			selectLatestDownstreamRelease([
				{ tag_name: "v99.0.0", draft: false, prerelease: false },
				{ tag_name: "v100.0.0-lcm.0", draft: false, prerelease: true },
				{ tag_name: "v017.2.0-lcm.100", draft: false, prerelease: true },
				{ tag_name: "v17.2.0-lcm.9", draft: true, prerelease: true },
				{ tag_name: "v17.2.0-lcm.2", draft: false, prerelease: true },
				{ tag_name: "v17.2.0-lcm.10", draft: false, prerelease: true },
			]),
		).toEqual({ tag: "v17.2.0-lcm.10", version: "17.2.0-lcm.10" });
	});

	it("retains the exact returned tag for the release asset URL", () => {
		expect(selectLatestDownstreamRelease([{ tag_name: "17.2.0-lcm.4", draft: false }])).toEqual({
			tag: "17.2.0-lcm.4",
			version: "17.2.0-lcm.4",
		});
	});
});

describe("update install origin handling", () => {
	it("exports the authenticated private-repository bootstrap command", () => {
		expect(DOWNSTREAM_INSTALL_COMMAND).toBe(
			'gh api -H "Accept: application/vnd.github.raw+json" repos/tickernelz/oh-my-pi/contents/scripts/install.sh | sh',
		);
	});

	it("preserves the legacy plugin update shorthand", () => {
		expect(parseUpdateArgs(["update", "-l"])).toEqual({ force: false, check: false, plugins: true });
	});

	it("detects package-manager origins only to reject them", () => {
		expect(resolveUpdateMethodForTest("/home/test/.bun/bin/omp", "/home/test/.bun/bin")).toBe("bun");
		expect(
			resolveUpdateMethodForTest("/home/test/.npm-global/bin/omp", undefined, {
				npmBinDir: "/home/test/.npm-global/bin",
			}),
		).toBe("npm");
		expect(
			resolveUpdateMethodForTest("/home/test/.local/share/mise/shims/omp", undefined, {
				miseDataDir: "/home/test/.local/share/mise",
			}),
		).toBe("mise");

		for (const method of ["bun", "npm", "brew", "mise"] as const) {
			expect(() => assertDownstreamUpdateTarget({ method })).toThrow(DOWNSTREAM_INSTALL_COMMAND);
		}
	});

	it("does not misclassify a standalone downstream binary in a package-manager bin directory", () => {
		expect(
			resolveUpdateMethodForTest("/home/test/.local/bin/omp", "/home/test/.local/bin", {
				npmBinDir: "/home/test/.local/bin",
				ompIsRegularFile: true,
			}),
		).toBe("binary");
	});

	it("keeps package-manager symlinks on the rejected managed-install path", () => {
		expect(
			resolveUpdateMethodForTest("/home/test/.local/bin/omp", "/home/test/.local/bin", {
				npmBinDir: "/home/test/.local/bin",
				ompIsRegularFile: false,
			}),
		).toBe("bun");
	});

	it("detects a Homebrew symlink without naming an upstream formula", async () => {
		const dir = await makeTempDir();
		const prefix = path.join(dir, "homebrew");
		const cellarBinary = path.join(prefix, "Cellar", "omp", "bin", "omp");
		const linkedBinary = path.join(prefix, "bin", "omp");
		await fs.mkdir(path.dirname(cellarBinary), { recursive: true });
		await fs.mkdir(path.dirname(linkedBinary), { recursive: true });
		await Bun.write(cellarBinary, "binary");
		await fs.symlink(cellarBinary, linkedBinary);

		expect(resolveUpdateMethodForTest(linkedBinary, undefined, { homebrewPrefix: prefix })).toBe("brew");
	});

	it("accepts only the downstream Linux x64 binary target", () => {
		const sourceCommand = `${DOWNSTREAM_INSTALL_COMMAND} -s -- --source`;
		expect(getDownstreamBinaryName("linux", "x64", false)).toBe("omp-linux-x64");
		expect(() => getDownstreamBinaryName("linux", "x64", true)).toThrow("requires glibc");
		expect(() => getDownstreamBinaryName("linux", "x64", true)).toThrow(sourceCommand);
		expect(() => getDownstreamBinaryName("darwin", "arm64", false)).toThrow("Linux x64 (including WSL) only");
		expect(() => getDownstreamBinaryName("darwin", "arm64", false)).toThrow(sourceCommand);
		expect(() => getDownstreamBinaryName("win32", "x64", false)).toThrow(sourceCommand);
	});
});

describe("downstream release binary integrity", () => {
	const tag = "v17.1.8-lcm.1";
	const binaryName = "omp-linux-x64";
	const url = `https://github.com/tickernelz/oh-my-pi/releases/download/${tag}/${binaryName}`;
	const content = "verified downstream binary";
	const digest = `sha256:${createHash("sha256").update(content).digest("hex")}`;

	function releaseAsset(overrides: Record<string, unknown> = {}): Record<string, unknown> {
		return {
			tag_name: tag,
			draft: false,
			prerelease: true,
			assets: [
				{
					name: binaryName,
					state: "uploaded",
					size: Buffer.byteLength(content),
					digest,
					browser_download_url: url,
					...overrides,
				},
			],
		};
	}

	it("accepts the exact uploaded downstream prerelease asset", () => {
		expect(resolveDownstreamReleaseBinaryAsset(releaseAsset(), tag, binaryName)).toEqual({
			url,
			size: Buffer.byteLength(content),
			digest,
		});
	});

	it("rejects malformed or ambiguous downstream release asset metadata", () => {
		expect(() => resolveDownstreamReleaseBinaryAsset(releaseAsset({ digest: null }), tag, binaryName)).toThrow(
			"has no digest",
		);
		expect(() =>
			resolveDownstreamReleaseBinaryAsset(
				{ ...releaseAsset(), assets: [releaseAsset().assets, releaseAsset().assets].flat() },
				tag,
				binaryName,
			),
		).toThrow(`has 2 assets named ${binaryName}`);
		expect(() => resolveDownstreamReleaseBinaryAsset({ ...releaseAsset(), draft: true }, tag, binaryName)).toThrow(
			"Invalid GitHub release metadata",
		);
	});

	it("writes a download only after its size and digest match with private-release credentials", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, binaryName);
		let requestHeaders: Headers | undefined;
		await downloadVerifiedBinary({
			url,
			targetPath,
			expectedSize: Buffer.byteLength(content),
			expectedDigest: digest,
			headers: { Authorization: "Bearer private-token" },
			fetchImpl: async (_input, init) => {
				requestHeaders = new Headers(init?.headers);
				return new Response(content);
			},
		});
		expect(requestHeaders?.get("authorization")).toBe("Bearer private-token");
		expect(await Bun.file(targetPath).text()).toBe(content);
		expect((await fs.stat(targetPath)).mode & 0o777).toBe(0o755);
	});

	it("stops an oversized response before it reads another chunk", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, binaryName);
		let pulls = 0;
		const body = new ReadableStream<Uint8Array>(
			{
				pull(controller) {
					pulls++;
					controller.enqueue(new Uint8Array(pulls === 1 ? 2 : 1));
					if (pulls === 2) controller.close();
				},
			},
			{ highWaterMark: 0 },
		);
		await expect(
			downloadVerifiedBinary({
				url,
				targetPath,
				expectedSize: 1,
				expectedDigest: digest,
				fetchImpl: async () => new Response(body),
			}),
		).rejects.toThrow("received at least 2");
		expect(pulls).toBe(1);
		expect(await Bun.file(targetPath).exists()).toBe(false);
	});

	it("wraps a timeout during body streaming and removes the partial download", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, binaryName);
		const body = new ReadableStream<Uint8Array>(
			{
				pull(controller) {
					controller.enqueue(new Uint8Array(1));
					controller.error(new DOMException("The operation timed out.", "TimeoutError"));
				},
			},
			{ highWaterMark: 0 },
		);
		await expect(
			downloadVerifiedBinary({
				url,
				targetPath,
				expectedSize: Buffer.byteLength(content),
				expectedDigest: digest,
				fetchImpl: async () => new Response(body),
			}),
		).rejects.toThrow("Timed out downloading release binary after 15 minutes");
		expect(await Bun.file(targetPath).exists()).toBe(false);
	});

	it("removes downloads whose size or digest does not match", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, binaryName);
		const fetchImpl = async () => new Response(content);
		await expect(
			downloadVerifiedBinary({
				url,
				targetPath,
				expectedSize: Buffer.byteLength(content) + 1,
				expectedDigest: digest,
				fetchImpl,
			}),
		).rejects.toThrow("size mismatch");
		expect(await Bun.file(targetPath).exists()).toBe(false);
		await expect(
			downloadVerifiedBinary({
				url,
				targetPath,
				expectedSize: Buffer.byteLength(content),
				expectedDigest: `sha256:${createHash("sha256").update("different binary").digest("hex")}`,
				fetchImpl,
			}),
		).rejects.toThrow("digest mismatch");
		expect(await Bun.file(targetPath).exists()).toBe(false);
	});
});

describe("downstream binary replacement", () => {
	it("restores the previous binary when the replacement fails verification", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, "omp");
		const tempPath = `${targetPath}.new`;
		const backupPath = `${targetPath}.bak`;
		await Bun.write(targetPath, "old binary");
		await Bun.write(tempPath, "broken binary");

		await expect(
			replaceBinaryForUpdate({
				targetPath,
				tempPath,
				backupPath,
				expectedVersion: "17.1.3-lcm.8",
				verifyInstalledVersion: async () => ({ ok: false, path: targetPath }),
			}),
		).rejects.toThrow("restored previous omp binary");
		expect(await Bun.file(targetPath).text()).toBe("old binary");
		expect(await Bun.file(tempPath).exists()).toBe(false);
		expect(await Bun.file(backupPath).exists()).toBe(false);
	});

	it("keeps the replacement only after it reports the exact downstream version", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, "omp");
		const tempPath = `${targetPath}.new`;
		const backupPath = `${targetPath}.bak`;
		await Bun.write(targetPath, "old binary");
		await Bun.write(tempPath, "new binary");

		await replaceBinaryForUpdate({
			targetPath,
			tempPath,
			backupPath,
			expectedVersion: "17.1.3-lcm.8",
			verifyInstalledVersion: async () => ({ ok: true, actual: "17.1.3-lcm.8", path: targetPath }),
		});
		expect(await Bun.file(targetPath).text()).toBe("new binary");
		expect(await Bun.file(backupPath).exists()).toBe(false);
	});

	it("treats a locked backup cleanup as a completed update", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, "omp");
		const tempPath = `${targetPath}.new`;
		const backupPath = `${targetPath}.1700000000000.4242.bak`;
		await Bun.write(targetPath, "old binary");
		await Bun.write(tempPath, "new binary");
		const realUnlink = nodeFs.promises.unlink.bind(nodeFs.promises);
		const unlinkSpy = spyOn(nodeFs.promises, "unlink").mockImplementation(async filePath => {
			if (String(filePath) === backupPath) {
				const error = new Error("locked") as NodeJS.ErrnoException;
				error.code = "EPERM";
				throw error;
			}
			return realUnlink(filePath);
		});
		try {
			const result = await replaceBinaryForUpdate({
				targetPath,
				tempPath,
				backupPath,
				expectedVersion: "17.1.3-lcm.8",
				verifyInstalledVersion: async () => ({ ok: true, actual: "17.1.3-lcm.8", path: targetPath }),
			});
			expect(result.ok).toBe(true);
		} finally {
			unlinkSpy.mockRestore();
		}
		expect(await Bun.file(targetPath).text()).toBe("new binary");
		expect(await Bun.file(backupPath).text()).toBe("old binary");
	});

	it("reclaims updater backups without deleting unrelated files", async () => {
		const dir = await makeTempDir();
		const targetPath = path.join(dir, "omp");
		await Bun.write(targetPath, "current");
		await Bun.write(`${targetPath}.bak`, "legacy");
		await Bun.write(`${targetPath}.1700000000000.42.bak`, "timestamped");
		await Bun.write(`${targetPath}.config.bak`, "keep");

		await sweepStaleBackups(targetPath);
		expect(await Bun.file(`${targetPath}.bak`).exists()).toBe(false);
		expect(await Bun.file(`${targetPath}.1700000000000.42.bak`).exists()).toBe(false);
		expect(await Bun.file(`${targetPath}.config.bak`).text()).toBe("keep");
	});
});
