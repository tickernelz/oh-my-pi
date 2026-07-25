/**
 * Downstream update CLI command handler.
 *
 * The LCM fork is distributed only as a binary from tickernelz/oh-my-pi.
 * Package-manager installations are detected solely so they can fail closed
 * instead of crossing over to an upstream distribution channel.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { pipeline } from "node:stream/promises";
import { $which, APP_NAME, isEnoent, VERSION } from "@oh-my-pi/pi-utils";
import { $ } from "bun";
import chalk from "chalk";
import { theme } from "../modes/theme/theme";
import { isTimeoutError, withTimeoutSignal } from "../utils/fetch-timeout";
import { verifyDownloadedArtifactHash, verifyPinnedChecksumManifest } from "./downstream-artifact-verification";
import {
	compareDownstreamVersions,
	DOWNSTREAM_INSTALL_COMMAND,
	DOWNSTREAM_REPO,
	type DownstreamReleaseInfo,
	getDownstreamGitHubHeaders,
	getLatestDownstreamRelease,
	resolveDownstreamGitHubToken,
} from "./downstream-release";

const DOWNSTREAM_SOURCE_INSTALL = `${DOWNSTREAM_INSTALL_COMMAND} -s -- --source`;
const RELEASE_AUTH_TIMEOUT_MS = 30_000;
const BINARY_DOWNLOAD_TIMEOUT_MS = 15 * 60_000;
const MAX_CHECKSUMS_BYTES = 1024 * 1024;
const MAX_SIGNATURE_BYTES = 64;
const REPORTED_VERSION_RE = /^(?:omp\/)?((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-lcm\.(?:0|[1-9]\d*))$/;

/** Result from running the installed binary and parsing its reported version. */
export interface InstalledVersionVerification {
	ok: boolean;
	actual?: string;
	path?: string;
}

/** Paths and verifier used while replacing a downloaded binary update. */
export interface BinaryReplacementOptions {
	targetPath: string;
	tempPath: string;
	backupPath: string;
	expectedVersion: string;
	verifyInstalledVersion: (expectedVersion: string) => Promise<InstalledVersionVerification>;
}

export type UpdateMethod = "brew" | "mise" | "bun" | "npm" | "binary";

interface UpdateMethodResolutionOptions {
	homebrewPrefix?: string;
	miseBinDirs?: readonly string[];
	miseDataDir?: string;
	npmBinDir?: string;
}

export type UpdateTarget =
	| { method: "brew" }
	| { method: "mise" }
	| { method: "bun" }
	| { method: "npm" }
	| { method: "binary"; path: string };

/** Parse the legacy update command shape used before command registration. */
export function parseUpdateArgs(args: string[]): { force: boolean; check: boolean; plugins: boolean } | undefined {
	if (args.length === 0 || args[0] !== "update") return undefined;
	return {
		force: args.includes("--force") || args.includes("-f"),
		check: args.includes("--check") || args.includes("-c"),
		plugins: args.includes("--plugins") || args.includes("-l"),
	};
}

async function getBunGlobalBinDir(): Promise<string | undefined> {
	if (!$which("bun")) return undefined;
	try {
		const result = await $`bun pm bin -g`.quiet().nothrow();
		if (result.exitCode !== 0) return undefined;
		return result.text().trim() || undefined;
	} catch {
		return undefined;
	}
}

async function getNpmGlobalBinDir(): Promise<string | undefined> {
	if (!$which("npm")) return undefined;
	try {
		const result = await $`npm prefix -g`.quiet().nothrow();
		if (result.exitCode !== 0) return undefined;
		const prefix = result.text().trim();
		if (!prefix) return undefined;
		return process.platform === "win32" ? prefix : path.join(prefix, "bin");
	} catch {
		return undefined;
	}
}

async function getHomebrewPrefix(): Promise<string | undefined> {
	if (!$which("brew")) return undefined;
	try {
		const result = await $`brew --prefix`.quiet().nothrow();
		if (result.exitCode !== 0) return undefined;
		return result.text().trim() || undefined;
	} catch {
		return undefined;
	}
}

async function getMiseBinDirs(): Promise<string[]> {
	if (!$which("mise")) return [];
	try {
		const result = await $`mise bin-paths`.quiet().nothrow();
		if (result.exitCode !== 0) return [];
		return result
			.text()
			.split(/\r?\n/)
			.map(line => line.trim())
			.filter(Boolean);
	} catch {
		return [];
	}
}

function getMiseDataDir(): string {
	const override = process.env.MISE_DATA_DIR;
	if (override) return override;
	if (process.platform === "win32" && process.env.LOCALAPPDATA) return path.join(process.env.LOCALAPPDATA, "mise");
	if (process.env.XDG_DATA_HOME) return path.join(process.env.XDG_DATA_HOME, "mise");
	return path.join(os.homedir(), ".local", "share", "mise");
}

function normalizePathForComparison(filePath: string): string {
	const normalized = path.normalize(filePath);
	return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

function tryRealpath(filePath: string): string | undefined {
	try {
		return fs.realpathSync.native(filePath);
	} catch {
		return undefined;
	}
}

function isPathInDirectoryLexical(filePath: string, directoryPath: string): boolean {
	const normalizedPath = normalizePathForComparison(path.resolve(filePath));
	const normalizedDirectory = normalizePathForComparison(path.resolve(directoryPath));
	const relativePath = path.relative(normalizedDirectory, normalizedPath);
	return relativePath === "" || (!relativePath.startsWith("..") && !path.isAbsolute(relativePath));
}

function isPathInDirectory(filePath: string, directoryPath: string): boolean {
	if (isPathInDirectoryLexical(filePath, directoryPath)) return true;
	const directoryRealPath = tryRealpath(path.resolve(directoryPath));
	if (!directoryRealPath) return false;
	const fileRealPath = tryRealpath(path.resolve(filePath));
	if (fileRealPath && isPathInDirectoryLexical(fileRealPath, directoryRealPath)) return true;
	const fileDirectoryRealPath = tryRealpath(path.dirname(path.resolve(filePath)));
	if (!fileDirectoryRealPath) return false;
	return isPathInDirectoryLexical(path.join(fileDirectoryRealPath, path.basename(filePath)), directoryRealPath);
}

function resolveUpdateMethod(
	ompPath: string,
	bunBinDir: string | undefined,
	options: UpdateMethodResolutionOptions = {},
): UpdateMethod {
	const { homebrewPrefix, miseBinDirs = [], miseDataDir, npmBinDir } = options;
	const launcherExtension = path.extname(ompPath).toLowerCase();
	const isWindowsScriptLauncher = [".cmd", ".ps1", ".bat"].includes(launcherExtension);
	if (homebrewPrefix && isPathInDirectory(ompPath, homebrewPrefix)) return "brew";
	if (miseBinDirs.some(dir => isPathInDirectory(ompPath, dir))) return "mise";
	if (
		miseDataDir &&
		(isPathInDirectory(ompPath, path.join(miseDataDir, "shims")) ||
			isPathInDirectory(ompPath, path.join(miseDataDir, "installs")))
	) {
		return "mise";
	}
	if (bunBinDir && isPathInDirectory(ompPath, bunBinDir)) return "bun";
	if ((npmBinDir && isPathInDirectory(ompPath, npmBinDir)) || isWindowsScriptLauncher) return "npm";
	return "binary";
}

export function resolveUpdateMethodForTest(
	ompPath: string,
	bunBinDir: string | undefined,
	options: UpdateMethodResolutionOptions = {},
): UpdateMethod {
	return resolveUpdateMethod(ompPath, bunBinDir, options);
}

function resolveOmpPath(): string | undefined {
	return $which(APP_NAME) ?? undefined;
}

async function resolveUpdateTarget(): Promise<UpdateTarget> {
	const [bunBinDir, npmBinDir, homebrewPrefix] = await Promise.all([
		getBunGlobalBinDir(),
		getNpmGlobalBinDir(),
		getHomebrewPrefix(),
	]);
	const miseAvailable = $which("mise") !== undefined;
	const miseBinDirs = miseAvailable ? await getMiseBinDirs() : [];
	const miseDataDir = miseAvailable ? getMiseDataDir() : undefined;
	const ompPath = resolveOmpPath();
	if (!ompPath) throw new Error(`Could not resolve ${APP_NAME} binary path in PATH`);

	const method = resolveUpdateMethod(ompPath, bunBinDir, { homebrewPrefix, miseBinDirs, miseDataDir, npmBinDir });
	return method === "binary" ? { method, path: ompPath } : { method };
}

export function assertDownstreamUpdateTarget(
	target: UpdateTarget,
): asserts target is { method: "binary"; path: string } {
	if (target.method === "binary") return;
	throw new Error(
		`This ${target.method}-managed installation cannot be updated by the downstream LCM updater. ` +
			`Package-manager channels publish upstream OMP, not tickernelz/oh-my-pi. ` +
			`Install the downstream binary with: ${DOWNSTREAM_INSTALL_COMMAND}`,
	);
}

function isMuslLinux(): boolean {
	if (process.platform !== "linux") return false;
	if (fs.existsSync("/etc/alpine-release")) return true;
	return fs.existsSync(`/lib/ld-musl-${process.arch === "arm64" ? "aarch64" : "x86_64"}.so.1`);
}

/** Downstream canary binaries are intentionally limited to Linux x64, including WSL. */
export function getDownstreamBinaryName(
	platform = process.platform,
	arch = process.arch,
	musl = platform === process.platform && arch === process.arch && isMuslLinux(),
): string {
	if (platform !== "linux" || arch !== "x64") {
		throw new Error(
			`Downstream LCM release binaries currently support Linux x64 (including WSL) only; detected ${platform}-${arch}. ` +
				`Build from source with: ${DOWNSTREAM_SOURCE_INSTALL}`,
		);
	}
	if (musl) {
		throw new Error(
			`The downstream Linux x64 release binary requires glibc; this host uses musl. ` +
				`Build for this host with: ${DOWNSTREAM_SOURCE_INSTALL}`,
		);
	}
	return `${APP_NAME}-linux-x64`;
}

async function verifyInstalledVersion(expectedVersion: string): Promise<InstalledVersionVerification> {
	const ompPath = resolveOmpPath();
	if (!ompPath) return { ok: false };
	try {
		const result = await $`${ompPath} --version`.quiet().nothrow();
		if (result.exitCode !== 0) return { ok: false, path: ompPath };
		const output = result.text();
		const reported = output.endsWith("\r\n")
			? output.slice(0, -2)
			: output.endsWith("\n")
				? output.slice(0, -1)
				: output;
		const actual = REPORTED_VERSION_RE.exec(reported)?.[1];
		return { ok: actual === expectedVersion, actual, path: ompPath };
	} catch {
		return { ok: false, path: ompPath };
	}
}

function printVerifiedVersion(expectedVersion: string): void {
	console.log(chalk.green(`\n${theme.status.success} Updated to ${expectedVersion}`));
}

function formatVerificationFailure(result: InstalledVersionVerification, expectedVersion: string): string {
	if (result.actual)
		return `${APP_NAME} at ${result.path} still reports ${result.actual} (expected ${expectedVersion})`;
	return `could not verify updated version${result.path ? ` at ${result.path}` : ""}`;
}

async function unlinkIfExists(filePath: string): Promise<void> {
	try {
		await fs.promises.unlink(filePath);
	} catch (err) {
		if (!isEnoent(err)) throw err;
	}
}

/** A locked Windows backup is harmless after the replacement was verified. */
async function removeBackupBestEffort(filePath: string): Promise<boolean> {
	try {
		await fs.promises.unlink(filePath);
		return true;
	} catch (err) {
		return isEnoent(err);
	}
}

/** Reclaim unique and legacy backup names left by prior binary updates. */
export async function sweepStaleBackups(targetPath: string): Promise<void> {
	const directory = path.dirname(targetPath);
	const base = path.basename(targetPath);
	let entries: string[];
	try {
		entries = await fs.promises.readdir(directory);
	} catch {
		return;
	}
	for (const entry of entries) {
		if (!entry.startsWith(`${base}.`) || !entry.endsWith(".bak")) continue;
		const middle = entry.slice(base.length + 1, entry.length - ".bak".length);
		if (middle && !/^\d+(?:\.\d+)*$/.test(middle)) continue;
		await removeBackupBestEffort(path.join(directory, entry));
	}
}

/** Atomically replace the binary and restore the previous executable if verification fails. */
export async function replaceBinaryForUpdate(options: BinaryReplacementOptions): Promise<InstalledVersionVerification> {
	let backupReady = false;
	try {
		await fs.promises.rename(options.targetPath, options.backupPath);
		backupReady = true;
		await fs.promises.rename(options.tempPath, options.targetPath);
		const verification = await options.verifyInstalledVersion(options.expectedVersion);
		if (!verification.ok) {
			throw new Error(
				`${formatVerificationFailure(verification, options.expectedVersion)}; restored previous ${APP_NAME} binary`,
			);
		}
		backupReady = false;
		await removeBackupBestEffort(options.backupPath);
		return verification;
	} catch (err) {
		if (backupReady) {
			await unlinkIfExists(options.targetPath);
			await fs.promises.rename(options.backupPath, options.targetPath);
		}
		await unlinkIfExists(options.tempPath);
		throw err;
	}
}

export async function fetchDownstreamReleaseAsset(
	url: string,
	assetName: string,
	token: string,
	timeoutMs: number,
	fetchImpl: typeof globalThis.fetch = globalThis.fetch,
): Promise<Response> {
	let response: Response;
	try {
		response = await fetchImpl(url, {
			headers: getDownstreamGitHubHeaders(token, "application/octet-stream"),
			redirect: "follow",
			signal: withTimeoutSignal(timeoutMs),
		});
	} catch (err) {
		if (isTimeoutError(err)) {
			throw new Error(`Timed out fetching ${assetName} after ${Math.ceil(timeoutMs / 1000)}s`, { cause: err });
		}
		throw err;
	}
	if (!response.ok || !response.body) {
		throw new Error(`Failed to fetch ${assetName}: ${response.status} ${response.statusText}`);
	}
	return response;
}

async function readBoundedResponse(response: Response, maxBytes: number, assetName: string): Promise<Uint8Array> {
	const announcedLength = Number(response.headers.get("content-length"));
	if (Number.isFinite(announcedLength) && announcedLength > maxBytes) {
		throw new Error(`${assetName} exceeds the ${maxBytes}-byte limit`);
	}
	const reader = response.body!.getReader();
	const chunks: Uint8Array[] = [];
	let totalBytes = 0;
	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			if (!value?.byteLength) continue;
			totalBytes += value.byteLength;
			if (totalBytes > maxBytes) {
				await reader.cancel();
				throw new Error(`${assetName} exceeds the ${maxBytes}-byte limit`);
			}
			chunks.push(value);
		}
	} finally {
		reader.releaseLock();
	}
	const bytes = new Uint8Array(totalBytes);
	let offset = 0;
	for (const chunk of chunks) {
		bytes.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return bytes;
}

async function updateViaBinaryAt(targetPath: string, release: DownstreamReleaseInfo, token: string): Promise<void> {
	const binaryName = getDownstreamBinaryName();
	const releaseUrl = `https://github.com/${DOWNSTREAM_REPO}/releases/download/${encodeURIComponent(release.tag)}`;
	const tempPath = `${targetPath}.${process.pid}.${crypto.randomUUID()}.new`;
	const backupPath = `${targetPath}.${Date.now()}.${process.pid}.bak`;
	let tempCreated = false;

	try {
		console.log(chalk.dim(`Authenticating ${release.tag} from ${DOWNSTREAM_REPO}…`));
		const [checksumsResponse, signatureResponse] = await Promise.all([
			fetchDownstreamReleaseAsset(`${releaseUrl}/SHA256SUMS`, "SHA256SUMS", token, RELEASE_AUTH_TIMEOUT_MS),
			fetchDownstreamReleaseAsset(`${releaseUrl}/SHA256SUMS.sig`, "SHA256SUMS.sig", token, RELEASE_AUTH_TIMEOUT_MS),
		]);
		const [checksums, signature] = await Promise.all([
			readBoundedResponse(checksumsResponse, MAX_CHECKSUMS_BYTES, "SHA256SUMS"),
			readBoundedResponse(signatureResponse, MAX_SIGNATURE_BYTES, "SHA256SUMS.sig"),
		]);
		const expectedHash = await verifyPinnedChecksumManifest({ checksums, signature, assetName: binaryName });

		console.log(chalk.dim(`Downloading ${binaryName}…`));
		const binaryResponse = await fetchDownstreamReleaseAsset(
			`${releaseUrl}/${binaryName}`,
			binaryName,
			token,
			BINARY_DOWNLOAD_TIMEOUT_MS,
		);
		const tempHandle = await fs.promises.open(tempPath, "wx", 0o600);
		tempCreated = true;
		try {
			await pipeline(binaryResponse.body!, tempHandle.createWriteStream({ autoClose: false }));
		} finally {
			await tempHandle.close();
		}
		await verifyDownloadedArtifactHash({ assetName: binaryName, assetPath: tempPath, expectedHash });
		await fs.promises.chmod(tempPath, 0o755);
	} catch (err) {
		if (tempCreated) await unlinkIfExists(tempPath);
		throw err;
	}

	console.log(chalk.dim("Installing authenticated update..."));
	await replaceBinaryForUpdate({
		targetPath,
		tempPath,
		backupPath,
		expectedVersion: release.version,
		verifyInstalledVersion,
	});
	await sweepStaleBackups(targetPath);
	printVerifiedVersion(release.version);
	console.log(chalk.dim(`Restart ${APP_NAME} to use the new version`));
}

export async function runUpdateCommand(opts: { force: boolean; check: boolean }): Promise<void> {
	console.log(chalk.dim(`Current version: ${VERSION}`));
	let target: { method: "binary"; path: string };
	try {
		const resolvedTarget = await resolveUpdateTarget();
		assertDownstreamUpdateTarget(resolvedTarget);
		getDownstreamBinaryName();
		target = resolvedTarget;
	} catch (err) {
		console.error(chalk.red(`Downstream update unavailable: ${err}`));
		process.exit(1);
	}

	let githubToken: string;
	let release: DownstreamReleaseInfo;
	try {
		githubToken = await resolveDownstreamGitHubToken();
		release = await getLatestDownstreamRelease(RELEASE_AUTH_TIMEOUT_MS, githubToken);
	} catch (err) {
		console.error(chalk.red(`Failed to check ${DOWNSTREAM_REPO} for updates: ${err}`));
		process.exit(1);
	}

	const comparison = compareDownstreamVersions(release.version, VERSION);
	if (comparison <= 0 && !opts.force) {
		console.log(chalk.green(`${theme.status.success} Already up to date`));
		return;
	}
	console.log(
		comparison > 0
			? chalk.cyan(`New downstream version available: ${release.version}`)
			: chalk.yellow(`Forcing reinstall of ${release.version}`),
	);
	if (opts.check) return;

	try {
		await updateViaBinaryAt(target.path, release, githubToken);
	} catch (err) {
		console.error(chalk.red(`Update failed: ${err}`));
		process.exit(1);
	}
}

export function printUpdateHelp(): void {
	console.log(`${chalk.bold(`${APP_NAME} update`)} - Check for and install downstream LCM updates

${chalk.bold("Usage:")}
  ${APP_NAME} update [options]

${chalk.bold("Options:")}
  -c, --check     Check for updates without installing
  -f, --force     Force reinstall even if up to date
  -l, --plugins   Update installed plugins

${chalk.bold("Distribution:")}
  App updates are Linux x64 binaries from https://github.com/${DOWNSTREAM_REPO}/releases.
  Bun, npm, Homebrew, and mise installations must be replaced with the downstream binary.
  Private release access requires gh auth login or GH_TOKEN/GITHUB_TOKEN.
  Install or replace with: ${DOWNSTREAM_INSTALL_COMMAND}

${chalk.bold("Examples:")}
  ${APP_NAME} update              Update to latest downstream version
  ${APP_NAME} update --check      Check if an update is available
  ${APP_NAME} update --force      Force reinstall
  ${APP_NAME} update -l           Update installed plugins
`);
}
