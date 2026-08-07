/**
 * Downstream update CLI command handler.
 *
 * The LCM fork is distributed only as a binary from tickernelz/oh-my-pi.
 * Package-manager installations are detected solely so they can fail closed
 * instead of crossing over to an upstream distribution channel.
 */
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { $which, APP_NAME, compareVersions, isEnoent, VERSION } from "@oh-my-pi/pi-utils";
import chalk from "@oh-my-pi/pi-utils/chalk";
import { $ } from "bun";
import { theme } from "../modes/theme/theme";
import { isTimeoutError, withTimeoutSignal } from "../utils/fetch-timeout";
import { verifyPinnedChecksumManifest } from "./downstream-artifact-verification";
import {
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

export interface ReleaseBinaryAsset {
	url: string;
	size: number;
	digest: string;
}

type Fetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

/** Validate the exact private release asset before downloading it. */
export function resolveDownstreamReleaseBinaryAsset(
	release: unknown,
	expectedTag: string,
	binaryName: string,
): ReleaseBinaryAsset {
	if (
		!isRecord(release) ||
		release.tag_name !== expectedTag ||
		release.draft !== false ||
		!Array.isArray(release.assets)
	) {
		throw new Error(`Invalid GitHub release metadata for ${expectedTag}`);
	}
	const matches = release.assets.filter(asset => isRecord(asset) && asset.name === binaryName);
	if (matches.length !== 1) {
		throw new Error(`GitHub release ${expectedTag} has ${matches.length} assets named ${binaryName}`);
	}
	const asset = matches[0];
	if (!isRecord(asset) || asset.state !== "uploaded") {
		throw new Error(`GitHub release asset ${binaryName} is not fully uploaded`);
	}
	if (typeof asset.size !== "number" || !Number.isSafeInteger(asset.size) || asset.size <= 0) {
		throw new Error(`GitHub release asset ${binaryName} has an invalid size`);
	}
	if (typeof asset.digest !== "string") {
		throw new Error(`GitHub release asset ${binaryName} has no digest`);
	}
	const digest = /^sha256:([0-9a-f]{64})$/i.exec(asset.digest)?.[1];
	if (!digest) {
		throw new Error(`GitHub release asset ${binaryName} has an unsupported digest`);
	}
	const url = `https://github.com/${DOWNSTREAM_REPO}/releases/download/${encodeURIComponent(expectedTag)}/${binaryName}`;
	if (asset.browser_download_url !== url) {
		throw new Error(`GitHub release asset ${binaryName} has an unexpected download URL`);
	}
	return { url, size: asset.size, digest: `sha256:${digest.toLowerCase()}` };
}

async function getDownstreamReleaseBinaryAsset(
	release: DownstreamReleaseInfo,
	binaryName: string,
	token: string,
	fetchImpl: Fetch = fetch,
): Promise<ReleaseBinaryAsset> {
	let response: Response;
	try {
		response = await fetchImpl(
			`https://api.github.com/repos/${DOWNSTREAM_REPO}/releases/tags/${encodeURIComponent(release.tag)}`,
			{
				headers: getDownstreamGitHubHeaders(token, "application/vnd.github+json"),
				signal: withTimeoutSignal(RELEASE_AUTH_TIMEOUT_MS),
			},
		);
	} catch (err) {
		if (isTimeoutError(err)) throw new Error("Timed out fetching GitHub release metadata after 30s", { cause: err });
		throw err;
	}
	if (!response.ok) throw new Error(`Failed to fetch GitHub release metadata: ${response.statusText}`);
	return resolveDownstreamReleaseBinaryAsset(await response.json(), release.tag, binaryName);
}

export interface VerifiedBinaryDownloadOptions {
	url: string;
	targetPath: string;
	expectedSize: number;
	expectedDigest: string;
	headers?: Record<string, string>;
	fetchImpl?: Fetch;
}

/** Download a binary only when its byte count and SHA-256 digest match release metadata. */
export async function downloadVerifiedBinary(options: VerifiedBinaryDownloadOptions): Promise<void> {
	const fetchImpl = options.fetchImpl ?? fetch;
	await unlinkIfExists(options.targetPath);
	let response: Response;
	try {
		response = await fetchImpl(options.url, {
			headers: options.headers,
			redirect: "follow",
			signal: withTimeoutSignal(BINARY_DOWNLOAD_TIMEOUT_MS),
		});
	} catch (err) {
		if (isTimeoutError(err)) throw new Error("Timed out downloading release binary after 15 minutes", { cause: err });
		throw err;
	}
	if (!response.ok || !response.body) throw new Error(`Download failed: ${response.statusText}`);

	const hash = createHash("sha256");
	let size = 0;
	const verifier = new Transform({
		transform(chunk, _encoding, callback) {
			size += chunk.byteLength;
			if (size > options.expectedSize) {
				callback(
					new Error(
						`Downloaded binary size mismatch: expected ${options.expectedSize} bytes, received at least ${size}`,
					),
				);
				return;
			}
			hash.update(chunk);
			callback(null, chunk);
		},
	});
	try {
		await pipeline(response.body, verifier, fs.createWriteStream(options.targetPath, { mode: 0o600 }));
		const digest = `sha256:${hash.digest("hex")}`;
		if (size !== options.expectedSize) {
			throw new Error(`Downloaded binary size mismatch: expected ${options.expectedSize} bytes, received ${size}`);
		}
		if (digest !== options.expectedDigest) {
			throw new Error(`Downloaded binary digest mismatch: expected ${options.expectedDigest}, received ${digest}`);
		}
		await fs.promises.chmod(options.targetPath, 0o755);
	} catch (err) {
		await unlinkIfExists(options.targetPath);
		if (isTimeoutError(err)) throw new Error("Timed out downloading release binary after 15 minutes", { cause: err });
		throw err;
	}
}

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
	/** Whether the path is a standalone executable instead of a package-manager symlink. */
	ompIsRegularFile?: boolean;
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
	const { homebrewPrefix, miseBinDirs = [], miseDataDir, npmBinDir, ompIsRegularFile = false } = options;
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
	// A plain POSIX executable in a package-manager bin directory is the
	// downstream standalone binary, not a managed install. Windows package
	// managers use regular-file launchers, so file type is not decisive there.
	const isStandaloneRegularFile = ompIsRegularFile && process.platform !== "win32";
	if (bunBinDir && isPathInDirectory(ompPath, bunBinDir) && !isStandaloneRegularFile) return "bun";
	if ((npmBinDir && isPathInDirectory(ompPath, npmBinDir) && !isStandaloneRegularFile) || isWindowsScriptLauncher)
		return "npm";
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

	let ompIsRegularFile = false;
	try {
		const stat = fs.lstatSync(ompPath);
		ompIsRegularFile = stat.isFile() && !stat.isSymbolicLink();
	} catch {}
	const method = resolveUpdateMethod(ompPath, bunBinDir, {
		homebrewPrefix,
		miseBinDirs,
		miseDataDir,
		npmBinDir,
		ompIsRegularFile,
	});
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
interface MuslDetectionOptions {
	platform?: NodeJS.Platform;
	alpineRelease?: boolean;
	lddOutput?: string;
}

function detectLddOutput(): string | undefined {
	try {
		const result = Bun.spawnSync(["ldd", "--version"], { stdout: "pipe", stderr: "pipe" });
		return `${result.stdout.toString("utf-8")}\n${result.stderr.toString("utf-8")}`;
	} catch {
		return undefined;
	}
}

function isMuslLinux(options: MuslDetectionOptions = {}): boolean {
	if ((options.platform ?? process.platform) !== "linux") return false;
	if (options.alpineRelease ?? fs.existsSync("/etc/alpine-release")) return true;
	return /\bmusl\b/i.test(options.lddOutput ?? detectLddOutput() ?? "");
}

/** Test seam for libc detection. */
export function isMuslLinuxForTest(options: Required<MuslDetectionOptions>): boolean {
	return isMuslLinux(options);
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
		const [checksumsResponse, signatureResponse, asset] = await Promise.all([
			fetchDownstreamReleaseAsset(`${releaseUrl}/SHA256SUMS`, "SHA256SUMS", token, RELEASE_AUTH_TIMEOUT_MS),
			fetchDownstreamReleaseAsset(`${releaseUrl}/SHA256SUMS.sig`, "SHA256SUMS.sig", token, RELEASE_AUTH_TIMEOUT_MS),
			getDownstreamReleaseBinaryAsset(release, binaryName, token),
		]);
		const [checksums, signature] = await Promise.all([
			readBoundedResponse(checksumsResponse, MAX_CHECKSUMS_BYTES, "SHA256SUMS"),
			readBoundedResponse(signatureResponse, MAX_SIGNATURE_BYTES, "SHA256SUMS.sig"),
		]);
		const expectedHash = await verifyPinnedChecksumManifest({ checksums, signature, assetName: binaryName });
		if (asset.digest !== `sha256:${expectedHash}`) {
			throw new Error(`GitHub release asset ${binaryName} digest does not match the signed SHA256SUMS entry`);
		}

		console.log(chalk.dim(`Downloading ${binaryName}…`));
		tempCreated = true;
		await downloadVerifiedBinary({
			url: asset.url,
			targetPath: tempPath,
			expectedSize: asset.size,
			expectedDigest: asset.digest,
			headers: getDownstreamGitHubHeaders(token, "application/octet-stream"),
		});
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

	const comparison = compareVersions(release.version, VERSION);
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
