import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

interface InstallHarness {
	root: string;
	binDir: string;
	installDir: string;
	logPath: string;
}

interface InstallerResult {
	exitCode: number;
	stdout: string;
	stderr: string;
}

const installerPath = path.join(import.meta.dir, "install.sh");
const tempDirs: string[] = [];

async function writeExecutable(filePath: string, content: string): Promise<void> {
	await Bun.write(filePath, content);
	await fs.chmod(filePath, 0o755);
}

async function createHarness(): Promise<InstallHarness> {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-downstream-install-test-"));
	tempDirs.push(root);
	const binDir = path.join(root, "bin");
	const installDir = path.join(root, "install");
	await fs.mkdir(binDir, { recursive: true });
	await writeExecutable(
		path.join(binDir, "uname"),
		'#!/bin/sh\ncase "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) echo Linux ;; esac\n',
	);
	return { root, binDir, installDir, logPath: path.join(root, "calls.log") };
}

async function installFakeGh(harness: InstallHarness): Promise<void> {
	await writeExecutable(
		path.join(harness.binDir, "gh"),
		`#!/bin/sh
printf 'gh %s\\n' "$*" >> "$TEST_CALL_LOG"
case "$1 $2" in
  "auth token")
    [ -z "\${GH_TOKEN+x}" ] && [ -z "\${GITHUB_TOKEN+x}" ] || exit 90
    [ "\${TEST_GH_AUTH_FAIL:-}" != "1" ] || exit 1
    printf '  %s  \\n' "\${TEST_GH_AUTH_TOKEN:-gh-login-token}"
    ;;
  "repo clone")
    [ -z "\${GITHUB_TOKEN+x}" ] || exit 90
    if [ -n "\${TEST_EXPECT_GH_TOKEN:-}" ] && [ "$GH_TOKEN" != "$TEST_EXPECT_GH_TOKEN" ]; then
      echo "gh received the wrong token" >&2
      exit 91
    fi
    mkdir -p "$4/packages/coding-agent"
    ;;
  *) exit 2 ;;
esac
`,
	);
}

async function runInstaller(
	harness: InstallHarness,
	args: readonly string[] = [],
	extraEnv: Readonly<Record<string, string>> = {},
): Promise<InstallerResult> {
	const child = Bun.spawn(["sh", installerPath, ...args], {
		env: {
			...process.env,
			GH_TOKEN: "test-gh-token",
			GITHUB_TOKEN: "",
			HOME: harness.root,
			PATH: `${harness.binDir}:/usr/bin:/bin`,
			PI_INSTALL_DIR: harness.installDir,
			TEST_CALL_LOG: harness.logPath,
			...extraEnv,
		},
		stdout: "pipe",
		stderr: "pipe",
	});
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout as ReadableStream<Uint8Array>).text(),
		new Response(child.stderr as ReadableStream<Uint8Array>).text(),
	]);
	return { exitCode, stdout, stderr };
}

async function installFakeReleaseCurl(harness: InstallHarness, installedBehavior = "ok"): Promise<void> {
	const installedBranch =
		installedBehavior === "fail-after-move"
			? 'case "$0" in *.new.*) echo 17.1.3-lcm.7 ;; *) echo "omp/17.1.3-lcm.7 forged" ;; esac'
			: "echo 17.1.3-lcm.7";
	const binaryContent = `#!/bin/sh\n${installedBranch}\n`;
	const binaryHash = new Bun.CryptoHasher("sha256").update(binaryContent).digest("hex");
	const signature = "S".repeat(64);
	await writeExecutable(
		path.join(harness.binDir, "curl"),
		`#!/bin/sh
github_config=""
case "$*" in
  *"--config -"*) github_config=$(cat) ;;
esac
printf '%s\\n' "$*" | sed 's/^/curl /' >> "$TEST_CALL_LOG"
if [ -n "$github_config" ]; then
  printf '%s\\n' "$github_config" | sed 's/^/curl-config /' >> "$TEST_CALL_LOG"
else
  printf '%s\\n' 'curl-config <none>' >> "$TEST_CALL_LOG"
fi
case "$*" in
  *api.github.com*)
    printf '%s\\n' '[{"tag_name":"v017.9.0-lcm.999","draft":false,"prerelease":true},{"tag_name":"v17.1.3-lcm.7","draft":false,"prerelease":true}]'
    exit 0
    ;;
  *github.com/tickernelz/oh-my-pi/releases/download/*)
    case "$*" in
      *SHA256SUMS.sig*) printf '%s' 'https://release-assets.invalid/SHA256SUMS.sig' ;;
      *SHA256SUMS*) printf '%s' 'https://release-assets.invalid/SHA256SUMS' ;;
      *) printf '%s' 'https://release-assets.invalid/omp-linux-x64' ;;
    esac
    exit 0
    ;;
  *release-assets.invalid*) [ -z "$github_config" ] || exit 89 ;;
esac
out=""
want_out=""
for arg in "$@"; do
  if [ "$want_out" = 1 ]; then out="$arg"; want_out=""; fi
  [ "$arg" != "-o" ] || want_out=1
done
case "$*" in
  *SHA256SUMS.sig) printf '%s' '${signature}' > "$out" ;;
  *SHA256SUMS) if [ "\${TEST_MANIFEST_MODE:-ok}" = "missing-final-lf" ]; then printf '%s  %s' '${binaryHash}' 'omp-linux-x64' > "$out"; else printf '%s  %s\\n' '${binaryHash}' 'omp-linux-x64' > "$out"; fi ;;
  *) cat > "$out" <<'BINARY'
${binaryContent}BINARY
     ;;
esac
`,
	);
	await writeExecutable(
		path.join(harness.binDir, "openssl"),
		`#!/bin/sh
printf 'openssl %s\\n' "$*" >> "$TEST_CALL_LOG"
case "$1" in
  version) echo 'OpenSSL 3.0.0 test'; exit 0 ;;
  pkeyutl)
    key=""
    previous=""
    for arg in "$@"; do
      [ "$previous" != "-inkey" ] || key="$arg"
      previous="$arg"
    done
    grep -q 'MCowBQYDK2VwAyEA1PObMDAzy1CcEElh48DM1yf3Ff1UqqmETbpbXP/iVIw=' "$key" || exit 1
    [ "\${TEST_VERIFY_MODE:-ok}" != "signature-failure" ] || exit 1
    exit 0
    ;;
  dgst)
    if [ "\${TEST_VERIFY_MODE:-ok}" = "hash-mismatch" ]; then
      printf '%064d *artifact\\n' 0
    else
      printf '%s *artifact\\n' '${binaryHash}'
    fi
    exit 0
    ;;
esac
exit 1
`,
	);
}

afterEach(async () => {
	await Promise.all(tempDirs.splice(0).map(dir => fs.rm(dir, { recursive: true, force: true })));
});

describe("downstream shell installer", () => {
	it("authenticates GitHub requests without forwarding credentials to asset redirects", async () => {
		const harness = await createHarness();
		await installFakeGh(harness);
		await installFakeReleaseCurl(harness);
		await writeExecutable(
			path.join(harness.binDir, "bun"),
			'#!/bin/sh\nprintf "bun %s\\n" "$*" >> "$TEST_CALL_LOG"\nexit 99\n',
		);

		const result = await runInstaller(harness, [], {
			GH_TOKEN: " \tgh-priority-token\t ",
			GITHUB_TOKEN: "github-lower-priority-token",
		});

		expect(result.exitCode).toBe(0);
		expect(result.stdout).toContain("Installed authenticated downstream omp");
		expect(await Bun.file(path.join(harness.installDir, "omp")).text()).toContain("17.1.3-lcm.7");
		const calls = await Bun.file(harness.logPath).text();
		expect(calls).toContain("api.github.com/repos/tickernelz/oh-my-pi/releases?per_page=100");
		expect(calls).toContain("github.com/tickernelz/oh-my-pi/releases/download/v17.1.3-lcm.7/omp-linux-x64");
		expect(calls).toContain("releases/download/v17.1.3-lcm.7/SHA256SUMS");
		expect(calls).toContain("releases/download/v17.1.3-lcm.7/SHA256SUMS.sig");
		expect(calls.match(/curl-config header = "Authorization: Bearer gh-priority-token"/g) ?? []).toHaveLength(4);
		expect(calls.match(/curl-config header = "User-Agent: oh-my-pi-installer"/g) ?? []).toHaveLength(4);
		expect(calls.match(/curl-config header = "Accept: application\/vnd\.github\+json"/g) ?? []).toHaveLength(1);
		expect(calls.match(/curl-config header = "Accept: application\/octet-stream"/g) ?? []).toHaveLength(3);
		expect(calls.match(/curl-config <none>/g) ?? []).toHaveLength(3);
		expect(calls.match(/release-assets\.invalid/g) ?? []).toHaveLength(3);
		expect(calls).not.toContain("github-lower-priority-token");
		expect(calls).not.toContain("gh auth token");
		expect(calls).toContain("openssl pkeyutl -verify -pubin");
		expect(calls).not.toContain("bun ");
		expect(`${result.stdout}${result.stderr}`).not.toContain("gh-priority-token");
	});

	it("uses trimmed GITHUB_TOKEN when GH_TOKEN is blank", async () => {
		const harness = await createHarness();
		await installFakeGh(harness);
		await installFakeReleaseCurl(harness);

		const result = await runInstaller(harness, [], {
			GH_TOKEN: " \t ",
			GITHUB_TOKEN: "  github-fallback-token\n",
		});

		expect(result.exitCode).toBe(0);
		const calls = await Bun.file(harness.logPath).text();
		expect(calls).toContain('curl-config header = "Authorization: Bearer github-fallback-token"');
		expect(calls).not.toContain("gh auth token");
		expect(`${result.stdout}${result.stderr}`).not.toContain("github-fallback-token");
	});

	it("falls back to a trimmed gh auth token", async () => {
		const harness = await createHarness();
		await installFakeGh(harness);
		await installFakeReleaseCurl(harness);

		const result = await runInstaller(harness, [], {
			GH_TOKEN: "",
			GITHUB_TOKEN: " ",
			TEST_GH_AUTH_TOKEN: "gh-login-token",
		});

		expect(result.exitCode).toBe(0);
		const calls = await Bun.file(harness.logPath).text();
		expect(calls).toContain("gh auth token");
		expect(calls).toContain('curl-config header = "Authorization: Bearer gh-login-token"');
		expect(`${result.stdout}${result.stderr}`).not.toContain("gh-login-token");
	});

	it("fails without authentication before network access or installation", async () => {
		const harness = await createHarness();
		await installFakeGh(harness);
		await writeExecutable(
			path.join(harness.binDir, "curl"),
			'#!/bin/sh\nprintf "curl %s\\n" "$*" >> "$TEST_CALL_LOG"\nexit 88\n',
		);

		const result = await runInstaller(harness, [], {
			GH_TOKEN: " \t ",
			GITHUB_TOKEN: "",
			TEST_GH_AUTH_FAIL: "1",
		});

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("Run 'gh auth login' or set GH_TOKEN or GITHUB_TOKEN");
		const calls = await Bun.file(harness.logPath).text();
		expect(calls).toContain("gh auth token");
		expect(calls).not.toContain("curl ");
		expect(await Bun.file(harness.installDir).exists()).toBe(false);
	});

	it("--source uses an authenticated gh clone and builds without a global package install", async () => {
		const harness = await createHarness();
		await installFakeGh(harness);
		await writeExecutable(
			path.join(harness.binDir, "git"),
			'#!/bin/sh\nprintf \'git %s\\n\' "$*" >> "$TEST_CALL_LOG"\nexit 0\n',
		);
		await writeExecutable(
			path.join(harness.binDir, "bun"),
			`#!/bin/sh
printf 'bun %s\\n' "$*" >> "$TEST_CALL_LOG"
case "$1" in
  --version) echo 1.3.14 ;;
  -e) printf x64 ;;
  install) exit 0 ;;
  run)
    mkdir -p "$PWD/dist"
    cat > "$PWD/dist/omp" <<'BINARY'
#!/bin/sh
echo 17.1.3-lcm.0
BINARY
    chmod +x "$PWD/dist/omp"
    ;;
esac
`,
		);

		const result = await runInstaller(harness, ["--source", "--ref", "feature/lcm"], {
			GH_TOKEN: "  source-private-token ",
			GITHUB_TOKEN: "lower-priority-token",
			TEST_EXPECT_GH_TOKEN: "source-private-token",
		});

		expect(result.exitCode).toBe(0);
		const calls = await Bun.file(harness.logPath).text();
		expect(calls).toContain("gh repo clone tickernelz/oh-my-pi");
		expect(calls).toContain("-- --depth 1 --branch feature/lcm");
		expect(calls).not.toContain("https://github.com");
		expect(calls).not.toContain("source-private-token");
		expect(calls).toContain("bun install --frozen-lockfile");
		expect(calls).toContain("bun run build");
		expect(calls).not.toContain("install -g");
		expect(`${result.stdout}${result.stderr}`).not.toContain("source-private-token");
		expect(await Bun.file(path.join(harness.installDir, "omp")).text()).toContain("17.1.3-lcm.0");
	});

	it("restores an existing binary when post-move verification fails", async () => {
		const harness = await createHarness();
		await installFakeReleaseCurl(harness, "fail-after-move");
		await fs.mkdir(harness.installDir, { recursive: true });
		const oldBinary = "#!/bin/sh\necho 17.1.3-lcm.6\n";
		await writeExecutable(path.join(harness.installDir, "omp"), oldBinary);

		const result = await runInstaller(harness);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("previous installation was restored");
		expect(await Bun.file(path.join(harness.installDir, "omp")).text()).toBe(oldBinary);
	});

	it("rejects a manifest signature failure before downloading or replacing the binary", async () => {
		const harness = await createHarness();
		await installFakeReleaseCurl(harness);
		await fs.mkdir(harness.installDir, { recursive: true });
		const oldBinary = "#!/bin/sh\necho 17.1.3-lcm.6\n";
		await writeExecutable(path.join(harness.installDir, "omp"), oldBinary);

		const result = await runInstaller(harness, [], { TEST_VERIFY_MODE: "signature-failure" });

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("not valid for the pinned downstream release key");
		expect(await Bun.file(path.join(harness.installDir, "omp")).text()).toBe(oldBinary);
		const calls = await Bun.file(harness.logPath).text();
		expect(calls).not.toContain("releases/download/v17.1.3-lcm.7/omp-linux-x64");
	});

	it("rejects a signed manifest whose final record has no LF", async () => {
		const harness = await createHarness();
		await installFakeReleaseCurl(harness);
		await fs.mkdir(harness.installDir, { recursive: true });
		const oldBinary = "#!/bin/sh\necho 17.1.3-lcm.6\n";
		await writeExecutable(path.join(harness.installDir, "omp"), oldBinary);

		const result = await runInstaller(harness, [], { TEST_MANIFEST_MODE: "missing-final-lf" });

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("no unique, valid entry");
		expect(await Bun.file(path.join(harness.installDir, "omp")).text()).toBe(oldBinary);
		const calls = await Bun.file(harness.logPath).text();
		expect(calls).not.toContain("releases/download/v17.1.3-lcm.7/omp-linux-x64");
	});

	it("rejects an authenticated manifest when the downloaded asset hash differs", async () => {
		const harness = await createHarness();
		await installFakeReleaseCurl(harness);
		await fs.mkdir(harness.installDir, { recursive: true });
		const oldBinary = "#!/bin/sh\necho 17.1.3-lcm.6\n";
		await writeExecutable(path.join(harness.installDir, "omp"), oldBinary);

		const result = await runInstaller(harness, [], { TEST_VERIFY_MODE: "hash-mismatch" });

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("SHA-256 mismatch");
		expect(await Bun.file(path.join(harness.installDir, "omp")).text()).toBe(oldBinary);
	});

	it("fails explicitly when the available verifier is not OpenSSL 3", async () => {
		const harness = await createHarness();
		await writeExecutable(path.join(harness.binDir, "openssl"), "#!/bin/sh\necho 'LibreSSL 3.3.6'\n");
		await writeExecutable(
			path.join(harness.binDir, "curl"),
			'#!/bin/sh\nprintf "curl %s\\n" "$*" >> "$TEST_CALL_LOG"\nexit 88\n',
		);

		const result = await runInstaller(harness);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("require OpenSSL 3");
		expect(await Bun.file(harness.logPath).exists()).toBe(false);
	});

	it("fails unsupported binary platforms before contacting a release channel", async () => {
		const harness = await createHarness();
		await writeExecutable(
			path.join(harness.binDir, "uname"),
			'#!/bin/sh\ncase "$1" in -s) echo Darwin ;; -m) echo arm64 ;; esac\n',
		);
		await writeExecutable(
			path.join(harness.binDir, "curl"),
			'#!/bin/sh\nprintf "curl %s\\n" "$*" >> "$TEST_CALL_LOG"\nexit 88\n',
		);

		const result = await runInstaller(harness);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr).toContain("Linux x64 (including WSL) only");
		expect(await Bun.file(harness.logPath).exists()).toBe(false);
	});
});
