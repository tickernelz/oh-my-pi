import RELEASE_PUBLIC_KEY_PEM from "./downstream-release-ed25519.pem" with { type: "text" };

const ED25519_SIGNATURE_BYTES = 64;
const CHECKSUM_LINE_RE = /^([0-9a-f]{64}) {2}([A-Za-z0-9._-]+)$/;

export interface SignedChecksumManifestInput {
	readonly checksums: Uint8Array;
	readonly signature: Uint8Array;
	readonly assetName: string;
	readonly publicKeyPem: string;
}

export interface PinnedChecksumManifestInput {
	readonly checksums: Uint8Array;
	readonly signature: Uint8Array;
	readonly assetName: string;
}

export interface DownloadedArtifactHashInput {
	readonly assetName: string;
	readonly assetPath: string;
	readonly expectedHash: string;
}

function decodePublicKeyPem(pem: string): Uint8Array<ArrayBuffer> {
	const lines = pem.trim().split(/\r?\n/);
	if (lines[0] !== "-----BEGIN PUBLIC KEY-----" || lines.at(-1) !== "-----END PUBLIC KEY-----") {
		throw new Error("Release signing public key is not a SubjectPublicKeyInfo PEM");
	}
	const encoded = lines.slice(1, -1).join("");
	if (!encoded || !/^[A-Za-z0-9+/]+={0,2}$/.test(encoded)) {
		throw new Error("Release signing public key contains invalid base64");
	}
	return Uint8Array.fromBase64(encoded);
}

/** Verify the detached Ed25519 signature before trusting an asset checksum. */
export async function verifySignedChecksumManifest(input: SignedChecksumManifestInput): Promise<string> {
	if (input.signature.byteLength !== ED25519_SIGNATURE_BYTES) {
		throw new Error(
			`Invalid SHA256SUMS.sig length: expected ${ED25519_SIGNATURE_BYTES} bytes, received ${input.signature.byteLength}`,
		);
	}
	if (!input.assetName || input.assetName.includes("/") || input.assetName.includes("\\")) {
		throw new Error(`Invalid release asset name: ${input.assetName}`);
	}

	let key: CryptoKey;
	try {
		key = await crypto.subtle.importKey("spki", decodePublicKeyPem(input.publicKeyPem), { name: "Ed25519" }, false, [
			"verify",
		]);
	} catch (err) {
		throw new Error("Could not import the pinned Ed25519 release signing key", { cause: err });
	}
	const validSignature = await crypto.subtle.verify(
		{ name: "Ed25519" },
		key,
		new Uint8Array(input.signature),
		new Uint8Array(input.checksums),
	);
	if (!validSignature) throw new Error("SHA256SUMS.sig is not valid for the pinned downstream release key");

	let manifest: string;
	try {
		manifest = new TextDecoder("utf-8", { fatal: true }).decode(input.checksums);
	} catch (err) {
		throw new Error("SHA256SUMS is not valid UTF-8", { cause: err });
	}
	if (!manifest.endsWith("\n") || manifest.includes("\r")) {
		throw new Error("SHA256SUMS must use LF line endings and end with a newline");
	}

	const seenAssets = new Set<string>();
	let selectedHash: string | undefined;
	for (const line of manifest.slice(0, -1).split("\n")) {
		const match = CHECKSUM_LINE_RE.exec(line);
		if (!match) throw new Error(`Malformed SHA256SUMS line: ${line}`);
		const [, hash, assetName] = match;
		if (seenAssets.has(assetName)) throw new Error(`Duplicate SHA256SUMS entry: ${assetName}`);
		seenAssets.add(assetName);
		if (assetName === input.assetName) selectedHash = hash;
	}
	if (!selectedHash) throw new Error(`SHA256SUMS has no entry for ${input.assetName}`);
	return selectedHash;
}

export function verifyPinnedChecksumManifest(input: PinnedChecksumManifestInput): Promise<string> {
	return verifySignedChecksumManifest({ ...input, publicKeyPem: RELEASE_PUBLIC_KEY_PEM });
}

async function sha256File(filePath: string): Promise<string> {
	const hasher = new Bun.CryptoHasher("sha256");
	for await (const chunk of Bun.file(filePath).stream()) hasher.update(chunk);
	return hasher.digest("hex");
}

export async function verifyDownloadedArtifactHash(input: DownloadedArtifactHashInput): Promise<void> {
	if (!/^[0-9a-f]{64}$/.test(input.expectedHash)) throw new Error("Expected release asset hash is not SHA-256");
	const actualHash = await sha256File(input.assetPath);
	if (actualHash !== input.expectedHash) {
		throw new Error(
			`SHA-256 mismatch for ${input.assetName}: expected ${input.expectedHash}, received ${actualHash}`,
		);
	}
}
