import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import {
	verifyDownloadedArtifactHash,
	verifySignedChecksumManifest,
} from "@oh-my-pi/pi-coding-agent/cli/downstream-artifact-verification";
import { removeWithRetries } from "@oh-my-pi/pi-utils";

interface SigningKey {
	readonly privateKey: CryptoKey;
	readonly publicKeyPem: string;
}

function isCryptoKeyPair(key: CryptoKey | CryptoKeyPair): key is CryptoKeyPair {
	return "privateKey" in key && "publicKey" in key;
}

const tempDirs: string[] = [];

async function generateSigningKey(): Promise<SigningKey> {
	const generated = await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
	if (!isCryptoKeyPair(generated)) throw new Error("Ed25519 key generation returned no keypair");
	const publicKey = new Uint8Array(await crypto.subtle.exportKey("spki", generated.publicKey));
	const lines = publicKey.toBase64().match(/.{1,64}/g);
	if (!lines) throw new Error("Could not encode test public key");
	return {
		privateKey: generated.privateKey,
		publicKeyPem: `-----BEGIN PUBLIC KEY-----\n${lines.join("\n")}\n-----END PUBLIC KEY-----\n`,
	};
}

async function signManifest(privateKey: CryptoKey, manifest: Uint8Array<ArrayBuffer>): Promise<Uint8Array> {
	return new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, privateKey, manifest));
}

async function createArtifact(content: string): Promise<{ path: string; hash: string }> {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-release-verification-test-"));
	tempDirs.push(dir);
	const artifactPath = path.join(dir, "omp-linux-x64");
	await Bun.write(artifactPath, content);
	const hash = new Bun.CryptoHasher("sha256").update(content).digest("hex");
	return { path: artifactPath, hash };
}

afterEach(async () => {
	await Promise.all(tempDirs.splice(0).map(dir => removeWithRetries(dir)));
});

describe("downstream release artifact verification", () => {
	it("accepts a correctly signed checksum entry and matching artifact", async () => {
		const signingKey = await generateSigningKey();
		const artifact = await createArtifact("authenticated downstream binary");
		const checksums = new TextEncoder().encode(`${artifact.hash}  omp-linux-x64\n`);
		const signature = await signManifest(signingKey.privateKey, checksums);

		const expectedHash = await verifySignedChecksumManifest({
			checksums,
			signature,
			assetName: "omp-linux-x64",
			publicKeyPem: signingKey.publicKeyPem,
		});
		await expect(
			verifyDownloadedArtifactHash({
				assetName: "omp-linux-x64",
				assetPath: artifact.path,
				expectedHash,
			}),
		).resolves.toBeUndefined();
	});

	it("rejects a checksum manifest changed after signing", async () => {
		const signingKey = await generateSigningKey();
		const original = new TextEncoder().encode(`${"1".repeat(64)}  omp-linux-x64\n`);
		const signature = await signManifest(signingKey.privateKey, original);
		const tampered = original.slice();
		tampered[0] = "2".charCodeAt(0);

		await expect(
			verifySignedChecksumManifest({
				checksums: tampered,
				signature,
				assetName: "omp-linux-x64",
				publicKeyPem: signingKey.publicKeyPem,
			}),
		).rejects.toThrow("not valid");
	});

	it("rejects a correctly signed manifest without the required final LF", async () => {
		const signingKey = await generateSigningKey();
		const checksums = new TextEncoder().encode(`${"2".repeat(64)}  omp-linux-x64`);
		const signature = await signManifest(signingKey.privateKey, checksums);

		await expect(
			verifySignedChecksumManifest({
				checksums,
				signature,
				assetName: "omp-linux-x64",
				publicKeyPem: signingKey.publicKeyPem,
			}),
		).rejects.toThrow("use LF line endings and end with a newline");
	});

	it("rejects a valid signature made by the wrong Ed25519 key", async () => {
		const signer = await generateSigningKey();
		const pinned = await generateSigningKey();
		const checksums = new TextEncoder().encode(`${"3".repeat(64)}  omp-linux-x64\n`);
		const signature = await signManifest(signer.privateKey, checksums);

		await expect(
			verifySignedChecksumManifest({
				checksums,
				signature,
				assetName: "omp-linux-x64",
				publicKeyPem: pinned.publicKeyPem,
			}),
		).rejects.toThrow("pinned downstream release key");
	});

	it("rejects an artifact whose bytes do not match the signed hash", async () => {
		const signingKey = await generateSigningKey();
		const artifact = await createArtifact("tampered binary");
		const expectedHash = "4".repeat(64);
		const checksums = new TextEncoder().encode(`${expectedHash}  omp-linux-x64\n`);
		const signature = await signManifest(signingKey.privateKey, checksums);
		expect(
			await verifySignedChecksumManifest({
				checksums,
				signature,
				assetName: "omp-linux-x64",
				publicKeyPem: signingKey.publicKeyPem,
			}),
		).toBe(expectedHash);

		await expect(
			verifyDownloadedArtifactHash({
				assetName: "omp-linux-x64",
				assetPath: artifact.path,
				expectedHash,
			}),
		).rejects.toThrow("SHA-256 mismatch");
	});
});
