import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { buildExplorationSummary } from "@oh-my-pi/pi-coding-agent/utils/file-exploration";
import { removeWithRetries } from "@oh-my-pi/pi-utils";

const tempDirs: string[] = [];

afterEach(async () => {
	for (const dir of tempDirs.splice(0, tempDirs.length)) {
		await removeWithRetries(dir);
	}
});

async function writeTempFile(name: string, content: string): Promise<{ absolutePath: string; byteSize: number }> {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-file-exploration-"));
	tempDirs.push(dir);
	const absolutePath = path.join(dir, name);
	await Bun.write(absolutePath, content);
	return { absolutePath, byteSize: Buffer.byteLength(content, "utf8") };
}

describe("buildExplorationSummary", () => {
	test("extracts CSV columns and stays byte-identical across calls", async () => {
		const { absolutePath, byteSize } = await writeTempFile(
			"people.csv",
			"id,name,email\n1,Ada,ada@example.com\n2,Linus,linus@example.com\n",
		);

		const summary = await buildExplorationSummary(absolutePath, {
			byteSize,
			fileType: "csv",
			kind: "skippedLarge",
		});

		expect(summary).toContain("Columns (3): id, name, email");
		expect(summary).toStartWith("csv, ");
		expect(summary).toContain("contents not loaded into context.");
		// Determinism is what makes this output safe to persist as file metadata.
		expect(await buildExplorationSummary(absolutePath, { byteSize, fileType: "csv", kind: "skippedLarge" })).toBe(
			summary,
		);
	});

	test("reports JSONL record keys with their value types", async () => {
		const { absolutePath, byteSize } = await writeTempFile(
			"events.jsonl",
			'{"a":1,"b":"x","c":[1,2],"d":null}\n{"a":2,"b":"y"}\n',
		);

		const summary = await buildExplorationSummary(absolutePath, {
			byteSize,
			fileType: "jsonl",
			kind: "skippedLarge",
		});

		expect(summary).toContain("Record keys: a (number), b (string), c (array), d (null)");
	});

	test("reports JSON root kind and top-level keys without parsing the whole document", async () => {
		const { absolutePath, byteSize } = await writeTempFile(
			"config.json",
			'{"name":"omp","nested":{"inner":1,"deeper":{"skip":2}},"list":[{"ignored":3}],"version":2',
		);

		const summary = await buildExplorationSummary(absolutePath, {
			byteSize,
			fileType: "json",
			kind: "skippedLarge",
		});

		expect(summary).toContain("Root: object");
		// Only depth-1 keys: nested/array members must not leak in.
		expect(summary).toContain("Top-level keys: name, nested, list, version");
		expect(summary).not.toContain("inner");
		expect(summary).not.toContain("ignored");
		expect(summary).toContain("prefix scan");
	});

	test("extracts declarations from code and omits bodies", async () => {
		const { absolutePath, byteSize } = await writeTempFile(
			"module.ts",
			[
				"import { thing } from './thing';",
				"",
				"export function alpha(value: number) {",
				"\treturn value + 1;",
				"}",
				"",
				"class Beta {",
				"\tmethod() {}",
				"}",
			].join("\n"),
		);

		const summary = await buildExplorationSummary(absolutePath, {
			byteSize,
			fileType: "ts",
			kind: "skippedLarge",
		});

		expect(summary).toContain("export function alpha(value: number) {");
		expect(summary).toContain("class Beta {");
		expect(summary).not.toContain("return value + 1;");
	});

	test("lists SQLite tables and schema regardless of extension", async () => {
		const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-file-exploration-"));
		tempDirs.push(dir);
		const absolutePath = path.join(dir, "store.bin");
		const db = new Database(absolutePath);
		db.run("CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
		db.run("INSERT INTO widgets (label) VALUES ('one'), ('two')");
		db.close();
		const byteSize = (await Bun.file(absolutePath).stat()).size;

		const summary = await buildExplorationSummary(absolutePath, {
			byteSize,
			fileType: "binary",
			kind: "skippedBinary",
		});

		expect(summary).toContain("Tables (1):");
		expect(summary).toContain("- widgets (2 rows)");
		expect(summary).toContain("CREATE TABLE widgets");
	});

	test("degrades to the header alone when the file cannot be read", async () => {
		const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-file-exploration-"));
		tempDirs.push(dir);

		const summary = await buildExplorationSummary(path.join(dir, "missing.csv"), {
			byteSize: 1_024,
			fileType: "csv",
			kind: "skippedLarge",
		});
		expect(summary).toBe("csv, 1.0KB; contents not loaded into context.");
	});

	test("a truncated head gets a truthful header instead of the reference-only wording", async () => {
		const { absolutePath, byteSize } = await writeTempFile("wide.csv", "id,name\n1,a\n2,b\n");

		const truncated = await buildExplorationSummary(absolutePath, {
			byteSize,
			fileType: "csv",
			kind: "truncatedHead",
		});

		expect(truncated).toStartWith("csv, ");
		expect(truncated).toContain("a truncated head was inlined; the remainder was not loaded.");
		expect(truncated).not.toContain("contents not loaded into context.");
		// The body is unchanged: only the header wording depends on how the file reached the model.
		expect(truncated).toContain("Columns (2): id, name");
	});
});
