/**
 * Deterministic type-aware exploration summaries for files whose bytes were never
 * loaded into the model context.
 *
 * Every branch is pure and LLM-free on purpose: this runs while assembling a user
 * prompt, and its output is persisted as LCM file metadata that must be identical
 * for identical bytes across rebuilds.
 */
import { Database } from "bun:sqlite";
import * as path from "node:path";
import { search } from "@oh-my-pi/pi-natives";
import { formatBytes } from "@oh-my-pi/pi-utils";
import { getTableSchema, isSqliteFile, listTables } from "../tools/sqlite-reader";

/** Matches the `.slice(0, 4_000)` the LCM store applies to persisted exploration text. */
export const EXPLORATION_SUMMARY_MAX_CHARS = 4_000;

const STRUCTURED_PREFIX_BYTES = 64 * 1024;
const CODE_PREFIX_BYTES = 256 * 1024;
const TEXT_PREFIX_BYTES = 8 * 1024;

const MAX_TABLES = 30;
const MAX_TABLE_SCHEMAS = 5;
const MAX_TABLE_SCHEMA_CHARS = 400;
const MAX_KEYS = 50;
const MAX_COLUMNS = 50;
const MAX_DECLARATIONS = 60;
const MAX_TEXT_LINES = 20;
const MAX_TEXT_LINE_CHARS = 160;
const SQLITE_ROW_PROBE_CAP = 10_000;

const CODE_EXTENSIONS: Record<string, true> = {
	ts: true,
	tsx: true,
	js: true,
	jsx: true,
	mjs: true,
	cjs: true,
	py: true,
	rs: true,
	go: true,
	java: true,
	kt: true,
	swift: true,
	rb: true,
	php: true,
	c: true,
	h: true,
	cc: true,
	cpp: true,
	hpp: true,
	cs: true,
	scala: true,
	sh: true,
};

const DECLARATION_PATTERN =
	"^\\s*(export\\s+)?(default\\s+)?(async\\s+)?(pub\\s+)?(function|class|interface|type|enum|struct|trait|impl|def|fn|const|let|var|module|namespace)\\s+[A-Za-z_$][\\w$]*";

function valueType(value: unknown): string {
	if (value === null) return "null";
	return Array.isArray(value) ? "array" : typeof value;
}

function sqliteBody(absolutePath: string): string[] {
	const db = new Database(absolutePath, { readonly: true });
	try {
		const tables = listTables(db, { probeCap: SQLITE_ROW_PROBE_CAP });
		if (tables.length === 0) return ["No tables."];
		const lines = [`Tables (${tables.length}):`];
		for (const table of tables.slice(0, MAX_TABLES)) {
			const prefix = table.count.kind === "exact" ? "" : table.count.kind === "estimate" ? "~" : ">=";
			lines.push(`- ${table.name} (${prefix}${table.count.rows} rows)`);
		}
		if (tables.length > MAX_TABLES) lines.push(`- [${tables.length - MAX_TABLES} more tables]`);
		lines.push("Schema:");
		for (const table of tables.slice(0, MAX_TABLE_SCHEMAS)) {
			lines.push(getTableSchema(db, table.name).slice(0, MAX_TABLE_SCHEMA_CHARS));
		}
		return lines;
	} finally {
		db.close();
	}
}

async function jsonlBody(absolutePath: string): Promise<string[] | null> {
	const prefix = await Bun.file(absolutePath).slice(0, STRUCTURED_PREFIX_BYTES).text();
	const newline = prefix.indexOf("\n");
	const firstLine = (newline === -1 ? prefix : prefix.slice(0, newline)).trim();
	if (!firstLine) return null;
	let record: unknown;
	try {
		record = JSON.parse(firstLine);
	} catch {
		return null;
	}
	if (record === null || typeof record !== "object" || Array.isArray(record)) {
		return [`First record: ${valueType(record)}`];
	}
	const keys = Object.keys(record).slice(0, MAX_KEYS);
	const described = keys.map(key => `${key} (${valueType((record as Record<string, unknown>)[key])})`);
	return [`Record keys: ${described.join(", ")}`];
}

/**
 * Depth-tracking scan for depth-1 object keys. A partial prefix of a multi-megabyte
 * file will not parse, so this never calls `JSON.parse` on the container itself.
 */
function scanJsonTopLevelKeys(prefix: string): {
	root: "array" | "object" | "unknown";
	keys: string[];
	complete: boolean;
} {
	let index = 0;
	while (index < prefix.length && /\s/.test(prefix[index]!)) index++;
	const first = prefix[index];
	if (first === "[") return { root: "array", keys: [], complete: false };
	if (first !== "{") return { root: "unknown", keys: [], complete: false };

	const keys: string[] = [];
	let depth = 0;
	let expectKey = false;
	let complete = false;
	for (; index < prefix.length; index++) {
		const char = prefix[index]!;
		if (char === '"') {
			let end = index + 1;
			let literal = "";
			for (; end < prefix.length; end++) {
				const inner = prefix[end]!;
				if (inner === "\\") {
					end++;
					literal += prefix[end] ?? "";
					continue;
				}
				if (inner === '"') break;
				literal += inner;
			}
			if (expectKey && depth === 1 && keys.length < MAX_KEYS) keys.push(literal);
			expectKey = false;
			index = end;
			continue;
		}
		if (char === "{") {
			depth++;
			expectKey = depth === 1;
			continue;
		}
		if (char === "[") {
			depth++;
			expectKey = false;
			continue;
		}
		if (char === "}" || char === "]") {
			depth--;
			if (depth === 0) {
				complete = true;
				break;
			}
			expectKey = false;
			continue;
		}
		if (char === ",") expectKey = depth === 1;
	}
	return { root: "object", keys, complete };
}

async function jsonBody(absolutePath: string): Promise<string[]> {
	const prefix = await Bun.file(absolutePath).slice(0, STRUCTURED_PREFIX_BYTES).text();
	const scanned = scanJsonTopLevelKeys(prefix);
	const lines = [`Root: ${scanned.root}`];
	if (scanned.root === "object") {
		lines.push(scanned.keys.length > 0 ? `Top-level keys: ${scanned.keys.join(", ")}` : "Top-level keys: none found");
	}
	if (!scanned.complete) lines.push(`(prefix scan; file truncated at ${STRUCTURED_PREFIX_BYTES / 1024} KiB)`);
	return lines;
}

async function delimitedBody(absolutePath: string, delimiter: string): Promise<string[]> {
	const prefix = await Bun.file(absolutePath).slice(0, STRUCTURED_PREFIX_BYTES).text();
	const newline = prefix.indexOf("\n");
	const header = (newline === -1 ? prefix : prefix.slice(0, newline)).replace(/\r$/, "");
	if (!header) return ["No header row."];
	const columns = header.split(delimiter);
	const shown = columns.slice(0, MAX_COLUMNS).map(column => column.trim());
	return [`Columns (${columns.length}): ${shown.join(", ")}`];
}

async function codeBody(absolutePath: string): Promise<string[] | null> {
	const prefix = await Bun.file(absolutePath).slice(0, CODE_PREFIX_BYTES).text();
	const result = search(prefix, {
		pattern: DECLARATION_PATTERN,
		multiline: true,
		maxCount: MAX_DECLARATIONS,
		maxColumns: MAX_TEXT_LINE_CHARS,
	});
	if (result.error) return null;
	if (result.matches.length === 0) return ["No top-level declarations found in the scanned prefix."];
	return [`Declarations (first ${result.matches.length}):`, ...result.matches.map(match => match.line.trim())];
}

async function textBody(absolutePath: string): Promise<string[]> {
	const prefix = await Bun.file(absolutePath).slice(0, TEXT_PREFIX_BYTES).text();
	const lines = prefix
		.split("\n")
		.map(line => line.trimEnd())
		.filter(line => line.length > 0)
		.slice(0, MAX_TEXT_LINES)
		.map(line => (line.length > MAX_TEXT_LINE_CHARS ? line.slice(0, MAX_TEXT_LINE_CHARS) : line));
	return lines.length > 0 ? ["First lines:", ...lines] : ["Empty or whitespace-only prefix."];
}

async function explorationBody(absolutePath: string, extension: string, kind: FileExplorationKind): Promise<string[]> {
	if (await isSqliteFile(absolutePath)) return sqliteBody(absolutePath);
	if (extension === "jsonl" || extension === "ndjson") {
		const body = await jsonlBody(absolutePath);
		if (body) return body;
		return await textBody(absolutePath);
	}
	if (extension === "json") return await jsonBody(absolutePath);
	if (extension === "csv") return await delimitedBody(absolutePath, ",");
	if (extension === "tsv") return await delimitedBody(absolutePath, "\t");
	if (CODE_EXTENSIONS[extension]) {
		const body = await codeBody(absolutePath);
		if (body) return body;
		return await textBody(absolutePath);
	}
	if (kind === "skippedBinary") return [];
	return await textBody(absolutePath);
}

/**
 * How the file reached the model, which decides the truthful header wording.
 *
 * - `skippedLarge` / `skippedBinary`: no bytes entered the context.
 * - `truncatedHead`: a bounded head was inlined; the rest was not.
 */
export type FileExplorationKind = "skippedLarge" | "skippedBinary" | "truncatedHead";

const HEADER_SUFFIX: Record<FileExplorationKind, string> = {
	skippedLarge: "contents not loaded into context.",
	skippedBinary: "contents not loaded into context.",
	truncatedHead: "a truncated head was inlined; the remainder was not loaded.",
};

/**
 * Bounded, deterministic description of a file the model cannot see in full.
 * Never throws: a failure degrades to the header line alone.
 */
export async function buildExplorationSummary(
	absolutePath: string,
	info: { byteSize: number; fileType: string; kind: FileExplorationKind },
): Promise<string> {
	const header = `${info.fileType}, ${formatBytes(info.byteSize)}; ${HEADER_SUFFIX[info.kind]}`;
	let body: string[] = [];
	try {
		body = await explorationBody(absolutePath, path.extname(absolutePath).slice(1).toLowerCase(), info.kind);
	} catch {
		body = [];
	}
	return [header, ...body].join("\n").slice(0, EXPLORATION_SUMMARY_MAX_CHARS);
}
