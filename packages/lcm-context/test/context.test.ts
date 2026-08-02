import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import {
	activeSourceFingerprint,
	type ContextScope,
	isLcmSqliteContentionError,
	isLcmSqliteCorruptionError,
	type LcmContext,
	openLcmContext,
	type ProjectionRequest,
	type SearchHit,
	type SourceEntry,
	type SourceSnapshot,
} from "../src";
import {
	initializeLcmSchema,
	LCM_SCHEMA_VERSION,
	summaryHandleForInput,
	UnsupportedLcmSchemaError,
} from "../src/schema";

const MAIN: ContextScope = { projectId: "project", sessionId: "session", branchId: "main" };

interface AvailabilityFixture {
	request: ProjectionRequest;
	spanCount: number;
	parentSpanCount: number;
}

function retryClaimPolicy(context: LcmContext, projectId = MAIN.projectId) {
	const policy = context.configureSummaryRetryPolicy(projectId, "test-provider/test-model");
	return { retryKey: policy.retryKey, retryEpoch: policy.retryEpoch, maxTransportRetries: 5 };
}

function removeSchema10RetryAuthority(db: Database): void {
	for (const trigger of [
		"summary_jobs_authorized_insert",
		"summary_jobs_authorized_update",
		"summary_jobs_authorization_cleanup",
	]) {
		db.run(`DROP TRIGGER IF EXISTS ${trigger}`);
	}
	db.run("DROP TABLE IF EXISTS summary_retry_policies");
	const columns = new Set(
		db
			.query<{ name: string }, []>("PRAGMA table_info(summary_jobs)")
			.all()
			.map(column => column.name),
	);
	for (const column of ["lease_mutation_nonce", "lease_policy_token", "retry_epoch"]) {
		if (columns.has(column)) db.run(`ALTER TABLE summary_jobs DROP COLUMN ${column}`);
	}
}

/**
 * The package takes no runtime dependency on a regex engine, so tests inject one.
 * Production wires the linear-time Rust matcher instead.
 */
const TEST_REGEX_ENGINE = {
	compile(pattern: string) {
		const expression = new RegExp(pattern, "u");
		return (text: string) => expression.test(text);
	},
};

function contentAddress(parts: readonly string[]): string {
	const hasher = new Bun.CryptoHasher("sha256");
	for (const part of parts) {
		hasher.update(`${Buffer.byteLength(part, "utf8")}:`);
		hasher.update(part);
	}
	return hasher.digest("hex");
}

function legacyFilelessSourceKey(source: SourceEntry): string {
	return contentAddress([
		"lcm-source-v1",
		source.projectId,
		source.contentHash,
		String(source.timestamp),
		source.kind,
		source.redactedText,
		JSON.stringify([...new Set(source.artifactRefs)].sort()),
	]);
}

function entry(
	scope: ContextScope,
	entryId: string,
	redactedText: string,
	parentId: string | null = null,
	atomicGroupId?: string,
): SourceEntry {
	return {
		...scope,
		entryId,
		parentId,
		timestamp: 1_800_000_000_000 + Number(entryId.replace(/\D/g, "") || 0),
		kind: "message",
		atomicGroupId,
		redactedText,
		contentHash: new Bun.CryptoHasher("sha256").update(`journal:${entryId}:${redactedText}`).digest("hex"),
		artifactRefs: [`artifact://${entryId}`],
	};
}

function snapshot(scope: ContextScope, entries: readonly SourceEntry[]): SourceSnapshot {
	return { scope, entries };
}

function completeEveryJob(context: LcmContext): void {
	for (let round = 0; round < 100; round++) {
		const [job] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "summarizer",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		if (!job) return;
		const result = context.completeSummaryJob(job, {
			redactedText: `s${job.level}`,
			tokenCount: 1,
		});
		if (!result.accepted && result.reason === "lease_lost") throw new Error("summary helper lost its lease");
	}
	throw new Error("summary hierarchy did not settle");
}

function sqliteError(code: string, message = code): Error & { code: string } {
	return Object.assign(new Error(message), { code });
}

function seedPayloadDatabase(dbPath: string): { pageSize: number; rootPage: number } {
	const db = new Database(dbPath);
	try {
		db.run("CREATE TABLE unrelated_payloads (payload BLOB NOT NULL)");
		const insert = db.prepare("INSERT INTO unrelated_payloads (payload) VALUES (?)");
		for (let index = 0; index < 32; index++) insert.run(Buffer.alloc(4_096, index));
		return {
			pageSize: db.query<{ page_size: number }, []>("PRAGMA page_size").get()!.page_size,
			rootPage: db
				.query<{ rootpage: number }, []>("SELECT rootpage FROM sqlite_schema WHERE name = 'unrelated_payloads'")
				.get()!.rootpage,
		};
	} finally {
		db.close();
	}
}

async function corruptDatabasePage(dbPath: string, pageSize: number, pageNumber: number): Promise<void> {
	const file = await fs.open(dbPath, "r+");
	try {
		await file.write(Buffer.alloc(pageSize), 0, pageSize, (pageNumber - 1) * pageSize);
	} finally {
		await file.close();
	}
}

async function createLatentlyCorruptDatabase(dbPath: string): Promise<void> {
	const { pageSize, rootPage } = seedPayloadDatabase(dbPath);
	await corruptDatabasePage(dbPath, pageSize, rootPage);
}

async function quarantineDatabasePaths(dbPath: string): Promise<string[]> {
	const prefix = `${path.basename(dbPath)}.quarantine-`;
	const quarantineSuffix = /^(0|[1-9]\d*)-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
	return (await fs.readdir(path.dirname(dbPath)))
		.filter(name => name.startsWith(prefix) && quarantineSuffix.test(name.slice(prefix.length)))
		.map(name => path.join(path.dirname(dbPath), name))
		.sort();
}

describe("LCM SQLite error classification", () => {
	test("recognizes contention codes, canonical codeless messages, and wrapped causes", () => {
		expect(isLcmSqliteContentionError(sqliteError("SQLITE_BUSY"))).toBe(true);
		expect(isLcmSqliteContentionError(sqliteError("SQLITE_BUSY_RECOVERY"))).toBe(true);
		expect(isLcmSqliteContentionError(sqliteError("SQLITE_LOCKED_SHAREDCACHE"))).toBe(true);
		expect(isLcmSqliteContentionError(new Error("database is locked"))).toBe(true);
		expect(isLcmSqliteContentionError(new Error("database table is locked"))).toBe(true);
		expect(isLcmSqliteContentionError(new Error("database is locked", { cause: new Error("inner failure") }))).toBe(
			true,
		);
		expect(isLcmSqliteContentionError(new Error("outer", { cause: sqliteError("SQLITE_BUSY_SNAPSHOT") }))).toBe(true);
	});

	test("rejects non-contention codes even when their message resembles a lock", () => {
		expect(isLcmSqliteContentionError(sqliteError("SQLITE_FULL", "database is locked"))).toBe(false);
		expect(isLcmSqliteContentionError(sqliteError("SQLITE_IOERR"))).toBe(false);
		expect(isLcmSqliteContentionError(sqliteError("EACCES"))).toBe(false);
		for (const code of ["SQLITE_FULL", "EACCES", "SQLITE_IOERR"]) {
			expect(isLcmSqliteContentionError(new Error("database is locked", { cause: sqliteError(code) }))).toBe(false);
		}
		expect(isLcmSqliteContentionError(new Error("unknown sqlite failure"))).toBe(false);
		expect(isLcmSqliteContentionError(null)).toBe(false);
	});

	test("recognizes only explicit corruption codes through the cause chain", () => {
		for (const code of ["SQLITE_CORRUPT", "SQLITE_CORRUPT_VTAB", "SQLITE_IOERR_CORRUPTFS", "SQLITE_NOTADB"]) {
			expect(isLcmSqliteCorruptionError(sqliteError(code))).toBe(true);
		}
		expect(isLcmSqliteCorruptionError(new Error("outer", { cause: sqliteError("SQLITE_NOTADB") }))).toBe(true);
		for (const code of ["SQLITE_FULL", "SQLITE_IOERR", "SQLITE_CANTOPEN", "EACCES"]) {
			expect(isLcmSqliteCorruptionError(sqliteError(code))).toBe(false);
		}
		expect(isLcmSqliteCorruptionError(new Error("file is not a database"))).toBe(false);
	});
});

describe("LCM context contracts", () => {
	let tempDir = "";
	let dbPath = "";
	let context: LcmContext;
	let now = 1_900_000_000_000;

	beforeEach(async () => {
		tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "lcm-context-"));
		dbPath = path.join(tempDir, "context.db");
		context = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => now,
			regexEngine: TEST_REGEX_ENGINE,
		});
	});

	afterEach(async () => {
		context?.close();
		if (tempDir) await fs.rm(tempDir, { recursive: true, force: true });
	});

	test("reconciliation is atomic and idempotent when a replacement snapshot is rejected", () => {
		const sources = [entry(MAIN, "e1", "first"), entry(MAIN, "e2", "second", "e1")];
		const first = context.reconcile(snapshot(MAIN, sources));
		const second = context.reconcile(snapshot(MAIN, sources));
		expect(first).toMatchObject({ changed: true, revision: 1, activeSources: 2 });
		expect(second).toMatchObject({ changed: false, revision: 1, activeSources: 2 });
		expect(context.status()).toMatchObject({ schemaVersion: LCM_SCHEMA_VERSION, journalMode: "wal" });

		const duplicate = [sources[0]!, { ...sources[1]!, entryId: "e1" }];
		expect(() => context.reconcile(snapshot(MAIN, duplicate))).toThrow("duplicate source entry id");
		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 10, maxTokens: 100 } });
		expect(projection.freshTailSourceIds).toEqual(["e1", "e2"]);
		expect(projection.revision).toBe(1);
	});

	test("regex mode honors alternation that FTS token conjunction inverts", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "r1", "alpha one"), entry(MAIN, "r2", "beta two", "r1")]));

		// FTS reduces `alpha|beta` to `"alpha" AND "beta"`, and no single source has both.
		expect(context.search({ ...MAIN, query: "alpha|beta" })).toEqual([]);
		expect(context.search({ ...MAIN, query: "alpha|beta", mode: "text" })).toEqual([]);

		const matched = context.search({ ...MAIN, query: "alpha|beta", mode: "regex" });
		expect(matched.map(hit => hit.redactedText)).toEqual(["alpha one", "beta two"]);
		expect(matched.every(hit => hit.kind === "source")).toBe(true);

		// Anchors survive too: FTS would match `one` anywhere in either document.
		expect(context.search({ ...MAIN, query: "^beta", mode: "regex" }).map(hit => hit.redactedText)).toEqual([
			"beta two",
		]);
		expect(context.search({ ...MAIN, query: "^one", mode: "regex" })).toEqual([]);
	});

	test("regex mode rejects an uncompilable pattern and an unknown mode", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "r1", "alpha one")]));

		expect(() => context.search({ ...MAIN, query: "alpha(", mode: "regex" })).toThrow();
		expect(() => context.search({ ...MAIN, query: "alpha", mode: "regexx" as "regex" })).toThrow(
			'mode must be "text" or "regex"',
		);
	});

	test("source hits carry their branch position and the summary node currently covering them", () => {
		const sources = [
			entry(MAIN, "c1", "covered alpha"),
			entry(MAIN, "c2", "covered beta", "c1"),
			entry(MAIN, "c3", "covered gamma", "c2"),
			entry(MAIN, "c4", "covered delta", "c3"),
			entry(MAIN, "c5", "freshest epsilon", "c4"),
		];
		// `summarize` protects the tail from scheduling, so c4/c5 never get a span.
		context.reconcile(snapshot(MAIN, sources), {
			summarize: { tokenBudget: 1_000, freshTail: { maxSources: 2, maxTokens: 1_000 } },
		});
		completeEveryJob(context);

		const byText = new Map(
			context
				.search({ ...MAIN, query: "alpha|epsilon", mode: "regex" })
				.filter(hit => hit.kind === "source")
				.map(hit => [hit.redactedText, hit]),
		);
		const oldest = byText.get("covered alpha");
		const newest = byText.get("freshest epsilon");
		expect(oldest?.position).toBe(0);
		expect(newest?.position).toBe(4);

		// The oldest source sits inside a completed leaf span; the newest is always protected
		// by the fresh tail, so nothing covers it yet.
		expect(oldest?.coveringSummaryHandle).toBeDefined();
		expect(newest?.coveringSummaryHandle).toBeUndefined();

		const covering = context.describeSummary({ ...MAIN, summaryHandle: oldest!.coveringSummaryHandle! });
		expect(covering?.kind).toBe("leaf");
		expect(context.expandSummary({ ...MAIN, summaryHandle: oldest!.coveringSummaryHandle! })?.items).toContainEqual(
			expect.objectContaining({ kind: "source", citation: expect.objectContaining({ sourceId: "c1" }) }),
		);
	});

	test("a projected summary carries the files of every source it compacted", () => {
		const dataset = {
			fileId: "file_dataset",
			contentHash: "b".repeat(64),
			path: "/repo/events.jsonl",
			fileType: "jsonl",
			byteSize: 9_000_000,
			tokenCount: 2_250_000,
			explorationSummary: "Record keys: id (number), kind (string)",
		};
		const sources = [
			{ ...entry(MAIN, "p1", "mentioned the dataset"), files: [dataset] },
			entry(MAIN, "p2", "discussed the dataset", "p1"),
			entry(MAIN, "p3", "newest follow-up", "p2"),
		];
		context.reconcile(snapshot(MAIN, sources), {
			summarize: { tokenBudget: 1_000, freshTail: { maxSources: 1, maxTokens: 1_000 } },
		});
		completeEveryJob(context);

		const projection = context.project({
			...MAIN,
			tokenBudget: 1_000,
			freshTail: { maxSources: 1, maxTokens: 1_000 },
		});
		const covering = projection.historical.find(item => item.sourceIds.includes("p1"));
		expect(covering).toBeDefined();
		// The file survives compaction through source lineage, so the model can still re-read it.
		expect(covering?.files).toEqual([dataset]);

		// A summary covering no file-bearing source stays empty rather than inheriting siblings.
		for (const item of projection.historical) {
			if (!item.sourceIds.includes("p1")) expect(item.files).toEqual([]);
		}
	});

	test("a refreshed exploration summary updates file metadata without changing source identity", () => {
		const fileMetadata = (explorationSummary: string) => ({
			fileId: "file_big_csv",
			contentHash: "a".repeat(64),
			path: "/repo/big.csv",
			fileType: "csv",
			byteSize: 6_000_000,
			tokenCount: 1_500_000,
			explorationSummary,
		});
		const mention = entry(MAIN, "f1", "mentioned the oversized dataset");
		const sourceKeyFor = (): string => {
			const [hit] = context.search({ ...MAIN, query: "oversized" });
			if (hit?.kind !== "source") throw new Error("expected a source hit for the mention");
			return hit.id;
		};

		expect(context.reconcile(snapshot(MAIN, [{ ...mention, files: [fileMetadata("first pass")] }]))).toMatchObject({
			changed: true,
			revision: 1,
		});
		const originalKey = sourceKeyFor();
		expect(context.describeFile({ ...MAIN, fileId: "file_big_csv" })?.explorationSummary).toBe("first pass");

		// Only the derived summary differs; identity is content-addressed without it, so the
		// entry must keep its source key and its placement must not be rewritten.
		expect(
			context.reconcile(snapshot(MAIN, [{ ...mention, files: [fileMetadata("Columns (3): id, name, email")] }])),
		).toMatchObject({ changed: false, revision: 1 });
		expect(sourceKeyFor()).toBe(originalKey);
		expect(context.describeFile({ ...MAIN, fileId: "file_big_csv" })?.explorationSummary).toBe(
			"Columns (3): id, name, email",
		);
	});

	test("v4 migration enforces stable handles on later updates", () => {
		const migrationPath = path.join(tempDir, "v4.db");
		const db = new Database(migrationPath);
		try {
			initializeLcmSchema(db, 1_000);
			removeSchema10RetryAuthority(db);
			db.run("DROP TRIGGER summaries_stable_handle_required");
			db.run("DROP TRIGGER summaries_stable_handle_update_required");
			db.run("DROP INDEX summaries_stable_handle");
			db.run("DROP TABLE branch_summary_spans");
			db.run("DROP TABLE summary_attempts");
			db.run("DROP TABLE source_files");
			db.run("DROP TABLE file_records");
			db.run("ALTER TABLE summaries DROP COLUMN stable_handle");
			db.run(
				`INSERT INTO source_contents
					(source_key, project_id, content_hash, timestamp_ms, kind, redacted_text, artifact_refs, token_count, created_at)
				 VALUES ('source-v4', 'project', 'hash-v4', 1, 'message', 'legacy source', '[]', 3, 1)`,
			);
			db.run(
				`INSERT INTO summaries
					(summary_id, project_id, input_hash, level, redacted_text, token_count, created_at)
				 VALUES ('sum-v4', 'project', 'input-v4', 0, 'legacy summary', 3, 1)`,
			);
			db.run("INSERT INTO summary_lineage (summary_id, ordinal, source_key) VALUES ('sum-v4', 0, 'source-v4')");
			db.run("PRAGMA user_version = 4");

			initializeLcmSchema(db, 1_000);
			expect(db.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(
				LCM_SCHEMA_VERSION,
			);
			expect(
				db.query<{ stable_handle: string }, []>("SELECT stable_handle FROM summaries").get()?.stable_handle,
			).toStartWith("summary_");
			expect(() => db.run("UPDATE summaries SET stable_handle = NULL")).toThrow("summary stable_handle is required");
		} finally {
			db.close();
		}
	});

	test("v6 to v8 migration compacts terminal payloads and derives spans on first reconcile", async () => {
		const migrationPath = path.join(tempDir, "v5-terminal-payloads.db");
		const db = new Database(migrationPath);
		try {
			initializeLcmSchema(db, 1_000);
			removeSchema10RetryAuthority(db);
			db.run(
				`INSERT INTO source_contents
					(source_key, project_id, content_hash, timestamp_ms, kind, redacted_text, artifact_refs, token_count, created_at)
				 VALUES ('source-v5', 'project', 'hash-v5', 1, 'message', 'source text long enough to summarize', '[]', 10, 1)`,
			);
			const branch = db.run(
				"INSERT INTO branches (project_id, session_id, branch_id, reconciled_at) VALUES (?, ?, ?, 1)",
				[MAIN.projectId, MAIN.sessionId, MAIN.branchId],
			);
			db.run(
				`INSERT INTO branch_sources
					(branch_row_id, entry_id, parent_entry_id, position, source_key, active, created_at)
				 VALUES (?, 'e1', NULL, 0, 'source-v5', 1, 1)`,
				[Number(branch.lastInsertRowid)],
			);
			for (const status of ["completed", "obsolete", "pending", "failed"] as const) {
				const jobId = `job-${status}`;
				db.run(
					`INSERT INTO summary_jobs
						(job_id, project_id, input_hash, level, origin_revision, status, available_at, created_at, updated_at)
					 VALUES (?, 'project', ?, 0, 0, ?, 1, 1, 1)`,
					[jobId, `hash-${status}`, status],
				);
				db.run(
					"INSERT INTO job_inputs (job_id, ordinal, input_kind, ref_id) VALUES (?, 0, 'source', 'source-v5')",
					[jobId],
				);
				db.run("INSERT INTO job_lineage (job_id, ordinal, source_key) VALUES (?, 0, 'source-v5')", [jobId]);
			}
			db.run("DROP TABLE branch_summary_spans");
			db.run("DROP TABLE summary_attempts");
			db.run("PRAGMA user_version = 5");
		} finally {
			db.close();
		}

		const migrated = await openLcmContext({ dbPath: migrationPath, now: () => now });
		try {
			const observer = new Database(migrationPath, { readonly: true, strict: true });
			try {
				expect(observer.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(
					LCM_SCHEMA_VERSION,
				);
				expect(
					observer
						.query<{ status: string; inputs: number; lineage: number }, []>(
							`SELECT status,
								(SELECT COUNT(*) FROM job_inputs input WHERE input.job_id = job.job_id) AS inputs,
								(SELECT COUNT(*) FROM job_lineage lineage WHERE lineage.job_id = job.job_id) AS lineage
							 FROM summary_jobs job ORDER BY status`,
						)
						.all(),
				).toEqual([
					{ status: "completed", inputs: 0, lineage: 0 },
					{ status: "failed", inputs: 1, lineage: 1 },
					{ status: "obsolete", inputs: 0, lineage: 0 },
					{ status: "pending", inputs: 1, lineage: 1 },
				]);
			} finally {
				observer.close();
			}
			// Placement now comes from branch spans, so a freshly upgraded store has none
			// and legacy jobs keyed by unrelated hashes are not claimable yet.
			expect(
				migrated.claimSummaryJobs({
					...retryClaimPolicy(migrated),
					workerId: "migration-worker",
					leaseMs: 1_000,
					limit: 2,
					maxOutputTokens: 5,
					preferredScope: MAIN,
					allowFallback: false,
				}),
			).toEqual([]);

			const source = entry(MAIN, "e1", "source text long enough to summarize");
			expect(migrated.reconcile(snapshot(MAIN, [source]))).toMatchObject({ queuedJobs: 1 });
			const claimed = migrated.claimSummaryJobs({
				...retryClaimPolicy(migrated),
				workerId: "migration-worker",
				leaseMs: 1_000,
				limit: 2,
				maxOutputTokens: 5,
				preferredScope: MAIN,
				allowFallback: false,
			});
			expect(claimed).toHaveLength(1);
			expect(claimed[0]!.inputs.map(input => input.kind)).toEqual(["source"]);
		} finally {
			migrated.close();
		}
	});

	test("v5 reconciliation preserves fileless v4 source identity and migrated summary alignment", async () => {
		const migrationPath = path.join(tempDir, "v4-fileless.db");
		const source = entry(
			MAIN,
			"e1",
			"legacy fileless source with enough content to keep its migrated summary useful",
		);
		const sourceKey = legacyFilelessSourceKey(source);
		const summaryHandle = summaryHandleForInput(MAIN.projectId, 0, [{ kind: "source", id: sourceKey }]);
		const inputHash = contentAddress(["lcm-summary-input-v1", MAIN.projectId, "0", "source", sourceKey]);
		const db = new Database(migrationPath);
		try {
			initializeLcmSchema(db, 1_000);
			removeSchema10RetryAuthority(db);
			db.run("DROP TRIGGER summaries_stable_handle_required");
			db.run("DROP TRIGGER summaries_stable_handle_update_required");
			db.run("DROP INDEX summaries_stable_handle");
			db.run("DROP TABLE branch_summary_spans");
			db.run("DROP TABLE summary_attempts");
			db.run("DROP TABLE source_files");
			db.run("DROP TABLE file_records");
			db.run("ALTER TABLE summaries DROP COLUMN stable_handle");
			db.run("PRAGMA user_version = 4");

			const branch = db.run(
				"INSERT INTO branches (project_id, session_id, branch_id, reconciled_at) VALUES (?, ?, ?, ?)",
				[MAIN.projectId, MAIN.sessionId, MAIN.branchId, now],
			);
			db.run(
				`INSERT INTO source_contents
					(source_key, project_id, content_hash, timestamp_ms, kind, redacted_text, artifact_refs, token_count, created_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				[
					sourceKey,
					MAIN.projectId,
					source.contentHash,
					source.timestamp,
					source.kind,
					source.redactedText,
					JSON.stringify(source.artifactRefs),
					Math.ceil(Buffer.byteLength(source.redactedText, "utf8") / 4),
					now,
				],
			);
			db.run(
				`INSERT INTO branch_sources
					(branch_row_id, entry_id, parent_entry_id, position, source_key, active, created_at)
				 VALUES (?, ?, NULL, 0, ?, 1, ?)`,
				[Number(branch.lastInsertRowid), source.entryId, sourceKey, now],
			);
			db.run(
				`INSERT INTO summaries
					(summary_id, project_id, input_hash, level, redacted_text, token_count, created_at)
				 VALUES ('sum-v4-fileless', ?, ?, 0, 'migrated summary', 1, ?)`,
				[MAIN.projectId, inputHash, now],
			);
			db.run("INSERT INTO summary_lineage (summary_id, ordinal, source_key) VALUES ('sum-v4-fileless', 0, ?)", [
				sourceKey,
			]);
		} finally {
			db.close();
		}

		const migrated = await openLcmContext({ dbPath: migrationPath, now: () => now });
		try {
			expect(migrated.reconcile(snapshot(MAIN, [source]))).toMatchObject({
				changed: false,
				revision: 0,
				insertedSources: 0,
				tombstonedSources: 0,
				queuedJobs: 0,
				reusedSummaries: 1,
			});
			expect(migrated.describeSummary({ ...MAIN, summaryHandle })).toMatchObject({
				summaryHandle,
				sourceCount: 1,
				files: [],
			});
			expect(migrated.expandSummary({ ...MAIN, summaryHandle })?.items).toMatchObject([
				{ kind: "source", citation: { sourceId: source.entryId, sourceKey, position: 0 } },
			]);
		} finally {
			migrated.close();
		}
	});

	test("independent processes atomically migrate one fresh store without quarantine", async () => {
		const migrationPath = path.join(tempDir, "concurrent-migration.db");
		const moduleUrl = pathToFileURL(path.resolve(import.meta.dir, "../src/index.ts")).href;
		const script = `
			import { openLcmContext } from ${JSON.stringify(moduleUrl)};
			console.log("ready");
			await Bun.stdin.text();
			const context = await openLcmContext({ dbPath: Bun.env.LCM_DB_PATH, recoverCorrupt: true });
			context.close();
		`;
		const children = Array.from({ length: 2 }, () =>
			Bun.spawn([process.execPath, "--eval", script], {
				env: { ...process.env, LCM_DB_PATH: migrationPath },
				stdin: "pipe",
				stdout: "pipe",
				stderr: "pipe",
			}),
		);
		const ready = await Promise.all(
			children.map(async child => {
				const reader = child.stdout.getReader();
				const chunk = await reader.read();
				reader.releaseLock();
				return new TextDecoder().decode(chunk.value).trim();
			}),
		);
		expect(ready).toEqual(["ready", "ready"]);
		for (const child of children) {
			child.stdin.write("start");
			child.stdin.end();
		}
		const results = await Promise.all(
			children.map(async child => ({
				exitCode: await child.exited,
				stderr: await new Response(child.stderr).text(),
			})),
		);
		expect(results.map(result => result.exitCode)).toEqual([0, 0]);
		expect(results.map(result => result.stderr)).toEqual(["", ""]);

		const observer = new Database(migrationPath);
		try {
			expect(observer.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(
				LCM_SCHEMA_VERSION,
			);
			expect(
				observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
			).toBe(0);
			expect(
				observer
					.query<{ last_recovery_path: string | null }, []>("SELECT last_recovery_path FROM store_state")
					.get()?.last_recovery_path,
			).toBeNull();
		} finally {
			observer.close();
		}
		expect((await fs.readdir(tempDir)).some(file => file.startsWith("concurrent-migration.db.quarantine-"))).toBe(
			false,
		);
	});

	test("established recovery guards admit simultaneous owners without mode writes", async () => {
		const sharedPath = path.join(tempDir, "shared-owner.db");
		const guardPath = `${sharedPath}.recovery-lock`;
		const seed = new Database(guardPath);
		try {
			seed.run("PRAGMA journal_mode = WAL");
			seed.run("CREATE TABLE lcm_owner_guard (id INTEGER PRIMARY KEY)");
		} finally {
			seed.close();
		}

		const first = await openLcmContext({ dbPath: sharedPath, busyTimeoutMs: 0 });
		let second: LcmContext | undefined;
		try {
			second = await openLcmContext({ dbPath: sharedPath, busyTimeoutMs: 0 });
			expect(first.status().schemaVersion).toBe(LCM_SCHEMA_VERSION);
			expect(second.status().schemaVersion).toBe(LCM_SCHEMA_VERSION);
		} finally {
			second?.close();
			first.close();
		}

		const observer = new Database(guardPath);
		try {
			expect(observer.query<{ journal_mode: string }, []>("PRAGMA journal_mode").get()?.journal_mode).toBe("delete");
		} finally {
			observer.close();
		}
	});

	test("exhausted SQLite contention preserves the original store without quarantine", async () => {
		const lockedPath = path.join(tempDir, "locked.db");
		const blocker = new Database(lockedPath);
		initializeLcmSchema(blocker, 0);
		blocker.run("BEGIN IMMEDIATE");
		const retryDelays: number[] = [];
		const nativeSleep = Bun.sleep;
		const sleepSpy = spyOn(Bun, "sleep").mockImplementation(async delay => {
			if (typeof delay !== "number") throw new TypeError("Expected numeric SQLite retry delay");
			retryDelays.push(delay);
			await nativeSleep(delay);
		});
		let failure: unknown;
		try {
			await openLcmContext({ dbPath: lockedPath, busyTimeoutMs: 0, recoverCorrupt: true });
		} catch (error) {
			failure = error;
		} finally {
			blocker.run("ROLLBACK");
			blocker.close();
			sleepSpy.mockRestore();
		}
		expect(isLcmSqliteContentionError(failure)).toBe(true);
		expect(retryDelays).toEqual([100, 200, 400]);
		expect((await fs.readdir(tempDir)).some(file => file.startsWith("locked.db.quarantine-"))).toBe(false);

		const reopened = await openLcmContext({ dbPath: lockedPath, recoverCorrupt: true });
		try {
			expect(reopened.status()).toMatchObject({
				schemaVersion: LCM_SCHEMA_VERSION,
				quarantined: false,
				latestRecovery: null,
			});
		} finally {
			reopened.close();
		}
		const observer = new Database(lockedPath);
		try {
			expect(observer.query<{ quick_check: string }, []>("PRAGMA quick_check(1)").get()?.quick_check).toBe("ok");
			expect(
				observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
			).toBe(0);
			expect(
				observer
					.query<{ last_recovery_path: string | null }, []>("SELECT last_recovery_path FROM store_state")
					.get()?.last_recovery_path,
			).toBeNull();
		} finally {
			observer.close();
		}
	});

	test("status reports only safe storage and recovery diagnostics", async () => {
		const quarantinePath = `${dbPath}.quarantine-${now - 1_000}-00000000-0000-4000-8000-000000000010`;
		const incompletePath = `${dbPath}.quarantine-${now - 2_000}-00000000-0000-4000-8000-000000000011`;
		await Promise.all([
			fs.writeFile(quarantinePath, Buffer.alloc(11)),
			fs.writeFile(`${quarantinePath}-wal`, Buffer.alloc(7)),
			fs.writeFile(`${quarantinePath}-shm`, Buffer.alloc(5)),
			fs.writeFile(`${incompletePath}-wal`, Buffer.alloc(100)),
			fs.writeFile(`${dbPath}.quarantine-${now}-not-a-uuid`, Buffer.alloc(100)),
		]);
		const rawSecret = `provider token and path must stay private: ${dbPath}`;
		const observer = new Database(dbPath);
		try {
			observer.run("UPDATE store_state SET last_recovery_path = ? WHERE id = 1", [quarantinePath]);
			observer.run("INSERT INTO recovery_events (quarantine_path, reason, created_at) VALUES (?, ?, ?)", [
				quarantinePath,
				"Error: LCM SQLite integrity check failed",
				now - 2,
			]);
			expect(context.status().latestRecovery).toEqual({ occurredAt: now - 2, category: "integrity_check" });
			observer.run("INSERT INTO recovery_events (quarantine_path, reason, created_at) VALUES (?, ?, ?)", [
				quarantinePath,
				"SQLITE_CORRUPT: private database detail",
				now - 1,
			]);
			expect(context.status().latestRecovery).toEqual({ occurredAt: now - 1, category: "corruption" });
			observer.run(
				"UPDATE store_state SET quarantined_at = ?, quarantine_reason = ?, last_recovery_path = ? WHERE id = 1",
				[now, rawSecret, quarantinePath],
			);
			observer.run("INSERT INTO recovery_events (quarantine_path, reason, created_at) VALUES (?, ?, ?)", [
				quarantinePath,
				rawSecret,
				now,
			]);
		} finally {
			observer.close();
		}

		const status = context.status();
		expect(status).toMatchObject({
			quarantined: true,
			storage: { quarantineBytes: 23 },
			latestRecovery: { occurredAt: now, category: "unknown" },
		});
		expect(status.storage.databaseBytes).toBeGreaterThan(0);
		expect(status.storage.walBytes).toBeGreaterThanOrEqual(0);
		expect("dbPath" in status).toBe(false);
		expect("quarantineReason" in status).toBe(false);
		expect("recoveredFrom" in status).toBe(false);
		const doctor = context.doctor();
		expect(doctor.checks.find(check => check.name === "quarantine")).toEqual({
			name: "quarantine",
			ok: false,
			detail: "store is quarantined",
		});
		const serialized = JSON.stringify({ status, doctor });
		expect(serialized).not.toContain(dbPath);
		expect(serialized).not.toContain(rawSecret);
		expect(serialized).not.toContain("SQLITE_CORRUPT");
	});

	test("recoverCorrupt detects latent B-tree corruption before returning a context", async () => {
		const corruptPath = path.join(tempDir, "latent-corrupt.db");
		await createLatentlyCorruptDatabase(corruptPath);

		const recovered = await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
		try {
			const [recoveredFrom] = await quarantineDatabasePaths(corruptPath);
			expect(recovered.status().latestRecovery).toEqual({ occurredAt: now, category: "integrity_check" });
			expect(recoveredFrom).toStartWith(`${corruptPath}.quarantine-${now}-`);
			expect(await Bun.file(recoveredFrom!).exists()).toBe(true);
		} finally {
			recovered.close();
		}
	});

	test("quarantine rolls back sidecars when a later move fails and remains safely retryable", async () => {
		for (const completedMoves of [1, 2]) {
			const corruptPath = path.join(tempDir, `rename-failure-${completedMoves}.db`);
			const mainBytes = Buffer.alloc(512, 0x40 + completedMoves);
			const walBytes = Buffer.from(`stale-wal-${completedMoves}`);
			const shmBytes = Buffer.from(`stale-shm-${completedMoves}`);
			await fs.writeFile(corruptPath, mainBytes);
			const nativeRename = fs.rename;
			const injectedFailure = Object.assign(new Error("injected quarantine rename failure"), { code: "EIO" });
			let quarantineAttempt = 0;
			let movesThisAttempt = 0;
			let injected = false;
			const markerPath = `${corruptPath}.quarantine-pending`;
			const renameSpy = spyOn(fs, "rename").mockImplementation(async (source, destination) => {
				const sourcePath = String(source);
				const destinationPath = String(destination);
				if (destinationPath === markerPath) {
					await nativeRename(source, destination);
					quarantineAttempt += 1;
					movesThisAttempt = 0;
					await Promise.all([
						fs.writeFile(`${corruptPath}-wal`, walBytes),
						fs.writeFile(`${corruptPath}-shm`, shmBytes),
					]);
					return;
				}
				const isForwardMove =
					[corruptPath, `${corruptPath}-wal`, `${corruptPath}-shm`].includes(sourcePath) &&
					destinationPath.startsWith(`${corruptPath}.quarantine-`);
				if (isForwardMove) {
					movesThisAttempt += 1;
					if (!injected && quarantineAttempt === 1 && movesThisAttempt === completedMoves + 1) {
						injected = true;
						throw injectedFailure;
					}
				}
				await nativeRename(source, destination);
			});
			let recovered: LcmContext | undefined;
			try {
				let failure: unknown;
				try {
					await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
				} catch (error) {
					failure = error;
				}
				expect(failure).toBe(injectedFailure);
				expect(injected).toBe(true);
				expect(await fs.readFile(corruptPath)).toEqual(mainBytes);
				expect(await fs.readFile(`${corruptPath}-wal`)).toEqual(walBytes);
				expect(await fs.readFile(`${corruptPath}-shm`)).toEqual(shmBytes);
				expect(
					(await fs.readdir(tempDir)).filter(file => file.startsWith(`${path.basename(corruptPath)}.quarantine-`)),
				).toEqual([]);

				recovered = await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
				const [recoveredFrom] = await quarantineDatabasePaths(corruptPath);
				expect(recoveredFrom).toStartWith(`${corruptPath}.quarantine-${now}-`);
				expect(await fs.readFile(`${recoveredFrom!}-wal`)).toEqual(walBytes);
				expect(await fs.readFile(`${recoveredFrom!}-shm`)).toEqual(shmBytes);
				expect(await Bun.file(`${corruptPath}.quarantine-pending`).exists()).toBe(false);
				recovered.close();
				recovered = undefined;
				const observer = new Database(corruptPath);
				try {
					expect(
						observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
					).toBe(1);
				} finally {
					observer.close();
				}
			} finally {
				recovered?.close();
				renameSpy.mockRestore();
			}
		}
	}, 10_000);

	test("a rollback failure falls forward to a coherent quarantined unit", async () => {
		const corruptPath = path.join(tempDir, "rollback-rename-failure.db");
		const markerPath = `${corruptPath}.quarantine-pending`;
		const mainBytes = Buffer.alloc(512, 0x52);
		const walBytes = Buffer.from("rollback-stale-wal");
		const shmBytes = Buffer.from("rollback-stale-shm");
		await fs.writeFile(corruptPath, mainBytes);
		const nativeRename = fs.rename;
		const forwardFailure = Object.assign(new Error("injected main quarantine failure"), { code: "EIO" });
		const rollbackFailure = Object.assign(new Error("injected sidecar rollback failure"), { code: "EIO" });
		let mainForwardAttempts = 0;
		let failedRollback = false;
		const renameSpy = spyOn(fs, "rename").mockImplementation(async (source, destination) => {
			const sourcePath = String(source);
			const destinationPath = String(destination);
			if (destinationPath === markerPath) {
				await nativeRename(source, destination);
				await Promise.all([
					fs.writeFile(`${corruptPath}-wal`, walBytes),
					fs.writeFile(`${corruptPath}-shm`, shmBytes),
				]);
				return;
			}
			if (sourcePath === corruptPath && destinationPath.startsWith(`${corruptPath}.quarantine-`)) {
				mainForwardAttempts += 1;
				if (mainForwardAttempts === 1) throw forwardFailure;
			}
			if (
				!failedRollback &&
				sourcePath.startsWith(`${corruptPath}.quarantine-`) &&
				sourcePath.endsWith("-shm") &&
				destinationPath === `${corruptPath}-shm`
			) {
				failedRollback = true;
				throw rollbackFailure;
			}
			await nativeRename(source, destination);
		});
		let recovered: LcmContext | undefined;
		try {
			recovered = await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
			const [recoveredFrom] = await quarantineDatabasePaths(corruptPath);
			expect(mainForwardAttempts).toBe(2);
			expect(failedRollback).toBe(true);
			expect(recoveredFrom).toStartWith(`${corruptPath}.quarantine-${now}-`);
			expect(await fs.readFile(recoveredFrom!)).toEqual(mainBytes);
			expect(await fs.readFile(`${recoveredFrom!}-wal`)).toEqual(walBytes);
			expect(await fs.readFile(`${recoveredFrom!}-shm`)).toEqual(shmBytes);
			expect(await Bun.file(markerPath).exists()).toBe(false);
			const observer = new Database(corruptPath);
			try {
				expect(observer.query<{ quick_check: string }, []>("PRAGMA quick_check(1)").get()?.quick_check).toBe("ok");
				expect(
					observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
				).toBe(1);
			} finally {
				observer.close();
			}
		} finally {
			recovered?.close();
			renameSpy.mockRestore();
		}
	}, 10_000);

	test("a second fall-forward failure retains a coherent replay marker", async () => {
		const corruptPath = path.join(tempDir, "fall-forward-main-failure.db");
		const markerPath = `${corruptPath}.quarantine-pending`;
		const mainBytes = Buffer.alloc(512, 0x53);
		const walBytes = Buffer.from("fall-forward-stale-wal");
		const shmBytes = Buffer.from("fall-forward-stale-shm");
		await fs.writeFile(corruptPath, mainBytes);
		const nativeRename = fs.rename;
		const firstForwardFailure = Object.assign(new Error("injected initial main move failure"), { code: "EIO" });
		const rollbackFailure = Object.assign(new Error("injected rollback failure"), { code: "EIO" });
		const secondForwardFailure = Object.assign(new Error("injected fall-forward main failure"), { code: "EIO" });
		let mainForwardAttempts = 0;
		let failedRollback = false;
		const renameSpy = spyOn(fs, "rename").mockImplementation(async (source, destination) => {
			const sourcePath = String(source);
			const destinationPath = String(destination);
			if (destinationPath === markerPath) {
				await nativeRename(source, destination);
				await Promise.all([
					fs.writeFile(`${corruptPath}-wal`, walBytes),
					fs.writeFile(`${corruptPath}-shm`, shmBytes),
				]);
				return;
			}
			if (sourcePath === corruptPath && destinationPath.startsWith(`${corruptPath}.quarantine-`)) {
				mainForwardAttempts += 1;
				if (mainForwardAttempts === 1) throw firstForwardFailure;
				if (mainForwardAttempts === 2) throw secondForwardFailure;
			}
			if (
				!failedRollback &&
				sourcePath.startsWith(`${corruptPath}.quarantine-`) &&
				sourcePath.endsWith("-shm") &&
				destinationPath === `${corruptPath}-shm`
			) {
				failedRollback = true;
				throw rollbackFailure;
			}
			await nativeRename(source, destination);
		});
		let failure: unknown;
		try {
			await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
		} catch (error) {
			failure = error;
		} finally {
			renameSpy.mockRestore();
		}
		expect(failure).toBeInstanceOf(AggregateError);
		expect(mainForwardAttempts).toBe(2);
		expect(failedRollback).toBe(true);
		expect(await fs.readFile(corruptPath)).toEqual(mainBytes);
		expect(await fs.readFile(`${corruptPath}-wal`)).toEqual(walBytes);
		expect(await fs.readFile(`${corruptPath}-shm`)).toEqual(shmBytes);
		expect(await Bun.file(markerPath).exists()).toBe(true);
		expect(
			(await fs.readdir(tempDir)).filter(
				file => file.startsWith("fall-forward-main-failure.db.quarantine-") && file !== path.basename(markerPath),
			),
		).toEqual([]);

		const recovered = await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
		try {
			expect((await quarantineDatabasePaths(corruptPath))[0]).toStartWith(`${corruptPath}.quarantine-${now}-`);
			expect(await Bun.file(markerPath).exists()).toBe(false);
		} finally {
			recovered.close();
		}
		const observer = new Database(corruptPath);
		try {
			expect(
				observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
			).toBe(1);
		} finally {
			observer.close();
		}
	}, 10_000);

	test("a completely staged quarantine is rebuilt and recorded before an ordinary create", async () => {
		const recoveredPath = path.join(tempDir, "complete-pending.db");
		const quarantinePath = `${recoveredPath}.quarantine-${now}-00000000-0000-4000-8000-000000000001`;
		const corruptBytes = Buffer.alloc(512, 0x63);
		const walBytes = Buffer.from("complete-pending-wal");
		const shmBytes = Buffer.from("complete-pending-shm");
		await Promise.all([
			fs.writeFile(quarantinePath, corruptBytes),
			fs.writeFile(`${quarantinePath}-wal`, walBytes),
			fs.writeFile(`${quarantinePath}-shm`, shmBytes),
			fs.writeFile(
				`${recoveredPath}.quarantine-pending`,
				JSON.stringify({ quarantinePath, reason: "injected failure after complete unit move" }),
			),
		]);

		const recovered = await openLcmContext({ dbPath: recoveredPath, recoverCorrupt: true, now: () => now });
		try {
			expect(recovered.status()).toMatchObject({
				quarantined: false,
				latestRecovery: { occurredAt: now, category: "unknown" },
			});
			expect(await fs.readFile(quarantinePath)).toEqual(corruptBytes);
			expect(await fs.readFile(`${quarantinePath}-wal`)).toEqual(walBytes);
			expect(await fs.readFile(`${quarantinePath}-shm`)).toEqual(shmBytes);
			expect(await Bun.file(`${recoveredPath}.quarantine-pending`).exists()).toBe(false);
		} finally {
			recovered.close();
		}
		const observer = new Database(recoveredPath);
		try {
			expect(observer.query<{ quick_check: string }, []>("PRAGMA quick_check(1)").get()?.quick_check).toBe("ok");
			expect(
				observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
			).toBe(1);
		} finally {
			observer.close();
		}
	});

	test("pending manifests are synced before database moves and clean failed temporaries", async () => {
		for (const phase of ["manifest", "directory"] as const) {
			const corruptPath = path.join(tempDir, `manifest-sync-${phase}.db`);
			const markerPath = `${corruptPath}.quarantine-pending`;
			const original = Buffer.alloc(512, phase === "manifest" ? 0x64 : 0x65);
			await fs.writeFile(corruptPath, original);
			const injectedFailure = Object.assign(new Error(`injected ${phase} sync failure`), { code: "EIO" });
			const nativeOpen = fs.open;
			const nativeRename = fs.rename;
			const events: string[] = [];
			const databaseRenames: string[] = [];
			const openSpy = spyOn(fs, "open").mockImplementation(async (file, flags, mode) => {
				const handle = await nativeOpen(file, flags, mode);
				const nativeSync = handle.sync.bind(handle);
				Object.defineProperty(handle, "sync", {
					configurable: true,
					value: async () => {
						const filePath = String(file);
						if (filePath === tempDir) {
							events.push("sync:directory");
							if (phase === "directory") throw injectedFailure;
						} else if (filePath.endsWith(".tmp") && filePath.includes(path.basename(markerPath))) {
							events.push("sync:manifest");
							if (phase === "manifest") throw injectedFailure;
						}
						await nativeSync();
					},
				});
				return handle;
			});
			const renameSpy = spyOn(fs, "rename").mockImplementation(async (source, destination) => {
				if ([corruptPath, `${corruptPath}-wal`, `${corruptPath}-shm`].includes(String(source))) {
					databaseRenames.push(`${String(source)} -> ${String(destination)}`);
				}
				events.push(String(destination) === markerPath ? "rename:manifest" : "rename:database");
				await nativeRename(source, destination);
			});
			let failure: unknown;
			try {
				await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
			} catch (error) {
				failure = error;
			} finally {
				renameSpy.mockRestore();
				openSpy.mockRestore();
			}
			expect(failure).toBe(injectedFailure);
			expect(databaseRenames).toEqual([]);
			expect(await fs.readFile(corruptPath)).toEqual(original);
			expect(events[0]).toBe("sync:manifest");
			if (phase === "manifest") {
				expect(await Bun.file(markerPath).exists()).toBe(false);
				expect(events).toEqual(["sync:manifest"]);
			} else {
				expect(await Bun.file(markerPath).exists()).toBe(true);
				expect(events.slice(0, 3)).toEqual(["sync:manifest", "rename:manifest", "sync:directory"]);
			}
			expect((await fs.readdir(tempDir)).filter(file => file.endsWith(".tmp"))).toEqual([]);
		}
	});

	test("sidecars are synced before the main quarantine commit", async () => {
		const corruptPath = path.join(tempDir, "sidecar-sync-failure.db");
		const markerPath = `${corruptPath}.quarantine-pending`;
		const mainBytes = Buffer.alloc(512, 0x66);
		const walBytes = Buffer.from("sync-stale-wal");
		const shmBytes = Buffer.from("sync-stale-shm");
		await fs.writeFile(corruptPath, mainBytes);
		const injectedFailure = Object.assign(new Error("injected sidecar directory sync failure"), { code: "EIO" });
		const nativeOpen = fs.open;
		const nativeRename = fs.rename;
		let directorySyncs = 0;
		let mainMovedForward = false;
		const openSpy = spyOn(fs, "open").mockImplementation(async (file, flags, mode) => {
			const handle = await nativeOpen(file, flags, mode);
			const nativeSync = handle.sync.bind(handle);
			Object.defineProperty(handle, "sync", {
				configurable: true,
				value: async () => {
					if (String(file) === tempDir && ++directorySyncs === 2) throw injectedFailure;
					await nativeSync();
				},
			});
			return handle;
		});
		const renameSpy = spyOn(fs, "rename").mockImplementation(async (source, destination) => {
			const sourcePath = String(source);
			const destinationPath = String(destination);
			if (destinationPath === markerPath) {
				await nativeRename(source, destination);
				await Promise.all([
					fs.writeFile(`${corruptPath}-wal`, walBytes),
					fs.writeFile(`${corruptPath}-shm`, shmBytes),
				]);
				return;
			}
			if (sourcePath === corruptPath && destinationPath.startsWith(`${corruptPath}.quarantine-`)) {
				mainMovedForward = true;
			}
			await nativeRename(source, destination);
		});
		let failure: unknown;
		try {
			await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
		} catch (error) {
			failure = error;
		} finally {
			renameSpy.mockRestore();
			openSpy.mockRestore();
		}
		expect(failure).toBe(injectedFailure);
		expect(mainMovedForward).toBe(false);
		expect(await fs.readFile(corruptPath)).toEqual(mainBytes);
		expect(await fs.readFile(`${corruptPath}-wal`)).toEqual(walBytes);
		expect(await fs.readFile(`${corruptPath}-shm`)).toEqual(shmBytes);
		expect(await Bun.file(markerPath).exists()).toBe(false);
		expect(
			(await fs.readdir(tempDir)).filter(file => file.startsWith("sidecar-sync-failure.db.quarantine-")),
		).toEqual([]);
	});

	test("recovery provenance is checkpointed before durable marker retirement", async () => {
		const corruptPath = path.join(tempDir, "recovery-checkpoint.db");
		const markerPath = `${corruptPath}.quarantine-pending`;
		await fs.writeFile(corruptPath, Buffer.alloc(512, 0x67));
		const retirementFailure = Object.assign(new Error("injected marker retirement failure"), { code: "EIO" });
		const nativeOpen = fs.open;
		const nativeUnlink = fs.unlink;
		let unlinkAttempts = 0;
		let markerUnlinked = false;
		let directorySyncedAfterUnlink = false;
		let durableRecoveryPath: string | null | undefined;
		let durableRecoveryEvents: number | undefined;
		const openSpy = spyOn(fs, "open").mockImplementation(async (file, flags, mode) => {
			const handle = await nativeOpen(file, flags, mode);
			const nativeSync = handle.sync.bind(handle);
			Object.defineProperty(handle, "sync", {
				configurable: true,
				value: async () => {
					if (String(file) === tempDir && markerUnlinked) directorySyncedAfterUnlink = true;
					await nativeSync();
				},
			});
			return handle;
		});
		const unlinkSpy = spyOn(fs, "unlink").mockImplementation(async file => {
			if (String(file) !== markerPath) return await nativeUnlink(file);
			unlinkAttempts += 1;
			const observer = new Database(corruptPath, { readonly: true, strict: true });
			try {
				durableRecoveryPath = observer
					.query<{ last_recovery_path: string | null }, []>(
						"SELECT last_recovery_path FROM store_state WHERE id = 1",
					)
					.get()?.last_recovery_path;
				durableRecoveryEvents = observer
					.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events")
					.get()?.count;
			} finally {
				observer.close();
			}
			if (unlinkAttempts === 1) throw retirementFailure;
			await nativeUnlink(file);
			markerUnlinked = true;
		});
		let failure: unknown;
		try {
			await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
		} catch (error) {
			failure = error;
		}
		expect(failure).toBe(retirementFailure);
		expect(durableRecoveryPath).toStartWith(`${corruptPath}.quarantine-${now}-`);
		expect(durableRecoveryEvents).toBe(1);
		if (!durableRecoveryPath) throw new Error("expected durable recovery provenance");
		expect(await Bun.file(markerPath).exists()).toBe(true);

		let recovered: LcmContext | undefined;
		try {
			recovered = await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
			expect((await quarantineDatabasePaths(corruptPath))[0]).toBe(durableRecoveryPath);
			expect(unlinkAttempts).toBe(2);
			expect(directorySyncedAfterUnlink).toBe(true);
			expect(await Bun.file(markerPath).exists()).toBe(false);
			const observer = new Database(corruptPath);
			try {
				expect(
					observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
				).toBe(1);
			} finally {
				observer.close();
			}
		} finally {
			recovered?.close();
			unlinkSpy.mockRestore();
			openSpy.mockRestore();
		}
	});

	test("rejects malformed and traversing quarantine manifests before filesystem probes", async () => {
		const corruptPath = path.join(tempDir, "confined-manifest.db");
		const markerPath = `${corruptPath}.quarantine-pending`;
		const unrelatedPath = path.join(tempDir, "unrelated.db");
		const original = Buffer.alloc(512, 0x68);
		const unrelated = Buffer.from("unrelated-data");
		await Promise.all([fs.writeFile(corruptPath, original), fs.writeFile(unrelatedPath, unrelated)]);
		const validUuid = "00000000-0000-4000-8000-000000000002";
		const candidates = [
			`${corruptPath}.quarantine-${now}-${validUuid}/../${path.basename(unrelatedPath)}`,
			`${corruptPath}.quarantine-${now}-not-a-uuid`,
		];
		const nativeStat = fs.stat;
		const nativeRename = fs.rename;
		const probedPaths: string[] = [];
		const renamedPaths: string[] = [];
		const statImplementation = (async (...args: unknown[]) => {
			probedPaths.push(String(args[0]));
			return await (nativeStat as unknown as (...statArgs: unknown[]) => Promise<unknown>)(...args);
		}) as unknown as typeof fs.stat;
		const statSpy = spyOn(fs, "stat").mockImplementation(statImplementation);
		const renameSpy = spyOn(fs, "rename").mockImplementation(async (source, destination) => {
			renamedPaths.push(`${String(source)} -> ${String(destination)}`);
			await nativeRename(source, destination);
		});
		try {
			for (const quarantinePath of candidates) {
				probedPaths.length = 0;
				renamedPaths.length = 0;
				await fs.writeFile(markerPath, JSON.stringify({ quarantinePath, reason: "malformed manifest" }));
				let failure: unknown;
				try {
					await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
				} catch (error) {
					failure = error;
				}
				expect(String(failure)).toContain("Invalid pending LCM quarantine manifest");
				expect(probedPaths).toEqual([]);
				expect(renamedPaths).toEqual([]);
				expect(await fs.readFile(corruptPath)).toEqual(original);
				expect(await fs.readFile(unrelatedPath)).toEqual(unrelated);
			}
		} finally {
			renameSpy.mockRestore();
			statSpy.mockRestore();
		}
	});

	test("live owners fence physical corruption recovery until close", async () => {
		const ownedPath = path.join(tempDir, "live-owner.db");
		const { pageSize, rootPage } = seedPayloadDatabase(ownedPath);
		const owner = await openLcmContext({ dbPath: ownedPath, recoverCorrupt: true, busyTimeoutMs: 0, now: () => now });
		let recovered: LcmContext | undefined;
		try {
			expect(owner.status()).toMatchObject({
				schemaVersion: LCM_SCHEMA_VERSION,
				quarantined: false,
				latestRecovery: null,
			});
			await corruptDatabasePage(ownedPath, pageSize, rootPage);

			let blockedContext: LcmContext | undefined;
			let blockedFailure: unknown;
			try {
				blockedContext = await openLcmContext({
					dbPath: ownedPath,
					recoverCorrupt: true,
					busyTimeoutMs: 0,
					now: () => now,
				});
			} catch (error) {
				blockedFailure = error;
			} finally {
				blockedContext?.close();
			}
			expect(isLcmSqliteContentionError(blockedFailure)).toBe(true);
			expect(await Bun.file(ownedPath).exists()).toBe(true);
			expect(
				(await fs.readdir(tempDir)).filter(
					file => file.startsWith("live-owner.db.quarantine-") && !file.endsWith("-wal") && !file.endsWith("-shm"),
				),
			).toHaveLength(0);

			owner.close();
			recovered = await openLcmContext({
				dbPath: ownedPath,
				recoverCorrupt: true,
				busyTimeoutMs: 0,
				now: () => now,
			});
			const [recoveredFrom] = await quarantineDatabasePaths(ownedPath);
			expect(recoveredFrom).toStartWith(`${ownedPath}.quarantine-${now}-`);
			expect(await Bun.file(recoveredFrom!).exists()).toBe(true);
		} finally {
			recovered?.close();
			owner.close();
		}

		const quarantines = (await fs.readdir(tempDir)).filter(
			file => file.startsWith("live-owner.db.quarantine-") && !file.endsWith("-wal") && !file.endsWith("-shm"),
		);
		expect(quarantines).toHaveLength(1);
		const observer = new Database(ownedPath);
		try {
			expect(
				observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
			).toBe(1);
			expect(
				observer
					.query<{ last_recovery_path: string | null }, []>("SELECT last_recovery_path FROM store_state")
					.get()?.last_recovery_path,
			).toBe(path.join(tempDir, quarantines[0]!));
		} finally {
			observer.close();
		}
	}, 10_000);

	test("independent processes serialize latent-corruption recovery", async () => {
		const corruptPath = path.join(tempDir, "concurrent-corrupt.db");
		await createLatentlyCorruptDatabase(corruptPath);
		const moduleUrl = pathToFileURL(path.resolve(import.meta.dir, "../src/index.ts")).href;
		const script = `
			import { openLcmContext } from ${JSON.stringify(moduleUrl)};
			console.log("ready");
			await Bun.stdin.text();
			const context = await openLcmContext({
				dbPath: Bun.env.LCM_DB_PATH,
				recoverCorrupt: true,
				now: () => 1_900_000_000_000,
			});
			context.close();
		`;
		const children = Array.from({ length: 4 }, () =>
			Bun.spawn([process.execPath, "--eval", script], {
				env: { ...process.env, LCM_DB_PATH: corruptPath },
				stdin: "pipe",
				stdout: "pipe",
				stderr: "pipe",
			}),
		);
		const ready = await Promise.all(
			children.map(async child => {
				const reader = child.stdout.getReader();
				const chunk = await reader.read();
				reader.releaseLock();
				return new TextDecoder().decode(chunk.value).trim();
			}),
		);
		expect(ready).toEqual(["ready", "ready", "ready", "ready"]);
		for (const child of children) {
			child.stdin.write("start");
			child.stdin.end();
		}
		const results = await Promise.all(
			children.map(async child => ({
				exitCode: await child.exited,
				stderr: await new Response(child.stderr).text(),
			})),
		);
		expect(results.map(result => result.exitCode)).toEqual([0, 0, 0, 0]);
		expect(results.map(result => result.stderr)).toEqual(["", "", "", ""]);

		const quarantines = (await fs.readdir(tempDir)).filter(
			file =>
				file.startsWith("concurrent-corrupt.db.quarantine-") && !file.endsWith("-wal") && !file.endsWith("-shm"),
		);
		expect(quarantines).toHaveLength(1);
		const observer = new Database(corruptPath);
		try {
			expect(observer.query<{ quick_check: string }, []>("PRAGMA quick_check(1)").get()?.quick_check).toBe("ok");
			expect(
				observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM recovery_events").get()?.count,
			).toBe(1);
		} finally {
			observer.close();
		}
	});

	test("recoverCorrupt quarantines only a genuinely corrupt SQLite store", async () => {
		const corruptPath = path.join(tempDir, "corrupt.db");
		await fs.writeFile(corruptPath, Buffer.alloc(512, 0x78));
		const recovered = await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
		try {
			expect(recovered.status()).toMatchObject({ schemaVersion: LCM_SCHEMA_VERSION, quarantined: false });
			const [recoveredFrom] = await quarantineDatabasePaths(corruptPath);
			expect(recoveredFrom).toStartWith(`${corruptPath}.quarantine-${now}-`);
			expect(await Bun.file(recoveredFrom!).exists()).toBe(true);
		} finally {
			recovered.close();
		}
	});

	test("read-only and immutable SQLite URIs never attempt physical corruption recovery", async () => {
		for (const [index, query] of [
			"mode=ro",
			"mode=rw",
			"immutable=1",
			"immutable=2",
			"immutable=2suffix",
			"immutable=+2",
			"immutable=yes",
			"immutable=true",
			"immutable=on",
			"immutable=2#immutable=0",
			"mode=ro#mode=rwc",
		].entries()) {
			const corruptPath = path.join(tempDir, `recovery-ineligible-uri-${index}.db`);
			const original = Buffer.alloc(512, 0x61 + index);
			await fs.writeFile(corruptPath, original);
			let opened: LcmContext | undefined;
			let failure: unknown;
			try {
				opened = await openLcmContext({
					dbPath: `${pathToFileURL(corruptPath).href}?${query}`,
					recoverCorrupt: true,
					now: () => now,
				});
			} catch (error) {
				failure = error;
			} finally {
				opened?.close();
			}
			expect(failure).toBeDefined();
			expect(await fs.readFile(corruptPath)).toEqual(original);
			expect(
				(await fs.readdir(tempDir)).filter(file => file.startsWith(`${path.basename(corruptPath)}.quarantine-`)),
			).toEqual([]);
		}
	});

	test("unsupported schemas and invalid paths propagate without quarantine", async () => {
		const futurePath = path.join(tempDir, "future.db");
		const future = new Database(futurePath);
		future.run("PRAGMA user_version = 999");
		future.close();
		let unsupported: unknown;
		try {
			await openLcmContext({ dbPath: futurePath, recoverCorrupt: true });
		} catch (error) {
			unsupported = error;
		}
		expect(unsupported).toBeInstanceOf(UnsupportedLcmSchemaError);
		expect((await fs.readdir(tempDir)).some(file => file.startsWith("future.db.quarantine-"))).toBe(false);

		const directoryPath = path.join(tempDir, "database-directory");
		await fs.mkdir(directoryPath);
		let invalidPath: unknown;
		try {
			await openLcmContext({ dbPath: directoryPath, recoverCorrupt: true });
		} catch (error) {
			invalidPath = error;
		}
		expect(invalidPath).toBeDefined();
		expect(isLcmSqliteCorruptionError(invalidPath)).toBe(false);
		expect((await fs.readdir(tempDir)).some(file => file.startsWith("database-directory.quarantine-"))).toBe(false);
	});

	test("successful completion removes duplicated job payload", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "source text long enough to summarize safely")]));
		const [claim] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(claim).toBeDefined();
		expect(context.completeSummaryJob(claim!, { redactedText: "ok" })).toMatchObject({
			accepted: true,
		});
		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<{ count: number }, [string]>("SELECT COUNT(*) AS count FROM job_inputs WHERE job_id = ?")
					.get(claim!.jobId)?.count,
			).toBe(0);
			expect(
				observer
					.query<{ count: number }, [string]>("SELECT COUNT(*) AS count FROM job_lineage WHERE job_id = ?")
					.get(claim!.jobId)?.count,
			).toBe(0);
		} finally {
			observer.close();
		}
	});

	test("an obsolete compacted job rebuilds both payload tables before requeue", () => {
		const original = entry(MAIN, "e1", "original source text long enough to summarize");
		context.reconcile(snapshot(MAIN, [original]));
		const [claim] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "first-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(claim).toBeDefined();
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e2", "replacement source text long enough to summarize")]));
		expect(context.completeSummaryJob(claim!, { redactedText: "stale" })).toEqual({
			accepted: false,
			reason: "stale",
		});

		context.reconcile(snapshot(MAIN, [original]));
		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<{ count: number }, [string]>("SELECT COUNT(*) AS count FROM job_inputs WHERE job_id = ?")
					.get(claim!.jobId)?.count,
			).toBe(1);
			expect(
				observer
					.query<{ count: number }, [string]>("SELECT COUNT(*) AS count FROM job_lineage WHERE job_id = ?")
					.get(claim!.jobId)?.count,
			).toBe(1);
		} finally {
			observer.close();
		}
		const [requeued] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "second-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(requeued?.jobId).toBe(claim!.jobId);
		expect(requeued?.inputs[0]?.redactedText).toBe(original.redactedText);
		expect(context.completeSummaryJob(requeued!, { redactedText: "ok" })).toMatchObject({
			accepted: true,
		});
	});

	test("a failed completion remains durably retryable without corrupting committed sources", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "durable source")]));
		const [crashed] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "crashing-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(crashed).toBeDefined();
		expect(context.failSummaryJob(crashed!, "CompletionError", 50)).toBe(true);
		expect(context.status().jobs.failed).toBe(1);
		now += 50;
		const retried = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "replacement-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(retried).toHaveLength(1);
		expect(retried[0]?.inputs[0]?.redactedText).toBe("durable source");
	});

	test("an abandoned lease is reclaimed after closing and reopening the store", async () => {
		context.reconcile(
			snapshot(MAIN, [entry(MAIN, "e1", "durable source survives an abandoned completion worker restart")]),
		);
		const [abandoned] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "departed-worker",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 5,
		});
		expect(abandoned).toBeDefined();
		context.close();
		now += 101;
		context = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => now,
		});
		const [reclaimed] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "replacement-worker",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 5,
		});
		expect(reclaimed?.jobId).toBe(abandoned?.jobId);
		expect(reclaimed?.leaseToken).not.toBe(abandoned?.leaseToken);
		expect(reclaimed?.inputs[0]?.redactedText).toBe("durable source survives an abandoned completion worker restart");
		expect(
			context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } })
				.freshTailSourceIds,
		).toEqual(["e1"]);
	});

	test("unchanged reconcile policy is write-free and one policy change updates one row", () => {
		const sources = [entry(MAIN, "policy-1", "policy source")];
		const summarize = { tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		context.reconcile(snapshot(MAIN, sources), { summarize });
		const baseline = context.status().performance?.reconcileRowsChanged;
		expect(baseline).toBeDefined();

		context.reconcile(snapshot(MAIN, sources), { summarize });
		expect(context.status().performance?.reconcileRowsChanged).toBe(baseline);

		context.reconcile(snapshot(MAIN, sources), {
			summarize: { ...summarize, freshTail: { ...summarize.freshTail, maxTokens: 101 } },
		});
		expect(context.status().performance?.reconcileRowsChanged).toBe(baseline! + 1);
	});

	test("empty projection has the canonical active-source fingerprint", () => {
		const projection = context.project({
			...MAIN,
			branchId: "missing",
			tokenBudget: 100,
			freshTail: { maxSources: 1, maxTokens: 100 },
		});
		expect(projection).toMatchObject({
			ready: true,
			historical: [],
			freshTailSourceIds: [],
			uncoveredSourceIds: [],
			activeSourceFingerprint: "7e4e64063bdbd631e20e792ad8b273c6a28859d6434a7ff6368e368e02cb41dd",
		});
	});

	test("required fresh start fails closed before the branch exists", () => {
		const projection = context.project({
			...MAIN,
			branchId: "missing",
			tokenBudget: 100,
			freshTail: { maxSources: 1, maxTokens: 100, requiredStartSourceId: "absent-user" },
		});
		expect(projection).toMatchObject({
			ready: false,
			historical: [],
			freshTailSourceIds: [],
			uncoveredSourceIds: [],
		});
	});

	test("active-source fingerprint preserves Unicode UTF-8 length framing", () => {
		const sourceIds = ["ascii", "é", "東京", "😀", "e\u0301"];
		const hasher = new Bun.CryptoHasher("sha256");
		const encoder = new TextEncoder();
		const length = new Uint8Array(4);
		for (const value of ["lcm-active-source-fingerprint-v1", ...sourceIds]) {
			const bytes = encoder.encode(value);
			length[0] = bytes.byteLength >>> 24;
			length[1] = bytes.byteLength >>> 16;
			length[2] = bytes.byteLength >>> 8;
			length[3] = bytes.byteLength;
			hasher.update(length);
			hasher.update(bytes);
		}
		expect(activeSourceFingerprint(sourceIds)).toBe(hasher.digest("hex"));
	});

	test("projection proves each active source exactly once in branch order", async () => {
		const sources = [
			entry(MAIN, "e1", "one"),
			entry(MAIN, "e2", "two", "e1"),
			entry(MAIN, "e3", "three", "e2"),
			entry(MAIN, "e4", "four", "e3"),
			entry(MAIN, "e5", "five", "e4"),
		];
		context.reconcile(snapshot(MAIN, sources));
		completeEveryJob(context);

		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 10 } });
		const covered = [...projection.historical.flatMap(item => item.sourceIds), ...projection.freshTailSourceIds];
		const historicalIds = projection.historical.flatMap(item => item.sourceIds);
		const tailIds = new Set(projection.freshTailSourceIds);
		expect(historicalIds.filter(sourceId => tailIds.has(sourceId))).toEqual([]);
		expect(projection.ready).toBe(true);
		expect(covered).toEqual(["e1", "e2", "e3", "e4", "e5"]);
		expect(new Set(covered).size).toBe(covered.length);
		expect(projection.activeSourceFingerprint).toBe(
			"f51cd95960048a52c03f683bc313b86566d41843c20473951496b118ca4be4e6",
		);
		expect(
			context.project({ ...MAIN, tokenBudget: 5, freshTail: { maxSources: 5, maxTokens: 5 } })
				.activeSourceFingerprint,
		).toBe(projection.activeSourceFingerprint);

		const reordered = [
			entry(MAIN, "e1", "one"),
			entry(MAIN, "e3", "three", "e1"),
			entry(MAIN, "e2", "two", "e3"),
			entry(MAIN, "e4", "four", "e2"),
			entry(MAIN, "e5", "five", "e4"),
		];
		context.reconcile(snapshot(MAIN, reordered), { summarize: false });
		const reorderedProjection = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 5, maxTokens: 100 },
		});
		expect(reorderedProjection.activeSourceFingerprint).toBe(
			"b7e6ada60580ecbfb33769f6814866fffb8bc4a2011e87f5f1b1cf40e735fb4b",
		);
		expect(reorderedProjection.activeSourceFingerprint).not.toBe(projection.activeSourceFingerprint);
	});

	test("projection keeps the complete canonical frontier beyond the token target", () => {
		const sources = [
			entry(MAIN, "budget-1", "first historical source long enough for a three token summary"),
			entry(MAIN, "budget-2", "second historical source long enough for a three token summary", "budget-1"),
			entry(MAIN, "budget-3", "third historical source long enough for a three token summary", "budget-2"),
			entry(MAIN, "budget-4", "fourth historical source long enough for a three token summary", "budget-3"),
			entry(MAIN, "budget-5", "tail x", "budget-4"),
		];
		context.reconcile(snapshot(MAIN, sources));
		let completed = 0;
		for (;;) {
			const [job] = context.claimSummaryJobs({
				...retryClaimPolicy(context),
				workerId: "coverage-worker",
				leaseMs: 60_000,
				limit: 1,
				maxOutputTokens: 100,
			});
			if (!job) break;
			const tokenCount = job.level === 1 ? 5 : job.sourceCount === 1 ? 1 : 3;
			expect(
				context.completeSummaryJob(job, {
					redactedText: "x",
					tokenCount,
				}),
			).toMatchObject({ accepted: true });
			completed++;
		}
		expect(completed).toBe(4);

		const projection = context.project({
			...MAIN,
			tokenBudget: 5,
			freshTail: { maxSources: 1, maxTokens: 5 },
		});
		expect(projection).toMatchObject({
			ready: true,
			uncoveredSourceIds: [],
			freshTailSourceIds: ["budget-5"],
			selectedLevelCounts: { 1: 1 },
		});
		expect(projection.historical).toHaveLength(1);
		expect(projection.historical[0]?.sourceIds).toEqual(["budget-1", "budget-2", "budget-3", "budget-4"]);
		expect(projection.estimatedTokens).toBeGreaterThan(5);
	});

	test("summary expansion cites each repeated source-key occurrence in branch order", async () => {
		const first = entry(MAIN, "e1", "identical source content long enough to produce one useful leaf summary");
		const second = { ...first, entryId: "e2", parentId: first.entryId };
		const fresh = entry(MAIN, "e3", "fresh tail", second.entryId);
		context.reconcile(snapshot(MAIN, [first, second, fresh]));
		completeEveryJob(context);

		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 0, maxTokens: 0 } });
		const summaryHandle = projection.historical[0]?.summaryHandle;
		if (!summaryHandle) throw new Error("expected a completed leaf summary");
		const expansion = context.expandSummary({ ...MAIN, summaryHandle });
		const citations = expansion?.items.flatMap(item => (item.kind === "source" ? [item.citation] : []));
		expect(citations?.map(citation => [citation.sourceId, citation.position])).toEqual([
			["e1", 0],
			["e2", 1],
		]);
		expect(citations?.map(citation => citation.sourceKey)).toEqual([
			legacyFilelessSourceKey(first),
			legacyFilelessSourceKey(first),
		]);
	});

	test("fresh-tail cut never splits an assistant and its parallel tool results", async () => {
		const sources = [
			entry(MAIN, "e1", "older context that can be summarized before the active tool turn"),
			{ ...entry(MAIN, "e2", "assistant issued two parallel tool calls", "e1", "tool-turn"), kind: "assistant" },
			{ ...entry(MAIN, "e3", "first parallel tool result", "e2", "tool-turn"), kind: "tool_result" },
			{ ...entry(MAIN, "e4", "second parallel tool result", "e3", "tool-turn"), kind: "tool_result" },
			entry(MAIN, "e5", "newest user follow-up", "e4"),
		];
		context.reconcile(snapshot(MAIN, sources));
		completeEveryJob(context);

		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 2, maxTokens: 100 } });
		const historicalIds = projection.historical.flatMap(item => item.sourceIds);
		expect(projection.freshTailSourceIds).toEqual(["e5"]);
		expect(historicalIds).toEqual(["e1", "e2", "e3", "e4"]);
		expect([...historicalIds, ...projection.freshTailSourceIds]).toEqual(sources.map(source => source.entryId));
		expect(historicalIds.filter(sourceId => projection.freshTailSourceIds.includes(sourceId))).toEqual([]);
	});

	test("fresh tail always includes an oversized newest atomic unit", () => {
		const sources = [
			entry(MAIN, "tail-old", "older source"),
			entry(MAIN, "tail-new-1", "oversized newest assistant", "tail-old", "newest-turn"),
			entry(MAIN, "tail-new-2", "oversized newest tool result", "tail-new-1", "newest-turn"),
		];
		context.reconcile(snapshot(MAIN, sources), { summarize: false });

		const projection = context.project({
			...MAIN,
			tokenBudget: 1,
			freshTail: { maxSources: 1, maxTokens: 1 },
		});
		expect(projection.freshTailSourceIds).toEqual(["tail-new-1", "tail-new-2"]);
		expect(projection.uncoveredSourceIds).toEqual(["tail-old"]);
	});

	test("required fresh start overrides tail targets without trimming its suffix", () => {
		const sources = [
			entry(MAIN, "anchor-old", "older context that may be summarized"),
			entry(MAIN, "anchor-user", "active user instruction", "anchor-old"),
			{ ...entry(MAIN, "anchor-call", "assistant tool call", "anchor-user", "anchor-tool-turn"), kind: "assistant" },
			{
				...entry(MAIN, "anchor-result", "matching tool result", "anchor-call", "anchor-tool-turn"),
				kind: "tool_result",
			},
			entry(MAIN, "anchor-final", "assistant continuation", "anchor-result"),
		];
		const request = {
			...MAIN,
			tokenBudget: 1,
			freshTail: { maxSources: 1, maxTokens: 1, requiredStartSourceId: "anchor-user" },
		};

		expect(context.reconcile(snapshot(MAIN, sources), { summarize: request })).toMatchObject({ queuedJobs: 1 });
		const pending = context.project(request);
		expect(pending).toMatchObject({
			ready: false,
			pendingJobs: 1,
			uncoveredSourceIds: ["anchor-old"],
			freshTailSourceIds: ["anchor-user", "anchor-call", "anchor-result", "anchor-final"],
		});
		completeEveryJob(context);
		const projection = context.project(request);

		expect(projection).toMatchObject({ ready: true, pendingJobs: 0, uncoveredSourceIds: [] });
		expect(projection.historical.flatMap(item => item.sourceIds)).toEqual(["anchor-old"]);
		expect(projection.freshTailSourceIds).toEqual(["anchor-user", "anchor-call", "anchor-result", "anchor-final"]);
	});

	test("required fresh start excludes older pending spans inside its suffix", () => {
		const sources = [
			entry(MAIN, "pending-old-1", "first older source"),
			entry(MAIN, "pending-old-2", "second older source", "pending-old-1"),
			entry(MAIN, "pending-user", "active user instruction", "pending-old-2"),
			{
				...entry(MAIN, "pending-call", "assistant tool call", "pending-user", "pending-tool-turn"),
				kind: "assistant",
			},
			{
				...entry(MAIN, "pending-result", "matching tool result", "pending-call", "pending-tool-turn"),
				kind: "tool_result",
			},
			entry(MAIN, "pending-final", "assistant continuation", "pending-result"),
		];
		const unanchored = { tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		expect(context.reconcile(snapshot(MAIN, sources), { summarize: unanchored })).toMatchObject({ queuedJobs: 3 });

		const anchored = {
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 1, maxTokens: 100, requiredStartSourceId: "pending-user" },
		};
		context.reconcile(snapshot(MAIN, sources), { summarize: anchored });
		const projection = context.project(anchored);

		expect(projection).toMatchObject({
			ready: false,
			pendingJobs: 1,
			uncoveredSourceIds: ["pending-old-1", "pending-old-2"],
			freshTailSourceIds: ["pending-user", "pending-call", "pending-result", "pending-final"],
		});
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/model");
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");
		expect(context.summaryJobAvailability(anchored, policy, 5)).toMatchObject({
			runnable: 1,
			leased: 0,
			backoff: 0,
			exhausted: 0,
			missing: 0,
			policyMismatch: 0,
		});
	});

	test("missing required fresh start never falls back to the configured tail", () => {
		const sources = [entry(MAIN, "missing-old", "old"), entry(MAIN, "missing-new", "new", "missing-old")];
		context.reconcile(snapshot(MAIN, sources), { summarize: false });

		const projection = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 1, maxTokens: 100, requiredStartSourceId: "absent-user" },
		});

		expect(projection).toMatchObject({
			ready: false,
			historical: [],
			freshTailSourceIds: [],
			uncoveredSourceIds: ["missing-old", "missing-new"],
		});
	});

	test("fresh tail counts the newest source against the source target", () => {
		const sources = [
			entry(MAIN, "source-old", "old"),
			entry(MAIN, "source-middle", "middle", "source-old"),
			entry(MAIN, "source-new", "new", "source-middle"),
		];
		const summarize = { tokenBudget: 100, freshTail: { maxSources: 2, maxTokens: 100 } };

		expect(context.reconcile(snapshot(MAIN, sources), { summarize })).toMatchObject({ queuedJobs: 1 });
		const projection = context.project({ ...MAIN, ...summarize });
		expect(projection.freshTailSourceIds).toEqual(["source-middle", "source-new"]);
		expect(projection.uncoveredSourceIds).toEqual(["source-old"]);
	});

	test("fresh tail counts the newest tokens against the token target", () => {
		const sources = [
			entry(MAIN, "token-old", "12345678"),
			entry(MAIN, "token-middle", "abcdefgh", "token-old"),
			entry(MAIN, "token-new", "ABCDEFGH", "token-middle"),
		];
		const summarize = { tokenBudget: 100, freshTail: { maxSources: 10, maxTokens: 4 } };

		expect(context.reconcile(snapshot(MAIN, sources), { summarize })).toMatchObject({ queuedJobs: 1 });
		const projection = context.project({ ...MAIN, ...summarize });
		expect(projection.freshTailSourceIds).toEqual(["token-middle", "token-new"]);
		expect(projection.uncoveredSourceIds).toEqual(["token-old"]);
	});

	test("branches stay isolated while identical source prefixes reuse summaries", async () => {
		const mainSources = [
			entry(MAIN, "e1", "shared one"),
			entry(MAIN, "e2", "shared two", "e1"),
			entry(MAIN, "e3", "main three", "e2"),
			entry(MAIN, "e4", "main four", "e3"),
		];
		context.reconcile(snapshot(MAIN, mainSources));
		completeEveryJob(context);

		const fork = { ...MAIN, branchId: "fork" };
		const forkSources = [
			entry(fork, "e1", "shared one"),
			entry(fork, "e2", "shared two", "e1"),
			entry(fork, "f3", "fork three", "e2"),
			entry(fork, "f4", "fork four", "f3"),
		];
		const reconciled = context.reconcile(snapshot(fork, forkSources));
		expect(reconciled.reusedSummaries).toBeGreaterThan(0);
		completeEveryJob(context);

		const mainProjection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 0, maxTokens: 0 } });
		const forkProjection = context.project({ ...fork, tokenBudget: 100, freshTail: { maxSources: 0, maxTokens: 0 } });
		expect([
			...mainProjection.historical.flatMap(item => item.sourceIds),
			...mainProjection.freshTailSourceIds,
		]).toEqual(["e1", "e2", "e3", "e4"]);
		expect([
			...forkProjection.historical.flatMap(item => item.sourceIds),
			...forkProjection.freshTailSourceIds,
		]).toEqual(["e1", "e2", "f3", "f4"]);
		expect(mainProjection.freshTailSourceIds).toEqual(["e3", "e4"]);
		expect(forkProjection.freshTailSourceIds).toEqual(["f3", "f4"]);
	});

	test("projection frontier and fingerprint come from one SQLite read snapshot", async () => {
		const original = [
			entry(MAIN, "snap-old-1", "old source one long enough to summarize"),
			entry(MAIN, "snap-old-2", "old source two long enough to summarize", "snap-old-1"),
			entry(MAIN, "snap-old-3", "old source three long enough to summarize", "snap-old-2"),
		];
		const replacement = [
			entry(MAIN, "snap-new-1", "new source one"),
			entry(MAIN, "snap-new-2", "new source two", "snap-new-1"),
			entry(MAIN, "snap-new-3", "new source three", "snap-new-2"),
		];
		context.reconcile(snapshot(MAIN, original));
		completeEveryJob(context);
		const writer = await openLcmContext({ dbPath, now: () => now });
		const realQuery = Database.prototype.query;
		let injected = false;
		const queryImplementation = function (this: Database, sql: string): unknown {
			const statement = Reflect.apply(realQuery, this, [sql]) as object & {
				all: (...bindings: unknown[]) => unknown[];
			};
			if (injected || !sql.includes("JOIN source_contents sc") || !sql.includes("ORDER BY bs.position")) {
				return statement;
			}
			const all = statement.all.bind(statement);
			return new Proxy(statement, {
				get(target, property) {
					if (property !== "all") return Reflect.get(target, property, target);
					return (...bindings: unknown[]) => {
						const rows = all(...bindings);
						injected = true;
						expect(writer.reconcile(snapshot(MAIN, replacement), { summarize: false }).changed).toBe(true);
						return rows;
					};
				},
			});
		} as unknown as typeof Database.prototype.query;
		const querySpy = spyOn(Database.prototype, "query").mockImplementation(queryImplementation);
		try {
			const duringWrite = context.project({
				...MAIN,
				tokenBudget: 100,
				freshTail: { maxSources: 0, maxTokens: 0 },
			});
			expect(injected).toBe(true);
			expect(duringWrite).toMatchObject({
				revision: 1,
				ready: true,
				uncoveredSourceIds: [],
				freshTailSourceIds: ["snap-old-3"],
				activeSourceFingerprint: "9e30d3b69de5056091117b7512a363d24488f4ff4760b42cca0fd4061fdfaab8",
			});
			expect(duringWrite.historical.flatMap(item => item.sourceIds)).toEqual(["snap-old-1", "snap-old-2"]);
		} finally {
			querySpy.mockRestore();
			writer.close();
		}

		const afterWrite = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 3, maxTokens: 100 },
		});
		expect(afterWrite).toMatchObject({
			revision: 2,
			ready: true,
			historical: [],
			freshTailSourceIds: ["snap-new-1", "snap-new-2", "snap-new-3"],
			uncoveredSourceIds: [],
			activeSourceFingerprint: "6c02b8c7c700169ff070f919299f253badebcb4cec9daf91471b67e4260f9777",
		});
	});

	test("shared summaries never cross a different branch's atomic boundaries", async () => {
		const mainSources = [
			entry(MAIN, "e1", "first shared source has enough text for its own summary"),
			entry(MAIN, "e2", "second shared source has enough text for summary reuse", "e1"),
			entry(MAIN, "e3", "third shared source closes the alternate branch group", "e2"),
		];
		context.reconcile(snapshot(MAIN, mainSources));
		completeEveryJob(context);

		const fork = { ...MAIN, branchId: "grouped-fork" };
		context.reconcile(
			snapshot(fork, [
				entry(fork, "e1", mainSources[0]!.redactedText),
				entry(fork, "e2", mainSources[1]!.redactedText, "e1", "fork-tool-turn"),
				entry(fork, "e3", mainSources[2]!.redactedText, "e2", "fork-tool-turn"),
			]),
		);
		const projection = context.project({ ...fork, tokenBudget: 100, freshTail: { maxSources: 0, maxTokens: 0 } });
		expect(projection.freshTailSourceIds).toEqual(["e2", "e3"]);
		expect(projection.historical.flatMap(item => item.sourceIds)).toEqual([]);
		expect(projection.uncoveredSourceIds).toEqual(["e1"]);
	});

	test("projection pendingJobs counts only current-branch historical work while status stays project-wide", () => {
		const fork = { ...MAIN, branchId: "pending-fork" };
		context.reconcile(
			snapshot(MAIN, [
				entry(MAIN, "m1", "main historical source one long enough to summarize"),
				entry(MAIN, "m2", "main historical source two long enough to summarize", "m1"),
			]),
		);
		context.reconcile(
			snapshot(fork, [
				entry(fork, "f1", "fork historical source one long enough to summarize"),
				entry(fork, "f2", "fork historical source two long enough to summarize", "f1"),
			]),
		);

		const historical = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 0, maxTokens: 0 },
		});
		expect(historical.pendingJobs).toBe(1);
		expect(context.status().jobs.pending).toBe(2);
		expect(
			context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 2, maxTokens: 100 } }).pendingJobs,
		).toBe(0);

		const [preferred] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "main-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(preferred?.queueClass).toBe("preferred");
		expect(
			context.completeSummaryJob(preferred!, {
				redactedText: "main summary",
				tokenCount: 1,
			}),
		).toMatchObject({ accepted: true });
		const ready = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 0, maxTokens: 0 },
		});
		expect(ready).toMatchObject({ ready: true, pendingJobs: 0 });
		expect(context.status().jobs.pending).toBe(1);
	});

	test("preferred-only claims exclude fallback and allowed fallback fills remaining capacity", () => {
		const fork = { ...MAIN, branchId: "claim-fork" };
		context.reconcile(snapshot(MAIN, [entry(MAIN, "p1", "preferred branch work long enough to summarize")]));
		context.reconcile(snapshot(fork, [entry(fork, "f1", "fallback branch work long enough to summarize")]));

		const preferredOnly = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "preferred-only",
			leaseMs: 1_000,
			limit: 2,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(preferredOnly).toHaveLength(1);
		expect(preferredOnly[0]?.queueClass).toBe("preferred");
		expect(context.releaseSummaryJob(preferredOnly[0]!)).toBe(true);

		const mixed = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "mixed",
			leaseMs: 1_000,
			limit: 2,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: true,
		});
		expect(mixed.map(job => job.queueClass)).toEqual(["preferred", "fallback"]);
	});

	test("only a full atomically aligned active-branch lineage is preferred", () => {
		const origin = { ...MAIN, branchId: "lineage-origin" };
		const sharedOne = "shared source one long enough to summarize";
		const sharedTwo = "shared source two long enough to summarize";
		context.reconcile(snapshot(origin, [entry(origin, "e1", sharedOne), entry(origin, "e2", sharedTwo, "e1")]));
		context.reconcile(
			snapshot(MAIN, [
				entry(MAIN, "e1", sharedOne),
				entry(MAIN, "e2", sharedTwo, "e1", "tool-turn"),
				entry(MAIN, "e3", "tool result completing the atomic unit", "e2", "tool-turn"),
			]),
			{ summarize: false },
		);
		expect(
			context.claimSummaryJobs({
				...retryClaimPolicy(context),
				workerId: "preferred-only",
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
			}),
		).toEqual([]);
		const [fallback] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "fallback-allowed",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: true,
		});
		expect(fallback?.queueClass).toBe("fallback");
	});

	test("durable failure classes and scope-aware delays survive reopen and prune after completion", async () => {
		const fork = { ...MAIN, branchId: "failure-fork" };
		context.reconcile(snapshot(MAIN, [entry(MAIN, "p1", "preferred failure input long enough to summarize")]));
		context.reconcile(snapshot(fork, [entry(fork, "f1", "fallback failure input long enough to summarize")]));
		const [preferred] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "preferred-failure",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(context.failSummaryJob(preferred!, "ProviderError", 100)).toBe(true);
		const [fallback] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "fallback-failure",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: true,
		});
		expect(fallback?.queueClass).toBe("fallback");
		expect(context.failSummaryJob(fallback!, "ProviderError", 0)).toBe(true);

		context.close();
		context = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => now,
		});
		const retryPolicy = retryClaimPolicy(context);
		const failures = context.summaryJobFailures(retryPolicy, retryPolicy.maxTransportRetries, MAIN);
		expect(failures.find(failure => failure.queueClass === "preferred")).toMatchObject({
			jobId: preferred!.jobId,
			availableAt: now + 100,
		});
		expect(failures.find(failure => failure.queueClass === "fallback")).toMatchObject({
			jobId: fallback!.jobId,
			availableAt: now + 1,
		});
		expect(context.nextSummaryJobDelayMs(retryPolicy, retryPolicy.maxTransportRetries, MAIN, false)).toBe(100);
		expect(context.nextSummaryJobDelayMs(retryPolicy, retryPolicy.maxTransportRetries, MAIN, true)).toBe(1);

		now += 100;
		const [retry] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "preferred-retry",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(context.completeSummaryJob(retry!, { redactedText: "recovered", tokenCount: 1 })).toMatchObject({
			accepted: true,
		});
		expect(context.summaryJobFailures(retryPolicy, retryPolicy.maxTransportRetries, MAIN)).toEqual([
			{ jobId: fallback!.jobId, availableAt: now - 99, queueClass: "fallback" },
		]);
	});

	test("release accepts an expired matching token but rejects a replaced owner", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "release input long enough to summarize")]));
		const [expired] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker-a",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		now += 101;
		expect(context.releaseSummaryJob(expired!)).toBe(true);
		const [replaced] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker-b",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		now += 101;
		const [owner] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker-c",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(owner?.leaseToken).not.toBe(replaced?.leaseToken);
		expect(context.releaseSummaryJob(replaced!)).toBe(false);
		expect(context.releaseSummaryJob(owner!)).toBe(true);
		expect(context.status().jobs.pending).toBe(1);
	});

	test("leases are exclusive, reclaimable, and reject an expired owner's result", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "lease input")]));
		const first = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker-a",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(first).toHaveLength(1);
		expect(
			context.claimSummaryJobs({
				...retryClaimPolicy(context),
				workerId: "worker-b",
				leaseMs: 100,
				limit: 1,
				maxOutputTokens: 100,
			}),
		).toEqual([]);

		now += 101;
		const replacement = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker-b",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(replacement).toHaveLength(1);
		expect(replacement[0]?.leaseToken).not.toBe(first[0]?.leaseToken);
		expect(context.completeSummaryJob(first[0]!, { redactedText: "late" })).toEqual({
			accepted: false,
			reason: "lease_lost",
		});
		expect(context.completeSummaryJob(replacement[0]!, { redactedText: "accepted" })).toMatchObject({
			accepted: true,
		});
	});

	test("completion rejects forged token counts and non-compressing output, then remains retryable", () => {
		const sourceText =
			"this source is intentionally long so returning it unchanged cannot be mistaken for compression";
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", sourceText)]));
		const [claim] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 2,
		});
		expect(claim?.inputTokenCount).toBeGreaterThan(claim?.outputTokenBudget ?? 0);
		// The nominal cap is a floor now: the ratio-derived budget wins when it is larger.
		expect(claim?.outputTokenBudget).toBe(Math.ceil(claim!.inputTokenCount / 2));
		expect(context.completeSummaryJob(claim!, { redactedText: sourceText, tokenCount: 1 })).toEqual({
			accepted: false,
			reason: "escalated",
			stage: "aggressive",
		});
		expect(context.status().jobs.pending).toBe(1);

		const [retry] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "replacement",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 2,
		});
		expect(retry?.transportRetryCount).toBe(0);
		expect(context.completeSummaryJob(retry!, { redactedText: "ok", tokenCount: 1 })).toMatchObject({
			accepted: true,
		});
	});

	test("stage budgets track input size under a hard node ceiling", () => {
		// ceil(19388/4) = 4847 estimated tokens in one leaf.
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "x".repeat(19_388))]));
		const [normal] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		expect(normal?.inputTokenCount).toBe(4_847);
		expect(normal?.outputTokenBudget).toBe(2_424);
		// 3,200 estimated tokens compresses the input but clears ceil(2424 * 1.3) = 3152.
		expect(context.completeSummaryJob(normal!, { redactedText: "y".repeat(12_800) })).toEqual({
			accepted: false,
			reason: "escalated",
			stage: "aggressive",
		});

		const [aggressive] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		expect(aggressive?.outputTokenBudget).toBe(1_212);
		expect(context.completeSummaryJob(aggressive!, { redactedText: "y".repeat(8_000) })).toEqual({
			accepted: false,
			reason: "escalated",
			stage: "deterministic",
		});

		// Deterministic keeps the aggressive budget instead of collapsing to 512.
		const [deterministic] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		expect(deterministic?.outputTokenBudget).toBe(1_212);
		expect(deterministic?.transportRetryCount).toBe(0);
		expect(context.completeSummaryJob(deterministic!, { redactedText: "y".repeat(8_000) })).toEqual({
			accepted: false,
			reason: "deterministic_failed",
		});
		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<{ status: string; transport_retry_count: number }, [string]>(
						"SELECT status, transport_retry_count FROM summary_jobs WHERE job_id = ?",
					)
					.get(deterministic!.jobId),
			).toEqual({ status: "obsolete", transport_retry_count: 0 });
		} finally {
			observer.close();
		}
	});

	test("the node ceiling, not the tolerance, bounds an oversized atomic leaf", () => {
		// ceil(70920/4) = 17730 tokens: one indivisible unit far above any leaf chunk target.
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "x".repeat(70_920))]));
		const [normal] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		expect(normal?.inputTokenCount).toBe(17_730);
		expect(normal?.outputTokenBudget).toBe(4_096);
		// 4,500 tokens sits below ceil(4096 * 1.3) = 5325 and is still rejected by the ceiling.
		expect(context.completeSummaryJob(normal!, { redactedText: "y".repeat(18_000) })).toEqual({
			accepted: false,
			reason: "escalated",
			stage: "aggressive",
		});

		const [aggressive] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		expect(aggressive?.outputTokenBudget).toBe(2_048);
	});

	test("accepts a cap-honoring completion exactly at the leased budget", () => {
		const summarized = entry(MAIN, "e1", "x".repeat(19_388));
		context.reconcile(snapshot(MAIN, [summarized]));
		const [claim] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		expect(claim?.outputTokenBudget).toBe(2_424);
		expect(context.completeSummaryJob(claim!, { redactedText: "y".repeat(2_424 * 4) })).toMatchObject({
			accepted: true,
		});
		context.reconcile(snapshot(MAIN, [summarized, entry(MAIN, "e2", "fresh tail", "e1")]), { summarize: false });
		expect(
			context.project({ ...MAIN, tokenBudget: 4_000, freshTail: { maxSources: 0, maxTokens: 0 } }).historical[0]
				?.tokenCount,
		).toBe(2_424);
	});

	test("accepts bounded overshoot and still projects a complete cover", () => {
		const summarized = entry(MAIN, "e1", "x".repeat(19_388));
		context.reconcile(snapshot(MAIN, [summarized]));
		const [claim] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		expect(claim?.outputTokenBudget).toBe(2_424);
		// 2,600 tokens overshoots the budget 1.07x, inside ceil(2424 * 1.3) = 3152.
		expect(context.completeSummaryJob(claim!, { redactedText: "y".repeat(10_400) })).toMatchObject({
			accepted: true,
		});
		context.reconcile(snapshot(MAIN, [summarized, entry(MAIN, "e2", "fresh tail", "e1")]), { summarize: false });

		const projection = context.project({
			...MAIN,
			tokenBudget: 4_000,
			freshTail: { maxSources: 0, maxTokens: 0 },
		});
		expect(projection.ready).toBe(true);
		expect(projection.historical).toHaveLength(1);
		expect(projection.historical[0]?.tokenCount).toBe(2_600);
		expect(projection.uncoveredSourceIds).toEqual([]);
		expect(projection.pendingJobs).toBe(0);
	});

	test("a peer handle's completion becomes visible to another open handle with no local signal", async () => {
		const summarized = entry(MAIN, "e1", "x".repeat(19_388));
		context.reconcile(snapshot(MAIN, [summarized]));
		const peer = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => now,
			regexEngine: TEST_REGEX_ENGINE,
		});
		try {
			const [leased] = peer.claimSummaryJobs({
				...retryClaimPolicy(peer),
				workerId: "peer",
				leaseMs: 600_000,
				limit: 1,
				maxOutputTokens: 2_048,
			});
			expect(leased).toBeDefined();
			context.reconcile(snapshot(MAIN, [summarized, entry(MAIN, "e2", "fresh tail", "e1")]), {
				summarize: false,
			});

			// The local handle can neither claim the peer's job nor learn anything sooner than
			// the peer lease expiry, so a wake-only strategy would sleep for the full 600 s.
			expect(
				context.claimSummaryJobs({
					...retryClaimPolicy(context),
					workerId: "local",
					leaseMs: 60_000,
					limit: 1,
					maxOutputTokens: 2_048,
				}),
			).toEqual([]);
			const sharedPolicy = retryClaimPolicy(context);
			expect(context.nextSummaryJobDelayMs(sharedPolicy, sharedPolicy.maxTransportRetries, MAIN)).toBe(600_000);
			const before = context.project({
				...MAIN,
				tokenBudget: 4_000,
				freshTail: { maxSources: 0, maxTokens: 0 },
			});
			expect(before.historical).toEqual([]);
			expect(before.pendingJobs).toBe(1);

			expect(peer.completeSummaryJob(leased!, { redactedText: "y".repeat(4_000) })).toMatchObject({
				accepted: true,
			});

			// Re-reading is the only notification that exists across handles.
			const after = context.project({
				...MAIN,
				tokenBudget: 4_000,
				freshTail: { maxSources: 0, maxTokens: 0 },
			});
			expect(after.historical).toHaveLength(1);
			expect(after.historical[0]?.tokenCount).toBe(1_000);
			expect(after.pendingJobs).toBe(0);
			expect(after.uncoveredSourceIds).toEqual([]);
		} finally {
			peer.close();
		}
	});

	test("completion rejects lineage that became misaligned with a branch atomic group", () => {
		const original = [
			entry(MAIN, "e1", "first source has enough content for compression"),
			entry(MAIN, "e2", "assistant starts a tool turn here", "e1"),
			entry(MAIN, "e3", "tool result closes that turn", "e2"),
		];
		context.reconcile(snapshot(MAIN, original));
		const claim = context
			.claimSummaryJobs({
				...retryClaimPolicy(context),
				workerId: "worker",
				leaseMs: 1_000,
				limit: 10,
				maxOutputTokens: 2,
			})
			.find(job => job.sourceCount === 2);
		expect(claim).toBeDefined();
		context.reconcile(
			snapshot(MAIN, [
				original[0]!,
				{ ...original[1]!, atomicGroupId: "tool-turn" },
				{ ...original[2]!, atomicGroupId: "tool-turn" },
			]),
		);
		expect(context.completeSummaryJob(claim!, { redactedText: "ok" })).toEqual({
			accepted: false,
			reason: "stale",
		});
	});

	test("completion rejects a still-leased result whose source lineage disappeared", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "old lineage")]));
		const [claim] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(claim).toBeDefined();
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e2", "replacement lineage")]));
		expect(context.completeSummaryJob(claim!, { redactedText: "stale summary" })).toEqual({
			accepted: false,
			reason: "stale",
		});
	});

	test("FTS searches only redacted source and summary text and returns resolvable citations", async () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "public alpha marker [redacted]")]));
		expect(context.search({ ...MAIN, query: "never-store-this-exact-value" })).toEqual([]);
		const sourceHits = context.search({ ...MAIN, query: "alpha + (marker)" });
		expect(sourceHits[0]).toMatchObject({ kind: "source", redactedText: "public alpha marker [redacted]" });
		expect(context.describe(sourceHits[0]!.citations[0]!)).toMatchObject({
			sourceId: "e1",
			redactedText: "public alpha marker [redacted]",
			artifactRefs: ["artifact://e1"],
		});

		completeEveryJob(context);
		const summaryHit = context.search({ ...MAIN, query: "s0" }).find(hit => hit.kind === "summary");
		expect(summaryHit?.citations.map(citation => citation.sourceId)).toEqual(["e1"]);
	});

	test("branch search scopes equal-ranked candidates before pagination", () => {
		const fork = { ...MAIN, branchId: "search-noise" };
		const redactedText = "branchscope equal ranked document";
		context.reconcile(
			snapshot(
				fork,
				Array.from({ length: 141 }, (_, index) => entry(fork, `noise-${index}`, redactedText)),
			),
			{ summarize: false },
		);
		context.reconcile(snapshot(MAIN, [entry(MAIN, "target", redactedText)]), { summarize: false });

		const hits = context.search({ ...MAIN, query: "branchscope", limit: 1 });
		expect(hits).toHaveLength(1);
		expect(hits[0]?.citations.map(citation => citation.sourceId)).toEqual(["target"]);
	});

	test("branch and project search paginate one read snapshot at the maximum offset", async () => {
		const staging = { ...MAIN, branchId: "search-staging" };
		const redactedText = "offsetboundary equal ranked document";
		const original = Array.from({ length: 1_001 }, (_, index) => entry(MAIN, `source-${index}`, redactedText));
		const replacements = Array.from({ length: 1_001 }, (_, index) =>
			entry(MAIN, `replacement-${index}`, redactedText),
		);
		context.reconcile(
			snapshot(
				staging,
				replacements.map(source => ({ ...source, ...staging })),
			),
			{ summarize: false },
		);
		context.reconcile(snapshot(staging, []), { summarize: false });
		context.reconcile(snapshot(MAIN, original), { summarize: false });

		expect(context.status().journalMode).toBe("wal");

		const writer = await openLcmContext({ dbPath, now: () => now });
		const searchDuringReplacement = (scopeQuery: string, action: () => SearchHit[]): SearchHit[] => {
			const realQuery = Database.prototype.query;
			let injected = false;
			let replacementApplied = false;
			const queryImplementation = function (this: Database, sql: string): unknown {
				const statement = Reflect.apply(realQuery, this, [sql]) as object & {
					all: (...bindings: unknown[]) => unknown[];
				};
				if (injected || !sql.includes(scopeQuery)) return statement;
				const all = statement.all.bind(statement);
				return new Proxy(statement, {
					get(target, property) {
						if (property !== "all") return Reflect.get(target, property, target);
						return (...bindings: unknown[]) => {
							const rows = all(...bindings);
							injected = true;
							replacementApplied = writer.reconcile(snapshot(MAIN, replacements), { summarize: false }).changed;
							return rows;
						};
					},
				});
			} as unknown as typeof Database.prototype.query;
			const querySpy = spyOn(Database.prototype, "query").mockImplementation(queryImplementation);
			try {
				const hits = action();
				expect(injected).toBe(true);
				expect(replacementApplied).toBe(true);
				return hits;
			} finally {
				querySpy.mockRestore();
			}
		};

		try {
			const branchHits = searchDuringReplacement("AND b.session_id = ? AND b.branch_id = ?", () =>
				context.search({ ...MAIN, query: "offsetboundary", offset: 1_000, limit: 1 }),
			);
			expect(branchHits).toHaveLength(1);
			expect(branchHits[0]?.citations.map(citation => citation.sourceId)).toEqual(["source-1000"]);

			writer.reconcile(snapshot(MAIN, original), { summarize: false });
			const projectHits = searchDuringReplacement("ORDER BY bs.branch_row_id, bs.position", () =>
				context.searchProject({ projectId: MAIN.projectId, query: "offsetboundary", offset: 1_000, limit: 1 }),
			);
			expect(projectHits).toHaveLength(1);
			expect(projectHits[0]?.citations.map(citation => citation.sourceId)).toEqual(["source-1000"]);
		} finally {
			writer.close();
		}
	});

	test("project search is explicitly isolated and cites only active placements", async () => {
		const fork = { ...MAIN, branchId: "fork" };
		const other = { projectId: "other-project", sessionId: "session", branchId: "main" };
		const authorizedText = "catalogneedle authorized project source with enough content to summarize";
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", authorizedText)]));
		context.reconcile(snapshot(fork, [entry(fork, "e1", authorizedText)]));
		context.reconcile(
			snapshot(other, [entry(other, "e1", "catalogneedle foreign project source with enough content to summarize")]),
		);

		const shared = context.searchProject({ projectId: MAIN.projectId, query: "catalogneedle" });
		const sharedSource = shared.find(hit => hit.kind === "source");
		expect(sharedSource?.citations.map(citation => citation.branchId).sort()).toEqual(["fork", "main"]);
		expect(sharedSource?.citations.every(citation => citation.projectId === MAIN.projectId)).toBe(true);
		expect(sharedSource?.redactedText).toBe(authorizedText);

		context.reconcile(snapshot(fork, []));
		const activeOnly = context.searchProject({ projectId: MAIN.projectId, query: "catalogneedle" });
		expect(activeOnly.find(hit => hit.kind === "source")?.citations.map(citation => citation.branchId)).toEqual([
			"main",
		]);
		expect(context.searchProject({ projectId: "missing-project", query: "catalogneedle" })).toEqual([]);

		const foreign = context.searchProject({ projectId: other.projectId, query: "catalogneedle" });
		expect(
			foreign
				.find(hit => hit.kind === "source")
				?.citations.every(citation => citation.projectId === other.projectId),
		).toBe(true);
		completeEveryJob(context);
		const summary = context
			.searchProject({ projectId: MAIN.projectId, query: "s0" })
			.find(hit => hit.kind === "summary");
		expect(summary?.citations.map(citation => [citation.projectId, citation.branchId])).toEqual([
			[MAIN.projectId, "main"],
		]);
		context.reconcile(snapshot(MAIN, []));
		expect(context.searchProject({ projectId: MAIN.projectId, query: "catalogneedle" })).toEqual([]);
		expect(context.searchProject({ projectId: MAIN.projectId, query: "s0" })).toEqual([]);
	});

	test("physical corruption recovery replaces every derived index", async () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "obsolete searchable term")]));
		expect(context.search({ ...MAIN, query: "obsolete" })).toHaveLength(1);
		context.close();
		await fs.writeFile(dbPath, Buffer.alloc(512, 0x78));

		const recovered = await openLcmContext({ dbPath, recoverCorrupt: true, now: () => now });
		try {
			const [recoveredFrom] = await quarantineDatabasePaths(dbPath);
			expect(recovered.status()).toMatchObject({ schemaVersion: LCM_SCHEMA_VERSION, quarantined: false });
			expect(recoveredFrom).toStartWith(`${dbPath}.quarantine-${now}-`);
			expect(await Bun.file(recoveredFrom!).exists()).toBe(true);
			expect(recovered.search({ ...MAIN, query: "obsolete" })).toEqual([]);

			expect(recovered.reconcile(snapshot(MAIN, [entry(MAIN, "e2", "replacement searchable term")]))).toMatchObject({
				changed: true,
				activeSources: 1,
			});
			expect(recovered.search({ ...MAIN, query: "obsolete" })).toEqual([]);
			expect(recovered.search({ ...MAIN, query: "replacement" })[0]?.citations[0]?.sourceId).toBe("e2");
			expect(recovered.doctor().ok).toBe(true);
		} finally {
			recovered.close();
		}
	});

	test("purge removes only old unreferenced complete quarantine units and is idempotent", async () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "active source remains untouched by storage cleanup")]));
		const retentionMs = 30 * 24 * 60 * 60_000;
		const oldTimestamp = now - retentionMs - 1;
		const oldPath = `${dbPath}.quarantine-${oldTimestamp}-00000000-0000-4000-8000-000000000020`;
		const recentPath = `${dbPath}.quarantine-${now - retentionMs + 1}-00000000-0000-4000-8000-000000000021`;
		const latestPath = `${dbPath}.quarantine-${oldTimestamp - 1}-00000000-0000-4000-8000-000000000022`;
		const pendingPath = `${dbPath}.quarantine-${oldTimestamp - 2}-00000000-0000-4000-8000-000000000023`;
		const incompletePath = `${dbPath}.quarantine-${oldTimestamp - 3}-00000000-0000-4000-8000-000000000024`;
		await Promise.all([
			fs.writeFile(oldPath, Buffer.alloc(11)),
			fs.writeFile(`${oldPath}-wal`, Buffer.alloc(7)),
			fs.writeFile(recentPath, Buffer.alloc(13)),
			fs.writeFile(latestPath, Buffer.alloc(17)),
			fs.writeFile(`${latestPath}-shm`, Buffer.alloc(5)),
			fs.writeFile(pendingPath, Buffer.alloc(19)),
			fs.writeFile(`${pendingPath}-wal`, Buffer.alloc(3)),
			fs.writeFile(`${incompletePath}-shm`, Buffer.alloc(23)),
			fs.writeFile(
				`${dbPath}.quarantine-pending`,
				JSON.stringify({ quarantinePath: pendingPath, reason: "pending" }),
			),
		]);
		const observer = new Database(dbPath);
		try {
			observer.run("UPDATE store_state SET last_recovery_path = ? WHERE id = 1", [latestPath]);
			observer.run("INSERT INTO recovery_events (quarantine_path, reason, created_at) VALUES (?, 'recovered', ?)", [
				latestPath,
				now,
			]);
		} finally {
			observer.close();
		}

		const before = context.status();
		const first = context.purge();
		expect(first).toEqual({
			tombstones: 0,
			jobs: 0,
			summaries: 0,
			sourceContents: 0,
			files: 0,
			quarantineFiles: 2,
			quarantineBytes: 18,
		});
		const afterFirst = context.status();
		expect({ branches: afterFirst.branches, activeSources: afterFirst.activeSources }).toEqual({
			branches: before.branches,
			activeSources: before.activeSources,
		});
		expect(await Bun.file(oldPath).exists()).toBe(false);
		expect(await Bun.file(`${oldPath}-wal`).exists()).toBe(false);
		for (const retained of [recentPath, latestPath, `${latestPath}-shm`, pendingPath, `${pendingPath}-wal`]) {
			expect(await Bun.file(retained).exists()).toBe(true);
		}
		expect(await Bun.file(`${incompletePath}-shm`).exists()).toBe(true);
		expect(await Bun.file(`${dbPath}.quarantine-pending`).exists()).toBe(true);

		expect(context.purge()).toEqual({
			tombstones: 0,
			jobs: 0,
			summaries: 0,
			sourceContents: 0,
			files: 0,
			quarantineFiles: 0,
			quarantineBytes: 0,
		});
		const after = context.status();
		expect({ branches: after.branches, activeSources: after.activeSources }).toEqual({
			branches: before.branches,
			activeSources: before.activeSources,
		});
	});

	test("tombstones remain until the configured retention horizon and are then purged", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "temporary")]));
		context.reconcile(snapshot(MAIN, []));
		expect(context.status().tombstones).toBe(1);
		expect(context.purge().tombstones).toBe(0);
		now += 101;
		expect(context.purge().tombstones).toBe(1);
		expect(context.status().tombstones).toBe(0);
	});
	test("v7 to v8 migration derives an empty span index on first reconcile", async () => {
		const migrationPath = path.join(tempDir, "v7-spans.db");
		const db = new Database(migrationPath);
		try {
			initializeLcmSchema(db, 1_000);
			removeSchema10RetryAuthority(db);
			db.run("DROP TABLE branch_summary_spans");
			// v7 predates session attribution, so the synthetic database must not carry it either.
			db.run("ALTER TABLE summary_attempts DROP COLUMN session_id");
			db.run("PRAGMA user_version = 7");
		} finally {
			db.close();
		}

		const migrated = await openLcmContext({ dbPath: migrationPath, now: () => now });
		try {
			const observer = new Database(migrationPath, { readonly: true, strict: true });
			try {
				expect(observer.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(
					LCM_SCHEMA_VERSION,
				);
				expect(
					observer.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM branch_summary_spans").get()?.count,
				).toBe(0);
			} finally {
				observer.close();
			}

			const source = entry(MAIN, "v7-e1", "ordinary reconciliation derives the migrated placement index");
			expect(migrated.reconcile(snapshot(MAIN, [source]))).toMatchObject({ queuedJobs: 1 });
			const derived = new Database(migrationPath, { readonly: true, strict: true });
			try {
				expect(
					derived.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM branch_summary_spans").get()?.count,
				).toBe(1);
			} finally {
				derived.close();
			}
			expect(
				migrated.claimSummaryJobs({
					...retryClaimPolicy(migrated),
					workerId: "v7-migration-worker",
					leaseMs: 1_000,
					limit: 1,
					maxOutputTokens: 100,
				}),
			).toHaveLength(1);
		} finally {
			migrated.close();
		}
	});

	test("attributes attempt spend to its session and excludes anything at or after the epoch", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "x".repeat(19_388))]));
		const [job] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "worker",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 2_048,
		});
		const usage = {
			input: 10,
			output: 5,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 15,
			cost: { input: 0.2, output: 0.05, cacheRead: 0, cacheWrite: 0, total: 0.25 },
		};
		expect(
			context.beginSummaryAttempt(
				job!,
				{ attemptId: "attempt-s1", startedAt: now, provider: "p", model: "m" },
				{ promptHash: "hash-s1", sessionId: "s1", strategy: "preserve_details" },
			),
		).toBe(true);
		expect(
			context.completeSummaryJob(job!, {
				redactedText: "y".repeat(4_000),
				attempt: { attemptId: "attempt-s1", startedAt: now, completedAt: now, provider: "p", model: "m", usage },
			}),
		).toMatchObject({ accepted: true });

		// Only this session's rows count, and only those started before the epoch.
		expect(context.priorSummarySpendUsd("s1", now + 1)).toBeCloseTo(0.25, 10);
		expect(context.priorSummarySpendUsd("s2", now + 1)).toBe(0);
		expect(context.priorSummarySpendUsd("s1", now)).toBe(0);
	});

	test("v8 to v9 migration adds session attribution and leaves historic attempts unattributed", async () => {
		const migrationPath = path.join(tempDir, "v8-attempt-sessions.db");
		const db = new Database(migrationPath);
		try {
			initializeLcmSchema(db, 1_000);
			removeSchema10RetryAuthority(db);
			db.run("ALTER TABLE summary_attempts DROP COLUMN session_id");
			db.run(
				`INSERT INTO summary_attempts
					(attempt_id, job_id, project_id, input_hash, attempt_count, started_at, completed_at, outcome,
					 provider, model, stage, strategy, prompt_hash, cost_total)
				 VALUES ('legacy-attempt', 'legacy-job', 'project', 'legacy-hash', 1, 10, 20, 'completed',
					 'p', 'm', 'normal', 'preserve_details', 'legacy-prompt', 0.5)`,
			);
			db.run("PRAGMA user_version = 8");
		} finally {
			db.close();
		}

		const migrated = await openLcmContext({ dbPath: migrationPath, now: () => now });
		try {
			const observer = new Database(migrationPath, { readonly: true, strict: true });
			try {
				expect(observer.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(
					LCM_SCHEMA_VERSION,
				);
				expect(
					observer
						.query<{ session_id: string | null; cost_total: number }, []>(
							"SELECT session_id, cost_total FROM summary_attempts WHERE attempt_id = 'legacy-attempt'",
						)
						.get(),
				).toEqual({ session_id: null, cost_total: 0.5 });
			} finally {
				observer.close();
			}
			// A historic row belongs to no session, so it is never billed to one.
			expect(migrated.priorSummarySpendUsd("s1", now + 1)).toBe(0);
		} finally {
			migrated.close();
		}
	});

	test("condensation uses exact fan-in groups without a partial parent or synthetic root", () => {
		const sources = Array.from({ length: 5 }, (_, index) =>
			entry(MAIN, `odd-${index + 1}`, `odd leaf source ${index + 1}`, index === 0 ? null : `odd-${index}`),
		);
		context.reconcile(snapshot(MAIN, sources));
		completeEveryJob(context);

		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			const spans = observer
				.query<
					{
						level: number;
						start_position: number;
						end_position: number;
						summary_id: string | null;
						frontier: number;
					},
					[]
				>(
					`SELECT level, start_position, end_position, summary_id, frontier
					 FROM branch_summary_spans ORDER BY level, start_position`,
				)
				.all();
			expect(
				spans.find(span => span.level === 0 && span.start_position === 4 && span.end_position === 5),
			).toMatchObject({
				summary_id: expect.any(String),
				frontier: 1,
			});
			expect(spans.filter(span => span.level > 0 && span.start_position === 4)).toEqual([]);
			expect(spans.some(span => span.level > 1 || (span.start_position === 0 && span.end_position === 5))).toBe(
				false,
			);
		} finally {
			observer.close();
		}
	});

	test("projection selects completed descendants when their parent crosses the effective tail", () => {
		const sources = Array.from({ length: 8 }, (_, index) =>
			entry(MAIN, `tail-${index + 1}`, `tail source ${index + 1}`, index === 0 ? null : `tail-${index}`),
		);
		context.reconcile(snapshot(MAIN, sources));
		completeEveryJob(context);

		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<{ count: number }, []>(
						"SELECT COUNT(*) AS count FROM branch_summary_spans WHERE level = 2 AND start_position = 0 AND end_position = 8 AND summary_id IS NOT NULL",
					)
					.get()?.count,
			).toBe(1);
		} finally {
			observer.close();
		}

		const projection = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 1, maxTokens: 100 },
		});
		expect(projection).toMatchObject({
			ready: true,
			selectedLevelCounts: { 0: 1, 1: 1 },
			uncoveredSourceIds: [],
			freshTailSourceIds: ["tail-7", "tail-8"],
		});
		expect(projection.historical.map(item => item.level)).toEqual([1, 0]);
		expect(projection.historical.flatMap(item => item.sourceIds)).toEqual(
			sources.slice(0, 6).map(source => source.entryId),
		);
	});

	test("completed children make projection ready while their condensation parent is pending", () => {
		const sources = Array.from({ length: 6 }, (_, index) => ({
			...entry(
				MAIN,
				`pending-${index + 1}`,
				`pending source ${index + 1}`,
				index === 0 ? null : `pending-${index}`,
				index >= 4 ? "pending-tail" : undefined,
			),
		}));
		context.reconcile(snapshot(MAIN, sources));
		for (let index = 0; index < 3; index++) {
			const [leaf] = context.claimSummaryJobs({
				...retryClaimPolicy(context),
				workerId: `leaf-worker-${index}`,
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
			});
			expect(leaf?.level).toBe(0);
			expect(context.completeSummaryJob(leaf!, { redactedText: `leaf ${index}`, tokenCount: 1 })).toMatchObject({
				accepted: true,
			});
		}

		const projection = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 0, maxTokens: 0 },
		});
		expect(projection).toMatchObject({
			ready: true,
			pendingJobs: 0,
			uncoveredSourceIds: [],
			freshTailSourceIds: ["pending-5", "pending-6"],
		});
		expect(projection.historical.flatMap(item => item.sourceIds)).toEqual(
			sources.slice(0, 4).map(source => source.entryId),
		);
		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<{ summary_id: string | null; frontier: number }, []>(
						"SELECT summary_id, frontier FROM branch_summary_spans WHERE level = 1 AND start_position = 0 AND end_position = 4",
					)
					.get(),
			).toEqual({ summary_id: null, frontier: 0 });
		} finally {
			observer.close();
		}
	});

	test("safePrefix copies only unchanged complete spans and respects final atomic-group extension", () => {
		const original = [
			entry(MAIN, "safe-1", "safe source one"),
			entry(MAIN, "safe-2", "safe source two", "safe-1"),
			entry(MAIN, "safe-3", "stale source three", "safe-2"),
			entry(MAIN, "safe-4", "stale source four", "safe-3"),
		];
		context.reconcile(snapshot(MAIN, original));
		const [prefixJob] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "prefix-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(context.completeSummaryJob(prefixJob!, { redactedText: "prefix", tokenCount: 1 })).toMatchObject({
			accepted: true,
		});
		const [staleJob] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "stale-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		const before = new Database(dbPath, { readonly: true, strict: true });
		let prefixSummaryId: string;
		let staleInputHash: string;
		try {
			prefixSummaryId = before
				.query<{ summary_id: string }, []>(
					"SELECT summary_id FROM branch_summary_spans WHERE revision = 1 AND level = 0 AND start_position = 0 AND end_position = 2",
				)
				.get()!.summary_id;
			staleInputHash = before
				.query<{ input_hash: string }, [string]>("SELECT input_hash FROM summary_jobs WHERE job_id = ?")
				.get(staleJob!.jobId)!.input_hash;
		} finally {
			before.close();
		}

		const diverged = [
			original[0]!,
			original[1]!,
			entry(MAIN, "safe-3", "replacement source three", "safe-2"),
			entry(MAIN, "safe-4", "replacement source four", "safe-3"),
		];
		expect(context.reconcile(snapshot(MAIN, diverged)).revision).toBe(2);
		expect(context.completeSummaryJob(staleJob!, { redactedText: "late", tokenCount: 1 })).toEqual({
			accepted: false,
			reason: "stale",
		});
		const divergedRows = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				divergedRows
					.query<{ summary_id: string | null }, []>(
						"SELECT summary_id FROM branch_summary_spans WHERE revision = 2 AND level = 0 AND start_position = 0 AND end_position = 2",
					)
					.get()?.summary_id,
			).toBe(prefixSummaryId);
			expect(
				divergedRows
					.query<{ count: number }, [string]>(
						"SELECT COUNT(*) AS count FROM branch_summary_spans WHERE revision = 2 AND input_hash = ?",
					)
					.get(staleInputHash)?.count,
			).toBe(0);
			expect(
				divergedRows
					.query<{ input_hash: string }, []>(
						"SELECT input_hash FROM branch_summary_spans WHERE revision = 2 AND level = 0 AND start_position = 2 AND end_position = 4",
					)
					.get()?.input_hash,
			).not.toBe(staleInputHash);
		} finally {
			divergedRows.close();
		}

		const append = { ...MAIN, branchId: "atomic-append" };
		const appendedPrefix = [
			entry(append, "append-1", "ungrouped prefix"),
			entry(append, "append-2", "atomic tail start", "append-1", "final-group"),
		];
		context.reconcile(snapshot(append, appendedPrefix));
		const appendBefore = new Database(dbPath, { readonly: true, strict: true });
		let splitLeafHash: string;
		try {
			splitLeafHash = appendBefore
				.query<{ input_hash: string }, []>(
					`SELECT s.input_hash FROM branch_summary_spans s JOIN branches b ON b.id = s.branch_row_id
					 WHERE b.branch_id = 'atomic-append' AND s.revision = 1 AND s.start_position = 0 AND s.end_position = 2`,
				)
				.get()!.input_hash;
		} finally {
			appendBefore.close();
		}
		context.reconcile(
			snapshot(append, [
				...appendedPrefix,
				entry(append, "append-3", "atomic tail continuation", "append-2", "final-group"),
			]),
		);
		const appendAfter = new Database(dbPath, { readonly: true, strict: true });
		try {
			const spans = appendAfter
				.query<{ start_position: number; end_position: number; input_hash: string }, []>(
					`SELECT s.start_position, s.end_position, s.input_hash FROM branch_summary_spans s
					 JOIN branches b ON b.id = s.branch_row_id AND b.revision = s.revision
					 WHERE b.branch_id = 'atomic-append' AND s.level = 0 ORDER BY s.start_position`,
				)
				.all();
			expect(spans.map(span => [span.start_position, span.end_position])).toEqual([
				[0, 1],
				[1, 3],
			]);
			expect(spans.some(span => span.input_hash === splitLeafHash)).toBe(false);
		} finally {
			appendAfter.close();
		}
	});

	test("completion advances only revisions that reference its input hash", () => {
		const branchA = { ...MAIN, branchId: "affected-a" };
		const branchB = { ...MAIN, branchId: "unaffected-b" };
		const sourcesA = Array.from({ length: 4 }, (_, index) =>
			entry(branchA, `a-${index + 1}`, `branch A source ${index + 1}`, index === 0 ? null : `a-${index}`),
		);
		const sourcesB = Array.from({ length: 4 }, (_, index) =>
			entry(branchB, `b-${index + 1}`, `branch B source ${index + 1}`, index === 0 ? null : `b-${index}`),
		);
		context.reconcile(snapshot(branchA, sourcesA));
		context.reconcile(snapshot(branchB, sourcesB));
		const readBranchB = () => {
			const observer = new Database(dbPath, { readonly: true, strict: true });
			try {
				return observer
					.query<
						{
							level: number;
							start_position: number;
							end_position: number;
							input_hash: string;
							summary_id: string | null;
							frontier: number;
						},
						[]
					>(
						`SELECT s.level, s.start_position, s.end_position, s.input_hash, s.summary_id, s.frontier
						 FROM branch_summary_spans s JOIN branches b ON b.id = s.branch_row_id AND b.revision = s.revision
						 WHERE b.branch_id = 'unaffected-b' ORDER BY s.level, s.start_position, s.end_position`,
					)
					.all();
			} finally {
				observer.close();
			}
		};
		const before = readBranchB();
		for (let index = 0; index < 2; index++) {
			const [job] = context.claimSummaryJobs({
				...retryClaimPolicy(context),
				workerId: `affected-worker-${index}`,
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: branchA,
				allowFallback: false,
			});
			expect(job?.level).toBe(0);
			expect(context.completeSummaryJob(job!, { redactedText: `a${index}`, tokenCount: 1 })).toMatchObject({
				accepted: true,
			});
		}
		expect(readBranchB()).toEqual(before);
	});

	test("reconcile repairs orphaned spans and corrupt hashes fail open", () => {
		const sources = [
			entry(MAIN, "repair-1", "repair source one"),
			entry(MAIN, "repair-2", "repair source two", "repair-1"),
		];
		const expectedInputHash = contentAddress([
			"lcm-summary-input-v1",
			MAIN.projectId,
			"0",
			...sources.flatMap(source => ["source", legacyFilelessSourceKey(source)]),
		]);
		context.reconcile(snapshot(MAIN, sources));
		const writer = new Database(dbPath, { strict: true });
		let inputHash: string;
		let jobId: string;
		try {
			writer.run("PRAGMA foreign_keys = ON");
			const span = writer
				.query<{ input_hash: string }, []>("SELECT input_hash FROM branch_summary_spans WHERE summary_id IS NULL")
				.get()!;
			inputHash = span.input_hash;
			jobId = writer
				.query<{ job_id: string }, [string]>("SELECT job_id FROM summary_jobs WHERE input_hash = ?")
				.get(inputHash)!.job_id;
			writer.run("DELETE FROM summary_jobs WHERE job_id = ?", [jobId]);
		} finally {
			writer.close();
		}

		expect(context.reconcile(snapshot(MAIN, sources))).toMatchObject({ changed: false, queuedJobs: 1 });
		const repaired = new Database(dbPath, { strict: true });
		try {
			expect(
				repaired
					.query<{ input_hash: string; status: string }, [string]>(
						"SELECT input_hash, status FROM summary_jobs WHERE job_id = ?",
					)
					.get(jobId),
			).toEqual({ input_hash: expectedInputHash, status: "pending" });
			expect(inputHash).toBe(expectedInputHash);
			repaired.run("UPDATE branch_summary_spans SET input_hash = 'corrupt-input-hash' WHERE input_hash = ?", [
				inputHash,
			]);
		} finally {
			repaired.close();
		}
		const withFresh = [...sources, entry(MAIN, "repair-3", "fresh tail", "repair-2")];
		context.reconcile(snapshot(MAIN, withFresh), { summarize: false });
		const projection = context.project({
			...MAIN,
			tokenBudget: 100,
			freshTail: { maxSources: 0, maxTokens: 0 },
		});
		expect(projection.ready).toBe(false);
		expect(projection.uncoveredSourceIds).toEqual(["repair-1", "repair-2"]);
		const corrupted = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				corrupted
					.query<{ input_hash: string; summary_id: string | null }, []>(
						"SELECT input_hash, summary_id FROM branch_summary_spans WHERE start_position = 0 AND end_position = 2",
					)
					.get(),
			).toEqual({ input_hash: "corrupt-input-hash", summary_id: null });
		} finally {
			corrupted.close();
		}
	});

	test("doctor reports every branch-summary-spans invariant independently", () => {
		const sources = Array.from({ length: 4 }, (_, index) =>
			entry(MAIN, `doctor-${index + 1}`, `doctor source ${index + 1}`, index === 0 ? null : `doctor-${index}`),
		);
		context.reconcile(snapshot(MAIN, sources));
		completeEveryJob(context);
		const check = () => context.doctor().checks.find(item => item.name === "branch-summary-spans");
		expect(check()).toEqual({ name: "branch-summary-spans", ok: true });

		const writer = new Database(dbPath, { strict: true });
		try {
			const branch = writer.query<{ id: number; revision: number }, []>("SELECT id, revision FROM branches").get()!;
			const root = writer
				.query<{ level: number; summary_id: string }, []>(
					"SELECT level, summary_id FROM branch_summary_spans WHERE start_position = 0 AND end_position = 4 AND frontier = 1",
				)
				.get()!;

			writer.run(
				`INSERT INTO branch_summary_spans
				 (branch_row_id, revision, level, start_position, end_position, input_hash, summary_id, frontier)
				 VALUES (?, ?, 99, 1, 2, 'doctor-overlap', NULL, 1)`,
				[branch.id, branch.revision],
			);
			expect(check()).toMatchObject({ name: "branch-summary-spans", ok: false });
			writer.run("DELETE FROM branch_summary_spans WHERE branch_row_id = ? AND revision = ? AND level = 99", [
				branch.id,
				branch.revision,
			]);
			expect(check()).toEqual({ name: "branch-summary-spans", ok: true });

			writer.run("UPDATE branch_summary_spans SET frontier = 0 WHERE branch_row_id = ? AND revision = ?", [
				branch.id,
				branch.revision,
			]);
			writer.run(
				`UPDATE branch_summary_spans SET frontier = 1
				 WHERE branch_row_id = ? AND revision = ? AND level = 0 AND start_position = 2 AND end_position = 4`,
				[branch.id, branch.revision],
			);
			expect(check()).toMatchObject({ name: "branch-summary-spans", ok: false });
			writer.run("UPDATE branch_summary_spans SET frontier = 0 WHERE branch_row_id = ? AND revision = ?", [
				branch.id,
				branch.revision,
			]);
			writer.run(
				`UPDATE branch_summary_spans SET frontier = 1
				 WHERE branch_row_id = ? AND revision = ? AND level = ? AND start_position = 0 AND end_position = 4`,
				[branch.id, branch.revision, root.level],
			);
			expect(check()).toEqual({ name: "branch-summary-spans", ok: true });

			writer.run(
				"DELETE FROM summary_lineage WHERE summary_id = ? AND ordinal = (SELECT MAX(ordinal) FROM summary_lineage WHERE summary_id = ?)",
				[root.summary_id, root.summary_id],
			);
			expect(check()).toMatchObject({ name: "branch-summary-spans", ok: false });
		} finally {
			writer.close();
		}
	});

	test("purge retains current span graphs, unresolved jobs, and provider attempts", () => {
		const sources = [
			entry(MAIN, "purge-1", "purge source one is long enough to compress"),
			entry(MAIN, "purge-2", "purge source two is long enough to compress", "purge-1"),
			entry(MAIN, "purge-3", "purge source three remains unresolved", "purge-2"),
		];
		context.reconcile(snapshot(MAIN, sources));
		const [job] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "purge-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		const attempt = {
			attemptId: "purge-attempt",
			startedAt: now,
			completedAt: now + 1,
			provider: "test-provider",
			model: "test-model",
			usage: {
				input: 11,
				output: 2,
				cacheRead: 3,
				cacheWrite: 4,
				totalTokens: 20,
				cost: { input: 0.11, output: 0.02, cacheRead: 0.03, cacheWrite: 0.04, total: 0.2 },
			},
		};
		const provenance = { promptHash: "purge-prompt", strategy: job!.strategy };
		expect(context.beginSummaryAttempt(job!, attempt, provenance)).toBe(true);
		const completion = context.completeSummaryJob(job!, {
			redactedText: "purged",
			tokenCount: 1,
			attempt,
		});
		expect(completion).toMatchObject({ accepted: true });
		const summaryId = completion.accepted ? completion.summaryId : "";
		const before = new Database(dbPath, { readonly: true, strict: true });
		let unresolvedJobId: string;
		try {
			unresolvedJobId = before
				.query<{ job_id: string }, [string]>(
					"SELECT job_id FROM summary_jobs WHERE job_id <> ? AND status = 'pending'",
				)
				.get(job!.jobId)!.job_id;
		} finally {
			before.close();
		}

		now += 101;
		context.purge();
		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<{ count: number }, [string]>("SELECT COUNT(*) AS count FROM summaries WHERE summary_id = ?")
					.get(summaryId)?.count,
			).toBe(1);
			expect(
				observer
					.query<{ count: number }, [string]>(
						"SELECT COUNT(*) AS count FROM summary_jobs WHERE job_id = ? AND status = 'pending'",
					)
					.get(unresolvedJobId)?.count,
			).toBe(1);
			expect(
				observer
					.query<{ count: number }, [string]>("SELECT COUNT(*) AS count FROM summary_jobs WHERE job_id = ?")
					.get(job!.jobId)?.count,
			).toBe(0);
			expect(
				observer
					.query<{ outcome: string; total_tokens: number }, [string]>(
						"SELECT outcome, total_tokens FROM summary_attempts WHERE attempt_id = ?",
					)
					.get(attempt.attemptId),
			).toEqual({ outcome: "completed", total_tokens: 20 });
		} finally {
			observer.close();
		}
	});

	test("settles a start-only cancellation and enriches late billed usage", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "cancel-attempt", "cancelled attempt source to summarize")]));
		const [job] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "cancel-attempt-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		const start = { attemptId: "cancel-attempt", startedAt: now, provider: "provider", model: "model" };
		expect(context.beginSummaryAttempt(job!, start, { promptHash: "cancel-prompt", strategy: job!.strategy })).toBe(
			true,
		);
		now += 50;
		expect(context.settleSummaryAttempt(job!, start, "aborted")).toBe("aborted");
		expect(
			context.settleSummaryAttempt(
				job!,
				{
					...start,
					completedAt: now + 1,
					usage: {
						input: 4,
						output: 2,
						cacheRead: 0,
						cacheWrite: 0,
						totalTokens: 6,
						cost: { input: 0.4, output: 0.2, cacheRead: 0, cacheWrite: 0, total: 0.6 },
					},
				},
				"aborted",
			),
		).toBe("aborted");
		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<
						{
							outcome: string;
							started_at: number;
							completed_at: number;
							total_tokens: number;
							cost_total: number;
						},
						[string]
					>(
						"SELECT outcome, started_at, completed_at, total_tokens, cost_total FROM summary_attempts WHERE attempt_id = ?",
					)
					.get(start.attemptId),
			).toEqual({
				outcome: "aborted",
				started_at: start.startedAt,
				completed_at: now + 1,
				total_tokens: 6,
				cost_total: 0.6,
			});
		} finally {
			observer.close();
		}
	});

	test("attempt-bearing failure settles stale placement without retry mutation", () => {
		context.reconcile(
			snapshot(MAIN, [entry(MAIN, "stale-attempt-1", "stale attempt source long enough to summarize")]),
		);
		const [job] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "stale-attempt-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		const start = { attemptId: "stale-attempt", startedAt: now, provider: "provider", model: "model" };
		const provenance = { promptHash: "stale-prompt", strategy: job!.strategy };
		expect(context.beginSummaryAttempt(job!, start, provenance)).toBe(true);
		const observer = new Database(dbPath, { readonly: true, strict: true });
		const before = observer
			.query<{ status: string; available_at: number; transport_retry_count: number }, [string]>(
				"SELECT status, available_at, transport_retry_count FROM summary_jobs WHERE job_id = ?",
			)
			.get(job!.jobId)!;
		observer.close();
		context.reconcile(snapshot(MAIN, [entry(MAIN, "stale-attempt-2", "replacement attempt source")]));
		const attempt = {
			...start,
			completedAt: now + 1,
			usage: {
				input: 17,
				output: 5,
				cacheRead: 2,
				cacheWrite: 1,
				totalTokens: 25,
				cost: { input: 1.7, output: 0.5, cacheRead: 0.2, cacheWrite: 0.1, total: 2.5 },
			},
		};
		expect(
			context.failSummaryJob(job!, "provider failure", 500, provenance, {
				attempt,
				outcome: "provider_error",
			}),
		).toBe(false);
		expect(context.settleSummaryAttempt(job!, attempt, "aborted")).toBeNull();

		const settled = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				settled
					.query<{ status: string; available_at: number; transport_retry_count: number }, [string]>(
						"SELECT status, available_at, transport_retry_count FROM summary_jobs WHERE job_id = ?",
					)
					.get(job!.jobId),
			).toEqual(before);
			expect(
				settled
					.query<
						{
							outcome: string;
							input_tokens: number;
							output_tokens: number;
							cache_read_tokens: number;
							cache_write_tokens: number;
							total_tokens: number;
							premium_requests: number | null;
							orchestration_input_tokens: number | null;
							cost_total: number;
						},
						[string]
					>(
						`SELECT outcome, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, total_tokens,
						 premium_requests, orchestration_input_tokens, cost_total
						 FROM summary_attempts WHERE attempt_id = ?`,
					)
					.get(start.attemptId),
			).toEqual({
				outcome: "stale",
				input_tokens: 17,
				output_tokens: 5,
				cache_read_tokens: 2,
				cache_write_tokens: 1,
				total_tokens: 25,
				premium_requests: null,
				orchestration_input_tokens: null,
				cost_total: 2.5,
			});
		} finally {
			settled.close();
		}
	});

	test("attempt-bearing failure settles a replaced lease without mutating its successor", () => {
		context.reconcile(
			snapshot(MAIN, [entry(MAIN, "lease-attempt", "lease attempt source long enough to summarize")]),
		);
		const [original] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "original-worker",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		const start = { attemptId: "lease-lost-attempt", startedAt: now, provider: "provider", model: "model" };
		const provenance = { promptHash: "lease-prompt", strategy: original!.strategy };
		expect(context.beginSummaryAttempt(original!, start, provenance)).toBe(true);
		now += 101;
		const [successor] = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "successor-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		const beforeDb = new Database(dbPath, { readonly: true, strict: true });
		const before = beforeDb
			.query<{ status: string; available_at: number; transport_retry_count: number; lease_token: string }, [string]>(
				"SELECT status, available_at, transport_retry_count, lease_token FROM summary_jobs WHERE job_id = ?",
			)
			.get(original!.jobId)!;
		beforeDb.close();
		const attempt = {
			...start,
			completedAt: now,
			usage: {
				input: 19,
				output: 7,
				cacheRead: 3,
				cacheWrite: 2,
				totalTokens: 31,
				orchestration: { input: 4, cacheRead: 5, output: 6 },
				premiumRequests: 0.5,
				reasoningTokens: 8,
				cttl: { ephemeral5m: 9, ephemeral1h: 10 },
				server: { webSearch: 11, webFetch: 12 },
				cost: { input: 1.9, output: 0.7, cacheRead: 0.3, cacheWrite: 0.2, total: 3.1 },
			},
		};
		expect(
			context.failSummaryJob(original!, "late transport failure", 500, provenance, {
				attempt,
				outcome: "transport_error",
			}),
		).toBe(false);

		const settled = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				settled
					.query<
						{ status: string; available_at: number; transport_retry_count: number; lease_token: string },
						[string]
					>("SELECT status, available_at, transport_retry_count, lease_token FROM summary_jobs WHERE job_id = ?")
					.get(original!.jobId),
			).toEqual(before);
			expect(before.lease_token).toBe(successor!.leaseToken);
			expect(
				settled
					.query<
						{
							outcome: string;
							total_tokens: number;
							orchestration_input_tokens: number;
							reasoning_tokens: number;
							premium_requests: number;
							server_web_fetch_requests: number;
							cost_total: number;
						},
						[string]
					>(
						`SELECT outcome, total_tokens, orchestration_input_tokens, reasoning_tokens,
						 premium_requests, server_web_fetch_requests, cost_total
						 FROM summary_attempts WHERE attempt_id = ?`,
					)
					.get(start.attemptId),
			).toEqual({
				outcome: "lease_lost",
				total_tokens: 31,
				orchestration_input_tokens: 4,
				reasoning_tokens: 8,
				premium_requests: 0.5,
				server_web_fetch_requests: 12,
				cost_total: 3.1,
			});
		} finally {
			settled.close();
		}
	});

	test("migrates a live v9 connection into blocked schema-10 retry authority", () => {
		const migrationPath = path.join(tempDir, "retry-v9.db");
		const legacy = new Database(migrationPath, { strict: true });
		let migrator: Database | undefined;
		try {
			initializeLcmSchema(legacy, 1_000);
			if (LCM_SCHEMA_VERSION >= 10) {
				legacy.run("DROP TRIGGER summary_jobs_authorized_insert");
				legacy.run("DROP TRIGGER summary_jobs_authorized_update");
				legacy.run("DROP TRIGGER summary_jobs_authorization_cleanup");
				legacy.run("DROP TABLE summary_retry_policies");
				legacy.run("ALTER TABLE summary_jobs DROP COLUMN lease_mutation_nonce");
				legacy.run("ALTER TABLE summary_jobs DROP COLUMN lease_policy_token");
				legacy.run("ALTER TABLE summary_jobs DROP COLUMN retry_epoch");
				legacy.run("PRAGMA user_version = 9");
			}
			legacy.run(
				"INSERT INTO branches(project_id, session_id, branch_id, reconciled_at) VALUES ('project', 'session', 'main', 1)",
			);
			const branchId = Number(legacy.query<{ id: number }, []>("SELECT id FROM branches").get()!.id);
			legacy.run(
				`INSERT INTO summary_jobs(
					job_id, project_id, input_hash, level, origin_branch_row_id, origin_revision, status,
					worker_id, lease_token, lease_expires_at, attempt_count, available_at, created_at, updated_at,
					lease_input_tokens, lease_output_budget, transport_retry_count
				) VALUES ('job-v9', 'project', 'input-v9', 0, ?, 1, 'leased',
					'legacy-worker', 'legacy-lease', 999999, 4, 1, 1, 1, 10, 5, 4)`,
				[branchId],
			);

			migrator = new Database(migrationPath, { strict: true });
			initializeLcmSchema(migrator, 1_000);
			expect(migrator.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(10);
			expect(
				migrator
					.query<{ retry_key: string | null; epoch: number; claim_token: string | null }, [string]>(
						"SELECT retry_key, epoch, claim_token FROM summary_retry_policies WHERE project_id = ?",
					)
					.get("project"),
			).toEqual({ retry_key: null, epoch: 0, claim_token: null });
			expect(
				migrator
					.query<
						{
							status: string;
							worker_id: string | null;
							lease_token: string | null;
							retry_epoch: number;
							lease_policy_token: string | null;
							lease_mutation_nonce: string | null;
						},
						[string]
					>(
						"SELECT status, worker_id, lease_token, retry_epoch, lease_policy_token, lease_mutation_nonce FROM summary_jobs WHERE job_id = ?",
					)
					.get("job-v9"),
			).toEqual({
				status: "pending",
				worker_id: null,
				lease_token: null,
				retry_epoch: 0,
				lease_policy_token: null,
				lease_mutation_nonce: null,
			});
			expect(() =>
				legacy.run(
					"UPDATE summary_jobs SET status = 'leased', worker_id = 'legacy', lease_token = 'legacy-new', lease_expires_at = 1000000 WHERE job_id = 'job-v9'",
				),
			).toThrow("unauthorized lease");
		} finally {
			migrator?.close(false);
			legacy.close(false);
		}
	});

	test("initializes migrated selector history from the caller-resolved concrete model", async () => {
		const migrationPath = path.join(tempDir, "retry-v9-model-identity.db");
		const legacyAvailableAt = now + 10_000;
		const legacy = new Database(migrationPath, { strict: true });
		try {
			initializeLcmSchema(legacy, 1_000);
			removeSchema10RetryAuthority(legacy);
			legacy.run("PRAGMA user_version = 9");
			for (const projectId of ["same-model", "changed-model"]) {
				legacy.run(
					"INSERT INTO branches(project_id, session_id, branch_id, reconciled_at) VALUES (?, ?, 'main', 1)",
					[projectId, `${projectId}-session`],
				);
				const branchId = Number(
					legacy.query<{ id: number }, [string]>("SELECT id FROM branches WHERE project_id = ?").get(projectId)!
						.id,
				);
				legacy.run(
					`INSERT INTO summary_jobs(
						job_id, project_id, input_hash, level, origin_branch_row_id, origin_revision, status,
						attempt_count, available_at, last_error, created_at, updated_at, transport_retry_count,
						last_model_selector, last_resolved_model
					) VALUES (?, ?, ?, 0, ?, 1, 'failed', 4, ?, 'legacy transport error', 1, 1, 4, '@smol', 'provider/original')`,
					[`${projectId}-job`, projectId, `${projectId}-input`, branchId, legacyAvailableAt],
				);
			}
		} finally {
			legacy.close(false);
		}

		const migrated = await openLcmContext({ dbPath: migrationPath, now: () => now, regexEngine: TEST_REGEX_ENGINE });
		try {
			expect(migrated.configureSummaryRetryPolicy("same-model", "provider/original")).toMatchObject({
				kind: "ready",
				retryKey: "provider/original",
				retryEpoch: 1,
			});
			expect(migrated.configureSummaryRetryPolicy("changed-model", "provider/replacement")).toMatchObject({
				kind: "ready",
				retryKey: "provider/replacement",
				retryEpoch: 1,
			});
			const observer = new Database(migrationPath, { readonly: true, strict: true });
			try {
				const readState = (jobId: string) =>
					observer
						.query<
							{
								status: string;
								transport_retry_count: number;
								available_at: number;
								last_error: string | null;
								retry_epoch: number;
							},
							[string]
						>(
							"SELECT status, transport_retry_count, available_at, last_error, retry_epoch FROM summary_jobs WHERE job_id = ?",
						)
						.get(jobId);
				expect(readState("same-model-job")).toEqual({
					status: "failed",
					transport_retry_count: 4,
					available_at: legacyAvailableAt,
					last_error: "legacy transport error",
					retry_epoch: 1,
				});
				expect(readState("changed-model-job")).toEqual({
					status: "pending",
					transport_retry_count: 0,
					available_at: now,
					last_error: null,
					retry_epoch: 1,
				});
			} finally {
				observer.close();
			}
		} finally {
			migrated.close();
		}
	});

	test("reads summary job availability from one snapshot during peer completion", async () => {
		const sources = [
			entry(MAIN, "snapshot-1", "first availability source"),
			entry(MAIN, "snapshot-2", "second availability source", "snapshot-1"),
			entry(MAIN, "snapshot-3", "mandatory fresh source", "snapshot-2"),
		];
		context.reconcile(snapshot(MAIN, sources));
		const request = { ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/model");
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");
		const [leased] = context.claimSummaryJobs({
			...policy,
			maxTransportRetries: 5,
			workerId: "peer-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(leased).toBeDefined();

		let armed = false;
		let completed = false;
		const observer = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => {
				if (armed && !completed) {
					completed = context.completeSummaryJob(leased!, { redactedText: "ok", tokenCount: 1 }).accepted;
				}
				return now;
			},
			regexEngine: TEST_REGEX_ENGINE,
		});
		try {
			armed = true;
			expect(observer.summaryJobAvailability(request, policy, 5)).toMatchObject({
				runnable: 0,
				leased: 1,
				backoff: 0,
				exhausted: 0,
				missing: 0,
				policyMismatch: 0,
			});
			expect(completed).toBe(true);
			expect(observer.project(request)).toMatchObject({ ready: true, pendingJobs: 0, uncoveredSourceIds: [] });
		} finally {
			observer.close();
		}
	});

	test("same-key and conflict retry-policy checks remain read-only under a peer writer", async () => {
		const policyPath = path.join(tempDir, "policy-fast-path.db");
		const policyContext = await openLcmContext({
			dbPath: policyPath,
			busyTimeoutMs: 0,
			now: () => now,
			regexEngine: TEST_REGEX_ENGINE,
		});
		try {
			const policy = policyContext.configureSummaryRetryPolicy(MAIN.projectId, "provider/current");
			if (policy.kind !== "ready") throw new Error("retry policy did not initialize");
			const writer = new Database(policyPath, { strict: true });
			try {
				writer.run("BEGIN IMMEDIATE");
				expect(policyContext.configureSummaryRetryPolicy(MAIN.projectId, policy.retryKey)).toEqual(policy);
				expect(policyContext.configureSummaryRetryPolicy(MAIN.projectId, "provider/other")).toEqual({
					kind: "conflict",
					retryKey: policy.retryKey,
					retryEpoch: policy.retryEpoch,
				});
			} finally {
				if (writer.inTransaction) writer.run("ROLLBACK");
				writer.close();
			}
		} finally {
			policyContext.close();
		}
	});

	test("classifies mixed summary job availability in one constant executed-statement snapshot", () => {
		const createFixture = (branchId: string, repetitions: number): AvailabilityFixture => {
			const scope = { ...MAIN, branchId };
			const spanCount = repetitions * 6;
			const parentSpanCount = repetitions;
			const leafSpanCount = spanCount + parentSpanCount * 2;
			const sources = Array.from({ length: leafSpanCount * 2 + 1 }, (_, index) =>
				entry(
					scope,
					`${branchId}-${index + 1}`,
					`${branchId} availability source ${index + 1}`,
					index === 0 ? null : `${branchId}-${index}`,
				),
			);
			const request: ProjectionRequest = {
				...scope,
				tokenBudget: 100,
				freshTail: { maxSources: 1, maxTokens: 100 },
			};
			context.reconcile(snapshot(scope, sources), { summarize: request });
			return { request, spanCount, parentSpanCount };
		};

		const smallFixture = createFixture("availability-small", 1);
		const largeFixture = createFixture("availability-large", 10);
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/model");
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");

		const fixtureDb = new Database(dbPath, { strict: true });
		try {
			const claimToken = fixtureDb
				.query<{ claim_token: string | null }, [string]>(
					"SELECT claim_token FROM summary_retry_policies WHERE project_id = ?",
				)
				.get(MAIN.projectId)?.claim_token;
			if (!claimToken) throw new Error("retry policy claim token is missing");

			const seedFixture = (fixture: AvailabilityFixture) => {
				const jobs = fixtureDb
					.query<
						{
							job_id: string;
							input_hash: string;
							branch_row_id: number;
							revision: number;
							start_position: number;
							end_position: number;
						},
						[string, string, string]
					>(
						`SELECT j.job_id, j.input_hash, s.branch_row_id, s.revision, s.start_position, s.end_position
						 FROM branches b
						 JOIN branch_summary_spans s ON s.branch_row_id = b.id AND s.revision = b.revision
						 JOIN summary_jobs j ON j.project_id = b.project_id AND j.input_hash = s.input_hash
						 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ?
						   AND s.level = 0 AND s.frontier = 1 AND s.summary_id IS NULL
						 ORDER BY s.start_position`,
					)
					.all(fixture.request.projectId, fixture.request.sessionId, fixture.request.branchId);
				expect(jobs).toHaveLength(fixture.spanCount + fixture.parentSpanCount * 2);

				const classificationJobs: typeof jobs = [];
				for (let parentIndex = 0; parentIndex < fixture.parentSpanCount; parentIndex++) {
					const groupStart = parentIndex * 8;
					const children = jobs.slice(groupStart, groupStart + 2);
					const first = children[0];
					const last = children[1];
					if (!first || !last) throw new Error("parent fixture children are missing");
					for (const child of children) {
						const summaryId = `fixture-summary-${child.job_id}`;
						fixtureDb.run(
							`INSERT INTO summaries
							 (summary_id, stable_handle, project_id, input_hash, level, redacted_text, token_count, created_at)
							 VALUES (?, ?, ?, ?, 0, 'covered child', 1, ?)`,
							[summaryId, `fixture-handle-${child.job_id}`, MAIN.projectId, child.input_hash, now],
						);
						fixtureDb.run(
							"UPDATE summary_jobs SET status = 'completed', result_summary_id = ?, updated_at = ? WHERE job_id = ?",
							[summaryId, now, child.job_id],
						);
						fixtureDb.run(
							`UPDATE branch_summary_spans SET summary_id = ?, frontier = 0
							 WHERE branch_row_id = ? AND revision = ? AND level = 0
							   AND start_position = ? AND end_position = ?`,
							[summaryId, child.branch_row_id, child.revision, child.start_position, child.end_position],
						);
					}
					const parentInputHash = `fixture-parent-${fixture.request.branchId}-${parentIndex}`;
					fixtureDb.run(
						`INSERT INTO summary_jobs
						 (job_id, project_id, input_hash, level, origin_branch_row_id, origin_revision,
						  status, available_at, created_at, updated_at, retry_epoch)
						 VALUES (?, ?, ?, 1, ?, ?, 'pending', ?, ?, ?, ?)`,
						[
							`fixture-parent-job-${fixture.request.branchId}-${parentIndex}`,
							MAIN.projectId,
							parentInputHash,
							first.branch_row_id,
							first.revision,
							now,
							now,
							now,
							policy.retryEpoch,
						],
					);
					fixtureDb.run(
						`INSERT INTO branch_summary_spans
						 (branch_row_id, revision, level, start_position, end_position, input_hash, summary_id, frontier)
						 VALUES (?, ?, 1, ?, ?, ?, NULL, 1)`,
						[first.branch_row_id, first.revision, first.start_position, last.end_position, parentInputHash],
					);
					classificationJobs.push(...jobs.slice(groupStart + 2, groupStart + 8));
				}
				expect(classificationJobs).toHaveLength(fixture.spanCount);
				for (const [index, job] of classificationJobs.entries()) {
					switch (index % 6) {
						case 0:
							break;
						case 1:
							fixtureDb.run(
								`UPDATE summary_jobs SET status = 'leased', worker_id = ?, lease_token = ?,
								 lease_expires_at = ?, lease_input_tokens = 1, lease_output_budget = 100,
								 lease_policy_token = ?, lease_mutation_nonce = ?, updated_at = ? WHERE job_id = ?`,
								[
									`worker-${job.job_id}`,
									`lease-${job.job_id}`,
									now + 1_000,
									claimToken,
									`nonce-${job.job_id}`,
									now,
									job.job_id,
								],
							);
							break;
						case 2:
							fixtureDb.run(
								"UPDATE summary_jobs SET status = 'failed', transport_retry_count = 1, available_at = ?, last_error = 'transport', updated_at = ? WHERE job_id = ?",
								[now + 500, now, job.job_id],
							);
							break;
						case 3:
							fixtureDb.run(
								"UPDATE summary_jobs SET status = 'failed', transport_retry_count = 5, last_error = 'transport', updated_at = ? WHERE job_id = ?",
								[now, job.job_id],
							);
							break;
						case 4:
							fixtureDb.run("DELETE FROM summary_jobs WHERE job_id = ?", [job.job_id]);
							break;
						case 5:
							fixtureDb.run("UPDATE summary_jobs SET retry_epoch = ? WHERE job_id = ?", [
								policy.retryEpoch + 1,
								job.job_id,
							]);
							break;
					}
				}
				expect(
					fixtureDb
						.query<{ count: number }, [string, string, string]>(
							`SELECT COUNT(*) AS count FROM branches b
							 JOIN branch_summary_spans s ON s.branch_row_id = b.id AND s.revision = b.revision
							 WHERE b.project_id = ? AND b.session_id = ? AND b.branch_id = ?
							   AND s.level = 1 AND s.summary_id IS NULL AND s.frontier = 1`,
						)
						.get(fixture.request.projectId, fixture.request.sessionId, fixture.request.branchId)?.count,
				).toBe(fixture.parentSpanCount);
			};
			seedFixture(smallFixture);
			seedFixture(largeFixture);
		} finally {
			fixtureDb.close();
		}

		const realQuery = Database.prototype.query;
		let statementCount = 0;
		const queryImplementation = function (this: Database, sql: string): unknown {
			const statement = Reflect.apply(realQuery, this, [sql]);
			return new Proxy(statement, {
				get(target, property) {
					const member = Reflect.get(target, property, target);
					if ((property === "all" || property === "get") && typeof member === "function") {
						return (...bindings: unknown[]) => {
							statementCount++;
							return Reflect.apply(member, target, bindings);
						};
					}
					return typeof member === "function" ? member.bind(target) : member;
				},
			});
		} as unknown as typeof Database.prototype.query;
		const querySpy = spyOn(Database.prototype, "query").mockImplementation(queryImplementation);
		try {
			const measure = (request: ProjectionRequest) => {
				statementCount = 0;
				const availability = context.summaryJobAvailability(request, policy, 5);
				return { availability, statements: statementCount };
			};
			const small = measure(smallFixture.request);
			const large = measure(largeFixture.request);

			expect(small.availability).toEqual({
				runnable: 1,
				leased: 1,
				backoff: 1,
				exhausted: 1,
				missing: 1,
				policyMismatch: 1,
				nextAvailableAt: now + 500,
				nextLeaseExpiryAt: now + 1_000,
			});
			expect(large.availability).toEqual({
				runnable: 10,
				leased: 10,
				backoff: 10,
				exhausted: 10,
				missing: 10,
				policyMismatch: 10,
				nextAvailableAt: now + 500,
				nextLeaseExpiryAt: now + 1_000,
			});
			expect(small.statements).toBeGreaterThan(0);
			expect(large.statements).toBe(small.statements);
		} finally {
			querySpy.mockRestore();
		}
	});

	test("counts dispatched non-compressing responses with transport failures against the cap", () => {
		const sources = [
			entry(MAIN, "escalate-1", "x".repeat(400)),
			entry(MAIN, "escalate-2", "x".repeat(400), "escalate-1"),
			entry(MAIN, "escalate-3", "fresh", "escalate-2"),
		];
		const request = { ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		context.reconcile(snapshot(MAIN, sources), { summarize: request });
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/model");
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");
		const claim = (workerId: string) =>
			context.claimSummaryJobs({
				...policy,
				maxTransportRetries: 5,
				workerId,
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
			})[0];
		const jobAvailableAt = (jobId: string) => {
			const observer = new Database(dbPath, { readonly: true, strict: true });
			try {
				return observer
					.query<{ available_at: number }, [string]>("SELECT available_at FROM summary_jobs WHERE job_id = ?")
					.get(jobId)!.available_at;
			} finally {
				observer.close();
			}
		};

		const undispatched = claim("provider-preparation");
		expect(undispatched?.transportRetryCount).toBe(0);
		expect(context.failSummaryJob(undispatched!, "provider preparation", 1, undefined, undefined, false)).toBe(true);
		now += 1;

		for (let failure = 0; failure < 4; failure++) {
			const job = claim(`transport-${failure}`);
			expect(job?.transportRetryCount).toBe(failure);
			expect(context.failSummaryJob(job!, "transport", 1)).toBe(true);
			now += failure === 3 ? 10 : 1;
		}
		const normal = claim("non-compressing");
		expect(normal?.transportRetryCount).toBe(4);
		const retryAvailableAt = jobAvailableAt(normal!.jobId);
		expect(retryAvailableAt).toBeLessThan(now);
		const start = { attemptId: "non-compressing-attempt", startedAt: now, provider: "provider", model: "model" };
		const provenance = { promptHash: "non-compressing-prompt", strategy: normal!.strategy };
		expect(context.beginSummaryAttempt(normal!, start, provenance)).toBe(true);
		expect(
			context.completeSummaryJob(normal!, {
				redactedText: "x".repeat(normal!.inputTokenCount * 4),
				provenance,
				attempt: { ...start, completedAt: now + 1 },
			}),
		).toEqual({ accepted: false, reason: "escalated", stage: "aggressive" });
		expect(jobAvailableAt(normal!.jobId)).toBe(retryAvailableAt);
		expect(context.summaryJobAvailability(request, policy, 5)).toMatchObject({ exhausted: 1, runnable: 0 });

		const afterCap = context.claimSummaryJobs({
			...policy,
			maxTransportRetries: 5,
			workerId: "over-cap",
			leaseMs: 1_000,
			limit: 10,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(afterCap.some(job => job.jobId === normal!.jobId)).toBe(false);
		for (const job of afterCap) expect(context.releaseSummaryJob(job)).toBe(true);
	});

	test("keeps supplied completion timing for provider failures without usage", () => {
		const sources = [
			entry(MAIN, "no-usage-1", "provider failure source long enough to summarize"),
			entry(MAIN, "no-usage-2", "fresh", "no-usage-1"),
		];
		const request = { ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		context.reconcile(snapshot(MAIN, sources), { summarize: request });
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/model");
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");
		const [job] = context.claimSummaryJobs({
			...policy,
			maxTransportRetries: 5,
			workerId: "no-usage-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		const start = { attemptId: "no-usage-attempt", startedAt: now, provider: "provider", model: "model" };
		const provenance = { promptHash: "no-usage-prompt", strategy: job!.strategy };
		expect(context.beginSummaryAttempt(job!, start, provenance)).toBe(true);
		expect(
			context.failSummaryJob(job!, "provider failure", 1, provenance, {
				attempt: { ...start, completedAt: now + 37 },
				outcome: "provider_error",
			}),
		).toBe(true);

		const observer = new Database(dbPath, { readonly: true, strict: true });
		try {
			expect(
				observer
					.query<{ completed_at: number; outcome: string; total_tokens: number | null }, [string]>(
						"SELECT completed_at, outcome, total_tokens FROM summary_attempts WHERE attempt_id = ?",
					)
					.get(start.attemptId),
			).toEqual({ completed_at: now + 37, outcome: "provider_error", total_tokens: null });
		} finally {
			observer.close();
		}
	});

	test("preserves transport failures and backoff across obsolete reactivation", () => {
		const original = [
			entry(MAIN, "reactivate-1", "first original source"),
			entry(MAIN, "reactivate-2", "second original source", "reactivate-1"),
			entry(MAIN, "reactivate-3", "original fresh source", "reactivate-2"),
		];
		const replacement = [
			entry(MAIN, "replacement-1", "first replacement source"),
			entry(MAIN, "replacement-2", "second replacement source", "replacement-1"),
			entry(MAIN, "replacement-3", "replacement fresh source", "replacement-2"),
		];
		context.reconcile(snapshot(MAIN, original));
		const request = { ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/model");
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");
		const claim = (workerId: string, limit = 1) =>
			context.claimSummaryJobs({
				...policy,
				maxTransportRetries: 5,
				workerId,
				leaseMs: 1_000,
				limit,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
			});

		let originalJobId = "";
		for (let failure = 0; failure < 4; failure++) {
			const [job] = claim(`transport-${failure}`);
			originalJobId ||= job!.jobId;
			expect(job?.jobId).toBe(originalJobId);
			expect(context.failSummaryJob(job!, "transport", failure === 3 ? 100 : 1)).toBe(true);
			if (failure < 3) now += 1;
		}
		const readRetryState = () => {
			const observer = new Database(dbPath, { readonly: true, strict: true });
			try {
				return observer
					.query<{ status: string; transport_retry_count: number; available_at: number }, [string]>(
						"SELECT status, transport_retry_count, available_at FROM summary_jobs WHERE job_id = ?",
					)
					.get(originalJobId)!;
			} finally {
				observer.close();
			}
		};
		const failedState = readRetryState();
		expect(failedState).toMatchObject({ status: "failed", transport_retry_count: 4 });

		context.reconcile(snapshot(MAIN, replacement));
		now += 200;
		for (const job of claim("replacement-worker", 10)) expect(context.releaseSummaryJob(job)).toBe(true);
		expect(readRetryState()).toEqual({
			status: "obsolete",
			transport_retry_count: 4,
			available_at: failedState.available_at,
		});

		context.reconcile(snapshot(MAIN, original));
		expect(readRetryState()).toEqual({
			status: "pending",
			transport_retry_count: 4,
			available_at: failedState.available_at,
		});
		const reactivated = claim("reactivated", 10).find(job => job.jobId === originalJobId);
		expect(reactivated?.transportRetryCount).toBe(4);
		expect(context.failSummaryJob(reactivated!, "transport", 1)).toBe(true);
		expect(context.summaryJobAvailability(request, policy, 5)).toMatchObject({ exhausted: 1, runnable: 0 });
		const afterCap = claim("over-cap", 10);
		expect(afterCap.some(job => job.jobId === originalJobId)).toBe(false);
		for (const job of afterCap) expect(context.releaseSummaryJob(job)).toBe(true);
	});

	test("uses durable policy epochs, lease nonces, due backoff, and a five-call cap", () => {
		const sources = [
			entry(MAIN, "retry-1", "first retry source"),
			entry(MAIN, "retry-2", "second retry source", "retry-1"),
			entry(MAIN, "retry-3", "mandatory fresh source", "retry-2"),
		];
		context.reconcile(snapshot(MAIN, sources));
		const request = { ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/model");
		expect(policy).toMatchObject({ kind: "ready", retryKey: "provider/model", retryEpoch: 1 });
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");

		expect(context.summaryJobAvailability(request, policy, 5)).toMatchObject({
			runnable: 1,
			leased: 0,
			backoff: 0,
			exhausted: 0,
			missing: 0,
			policyMismatch: 0,
		});
		let failedJobId: string | undefined;
		for (let failedCalls = 0; failedCalls < 5; failedCalls++) {
			const [job] = context.claimSummaryJobs({
				...retryClaimPolicy(context),
				workerId: `worker-${failedCalls}`,
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
				retryKey: policy.retryKey,
				retryEpoch: policy.retryEpoch,
				maxTransportRetries: 5,
			});
			failedJobId ??= job!.jobId;
			expect(job!.jobId).toBe(failedJobId);
			expect(job).toBeDefined();
			const rotatedNonce = context.extendSummaryJob(job!, 1_000);
			expect(rotatedNonce).not.toBeNull();
			expect(rotatedNonce).not.toBe(job!.leaseMutationNonce);
			expect(context.releaseSummaryJob(job!)).toBe(false);
			job!.leaseMutationNonce = rotatedNonce!;
			expect(context.failSummaryJob(job!, "transport", 100)).toBe(true);
			expect(context.summaryJobAvailability(request, policy, 5)).toMatchObject({
				runnable: 0,
				backoff: failedCalls === 4 ? 0 : 1,
				exhausted: failedCalls === 4 ? 1 : 0,
			});
			now += 100;
		}
		const afterCap = context.claimSummaryJobs({
			...retryClaimPolicy(context),
			workerId: "sixth-worker",
			leaseMs: 1_000,
			limit: 2,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
			retryKey: policy.retryKey,
			retryEpoch: policy.retryEpoch,
			maxTransportRetries: 5,
		});
		expect(afterCap.some(job => job.jobId === failedJobId)).toBe(false);
		for (const job of afterCap) expect(context.releaseSummaryJob(job)).toBe(true);
	});

	test("retry all authorizes the exact policy and wakes every relevant job with one update", () => {
		const sources = Array.from({ length: 7 }, (_, index) =>
			entry(
				MAIN,
				`retry-auth-${index + 1}`,
				`retry authorization source ${index + 1}`,
				index === 0 ? null : `retry-auth-${index}`,
			),
		);
		const request = { ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		context.reconcile(snapshot(MAIN, sources), { summarize: request });
		const policy = context.configureSummaryRetryPolicy(MAIN.projectId, "provider/current");
		if (policy.kind !== "ready") throw new Error("retry policy did not initialize");
		const jobs = context.claimSummaryJobs({
			...policy,
			maxTransportRetries: 5,
			workerId: "retry-auth-worker",
			leaseMs: 1_000,
			limit: 3,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(jobs).toHaveLength(3);
		for (const job of jobs) expect(context.failSummaryJob(job, "transport", 500)).toBe(true);
		const readAvailableAt = () => {
			const observer = new Database(dbPath, { readonly: true, strict: true });
			try {
				const statement = observer.query<{ available_at: number }, [string]>(
					"SELECT available_at FROM summary_jobs WHERE job_id = ?",
				);
				return jobs.map(job => statement.get(job.jobId)!.available_at);
			} finally {
				observer.close();
			}
		};
		const providerAvailableAt = readAvailableAt();
		expect(providerAvailableAt).toEqual(jobs.map(() => now + 500));

		const realRun = Database.prototype.run;
		let wakeUpdates = 0;
		const runSpy = spyOn(Database.prototype, "run").mockImplementation(function (
			this: Database,
			...args: Parameters<typeof realRun>
		) {
			if (String(args[0]).includes("UPDATE summary_jobs SET available_at =")) wakeUpdates++;
			return realRun.apply(this, args);
		});
		try {
			expect(
				context.retrySummaryJobs(request, { retryKey: "provider/forged", retryEpoch: policy.retryEpoch }, 5, "all"),
			).toMatchObject({ runnable: 0, backoff: 0, policyMismatch: 3 });
			expect(readAvailableAt()).toEqual(providerAvailableAt);
			expect(wakeUpdates).toBe(0);
			expect(context.retrySummaryJobs(request, policy, 5, "all")).toMatchObject({
				runnable: 3,
				backoff: 0,
				policyMismatch: 0,
			});
			expect(readAvailableAt()).toEqual(jobs.map(() => now));
			expect(wakeUpdates).toBe(1);
		} finally {
			runSpy.mockRestore();
		}
	});

	test("CAS rotation fences stale completion, manual retry preserves the cap, and rebuild advances the epoch", async () => {
		const retryPath = path.join(tempDir, "retry-cas.db");
		const retryContext = await openLcmContext({
			dbPath: retryPath,
			leafChunk: { maxSources: 24, maxTokens: 10_000 },
			tombstoneRetentionMs: 100,
			now: () => now,
			regexEngine: TEST_REGEX_ENGINE,
		});
		const sources = [
			entry(MAIN, "cas-1", "first CAS source"),
			entry(MAIN, "cas-2", "second CAS source", "cas-1"),
			entry(MAIN, "cas-3", "fresh CAS source", "cas-2"),
		];
		const request = { ...MAIN, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } };
		try {
			retryContext.reconcile(snapshot(MAIN, sources));
			const policyA = retryContext.configureSummaryRetryPolicy(MAIN.projectId, "provider/a");
			if (policyA.kind !== "ready") throw new Error("policy A did not initialize");
			const [oldLease] = retryContext.claimSummaryJobs({
				...policyA,
				maxTransportRetries: 5,
				workerId: "old-worker",
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
			});
			expect(oldLease).toBeDefined();
			const start = { attemptId: "cas-attempt", startedAt: now, provider: "provider", model: "a" };
			expect(
				retryContext.beginSummaryAttempt(oldLease!, start, {
					promptHash: "cas-prompt",
					resolvedModel: "provider/a",
					strategy: oldLease!.strategy,
				}),
			).toBe(true);
			const direct = new Database(retryPath, { strict: true });
			try {
				expect(() =>
					direct.run("UPDATE summary_jobs SET lease_policy_token = 'forged' WHERE job_id = ?", [oldLease!.jobId]),
				).toThrow("unauthorized lease mutation");
			} finally {
				direct.close(false);
			}

			const conflict = retryContext.configureSummaryRetryPolicy(MAIN.projectId, "provider/b");
			expect(conflict).toEqual({ kind: "conflict", retryKey: policyA.retryKey, retryEpoch: policyA.retryEpoch });
			const policyB = retryContext.configureSummaryRetryPolicy(MAIN.projectId, "provider/b", {
				expected: policyA,
			});
			expect(policyB).toMatchObject({ kind: "ready", retryKey: "provider/b", retryEpoch: 2 });
			if (policyB.kind !== "ready") throw new Error("policy B did not rotate");
			const staleAttempt = {
				...start,
				completedAt: now,
				usage: {
					input: 1,
					output: 1,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: 2,
					cost: { input: 0.1, output: 0.2, cacheRead: 0, cacheWrite: 0, total: 0.3 },
				},
			};
			expect(retryContext.completeSummaryJob(oldLease!, { redactedText: "stale", attempt: staleAttempt })).toEqual({
				accepted: false,
				reason: "lease_lost",
			});
			const staleObserver = new Database(retryPath, { readonly: true, strict: true });
			try {
				expect(
					staleObserver
						.query<{ outcome: string; cost_total: number }, [string]>(
							"SELECT outcome, cost_total FROM summary_attempts WHERE attempt_id = ?",
						)
						.get(start.attemptId),
				).toEqual({ outcome: "lease_lost", cost_total: 0.3 });
				expect(
					staleObserver.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM summaries").get()?.count,
				).toBe(0);
			} finally {
				staleObserver.close();
			}

			for (let failure = 0; failure < 5; failure++) {
				const [job] = retryContext.claimSummaryJobs({
					...policyB,
					maxTransportRetries: 5,
					workerId: `retry-${failure}`,
					leaseMs: 1_000,
					limit: 1,
					maxOutputTokens: 100,
					preferredScope: MAIN,
					allowFallback: false,
				});
				expect(job).toBeDefined();
				expect(
					retryContext.failSummaryJob(job!, "transport", 0, {
						promptHash: `failure-${failure}`,
						resolvedModel: policyB.retryKey,
						strategy: job!.strategy,
					}),
				).toBe(true);
				const availability = retryContext.summaryJobAvailability(request, policyB, 5);
				if (failure === 0) {
					expect(availability).toMatchObject({ runnable: 0, backoff: 1, nextAvailableAt: now + 1 });
					expect(retryContext.retrySummaryJobs(request, policyB, 5)).toMatchObject({ backoff: 1, runnable: 0 });
					expect(retryContext.retrySummaryJobs(request, policyB, 5, "all")).toMatchObject({
						backoff: 0,
						runnable: 1,
					});
				} else if (failure < 4) {
					retryContext.retrySummaryJobs(request, policyB, 5, "all");
				}
			}
			expect(retryContext.retrySummaryJobs(request, policyB, 5, "all")).toMatchObject({
				runnable: 0,
				exhausted: 1,
			});
			expect(retryContext.configureSummaryRetryPolicy(MAIN.projectId, policyB.retryKey)).toEqual(policyB);
			expect(retryContext.summaryJobAvailability(request, policyB, 5).exhausted).toBe(1);
			const policyCConflict = retryContext.configureSummaryRetryPolicy(MAIN.projectId, "provider/c");
			expect(policyCConflict).toEqual({
				kind: "conflict",
				retryKey: policyB.retryKey,
				retryEpoch: policyB.retryEpoch,
			});
			const policyC = retryContext.configureSummaryRetryPolicy(MAIN.projectId, "provider/c", { expected: policyB });
			if (policyC.kind !== "ready") throw new Error("policy C did not rotate");
			expect(retryContext.summaryJobAvailability(request, policyC, 5)).toMatchObject({ runnable: 1, exhausted: 0 });
			const [recovered] = retryContext.claimSummaryJobs({
				...policyC,
				maxTransportRetries: 5,
				workerId: "recovered-worker",
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
			});
			const timeoutStart = {
				attemptId: "attempt-timeout",
				startedAt: now,
				provider: "provider",
				model: "c",
			};
			const timeoutProvenance = {
				promptHash: "timeout",
				resolvedModel: policyC.retryKey,
				strategy: recovered!.strategy,
			};
			expect(retryContext.beginSummaryAttempt(recovered!, timeoutStart, timeoutProvenance)).toBe(true);
			expect(
				retryContext.failSummaryJob(recovered!, "provider timeout", 1, timeoutProvenance, {
					attempt: timeoutStart,
					outcome: "transport_error",
				}),
			).toBe(true);
			const timeoutObserver = new Database(retryPath, { readonly: true, strict: true });
			try {
				expect(
					timeoutObserver
						.query<{ outcome: string; total_tokens: number | null }, [string]>(
							"SELECT outcome, total_tokens FROM summary_attempts WHERE attempt_id = ?",
						)
						.get(timeoutStart.attemptId),
				).toEqual({ outcome: "transport_error", total_tokens: null });
			} finally {
				timeoutObserver.close();
			}
			expect(
				retryContext.settleSummaryAttempt(
					recovered!,
					{
						...timeoutStart,
						completedAt: now + 1,
						usage: {
							input: 1,
							output: 2,
							cacheRead: 0,
							cacheWrite: 0,
							totalTokens: 3,
							cost: { input: 0.1, output: 0.2, cacheRead: 0, cacheWrite: 0, total: 0.3 },
						},
					},
					"aborted",
				),
			).toBe("transport_error");
			const enrichedTimeout = new Database(retryPath, { readonly: true, strict: true });
			try {
				expect(
					enrichedTimeout
						.query<{ outcome: string; total_tokens: number; cost_total: number }, [string]>(
							"SELECT outcome, total_tokens, cost_total FROM summary_attempts WHERE attempt_id = ?",
						)
						.get(timeoutStart.attemptId),
				).toEqual({ outcome: "transport_error", total_tokens: 3, cost_total: 0.3 });
			} finally {
				enrichedTimeout.close();
			}
			now += 1;
			const [recoveredAfterTimeout] = retryContext.claimSummaryJobs({
				...policyC,
				maxTransportRetries: 5,
				workerId: "recovered-after-timeout",
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
			});
			expect(
				retryContext.completeSummaryJob(recoveredAfterTimeout!, {
					redactedText: "recovered",
					provenance: {
						promptHash: "recovered",
						resolvedModel: policyC.retryKey,
						strategy: recoveredAfterTimeout!.strategy,
					},
				}),
			).toMatchObject({ accepted: true });
			const successObserver = new Database(retryPath, { readonly: true, strict: true });
			try {
				expect(
					successObserver
						.query<{ transport_retry_count: number }, [string]>(
							"SELECT transport_retry_count FROM summary_jobs WHERE job_id = ?",
						)
						.get(recoveredAfterTimeout!.jobId)?.transport_retry_count,
				).toBe(0);
			} finally {
				successObserver.close();
			}

			retryContext.rebuild([snapshot(MAIN, sources)]);
			const rebuilt = new Database(retryPath, { readonly: true, strict: true });
			try {
				expect(
					rebuilt
						.query<{ retry_key: string; epoch: number }, [string]>(
							"SELECT retry_key, epoch FROM summary_retry_policies WHERE project_id = ?",
						)
						.get(MAIN.projectId),
				).toEqual({ retry_key: policyC.retryKey, epoch: policyC.retryEpoch + 1 });
				expect(
					rebuilt.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM summary_attempts").get()?.count,
				).toBe(2);
			} finally {
				rebuilt.close();
			}
			now += 101;
			retryContext.purge();
			const afterGc = new Database(retryPath, { readonly: true, strict: true });
			try {
				expect(
					afterGc.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM summary_retry_policies").get()
						?.count,
				).toBe(1);
			} finally {
				afterGc.close();
			}
		} finally {
			retryContext.close();
		}
	});
});
