import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import {
	type ContextScope,
	isLcmSqliteContentionError,
	isLcmSqliteCorruptionError,
	type LcmContext,
	openLcmContext,
	type SearchHit,
	type SourceEntry,
	type SourceSnapshot,
} from "../src";
import { initializeLcmSchema, summaryHandleForInput, UnsupportedLcmSchemaError } from "../src/schema";

const MAIN: ContextScope = { projectId: "project", sessionId: "session", branchId: "main" };

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
			workerId: "summarizer",
			leaseMs: 60_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		if (!job) return;
		const result = context.completeSummaryJob(job.jobId, job.leaseToken, {
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
		expect(context.status()).toMatchObject({ schemaVersion: 5, journalMode: "wal" });

		const duplicate = [sources[0]!, { ...sources[1]!, entryId: "e1" }];
		expect(() => context.reconcile(snapshot(MAIN, duplicate))).toThrow("duplicate source entry id");
		const projection = context.project({ ...MAIN, tokenBudget: 100, freshTail: { maxSources: 10, maxTokens: 100 } });
		expect(projection.freshTailSourceIds).toEqual(["e1", "e2"]);
		expect(projection.revision).toBe(1);
	});

	test("v4 migration enforces stable handles on later updates", () => {
		const migrationPath = path.join(tempDir, "v4.db");
		const db = new Database(migrationPath);
		try {
			db.run(
				"CREATE TABLE summaries (summary_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, level INTEGER NOT NULL)",
			);
			db.run(
				"CREATE TABLE summary_children (summary_id TEXT NOT NULL, ordinal INTEGER NOT NULL, child_summary_id TEXT NOT NULL)",
			);
			db.run(
				"CREATE TABLE summary_lineage (summary_id TEXT NOT NULL, ordinal INTEGER NOT NULL, source_key TEXT NOT NULL)",
			);
			db.run("INSERT INTO summaries (summary_id, project_id, level) VALUES ('sum-v4', 'project', 0)");
			db.run("INSERT INTO summary_lineage (summary_id, ordinal, source_key) VALUES ('sum-v4', 0, 'source-v4')");
			db.run("PRAGMA user_version = 4");

			initializeLcmSchema(db, 1_000);
			expect(db.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(5);
			expect(
				db.query<{ stable_handle: string }, []>("SELECT stable_handle FROM summaries").get()?.stable_handle,
			).toStartWith("summary_");
			expect(() => db.run("UPDATE summaries SET stable_handle = NULL")).toThrow("summary stable_handle is required");
		} finally {
			db.close();
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
			db.run("DROP TRIGGER summaries_stable_handle_required");
			db.run("DROP TRIGGER summaries_stable_handle_update_required");
			db.run("DROP INDEX summaries_stable_handle");
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
			expect(observer.query<{ user_version: number }, []>("PRAGMA user_version").get()?.user_version).toBe(5);
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
			expect(first.status().schemaVersion).toBe(5);
			expect(second.status().schemaVersion).toBe(5);
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
			expect(reopened.status()).toMatchObject({ schemaVersion: 5, quarantined: false, recoveredFrom: null });
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

	test("recoverCorrupt detects latent B-tree corruption before returning a context", async () => {
		const corruptPath = path.join(tempDir, "latent-corrupt.db");
		await createLatentlyCorruptDatabase(corruptPath);

		const recovered = await openLcmContext({ dbPath: corruptPath, recoverCorrupt: true, now: () => now });
		try {
			const recoveredFrom = recovered.status().recoveredFrom;
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
				const recoveredFrom = recovered.status().recoveredFrom;
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
			const recoveredFrom = recovered.status().recoveredFrom;
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
			expect(recovered.status().recoveredFrom).toStartWith(`${corruptPath}.quarantine-${now}-`);
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
			expect(recovered.status()).toMatchObject({ recoveredFrom: quarantinePath, quarantined: false });
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
			expect(recovered.status().recoveredFrom).toBe(durableRecoveryPath);
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
			expect(owner.status()).toMatchObject({ schemaVersion: 5, quarantined: false, recoveredFrom: null });
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
			const recoveredFrom = recovered.status().recoveredFrom;
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
			expect(recovered.status()).toMatchObject({ schemaVersion: 5, quarantined: false });
			const recoveredFrom = recovered.status().recoveredFrom;
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

	test("a failed completion remains durably retryable without corrupting committed sources", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "durable source")]));
		const [crashed] = context.claimSummaryJobs({
			workerId: "crashing-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(crashed).toBeDefined();
		expect(context.failSummaryJob(crashed!.jobId, crashed!.leaseToken, "CompletionError", 50)).toBe(true);
		expect(context.status().jobs.failed).toBe(1);
		now += 50;
		const retried = context.claimSummaryJobs({
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

	test("projection covers each source at most once and preserves branch order", async () => {
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
		expect(projection.estimatedTokens).toBeLessThanOrEqual(100);
	});

	test("summary expansion cites each repeated source-key occurrence in branch order", async () => {
		const first = entry(MAIN, "e1", "identical source content long enough to produce one useful leaf summary");
		const second = { ...first, entryId: "e2", parentId: first.entryId };
		context.reconcile(snapshot(MAIN, [first, second]));
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

	test("fresh-tail cut includes an assistant and every parallel tool result as one atomic closure", async () => {
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
		expect(projection.freshTailSourceIds).toEqual(["e2", "e3", "e4", "e5"]);
		expect(historicalIds).toEqual(["e1"]);
		expect([...historicalIds, ...projection.freshTailSourceIds]).toEqual(sources.map(source => source.entryId));
		expect(historicalIds.filter(sourceId => projection.freshTailSourceIds.includes(sourceId))).toEqual([]);
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
		expect(mainProjection.historical.flatMap(item => item.sourceIds)).toEqual(["e1", "e2", "e3", "e4"]);
		expect(forkProjection.historical.flatMap(item => item.sourceIds)).toEqual(["e1", "e2", "f3", "f4"]);
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
		const projection = context.project({ ...fork, tokenBudget: 100, freshTail: { maxSources: 1, maxTokens: 100 } });
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
			workerId: "main-worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(preferred?.queueClass).toBe("preferred");
		expect(
			context.completeSummaryJob(preferred!.jobId, preferred!.leaseToken, {
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
			workerId: "preferred-only",
			leaseMs: 1_000,
			limit: 2,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(preferredOnly).toHaveLength(1);
		expect(preferredOnly[0]?.queueClass).toBe("preferred");
		expect(context.releaseSummaryJob(preferredOnly[0]!.jobId, preferredOnly[0]!.leaseToken)).toBe(true);

		const mixed = context.claimSummaryJobs({
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
				workerId: "preferred-only",
				leaseMs: 1_000,
				limit: 1,
				maxOutputTokens: 100,
				preferredScope: MAIN,
				allowFallback: false,
			}),
		).toEqual([]);
		const [fallback] = context.claimSummaryJobs({
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
			workerId: "preferred-failure",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(context.failSummaryJob(preferred!.jobId, preferred!.leaseToken, "ProviderError", 100)).toBe(true);
		const [fallback] = context.claimSummaryJobs({
			workerId: "fallback-failure",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: true,
		});
		expect(fallback?.queueClass).toBe("fallback");
		expect(context.failSummaryJob(fallback!.jobId, fallback!.leaseToken, "ProviderError", 0)).toBe(true);

		context.close();
		context = await openLcmContext({
			dbPath,
			leafChunk: { maxSources: 2, maxTokens: 10_000 },
			condenseFanIn: 2,
			tombstoneRetentionMs: 100,
			now: () => now,
		});
		const failures = context.summaryJobFailures(MAIN);
		expect(failures.find(failure => failure.queueClass === "preferred")).toMatchObject({
			jobId: preferred!.jobId,
			availableAt: now + 100,
		});
		expect(failures.find(failure => failure.queueClass === "fallback")).toMatchObject({
			jobId: fallback!.jobId,
			availableAt: now,
		});
		expect(context.nextSummaryJobDelayMs(MAIN, false)).toBe(100);
		expect(context.nextSummaryJobDelayMs(MAIN, true)).toBe(0);

		now += 100;
		const [retry] = context.claimSummaryJobs({
			workerId: "preferred-retry",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 100,
			preferredScope: MAIN,
			allowFallback: false,
		});
		expect(
			context.completeSummaryJob(retry!.jobId, retry!.leaseToken, { redactedText: "recovered", tokenCount: 1 }),
		).toMatchObject({ accepted: true });
		expect(context.summaryJobFailures(MAIN)).toEqual([
			{ jobId: fallback!.jobId, availableAt: now - 100, queueClass: "fallback" },
		]);
	});

	test("release accepts an expired matching token but rejects a replaced owner", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "release input long enough to summarize")]));
		const [expired] = context.claimSummaryJobs({
			workerId: "worker-a",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		now += 101;
		expect(context.releaseSummaryJob(expired!.jobId, expired!.leaseToken)).toBe(true);
		const [replaced] = context.claimSummaryJobs({
			workerId: "worker-b",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		now += 101;
		const [owner] = context.claimSummaryJobs({
			workerId: "worker-c",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(owner?.leaseToken).not.toBe(replaced?.leaseToken);
		expect(context.releaseSummaryJob(replaced!.jobId, replaced!.leaseToken)).toBe(false);
		expect(context.releaseSummaryJob(owner!.jobId, owner!.leaseToken)).toBe(true);
		expect(context.status().jobs.pending).toBe(1);
	});

	test("leases are exclusive, reclaimable, and reject an expired owner's result", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "lease input")]));
		const first = context.claimSummaryJobs({ workerId: "worker-a", leaseMs: 100, limit: 1, maxOutputTokens: 100 });
		expect(first).toHaveLength(1);
		expect(context.claimSummaryJobs({ workerId: "worker-b", leaseMs: 100, limit: 1, maxOutputTokens: 100 })).toEqual(
			[],
		);

		now += 101;
		const replacement = context.claimSummaryJobs({
			workerId: "worker-b",
			leaseMs: 100,
			limit: 1,
			maxOutputTokens: 100,
		});
		expect(replacement).toHaveLength(1);
		expect(replacement[0]?.leaseToken).not.toBe(first[0]?.leaseToken);
		expect(context.completeSummaryJob(first[0]!.jobId, first[0]!.leaseToken, { redactedText: "late" })).toEqual({
			accepted: false,
			reason: "lease_lost",
		});
		expect(
			context.completeSummaryJob(replacement[0]!.jobId, replacement[0]!.leaseToken, { redactedText: "accepted" }),
		).toMatchObject({ accepted: true });
	});

	test("completion rejects forged token counts and non-compressing output, then remains retryable", () => {
		const sourceText =
			"this source is intentionally long so returning it unchanged cannot be mistaken for compression";
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", sourceText)]));
		const [claim] = context.claimSummaryJobs({
			workerId: "worker",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 2,
		});
		expect(claim?.inputTokenCount).toBeGreaterThan(claim?.outputTokenBudget ?? 0);
		expect(claim?.outputTokenBudget).toBe(2);
		expect(
			context.completeSummaryJob(claim!.jobId, claim!.leaseToken, { redactedText: sourceText, tokenCount: 1 }),
		).toEqual({
			accepted: false,
			reason: "escalated",
			stage: "aggressive",
		});
		expect(context.status().jobs.pending).toBe(1);

		const [retry] = context.claimSummaryJobs({
			workerId: "replacement",
			leaseMs: 1_000,
			limit: 1,
			maxOutputTokens: 2,
		});
		expect(
			context.completeSummaryJob(retry!.jobId, retry!.leaseToken, { redactedText: "ok", tokenCount: 1 }),
		).toMatchObject({
			accepted: true,
		});
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
		expect(context.completeSummaryJob(claim!.jobId, claim!.leaseToken, { redactedText: "ok" })).toEqual({
			accepted: false,
			reason: "stale",
		});
	});

	test("completion rejects a still-leased result whose source lineage disappeared", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "old lineage")]));
		const [claim] = context.claimSummaryJobs({ workerId: "worker", leaseMs: 1_000, limit: 1, maxOutputTokens: 100 });
		expect(claim).toBeDefined();
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e2", "replacement lineage")]));
		expect(context.completeSummaryJob(claim!.jobId, claim!.leaseToken, { redactedText: "stale summary" })).toEqual({
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
			const recoveredFrom = recovered.status().recoveredFrom;
			expect(recovered.status()).toMatchObject({ schemaVersion: 5, quarantined: false });
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

	test("tombstones remain until the configured retention horizon and are then purged", () => {
		context.reconcile(snapshot(MAIN, [entry(MAIN, "e1", "temporary")]));
		context.reconcile(snapshot(MAIN, []));
		expect(context.status().tombstones).toBe(1);
		expect(context.purge().tombstones).toBe(0);
		now += 101;
		expect(context.purge().tombstones).toBe(1);
		expect(context.status().tombstones).toBe(0);
	});
});
