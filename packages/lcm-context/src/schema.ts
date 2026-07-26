import type { Database } from "bun:sqlite";

export const LCM_SCHEMA_VERSION = 5;

export class UnsupportedLcmSchemaError extends Error {
	readonly foundVersion: number;

	constructor(foundVersion: number) {
		super(`LCM database schema ${foundVersion} is newer than supported schema ${LCM_SCHEMA_VERSION}`);
		this.name = "UnsupportedLcmSchemaError";
		this.foundVersion = foundVersion;
	}
}

export interface SummaryHandleInput {
	kind: "source" | "summary";
	id: string;
}

/** Stable executable handle derived only from ordered summarized input identity. */
export function summaryHandleForInput(projectId: string, level: number, inputs: readonly SummaryHandleInput[]): string {
	const hasher = new Bun.CryptoHasher("sha256");
	for (const part of [
		"lcm-summary-handle-v1",
		projectId,
		String(level),
		...inputs.flatMap(input => [input.kind, input.id]),
	]) {
		hasher.update(`${Buffer.byteLength(part, "utf8")}:`);
		hasher.update(part);
	}
	return `summary_${hasher.digest("hex")}`;
}

function migration1(db: Database): void {
	db.run(`
		CREATE TABLE store_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			quarantined_at INTEGER,
			quarantine_reason TEXT,
			rebuilt_at INTEGER,
			rebuild_count INTEGER NOT NULL DEFAULT 0
		) STRICT
	`);
	db.run("INSERT INTO store_state (id) VALUES (1)");

	db.run(`
		CREATE TABLE branches (
			id INTEGER PRIMARY KEY,
			project_id TEXT NOT NULL,
			session_id TEXT NOT NULL,
			branch_id TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 0,
			reconciled_at INTEGER NOT NULL,
			UNIQUE (project_id, session_id, branch_id)
		) STRICT
	`);

	db.run(`
		CREATE TABLE source_contents (
			source_key TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			kind TEXT NOT NULL,
			redacted_text TEXT NOT NULL,
			artifact_refs TEXT NOT NULL CHECK (json_valid(artifact_refs)),
			token_count INTEGER NOT NULL CHECK (token_count >= 0),
			created_at INTEGER NOT NULL
		) STRICT
	`);
	db.run("CREATE INDEX source_contents_project ON source_contents(project_id)");

	db.run(`
		CREATE TABLE branch_sources (
			id INTEGER PRIMARY KEY,
			branch_row_id INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
			entry_id TEXT NOT NULL,
			parent_entry_id TEXT,
			position INTEGER NOT NULL,
			source_key TEXT NOT NULL REFERENCES source_contents(source_key),
			active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
			created_at INTEGER NOT NULL,
			tombstoned_at INTEGER
		) STRICT
	`);
	db.run(
		"CREATE UNIQUE INDEX branch_sources_active_entry ON branch_sources(branch_row_id, entry_id) WHERE active = 1",
	);
	db.run(
		"CREATE UNIQUE INDEX branch_sources_active_position ON branch_sources(branch_row_id, position) WHERE active = 1",
	);
	db.run("CREATE INDEX branch_sources_sequence ON branch_sources(branch_row_id, active, position)");
	db.run("CREATE INDEX branch_sources_content ON branch_sources(source_key, active)");
	db.run("CREATE INDEX branch_sources_tombstones ON branch_sources(tombstoned_at) WHERE active = 0");

	db.run(`
		CREATE TABLE summaries (
			summary_id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			input_hash TEXT NOT NULL,
			level INTEGER NOT NULL CHECK (level >= 0),
			redacted_text TEXT NOT NULL,
			token_count INTEGER NOT NULL CHECK (token_count >= 0),
			created_at INTEGER NOT NULL,
			UNIQUE (project_id, input_hash)
		) STRICT
	`);
	db.run("CREATE INDEX summaries_project_level ON summaries(project_id, level)");

	db.run(`
		CREATE TABLE summary_lineage (
			summary_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
			ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
			source_key TEXT NOT NULL REFERENCES source_contents(source_key),
			PRIMARY KEY (summary_id, ordinal)
		) WITHOUT ROWID, STRICT
	`);
	db.run("CREATE INDEX summary_lineage_source ON summary_lineage(source_key, summary_id)");

	db.run(`
		CREATE TABLE summary_children (
			summary_id TEXT NOT NULL REFERENCES summaries(summary_id) ON DELETE CASCADE,
			ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
			child_summary_id TEXT NOT NULL REFERENCES summaries(summary_id),
			PRIMARY KEY (summary_id, ordinal)
		) WITHOUT ROWID, STRICT
	`);

	db.run(`
		CREATE TABLE summary_jobs (
			job_id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			input_hash TEXT NOT NULL UNIQUE,
			level INTEGER NOT NULL CHECK (level >= 0),
			origin_branch_row_id INTEGER REFERENCES branches(id) ON DELETE SET NULL,
			origin_revision INTEGER NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('pending', 'leased', 'failed', 'completed', 'obsolete')),
			worker_id TEXT,
			lease_token TEXT,
			lease_expires_at INTEGER,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			available_at INTEGER NOT NULL,
			last_error TEXT,
			result_summary_id TEXT REFERENCES summaries(summary_id),
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		) STRICT
	`);
	db.run("CREATE INDEX summary_jobs_claim ON summary_jobs(status, available_at, lease_expires_at, created_at)");
	db.run("CREATE INDEX summary_jobs_project ON summary_jobs(project_id, status)");

	db.run(`
		CREATE TABLE job_inputs (
			job_id TEXT NOT NULL REFERENCES summary_jobs(job_id) ON DELETE CASCADE,
			ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
			input_kind TEXT NOT NULL CHECK (input_kind IN ('source', 'summary')),
			ref_id TEXT NOT NULL,
			PRIMARY KEY (job_id, ordinal)
		) WITHOUT ROWID, STRICT
	`);

	db.run(`
		CREATE TABLE job_lineage (
			job_id TEXT NOT NULL REFERENCES summary_jobs(job_id) ON DELETE CASCADE,
			ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
			source_key TEXT NOT NULL REFERENCES source_contents(source_key),
			PRIMARY KEY (job_id, ordinal)
		) WITHOUT ROWID, STRICT
	`);
	db.run("CREATE INDEX job_lineage_source ON job_lineage(source_key, job_id)");
}

function migration2(db: Database): void {
	db.run("ALTER TABLE store_state ADD COLUMN last_recovery_path TEXT");
	db.run(`
		CREATE TABLE recovery_events (
			id INTEGER PRIMARY KEY,
			quarantine_path TEXT NOT NULL,
			reason TEXT NOT NULL,
			created_at INTEGER NOT NULL
		) STRICT
	`);

	db.run(`
		CREATE TABLE search_documents (
			id INTEGER PRIMARY KEY,
			project_id TEXT NOT NULL,
			document_kind TEXT NOT NULL CHECK (document_kind IN ('source', 'summary')),
			ref_id TEXT NOT NULL,
			redacted_text TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			UNIQUE (document_kind, ref_id)
		) STRICT
	`);
	db.run("CREATE INDEX search_documents_project ON search_documents(project_id, document_kind)");
	db.run(`
		CREATE VIRTUAL TABLE search_fts USING fts5(
			redacted_text,
			content='search_documents',
			content_rowid='id',
			tokenize='unicode61 remove_diacritics 2'
		)
	`);
	db.run(`
		CREATE TRIGGER search_documents_ai AFTER INSERT ON search_documents BEGIN
			INSERT INTO search_fts(rowid, redacted_text) VALUES (new.id, new.redacted_text);
		END
	`);
	db.run(`
		CREATE TRIGGER search_documents_ad AFTER DELETE ON search_documents BEGIN
			INSERT INTO search_fts(search_fts, rowid, redacted_text) VALUES ('delete', old.id, old.redacted_text);
		END
	`);
	db.run(`
		CREATE TRIGGER search_documents_au AFTER UPDATE OF redacted_text ON search_documents BEGIN
			INSERT INTO search_fts(search_fts, rowid, redacted_text) VALUES ('delete', old.id, old.redacted_text);
			INSERT INTO search_fts(rowid, redacted_text) VALUES (new.id, new.redacted_text);
		END
	`);

	db.run(`
		CREATE TRIGGER source_contents_search_ai AFTER INSERT ON source_contents BEGIN
			INSERT OR IGNORE INTO search_documents(project_id, document_kind, ref_id, redacted_text, created_at)
			VALUES (new.project_id, 'source', new.source_key, new.redacted_text, new.created_at);
		END
	`);
	db.run(`
		CREATE TRIGGER source_contents_search_ad AFTER DELETE ON source_contents BEGIN
			DELETE FROM search_documents WHERE document_kind = 'source' AND ref_id = old.source_key;
		END
	`);
	db.run(`
		CREATE TRIGGER summaries_search_ai AFTER INSERT ON summaries BEGIN
			INSERT OR IGNORE INTO search_documents(project_id, document_kind, ref_id, redacted_text, created_at)
			VALUES (new.project_id, 'summary', new.summary_id, new.redacted_text, new.created_at);
		END
	`);
	db.run(`
		CREATE TRIGGER summaries_search_ad AFTER DELETE ON summaries BEGIN
			DELETE FROM search_documents WHERE document_kind = 'summary' AND ref_id = old.summary_id;
		END
	`);

	db.run(`
		INSERT OR IGNORE INTO search_documents(project_id, document_kind, ref_id, redacted_text, created_at)
		SELECT project_id, 'source', source_key, redacted_text, created_at FROM source_contents
	`);
	db.run(`
		INSERT OR IGNORE INTO search_documents(project_id, document_kind, ref_id, redacted_text, created_at)
		SELECT project_id, 'summary', summary_id, redacted_text, created_at FROM summaries
	`);
	db.run("INSERT INTO search_fts(search_fts) VALUES('rebuild')");
}

function migration3(db: Database): void {
	db.run("ALTER TABLE branch_sources ADD COLUMN atomic_group_id TEXT");
	db.run("ALTER TABLE summary_jobs ADD COLUMN lease_input_tokens INTEGER");
	db.run("ALTER TABLE summary_jobs ADD COLUMN lease_output_budget INTEGER");
}

function migration4(db: Database): void {
	db.run(
		"ALTER TABLE summary_jobs ADD COLUMN stage TEXT NOT NULL DEFAULT 'normal' CHECK (stage IN ('normal', 'aggressive', 'deterministic'))",
	);
	db.run(
		"ALTER TABLE summary_jobs ADD COLUMN transport_retry_count INTEGER NOT NULL DEFAULT 0 CHECK (transport_retry_count >= 0)",
	);
	db.run(
		"ALTER TABLE summary_jobs ADD COLUMN non_compression_count INTEGER NOT NULL DEFAULT 0 CHECK (non_compression_count >= 0)",
	);
	db.run("ALTER TABLE summary_jobs ADD COLUMN last_strategy TEXT");
	db.run("ALTER TABLE summary_jobs ADD COLUMN last_prompt_hash TEXT");
	db.run("ALTER TABLE summary_jobs ADD COLUMN last_model_selector TEXT");
	db.run("ALTER TABLE summary_jobs ADD COLUMN last_resolved_model TEXT");
	db.run("ALTER TABLE summary_jobs ADD COLUMN last_input_tokens INTEGER");
	db.run("ALTER TABLE summary_jobs ADD COLUMN last_output_tokens INTEGER");
	db.run("ALTER TABLE summaries ADD COLUMN strategy TEXT");
	db.run("ALTER TABLE summaries ADD COLUMN prompt_hash TEXT");
	db.run("ALTER TABLE summaries ADD COLUMN model_selector TEXT");
	db.run("ALTER TABLE summaries ADD COLUMN resolved_model TEXT");
	db.run("ALTER TABLE summaries ADD COLUMN input_token_count INTEGER");
	db.run("ALTER TABLE summaries ADD COLUMN output_token_count INTEGER");
	db.run("ALTER TABLE branches ADD COLUMN summary_token_budget INTEGER");
	db.run("ALTER TABLE branches ADD COLUMN fresh_tail_max_sources INTEGER");
	db.run("ALTER TABLE branches ADD COLUMN fresh_tail_max_tokens INTEGER");
	// A v3 non-compression failure must resume at the next strategy, never repeat
	// the identical prompt after upgrade.
	db.run(
		`UPDATE summary_jobs SET stage = 'aggressive', non_compression_count = 1
		 WHERE status = 'failed' AND last_error = 'summary did not compress input'`,
	);
}

function migration5(db: Database): void {
	// Internal summary IDs remain graph-local. The executable handle follows
	// canonical ordered job inputs: source keys for leaves and already-stable
	// child handles for condensed nodes, independent of generated prose.
	db.run("ALTER TABLE summaries ADD COLUMN stable_handle TEXT");
	const summaries = db
		.query<{ summary_id: string; project_id: string; level: number }, []>(
			"SELECT summary_id, project_id, level FROM summaries ORDER BY level, summary_id",
		)
		.all();
	for (const summary of summaries) {
		const children = db
			.query<{ stable_handle: string | null }, [string]>(
				`SELECT child.stable_handle FROM summary_children edge
				 JOIN summaries child ON child.summary_id = edge.child_summary_id
				 WHERE edge.summary_id = ? ORDER BY edge.ordinal`,
			)
			.all(summary.summary_id);
		let inputs: SummaryHandleInput[];
		if (children.length > 0) {
			if (children.some(child => child.stable_handle === null)) {
				throw new Error(`Cannot derive stable handle for summary ${summary.summary_id}`);
			}
			inputs = children.map(child => ({ kind: "summary", id: child.stable_handle! }));
		} else {
			inputs = db
				.query<{ source_key: string }, [string]>(
					"SELECT source_key FROM summary_lineage WHERE summary_id = ? ORDER BY ordinal",
				)
				.all(summary.summary_id)
				.map(row => ({ kind: "source", id: row.source_key }));
		}
		db.run("UPDATE summaries SET stable_handle = ? WHERE summary_id = ?", [
			summaryHandleForInput(summary.project_id, summary.level, inputs),
			summary.summary_id,
		]);
	}
	db.run("CREATE UNIQUE INDEX summaries_stable_handle ON summaries(stable_handle)");
	db.run(`
		CREATE TRIGGER summaries_stable_handle_required
		BEFORE INSERT ON summaries WHEN new.stable_handle IS NULL
		BEGIN
			SELECT RAISE(ABORT, 'summary stable_handle is required');
		END
	`);
	db.run(`
		CREATE TRIGGER summaries_stable_handle_update_required
		BEFORE UPDATE OF stable_handle ON summaries WHEN new.stable_handle IS NULL
		BEGIN
			SELECT RAISE(ABORT, 'summary stable_handle is required');
		END
	`);

	db.run(`
		CREATE TABLE file_records (
			file_id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			path TEXT NOT NULL,
			file_type TEXT NOT NULL,
			byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
			token_count INTEGER NOT NULL CHECK (token_count >= 0),
			exploration_summary TEXT NOT NULL,
			created_at INTEGER NOT NULL
		) STRICT
	`);
	db.run("CREATE INDEX file_records_project ON file_records(project_id, file_id)");
	db.run(`
		CREATE TABLE source_files (
			source_key TEXT NOT NULL REFERENCES source_contents(source_key) ON DELETE CASCADE,
			ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
			file_id TEXT NOT NULL REFERENCES file_records(file_id),
			PRIMARY KEY (source_key, ordinal),
			UNIQUE (source_key, file_id)
		) WITHOUT ROWID, STRICT
	`);
	db.run("CREATE INDEX source_files_file ON source_files(file_id, source_key)");
}

const MIGRATIONS: ReadonlyArray<(db: Database) => void> = [migration1, migration2, migration3, migration4, migration5];

export function initializeLcmSchema(db: Database, busyTimeoutMs: number): void {
	// The busy handler must be installed before WAL recovery or any migration lock.
	db.run(`PRAGMA busy_timeout = ${busyTimeoutMs}`);
	db.run("PRAGMA journal_mode = WAL");
	db.run("PRAGMA synchronous = NORMAL");
	db.run("PRAGMA foreign_keys = ON");
	db.run("PRAGMA secure_delete = ON");

	const apply = db.transaction(() => {
		const row = db.query<{ user_version: number }, []>("PRAGMA user_version").get();
		const foundVersion = row?.user_version ?? 0;
		if (foundVersion > LCM_SCHEMA_VERSION) throw new UnsupportedLcmSchemaError(foundVersion);

		for (let version = foundVersion + 1; version <= LCM_SCHEMA_VERSION; version++) {
			const migrate = MIGRATIONS[version - 1];
			if (!migrate) throw new Error(`Missing LCM schema migration ${version}`);
			migrate(db);
			db.run(`PRAGMA user_version = ${version}`);
		}
	});
	apply.immediate();
}
