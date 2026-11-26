import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { RunAlreadyExistsError, WorkerLeaseLostError } from "@continuum/core";
import type {
  AppendEventOptions,
  ContinuumEvent,
  ContinuumEventType,
  IntegrityChainHead,
  JournalTransaction,
  NewContinuumEvent,
  RunLease,
  RunSnapshot,
  RunState,
  WorkflowJournal,
} from "@continuum/core";

export interface SqliteJournalOptions {
  filename: string;
  verbose?: boolean;
  /**
   * Existing connection to reuse. A supplied connection is not closed by
   * `close()`, mirroring `PostgresJournalOptions.pool`.
   */
  database?: Database.Database;
  /**
   * How long a writer waits for the database write lock before failing with
   * `SQLITE_BUSY`. better-sqlite3 defaults to 5000ms; this exposes the knob so
   * latency-sensitive callers can shorten it. Set `0` to fail fast.
   */
  busyTimeoutMs?: number;
}

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;

/**
 * Milliseconds since the Unix epoch according to the database, not the worker's
 * wall clock. Lease expiry is evaluated against this value so that clock skew
 * between worker hosts cannot extend or steal lease ownership.
 */
const DATABASE_NOW_MS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

interface EventRow {
  seq: number;
  event_id: string;
  run_id: string;
  workflow_name: string;
  type: string;
  at: number;
  data: string;
}

interface LeaseRow {
  run_id: string;
  worker_id: string;
  fencing_token: number;
  expires_at: number;
}

export class SqliteJournal implements WorkflowJournal {
  private readonly db: Database.Database;
  private owned = true;

  constructor(options: SqliteJournalOptions | string) {
    const filename = typeof options === "string" ? options : options.filename;
    const database = typeof options === "string" ? undefined : options.database;
    const busyTimeoutMs = typeof options === "string" ? undefined : options.busyTimeoutMs;
    this.db = database ?? new Database(filename);
    this.owned = database === undefined;
    // Only touch the busy timeout when this journal owns the connection or the
    // caller asked for a specific value; a supplied connection keeps its own.
    if (this.owned || busyTimeoutMs !== undefined) {
      this.db.pragma(`busy_timeout = ${Math.max(0, Math.trunc(busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS))}`);
    }
    this.migrate();
  }

  static memory(): SqliteJournal {
    return new SqliteJournal(":memory:");
  }

  async append(events: readonly NewContinuumEvent[], options: AppendEventOptions = {}): Promise<ContinuumEvent[]> {
    if (events.length === 0) return [];
    // Share the in-process file lock with transaction() so a plain append can
    // never busy-wait on a write lock held by an async transaction in this same
    // process: better-sqlite3 is synchronous, so that wait would stall the event
    // loop the transaction needs to finish. Cross-process serialization is
    // still SQLite's own write lock.
    const release = await acquireFileLock(this.db.name);
    try {
      this.db.exec("BEGIN IMMEDIATE");
      try {
        const appended = this.appendRows(events, options);
        this.db.exec("COMMIT");
        return appended;
      } catch (error) {
        this.rollbackQuietly();
        throw error;
      }
    } finally {
      release();
    }
  }

  /**
   * Run `work` inside one `BEGIN IMMEDIATE` transaction.
   *
   * `BEGIN IMMEDIATE` takes the database write lock up front, so the chain head
   * read inside `work` and every write it makes are serialized against all
   * other writers of this database file — in this process and in any other
   * process using SQLite. A deferred transaction would read a stale snapshot
   * and fail with SQLITE_BUSY_SNAPSHOT instead of serializing.
   *
   * Within one process, transactions on the same database file are additionally
   * queued cooperatively: better-sqlite3 is synchronous, so a second connection
   * in this process that busy-waited for the write lock would block the event
   * loop that the first transaction needs in order to finish.
   */
  async transaction<T>(work: (tx: JournalTransaction) => Promise<T>, options: AppendEventOptions = {}): Promise<T> {
    const release = await acquireFileLock(this.db.name);
    try {
      this.db.exec("BEGIN IMMEDIATE");
      try {
        const result = await work({
          appendEvents: async (events, appendOptions) => this.appendRows(events, appendOptions ?? options),
          readIntegrityHead: async () => this.readIntegrityHeadRow(),
          writeIntegrityHead: async (head) => {
            this.db.prepare(`
              INSERT INTO continuum_integrity_head (id, counter, last_hash)
              VALUES (0, ?, ?)
              ON CONFLICT(id) DO UPDATE SET counter = excluded.counter, last_hash = excluded.last_hash
            `).run(head.counter, head.lastHash);
          },
          lastEvent: async () => this.lastEventRow(),
          allEvents: async () => this.allEventRows(),
        });
        this.db.exec("COMMIT");
        return result;
      } catch (error) {
        this.rollbackQuietly();
        throw error;
      }
    } finally {
      release();
    }
  }

  async integrityHead(): Promise<IntegrityChainHead | undefined> {
    return this.readIntegrityHeadRow();
  }

  private readIntegrityHeadRow(): IntegrityChainHead | undefined {
    const row = this.db.prepare(`
      SELECT counter, last_hash FROM continuum_integrity_head WHERE id = 0
    `).get() as { counter: number; last_hash: string } | undefined;
    // A counter of 0 means "the chain is empty", which is different from "this
    // store keeps no durable head at all"; keep the distinction so verification
    // can tell an empty journal apart from a truncated one.
    if (!row) return undefined;
    return { counter: row.counter, lastHash: row.last_hash };
  }

  private lastEventRow(): ContinuumEvent | undefined {
    const row = this.db.prepare(`
      SELECT seq, event_id, run_id, workflow_name, type, at, data
      FROM continuum_events
      ORDER BY seq DESC
      LIMIT 1
    `).get() as EventRow | undefined;
    return row ? rowToEvent(row) : undefined;
  }

  private allEventRows(): ContinuumEvent[] {
    const rows = this.db.prepare(`
      SELECT seq, event_id, run_id, workflow_name, type, at, data
      FROM continuum_events
      ORDER BY seq ASC
    `).all() as unknown as EventRow[];
    return rows.map(rowToEvent);
  }

  /**
   * The journal's normal append semantics, shared by {@link append} and by
   * `transaction()`'s `appendEvents`. It never opens a transaction of its own:
   * the caller owns the transaction boundary so chain state and events commit
   * or roll back together.
   */
  private appendRows(events: readonly NewContinuumEvent[], options: AppendEventOptions = {}): ContinuumEvent[] {
    if (events.length === 0) return [];
    const insert = this.db.prepare(`
      INSERT INTO continuum_events (event_id, run_id, workflow_name, type, at, data)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    const insertRun = this.db.prepare(`
      INSERT INTO continuum_runs (run_id, created_at)
      VALUES (?, ?)
    `);

    const appended: ContinuumEvent[] = [];
    const lease = options.lease;
    if (lease) {
      const current = this.db.prepare(`
        SELECT worker_id, fencing_token
        FROM continuum_leases
        WHERE run_id = ?
      `).get(lease.runId) as Pick<LeaseRow, "worker_id" | "fencing_token"> | undefined;
      if (
        events.some((event) => event.runId !== lease.runId) ||
        !current ||
        current.worker_id !== lease.workerId ||
        current.fencing_token !== lease.fencingToken
      ) {
        throw new WorkerLeaseLostError(lease.runId);
      }
    }

    for (const event of events) {
      const row = {
        eventId: randomUUID(),
        runId: event.runId,
        workflowName: event.workflowName,
        type: event.type,
        at: event.at ?? Date.now(),
        data: event.data,
      };
      if (row.type === "run.created") {
        try {
          insertRun.run(row.runId, row.at);
        } catch {
          throw new RunAlreadyExistsError(row.runId);
        }
      }
      const result = insert.run(
        row.eventId,
        row.runId,
        row.workflowName,
        row.type,
        row.at,
        JSON.stringify(row.data),
      );
      appended.push({
        seq: Number(result.lastInsertRowid),
        eventId: row.eventId,
        runId: row.runId,
        workflowName: row.workflowName,
        type: row.type,
        at: row.at,
        data: row.data,
      });
    }
    return appended;
  }

  async events(runId: string): Promise<ContinuumEvent[]> {
    const rows = this.db.prepare(`
      SELECT seq, event_id, run_id, workflow_name, type, at, data
      FROM continuum_events
      WHERE run_id = ?
      ORDER BY seq ASC
    `).all(runId) as unknown as EventRow[];
    return rows.map(rowToEvent);
  }

  async allEvents(): Promise<ContinuumEvent[]> {
    const rows = this.db.prepare(`
      SELECT seq, event_id, run_id, workflow_name, type, at, data
      FROM continuum_events
      ORDER BY seq ASC
    `).all() as unknown as EventRow[];
    return rows.map(rowToEvent);
  }

  async eventsAfter(runId: string, afterSeq: number): Promise<ContinuumEvent[]> {
    const rows = this.db.prepare(`
      SELECT seq, event_id, run_id, workflow_name, type, at, data
      FROM continuum_events
      WHERE run_id = ? AND seq > ?
      ORDER BY seq ASC
    `).all(runId, afterSeq) as unknown as EventRow[];
    return rows.map(rowToEvent);
  }

  async runIds(): Promise<string[]> {
    const rows = this.db.prepare(`
      SELECT run_id
      FROM continuum_events
      GROUP BY run_id
      ORDER BY MIN(seq) ASC
    `).all() as unknown as Array<{ run_id: string }>;
    return rows.map((row) => row.run_id);
  }

  async saveRunSnapshot(snapshot: RunSnapshot): Promise<void> {
    this.db.prepare(`
      INSERT INTO continuum_snapshots (run_id, last_seq, state)
      VALUES (?, ?, ?)
      ON CONFLICT(run_id) DO UPDATE SET
        last_seq = excluded.last_seq,
        state = excluded.state
      WHERE excluded.last_seq >= continuum_snapshots.last_seq
    `).run(snapshot.runId, snapshot.lastSeq, JSON.stringify(snapshot.state));
  }

  async latestRunSnapshot(runId: string): Promise<RunSnapshot | undefined> {
    const row = this.db.prepare(`
      SELECT run_id, last_seq, state FROM continuum_snapshots WHERE run_id = ?
    `).get(runId) as { run_id: string; last_seq: number; state: string } | undefined;
    if (!row) return undefined;
    return {
      runId: row.run_id,
      lastSeq: row.last_seq,
      state: JSON.parse(row.state) as RunState,
    };
  }

  async acquireRunLease(runId: string, workerId: string, ttlMs: number): Promise<RunLease | undefined> {
    // A single immediate transaction acquires the write lock before reading, so
    // the read-check-write below cannot interleave with another connection's
    // acquisition. A deferred transaction would read a stale snapshot and fail
    // with SQLITE_BUSY_SNAPSHOT instead of serializing.
    const acquire = this.db.transaction((): RunLease | undefined => {
      const existing = this.db.prepare(`
        SELECT run_id, worker_id, fencing_token, expires_at
        FROM continuum_leases
        WHERE run_id = ?
      `).get(runId) as LeaseRow | undefined;
      // Expiry is evaluated against the database clock, not this process's wall
      // clock, so host clock skew cannot steal or extend a live lease.
      const clock = this.db.prepare(`SELECT ${DATABASE_NOW_MS} AS now`).get() as { now: number } | undefined;
      if (clock === undefined) throw new Error("failed to read the database clock");
      const now = clock.now;
      if (existing && existing.expires_at > now) return undefined;
      const fencingToken = (existing?.fencing_token ?? 0) + 1;
      const expiresAt = now + ttlMs;
      if (existing) {
        this.db.prepare(`
          UPDATE continuum_leases
          SET worker_id = ?, fencing_token = ?, expires_at = ?
          WHERE run_id = ?
        `).run(workerId, fencingToken, expiresAt, runId);
      } else {
        this.db.prepare(`
          INSERT INTO continuum_leases (run_id, worker_id, fencing_token, expires_at)
          VALUES (?, ?, ?, ?)
        `).run(runId, workerId, fencingToken, expiresAt);
      }
      return { runId, workerId, fencingToken, expiresAt };
    });
    return acquire.immediate();
  }

  async renewRunLease(lease: RunLease, ttlMs: number): Promise<boolean> {
    const result = this.db.prepare(`
      UPDATE continuum_leases
      SET expires_at = ${DATABASE_NOW_MS} + ?
      WHERE run_id = ? AND worker_id = ? AND fencing_token = ?
    `).run(ttlMs, lease.runId, lease.workerId, lease.fencingToken);
    return result.changes === 1;
  }

  async releaseRunLease(lease: RunLease): Promise<void> {
    this.db.prepare(`
      UPDATE continuum_leases
      SET expires_at = 0, fencing_token = fencing_token + 1
      WHERE run_id = ? AND worker_id = ? AND fencing_token = ?
    `).run(lease.runId, lease.workerId, lease.fencingToken);
  }

  async listRunLeases(): Promise<RunLease[]> {
    const rows = this.db.prepare(`
      SELECT run_id, worker_id, fencing_token, expires_at
      FROM continuum_leases
      ORDER BY run_id ASC
    `).all() as unknown as LeaseRow[];
    return rows.map(rowToLease);
  }

  async close(): Promise<void> {
    if (!this.owned) return;
    this.db.close();
    this.owned = false;
  }

  /**
   * Roll back without masking the error that triggered the rollback. When
   * `BEGIN IMMEDIATE` itself fails (e.g. SQLITE_BUSY) there is no transaction
   * to roll back, and an unconditional ROLLBACK would replace the real failure
   * with "cannot rollback - no transaction is active".
   */
  private rollbackQuietly(): void {
    if (!this.db.inTransaction) return;
    try {
      this.db.exec("ROLLBACK");
    } catch {
      // Preserve the caller's error; the connection state is not recoverable
      // from here beyond releasing the transaction.
    }
  }

  /**
   * Create the schema. Serialized across processes with `BEGIN IMMEDIATE` and
   * retried on SQLITE_BUSY: two processes opening the same brand-new file at
   * once would otherwise race on the WAL switch, which needs an exclusive lock
   * and fails immediately rather than waiting for the busy timeout.
   */
  private migrate(): void {
    let attempt = 0;
    for (;;) {
      try {
        this.migrateOnce();
        return;
      } catch (error) {
        const busy = (error as { code?: string }).code === "SQLITE_BUSY";
        if (!busy || attempt >= 50) throw error;
        attempt += 1;
        // Synchronous sleep: this runs in the constructor, before the journal
        // is usable, and the schema must exist before anything else touches it.
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10 * attempt);
      }
    }
  }

  private migrateOnce(): void {
    // Connection pragmas must be set outside a transaction; switching an
    // existing database to WAL takes an exclusive lock, so it also needs the
    // busy retry handled by the caller.
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = NORMAL");
    this.db.pragma("foreign_keys = ON");

    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db.exec(`
      CREATE TABLE IF NOT EXISTS continuum_events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        run_id TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        type TEXT NOT NULL,
        at INTEGER NOT NULL,
        data TEXT NOT NULL CHECK (json_valid(data))
      );

      CREATE INDEX IF NOT EXISTS idx_continuum_events_run_seq
        ON continuum_events (run_id, seq);

      CREATE INDEX IF NOT EXISTS idx_continuum_events_type
        ON continuum_events (type);

      CREATE TABLE IF NOT EXISTS continuum_runs (
        run_id TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS continuum_leases (
        run_id TEXT PRIMARY KEY,
        worker_id TEXT NOT NULL,
        fencing_token INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_continuum_leases_expiry
        ON continuum_leases (expires_at);

      CREATE TABLE IF NOT EXISTS continuum_snapshots (
        run_id TEXT PRIMARY KEY,
        last_seq INTEGER NOT NULL,
        state TEXT NOT NULL CHECK (json_valid(state))
      );

      -- Durable hash-chain head. One row, id 0. It is advanced in the same
      -- transaction as the events it covers, which is what makes tail
      -- truncation detectable: without a persisted head any prefix of a valid
      -- chain is itself a valid chain.
      CREATE TABLE IF NOT EXISTS continuum_integrity_head (
        id INTEGER PRIMARY KEY CHECK (id = 0),
        counter INTEGER NOT NULL,
        last_hash TEXT NOT NULL,
        CHECK (counter >= 0)
      );

      INSERT OR IGNORE INTO continuum_runs (run_id, created_at)
      SELECT run_id, MIN(at)
      FROM continuum_events
      WHERE type = 'run.created'
      GROUP BY run_id;

      -- Seed the head from the last journaled link when the table is new, so a
      -- database written by an earlier version (or by an adapter-only writer
      -- that predates the head) continues instead of forking at GENESIS.
      INSERT OR IGNORE INTO continuum_integrity_head (id, counter, last_hash)
      SELECT 0,
        COALESCE(
          (SELECT CAST(json_extract(data, '$."__integrity".i') AS INTEGER) + 1
           FROM continuum_events
           ORDER BY seq DESC
           LIMIT 1),
          0
        ),
        COALESCE(
          (SELECT json_extract(data, '$."__integrity".hash')
           FROM continuum_events
           ORDER BY seq DESC
           LIMIT 1),
          ''
        );
      `);
      this.db.exec("COMMIT");
    } catch (error) {
      this.rollbackQuietly();
      throw error;
    }
  }
}

/**
 * In-process async mutex per database file. Node is single-threaded and
 * better-sqlite3 is synchronous, so a second connection that busy-waits for the
 * write lock would stall the very event loop the lock holder needs; queueing
 * transactions cooperatively avoids that self-deadlock. Cross-process
 * serialization is still SQLite's own write lock.
 */
const fileLocks = new Map<string, Promise<void>>();

async function acquireFileLock(key: string): Promise<() => void> {
  const previous = fileLocks.get(key) ?? Promise.resolve();
  let release!: () => void;
  const current = new Promise<void>((resolve) => {
    release = resolve;
  });
  const chained = previous.then(() => current);
  fileLocks.set(key, chained);
  await previous;
  return () => {
    release();
    if (fileLocks.get(key) === chained) fileLocks.delete(key);
  };
}

function rowToEvent(row: EventRow): ContinuumEvent {
  return {
    seq: row.seq,
    eventId: row.event_id,
    runId: row.run_id,
    workflowName: row.workflow_name,
    type: row.type as ContinuumEventType,
    at: row.at,
    data: JSON.parse(row.data) as Record<string, unknown>,
  };
}

function rowToLease(row: LeaseRow): RunLease {
  return {
    runId: row.run_id,
    workerId: row.worker_id,
    fencingToken: row.fencing_token,
    expiresAt: row.expires_at,
  };
}
