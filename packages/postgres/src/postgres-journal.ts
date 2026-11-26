import { randomUUID } from "node:crypto";
import { Pool, type PoolClient, type PoolConfig } from "pg";
import { RunAlreadyExistsError, WorkerLeaseLostError } from "@continuum/core";
import type { AppendEventOptions, ContinuumEvent, ContinuumEventType, NewContinuumEvent, RunLease, RunSnapshot, RunState, WorkflowJournal } from "@continuum/core";

export type PostgresJournalOptions = PoolConfig & {
  /** Existing pools are not closed by the journal. */
  pool?: Pool;
};

interface EventRow {
  seq: number | string;
  event_id: string;
  run_id: string;
  workflow_name: string;
  type: string;
  at: number | string;
  data: unknown;
}

interface LeaseRow {
  run_id: string;
  worker_id: string;
  fencing_token: number | string;
  expires_at: number | string;
}

const MIGRATION_LOCK_KEY = 84192743;

export class PostgresJournal implements WorkflowJournal {
  private readonly pool: Pool;
  private readonly ownsPool: boolean;
  private readonly ready: Promise<void>;
  private closed = false;

  constructor(options: PostgresJournalOptions | string) {
    const { pool, ...poolConfig } = typeof options === "string" ? { connectionString: options } : options;
    this.pool = pool ?? new Pool(poolConfig);
    this.ownsPool = !pool;
    this.ready = this.migrate();
  }

  async append(events: readonly NewContinuumEvent[], options: AppendEventOptions = {}): Promise<ContinuumEvent[]> {
    if (events.length === 0) return [];
    await this.ready;
    const client = await this.pool.connect();
    const appended: ContinuumEvent[] = [];
    try {
      await client.query("BEGIN");
      if (options.lease) {
        const lease = options.lease;
        const leaseCheck = await client.query(
          `SELECT worker_id, fencing_token
           FROM continuum_leases
           WHERE run_id = $1
           FOR UPDATE`,
          [lease.runId],
        );
        const row = leaseCheck.rows[0] as Pick<LeaseRow, "worker_id" | "fencing_token"> | undefined;
        if (
          events.some((event) => event.runId !== lease.runId) ||
          !row ||
          row.worker_id !== lease.workerId ||
          Number(row.fencing_token) !== lease.fencingToken
        ) {
          throw new WorkerLeaseLostError(lease.runId);
        }
      }

      for (const event of events) {
        const eventId = randomUUID();
        const at = event.at ?? Date.now();
        if (event.type === "run.created") {
          const created = await client.query(
            `INSERT INTO continuum_runs (run_id, created_at)
             VALUES ($1, $2)
             ON CONFLICT (run_id) DO NOTHING`,
            [event.runId, at],
          );
          if (created.rowCount !== 1) throw new RunAlreadyExistsError(event.runId);
        }
        const inserted = await client.query<{ seq: number | string }>(
          `INSERT INTO continuum_events (event_id, run_id, workflow_name, type, at, data)
           VALUES ($1, $2, $3, $4, $5, $6::jsonb)
           RETURNING seq`,
          [eventId, event.runId, event.workflowName, event.type, at, JSON.stringify(event.data)],
        );
        appended.push({
          seq: Number(inserted.rows[0]?.seq),
          eventId,
          runId: event.runId,
          workflowName: event.workflowName,
          type: event.type,
          at,
          data: structuredClone(event.data),
        });
      }
      await client.query("COMMIT");
      return appended;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async events(runId: string): Promise<ContinuumEvent[]> {
    await this.ready;
    const result = await this.pool.query<EventRow>(
      `SELECT seq, event_id, run_id, workflow_name, type, at, data
       FROM continuum_events
       WHERE run_id = $1
       ORDER BY seq ASC`,
      [runId],
    );
    return result.rows.map(rowToEvent);
  }

  async eventsAfter(runId: string, afterSeq: number): Promise<ContinuumEvent[]> {
    await this.ready;
    const result = await this.pool.query<EventRow>(
      `SELECT seq, event_id, run_id, workflow_name, type, at, data
       FROM continuum_events
       WHERE run_id = $1 AND seq > $2
       ORDER BY seq ASC`,
      [runId, afterSeq],
    );
    return result.rows.map(rowToEvent);
  }

  async allEvents(): Promise<ContinuumEvent[]> {
    await this.ready;
    const result = await this.pool.query<EventRow>(
      `SELECT seq, event_id, run_id, workflow_name, type, at, data
       FROM continuum_events
       ORDER BY seq ASC`,
    );
    return result.rows.map(rowToEvent);
  }

  async runIds(): Promise<string[]> {
    await this.ready;
    const result = await this.pool.query<{ run_id: string }>(
      `SELECT run_id
       FROM continuum_runs
       ORDER BY created_at ASC, run_id ASC`,
    );
    return result.rows.map((row) => row.run_id);
  }

  async saveRunSnapshot(snapshot: RunSnapshot): Promise<void> {
    await this.ready;
    await this.pool.query(
      `INSERT INTO continuum_snapshots (run_id, last_seq, state)
       VALUES ($1, $2, $3::jsonb)
       ON CONFLICT (run_id) DO UPDATE
       SET last_seq = EXCLUDED.last_seq, state = EXCLUDED.state
       WHERE EXCLUDED.last_seq >= continuum_snapshots.last_seq`,
      [snapshot.runId, snapshot.lastSeq, JSON.stringify(snapshot.state)],
    );
  }

  async latestRunSnapshot(runId: string): Promise<RunSnapshot | undefined> {
    await this.ready;
    const result = await this.pool.query<{ run_id: string; last_seq: string | number; state: RunState }>(
      `SELECT run_id, last_seq, state FROM continuum_snapshots WHERE run_id = $1`,
      [runId],
    );
    const row = result.rows[0];
    if (!row) return undefined;
    return { runId: row.run_id, lastSeq: Number(row.last_seq), state: row.state };
  }

  async acquireRunLease(runId: string, workerId: string, ttlMs: number): Promise<RunLease | undefined> {
    await this.ready;
    const now = Date.now();
    const result = await this.pool.query<LeaseRow>(
      `INSERT INTO continuum_leases (run_id, worker_id, fencing_token, expires_at)
       VALUES ($1, $2, 1, $3)
       ON CONFLICT (run_id) DO UPDATE
       SET worker_id = EXCLUDED.worker_id,
           fencing_token = continuum_leases.fencing_token + 1,
           expires_at = EXCLUDED.expires_at
       WHERE continuum_leases.expires_at <= $4
       RETURNING run_id, worker_id, fencing_token, expires_at`,
      [runId, workerId, now + ttlMs, now],
    );
    const row = result.rows[0];
    return row ? rowToLease(row) : undefined;
  }

  async renewRunLease(lease: RunLease, ttlMs: number): Promise<boolean> {
    await this.ready;
    const result = await this.pool.query(
      `UPDATE continuum_leases
       SET expires_at = $4
       WHERE run_id = $1 AND worker_id = $2 AND fencing_token = $3`,
      [lease.runId, lease.workerId, lease.fencingToken, Date.now() + ttlMs],
    );
    return result.rowCount === 1;
  }

  async releaseRunLease(lease: RunLease): Promise<void> {
    await this.ready;
    await this.pool.query(
      `UPDATE continuum_leases
       SET expires_at = 0, fencing_token = fencing_token + 1
       WHERE run_id = $1 AND worker_id = $2 AND fencing_token = $3`,
      [lease.runId, lease.workerId, lease.fencingToken],
    );
  }

  async listRunLeases(): Promise<RunLease[]> {
    await this.ready;
    const result = await this.pool.query<LeaseRow>(
      `SELECT run_id, worker_id, fencing_token, expires_at
       FROM continuum_leases
       ORDER BY run_id ASC`,
    );
    return result.rows.map(rowToLease);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.ready.catch(() => undefined);
    if (this.ownsPool) await this.pool.end();
  }

  private async migrate(): Promise<void> {
    const client: PoolClient = await this.pool.connect();
    try {
      await client.query("SELECT pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
      await client.query(`
        CREATE TABLE IF NOT EXISTS continuum_events (
          seq BIGSERIAL PRIMARY KEY,
          event_id TEXT NOT NULL UNIQUE,
          run_id TEXT NOT NULL,
          workflow_name TEXT NOT NULL,
          type TEXT NOT NULL,
          at BIGINT NOT NULL,
          data JSONB NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_continuum_events_run_seq
          ON continuum_events (run_id, seq);

        CREATE INDEX IF NOT EXISTS idx_continuum_events_type
          ON continuum_events (type);

        CREATE TABLE IF NOT EXISTS continuum_runs (
          run_id TEXT PRIMARY KEY,
          created_at BIGINT NOT NULL
        );

        INSERT INTO continuum_runs (run_id, created_at)
        SELECT run_id, MIN(at)
        FROM continuum_events
        WHERE type = 'run.created'
        GROUP BY run_id
        ON CONFLICT (run_id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS continuum_leases (
          run_id TEXT PRIMARY KEY,
          worker_id TEXT NOT NULL,
          fencing_token BIGINT NOT NULL,
          expires_at BIGINT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_continuum_leases_expiry
          ON continuum_leases (expires_at);

        CREATE TABLE IF NOT EXISTS continuum_snapshots (
          run_id TEXT PRIMARY KEY,
          last_seq BIGINT NOT NULL,
          state JSONB NOT NULL
        );
      `);
    } finally {
      await client.query("SELECT pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]).catch(() => undefined);
      client.release();
    }
  }
}

function rowToEvent(row: EventRow): ContinuumEvent {
  return {
    seq: Number(row.seq),
    eventId: row.event_id,
    runId: row.run_id,
    workflowName: row.workflow_name,
    type: row.type as ContinuumEventType,
    at: Number(row.at),
    data: readJsonObject(row.data),
  };
}

function rowToLease(row: LeaseRow): RunLease {
  return {
    runId: row.run_id,
    workerId: row.worker_id,
    fencingToken: Number(row.fencing_token),
    expiresAt: Number(row.expires_at),
  };
}

function readJsonObject(value: unknown): Record<string, unknown> {
  if (typeof value === "string") return JSON.parse(value) as Record<string, unknown>;
  if (typeof value === "object" && value !== null && !Array.isArray(value)) return value as Record<string, unknown>;
  return {};
}
