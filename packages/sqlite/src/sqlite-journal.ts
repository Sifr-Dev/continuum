import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import type { ContinuumEvent, ContinuumEventType, NewContinuumEvent, WorkflowJournal } from "@continuum/core";

export interface SqliteJournalOptions {
  filename: string;
  verbose?: boolean;
}

interface EventRow {
  seq: number;
  event_id: string;
  run_id: string;
  workflow_name: string;
  type: string;
  at: number;
  data: string;
}

export class SqliteJournal implements WorkflowJournal {
  private readonly db: Database.Database;
  private owned = true;

  constructor(options: SqliteJournalOptions | string) {
    const filename = typeof options === "string" ? options : options.filename;
    this.db = new Database(filename);
    this.migrate();
  }

  static memory(): SqliteJournal {
    return new SqliteJournal(":memory:");
  }

  async append(events: readonly NewContinuumEvent[]): Promise<ContinuumEvent[]> {
    if (events.length === 0) return [];
    const insert = this.db.prepare(`
      INSERT INTO continuum_events (event_id, run_id, workflow_name, type, at, data)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

    const appended: ContinuumEvent[] = [];
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const event of events) {
        const row = {
          eventId: randomUUID(),
          runId: event.runId,
          workflowName: event.workflowName,
          type: event.type,
          at: event.at ?? Date.now(),
          data: event.data,
        };
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
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
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

  async runIds(): Promise<string[]> {
    const rows = this.db.prepare(`
      SELECT run_id
      FROM continuum_events
      GROUP BY run_id
      ORDER BY MIN(seq) ASC
    `).all() as unknown as Array<{ run_id: string }>;
    return rows.map((row) => row.run_id);
  }

  async close(): Promise<void> {
    if (!this.owned) return;
    this.db.close();
    this.owned = false;
  }

  private migrate(): void {
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

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
    `);
  }
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
