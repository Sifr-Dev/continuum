import { randomUUID } from "node:crypto";
import type { ContinuumEvent, NewContinuumEvent, WorkflowJournal } from "./types.js";

export class MemoryJournal implements WorkflowJournal {
  private readonly rows: ContinuumEvent[] = [];
  private seq = 0;

  async append(events: readonly NewContinuumEvent[]): Promise<ContinuumEvent[]> {
    const appended = events.map((event) => ({
      ...event,
      seq: ++this.seq,
      eventId: randomUUID(),
      at: event.at ?? Date.now(),
    }));
    this.rows.push(...appended);
    return appended;
  }

  async events(runId: string): Promise<ContinuumEvent[]> {
    return this.rows.filter((event) => event.runId === runId).sort((a, b) => a.seq - b.seq);
  }

  async allEvents(): Promise<ContinuumEvent[]> {
    return [...this.rows].sort((a, b) => a.seq - b.seq);
  }

  async runIds(): Promise<string[]> {
    return [...new Set(this.rows.map((event) => event.runId))];
  }
}
