import { randomUUID } from "node:crypto";
import type { ContinuumEvent, NewContinuumEvent, WorkflowJournal } from "./types.js";

export class MemoryJournal implements WorkflowJournal {
  private readonly rows: ContinuumEvent[] = [];
  private seq = 0;

  async append(events: readonly NewContinuumEvent[]): Promise<ContinuumEvent[]> {
    const createdInBatch = new Set<string>();
    for (const event of events) {
      if (event.type !== "run.created") continue;
      if (createdInBatch.has(event.runId) || this.rows.some((row) => row.runId === event.runId && row.type === "run.created")) {
        throw new Error(`Run "${event.runId}" already exists`);
      }
      createdInBatch.add(event.runId);
    }

    const appended: ContinuumEvent[] = [];
    for (const event of events) {
      const stored: ContinuumEvent = {
        ...event,
        data: structuredClone(event.data),
        seq: ++this.seq,
        eventId: randomUUID(),
        at: event.at ?? Date.now(),
      };
      this.rows.push(stored);
      appended.push(cloneEvent(stored));
    }
    return appended;
  }

  async events(runId: string): Promise<ContinuumEvent[]> {
    return this.rows.filter((event) => event.runId === runId).sort((a, b) => a.seq - b.seq).map(cloneEvent);
  }

  async allEvents(): Promise<ContinuumEvent[]> {
    return [...this.rows].sort((a, b) => a.seq - b.seq).map(cloneEvent);
  }

  async runIds(): Promise<string[]> {
    return [...new Set(this.rows.map((event) => event.runId))];
  }
}

function cloneEvent(event: ContinuumEvent): ContinuumEvent {
  return { ...event, data: structuredClone(event.data) };
}
