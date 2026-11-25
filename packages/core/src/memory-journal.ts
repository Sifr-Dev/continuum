import { randomUUID } from "node:crypto";
import { RunAlreadyExistsError, WorkerLeaseLostError } from "./errors.js";
import type { AppendEventOptions, ContinuumEvent, NewContinuumEvent, RunLease, WorkflowJournal } from "./types.js";

export class MemoryJournal implements WorkflowJournal {
  private readonly rows: ContinuumEvent[] = [];
  private readonly leases = new Map<string, RunLease>();
  private seq = 0;

  async append(events: readonly NewContinuumEvent[], options: AppendEventOptions = {}): Promise<ContinuumEvent[]> {
    if (options.lease) {
      const lease = options.lease;
      const current = this.leases.get(lease.runId);
      if (
        events.some((event) => event.runId !== lease.runId) ||
        !current ||
        current.workerId !== lease.workerId ||
        current.fencingToken !== lease.fencingToken
      ) {
        throw new WorkerLeaseLostError(lease.runId);
      }
    }

    const createdInBatch = new Set<string>();
    for (const event of events) {
      if (event.type !== "run.created") continue;
      if (createdInBatch.has(event.runId) || this.rows.some((row) => row.runId === event.runId && row.type === "run.created")) {
        throw new RunAlreadyExistsError(event.runId);
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

  async acquireRunLease(runId: string, workerId: string, ttlMs: number): Promise<RunLease | undefined> {
    const now = Date.now();
    const existing = this.leases.get(runId);
    if (existing && existing.expiresAt > now) return undefined;
    const lease: RunLease = {
      runId,
      workerId,
      fencingToken: (existing?.fencingToken ?? 0) + 1,
      expiresAt: now + ttlMs,
    };
    this.leases.set(runId, lease);
    return { ...lease };
  }

  async renewRunLease(lease: RunLease, ttlMs: number): Promise<boolean> {
    const existing = this.leases.get(lease.runId);
    if (!existing || existing.workerId !== lease.workerId || existing.fencingToken !== lease.fencingToken) return false;
    existing.expiresAt = Date.now() + ttlMs;
    return true;
  }

  async releaseRunLease(lease: RunLease): Promise<void> {
    const existing = this.leases.get(lease.runId);
    if (existing?.workerId === lease.workerId && existing.fencingToken === lease.fencingToken) {
      existing.expiresAt = 0;
      existing.fencingToken += 1;
    }
  }

  async listRunLeases(): Promise<RunLease[]> {
    return [...this.leases.values()].map((lease) => ({ ...lease }));
  }
}

function cloneEvent(event: ContinuumEvent): ContinuumEvent {
  return { ...event, data: structuredClone(event.data) };
}
