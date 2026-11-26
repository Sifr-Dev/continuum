import { randomUUID } from "node:crypto";
import { RunAlreadyExistsError, WorkerLeaseLostError } from "./errors.js";
import type { AppendEventOptions, ContinuumEvent, IntegrityChainHead, JournalTransaction, NewContinuumEvent, RunLease, RunSnapshot, WorkflowJournal } from "./types.js";

export class MemoryJournal implements WorkflowJournal {
  private readonly rows: ContinuumEvent[] = [];
  private readonly leases = new Map<string, RunLease>();
  private readonly snapshots = new Map<string, RunSnapshot>();
  private integrityChainHead: IntegrityChainHead | undefined;
  private chain: Promise<unknown> = Promise.resolve();
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

  /**
   * In-process transaction. There is no shared store to serialize against, so
   * this only guarantees that the callback sees one consistent state and that
   * an exception discards everything it wrote.
   */
  async transaction<T>(work: (tx: JournalTransaction) => Promise<T>, options: AppendEventOptions = {}): Promise<T> {
    let release = (): void => undefined;
    const previous = this.chain;
    this.chain = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    const rowsBefore = this.rows.length;
    const seqBefore = this.seq;
    const headBefore = this.integrityChainHead;
    const leasesBefore = new Map([...this.leases].map(([key, lease]) => [key, { ...lease }]));
    try {
      return await work({
        appendEvents: (events, appendOptions) => this.append(events, appendOptions ?? options),
        readIntegrityHead: async () => (this.integrityChainHead ? { ...this.integrityChainHead } : undefined),
        writeIntegrityHead: async (head) => {
          this.integrityChainHead = { counter: head.counter, lastHash: head.lastHash };
        },
        lastEvent: async () => {
          const last = this.rows[this.rows.length - 1];
          return last ? cloneEvent(last) : undefined;
        },
        allEvents: () => this.allEvents(),
      });
    } catch (error) {
      this.rows.length = rowsBefore;
      this.seq = seqBefore;
      this.integrityChainHead = headBefore;
      this.leases.clear();
      for (const [key, lease] of leasesBefore) this.leases.set(key, lease);
      throw error;
    } finally {
      release();
    }
  }

  async integrityHead(): Promise<IntegrityChainHead | undefined> {
    return this.integrityChainHead ? { ...this.integrityChainHead } : undefined;
  }

  async events(runId: string): Promise<ContinuumEvent[]> {
    return this.rows.filter((event) => event.runId === runId).sort((a, b) => a.seq - b.seq).map(cloneEvent);
  }

  async eventsAfter(runId: string, afterSeq: number): Promise<ContinuumEvent[]> {
    return this.rows
      .filter((event) => event.runId === runId && event.seq > afterSeq)
      .sort((a, b) => a.seq - b.seq)
      .map(cloneEvent);
  }

  async allEvents(): Promise<ContinuumEvent[]> {
    return [...this.rows].sort((a, b) => a.seq - b.seq).map(cloneEvent);
  }

  async runIds(): Promise<string[]> {
    return [...new Set(this.rows.map((event) => event.runId))];
  }

  async saveRunSnapshot(snapshot: RunSnapshot): Promise<void> {
    const existing = this.snapshots.get(snapshot.runId);
    if (!existing || snapshot.lastSeq >= existing.lastSeq) {
      this.snapshots.set(snapshot.runId, structuredClone(snapshot));
    }
  }

  async latestRunSnapshot(runId: string): Promise<RunSnapshot | undefined> {
    const snapshot = this.snapshots.get(runId);
    return snapshot ? structuredClone(snapshot) : undefined;
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
