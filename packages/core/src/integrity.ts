import { createHash } from "node:crypto";
import type {
  AppendEventOptions,
  ContinuumEvent,
  NewContinuumEvent,
  RunLease,
  WorkflowJournal,
} from "./types.js";

/**
 * Tamper-evident, hash-chained journal decorator.
 *
 * Wraps any {@link WorkflowJournal} and stamps every appended event with a
 * SHA-256 chain link stored under a reserved `data.__integrity` key. Each link
 * commits to the event's content and to the previous link, so the sequence is
 * cryptographically verifiable end to end:
 *
 *   - editing any field of any event breaks its hash,
 *   - reordering, deleting, or duplicating events breaks the prev/hash linkage,
 *   - truncation is detected because the final link no longer matches.
 *
 * The chain is persisted inside the event payload, so integrity survives
 * process restarts and works with any backend (memory, SQLite, Postgres)
 * without adapter changes. Appends are serialized through an internal queue so
 * concurrent writers cannot fork the chain.
 *
 * This is defense in depth, not a replacement for database access controls:
 * it makes tampering *detectable*, not impossible.
 */

const GENESIS = createHash("sha256").update("continuum:integrity:genesis:v1").digest("hex");
const INTEGRITY_KEY = "__integrity";

export interface IntegrityLink {
  /** Monotonic chain index, starting at 0. */
  i: number;
  /** Hex hash of the previous link (GENESIS for the first event). */
  prev: string;
  /** Hex hash of this link. */
  hash: string;
}

export interface IntegrityReport {
  ok: boolean;
  /** Number of links verified before failure (or total when ok). */
  verified: number;
  /** The 0-based index of the first broken link, when not ok. */
  brokenAt?: number;
  reason?: string;
}

export interface IntegrityJournalOptions {
  /** Injectable clock, useful for deterministic tests. Defaults to Date.now. */
  now?: () => number;
}

/** Deterministic JSON with sorted object keys, for stable hashing. */
export function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`).join(",")}}`;
}

function hashLink(prev: string, payload: unknown): string {
  return createHash("sha256").update(prev).update("\n").update(stableStringify(payload)).digest("hex");
}

/** Strip the reserved integrity metadata from an event's data. */
export function stripIntegrity(data: Record<string, unknown>): Record<string, unknown> {
  if (!Object.prototype.hasOwnProperty.call(data, INTEGRITY_KEY)) return data;
  const { [INTEGRITY_KEY]: _ignored, ...rest } = data;
  return rest;
}

function readLink(event: ContinuumEvent): IntegrityLink | undefined {
  const raw = (event.data as Record<string, unknown>)[INTEGRITY_KEY];
  if (raw === undefined) return undefined;
  const link = raw as Partial<IntegrityLink>;
  if (typeof link.i !== "number" || typeof link.prev !== "string" || typeof link.hash !== "string") {
    return undefined;
  }
  return { i: link.i, prev: link.prev, hash: link.hash };
}

/** Content that a link commits to (everything except the link itself). */
function linkPayload(event: ContinuumEvent, i: number, prev: string): unknown {
  return {
    i,
    prev,
    runId: event.runId,
    workflowName: event.workflowName,
    type: event.type,
    at: event.at,
    data: stripIntegrity(event.data as Record<string, unknown>),
  };
}

export class IntegrityJournal implements WorkflowJournal {
  private readonly inner: WorkflowJournal;
  private readonly now: () => number;
  private counter = 0;
  private lastHash = GENESIS;
  private initialized = false;
  private queue: Promise<unknown> = Promise.resolve();

  constructor(inner: WorkflowJournal, options: IntegrityJournalOptions = {}) {
    this.inner = inner;
    this.now = options.now ?? (() => Date.now());
    // Forward optional lease operations only when the inner journal supports
    // them, so the decorator advertises exactly the capabilities it can honor.
    if (inner.acquireRunLease) this.acquireRunLease = inner.acquireRunLease.bind(inner);
    if (inner.renewRunLease) this.renewRunLease = inner.renewRunLease.bind(inner);
    if (inner.releaseRunLease) this.releaseRunLease = inner.releaseRunLease.bind(inner);
    if (inner.listRunLeases) this.listRunLeases = inner.listRunLeases.bind(inner);
    if (inner.close) this.close = inner.close.bind(inner);
  }

  append(events: readonly NewContinuumEvent[], options?: AppendEventOptions): Promise<ContinuumEvent[]> {
    // Serialize appends so the chain order always matches commit order, even
    // when multiple runs append concurrently within one process.
    const run = this.queue.then(() => this.appendSerialized(events, options));
    // Keep the queue alive regardless of success/failure of this batch.
    this.queue = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async appendSerialized(
    events: readonly NewContinuumEvent[],
    options?: AppendEventOptions,
  ): Promise<ContinuumEvent[]> {
    await this.ensureInitialized();
    if (events.length === 0) return this.inner.append(events, options);

    const stamped: NewContinuumEvent[] = [];
    const links: IntegrityLink[] = [];
    let prev = this.lastHash;
    let i = this.counter;
    for (const event of events) {
      const at = event.at ?? this.now();
      const data = (event.data ?? {}) as Record<string, unknown>;
      const hash = hashLink(prev, { i, prev, runId: event.runId, workflowName: event.workflowName, type: event.type, at, data });
      const link: IntegrityLink = { i, prev, hash };
      links.push(link);
      stamped.push({ ...event, at, data: { ...data, [INTEGRITY_KEY]: link } });
      prev = hash;
      i += 1;
    }

    const persisted = await this.inner.append(stamped, options);
    // Commit chain state only after a successful write.
    this.counter = i;
    this.lastHash = prev;
    return persisted;
  }

  /**
   * Adopt the chain state of any events already in the store so a fresh
   * decorator (e.g. after a process restart) continues the existing chain
   * instead of forking it. Runs once, inside the serialized append path.
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;
    const existing = await this.inner.allEvents();
    let maxIndex = -1;
    let lastHash = GENESIS;
    for (const event of existing) {
      const link = readLink(event);
      if (link && link.i > maxIndex) {
        maxIndex = link.i;
        lastHash = link.hash;
      }
    }
    this.counter = maxIndex + 1;
    this.lastHash = lastHash;
    this.initialized = true;
  }

  events(runId: string): Promise<ContinuumEvent[]> {
    return this.inner.events(runId);
  }

  allEvents(): Promise<ContinuumEvent[]> {
    return this.inner.allEvents();
  }

  runIds(): Promise<string[]> {
    return this.inner.runIds();
  }

  acquireRunLease?: WorkflowJournal["acquireRunLease"];
  renewRunLease?: WorkflowJournal["renewRunLease"];
  releaseRunLease?: WorkflowJournal["releaseRunLease"];
  listRunLeases?: WorkflowJournal["listRunLeases"];
  close?: WorkflowJournal["close"];

  /**
   * Verify the entire journal chain. Returns a report rather than throwing so
   * callers can surface integrity failures without crashing a reader.
   */
  async verify(): Promise<IntegrityReport> {
    return verifyEventChain(await this.inner.allEvents());
  }
}

/**
 * Verify a hash chain over a sequence of events. Events are processed in
 * chain-index order so callers may pass them in any order.
 */
export function verifyEventChain(events: readonly ContinuumEvent[]): IntegrityReport {
  const links = events
    .map((event, index) => ({ event, index, link: readLink(event) }))
    .filter((entry): entry is { event: ContinuumEvent; index: number; link: IntegrityLink } => entry.link !== undefined);

  if (links.length !== events.length) {
    const missingIndex = events.findIndex((event) => readLink(event) === undefined);
    return { ok: false, verified: 0, brokenAt: missingIndex, reason: "event is missing integrity metadata" };
  }

  // Process in the caller-provided (persisted seq) order. The chain index must
  // match the position exactly, so any reordering, deletion, or duplication in
  // the store is detected rather than silently tolerated by sorting.
  let prev = GENESIS;
  for (let position = 0; position < links.length; position += 1) {
    const { event, link } = links[position] as { event: ContinuumEvent; link: IntegrityLink };
    if (link.i !== position) {
      return { ok: false, verified: position, brokenAt: position, reason: `expected chain index ${position} but found ${link.i}` };
    }
    if (link.prev !== prev) {
      return { ok: false, verified: position, brokenAt: position, reason: "previous-hash mismatch (reordered, deleted, or duplicated event)" };
    }
    const expected = hashLink(prev, linkPayload(event, link.i, link.prev));
    if (expected !== link.hash) {
      return { ok: false, verified: position, brokenAt: position, reason: "content hash mismatch (event was modified)" };
    }
    prev = link.hash;
  }

  return { ok: true, verified: links.length };
}