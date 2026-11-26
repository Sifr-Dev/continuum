import { createHash } from "node:crypto";
import type {
  AppendEventOptions,
  ContinuumEvent,
  IntegrityChainHead,
  JournalTransaction,
  NewContinuumEvent,
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
 *   - reordering, deleting, or duplicating an event breaks the prev/hash linkage,
 *   - truncation is detected against the durable chain head (see below).
 *
 * ## Where the chain state lives
 *
 * The link itself is persisted inside the event payload, so it survives
 * process restarts and travels with exports. The chain *head* — how many links
 * have been committed and the hash of the last one — is what makes tail
 * deletion detectable, because any prefix of a valid chain is itself a valid
 * chain. When the underlying journal implements `transaction()`, the decorator
 * reads the persisted head, appends the stamped events, and advances the head
 * inside a single database transaction, serialized by the database against
 * every other writer — including writers in *other processes*. Two processes
 * sharing one SQLite file or one Postgres database therefore extend one chain
 * instead of forking it.
 *
 * Journals without `transaction()` (for example {@link MemoryJournal}) fall
 * back to in-process serialization: correct within one process, but several
 * processes sharing such a store can still fork the chain, and tail truncation
 * cannot be detected because no durable head exists. `verify()` reports that
 * honestly rather than pretending otherwise.
 *
 * ## What this proves, and what it does not
 *
 * The chain uses **unkeyed SHA-256**. It is *tamper-evidence*, not
 * *authenticity*:
 *
 *   - it proves a history is internally consistent and that no event was
 *     edited, reordered, removed from the middle, or removed from the end
 *     after the fact — as long as the durable head survives;
 *   - it does **not** prove who wrote an event, and it does **not** stop
 *     someone with write access from recomputing the entire chain (and head)
 *     from scratch with the same public algorithm. Anyone who can rewrite the
 *     events can produce a different, perfectly valid chain.
 *
 * Authenticity needs a secret or an external anchor: an HMAC with a key held
 * outside the database, a signature by a private key, or periodically
 * publishing the head hash to append-only storage the attacker cannot rewrite.
 * Treat this as defense in depth over database access controls, not a
 * replacement for them.
 */

const GENESIS = createHash("sha256").update("continuum:integrity:genesis:v1").digest("hex");
const INTEGRITY_KEY = "__integrity";

/** Raised when the journal's durable chain state cannot be extended safely. */
export class IntegrityChainError extends Error {
  readonly code = "INTEGRITY_CHAIN_ERROR";

  constructor(message: string) {
    super(message);
    this.name = "IntegrityChainError";
  }
}

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
  /**
   * True when the journal holds no events at all. An empty journal is never
   * reported as a verified non-empty chain: when a durable head says links
   * existed, emptiness is a truncation failure.
   */
  empty?: boolean;
  /** Number of links the durable chain head records, when the journal keeps one. */
  expected?: number;
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

function assertUnstamped(event: NewContinuumEvent): void {
  if (Object.prototype.hasOwnProperty.call(event.data ?? {}, INTEGRITY_KEY)) {
    throw new IntegrityChainError(
      "event data already contains an integrity link; do not nest IntegrityJournal decorators or append pre-stamped events",
    );
  }
}

export class IntegrityJournal implements WorkflowJournal {
  private readonly inner: WorkflowJournal;
  private readonly now: () => number;
  /** In-process chain state, used only for journals without `transaction()`. */
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
    if (inner.eventsAfter) this.eventsAfter = inner.eventsAfter.bind(inner);
    if (inner.saveRunSnapshot) this.saveRunSnapshot = inner.saveRunSnapshot.bind(inner);
    if (inner.latestRunSnapshot) this.latestRunSnapshot = inner.latestRunSnapshot.bind(inner);
    if (inner.integrityHead) this.integrityHead = inner.integrityHead.bind(inner);
    if (inner.close) this.close = inner.close.bind(inner);
  }

  append(events: readonly NewContinuumEvent[], options?: AppendEventOptions): Promise<ContinuumEvent[]> {
    // Serialize appends within this decorator. Cross-process serialization is
    // the adapter transaction's job; this queue additionally keeps one
    // decorator's writes ordered and avoids piling concurrent writers onto the
    // same connection (important for synchronous SQLite connections).
    const run = this.queue.then(
      () => this.appendOne(events, options),
      () => this.appendOne(events, options),
    );
    // Keep the queue alive regardless of success/failure of this batch.
    this.queue = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async appendOne(
    events: readonly NewContinuumEvent[],
    options?: AppendEventOptions,
  ): Promise<ContinuumEvent[]> {
    if (events.length === 0) return this.inner.append(events, options);
    for (const event of events) assertUnstamped(event);
    if (this.inner.transaction) return this.appendTransactional(events, options);
    return this.appendFallback(events, options);
  }

  /**
   * Atomic path: read the durable head, append, and advance the head in ONE
   * transaction on the adapter. A concurrent writer in another process blocks
   * on the database lock (BEGIN IMMEDIATE / SELECT ... FOR UPDATE) and then
   * observes the head this transaction committed, so exactly one chain exists.
   */
  private async appendTransactional(
    events: readonly NewContinuumEvent[],
    options?: AppendEventOptions,
  ): Promise<ContinuumEvent[]> {
    const transaction = this.inner.transaction;
    if (!transaction) throw new IntegrityChainError("journal lost its transaction capability");
    // `transaction` is generic; bind it without losing inference.
    const runTransaction = transaction.bind(this.inner) as <T>(
      work: (tx: JournalTransaction) => Promise<T>,
      options?: AppendEventOptions,
    ) => Promise<T>;

    return runTransaction(async (tx: JournalTransaction) => {
      const tail = await this.readChainTail(tx);
      let prev = tail.lastHash;
      let i = tail.counter;
      const stamped: NewContinuumEvent[] = [];
      for (const event of events) {
        const at = event.at ?? this.now();
        const data = (event.data ?? {}) as Record<string, unknown>;
        const hash = hashLink(prev, { i, prev, runId: event.runId, workflowName: event.workflowName, type: event.type, at, data });
        stamped.push({ ...event, at, data: { ...data, [INTEGRITY_KEY]: { i, prev, hash } satisfies IntegrityLink } });
        prev = hash;
        i += 1;
      }

      // The adapter applies its normal append semantics (event ids, seq,
      // run.created uniqueness, lease fencing) inside this same transaction.
      const appended = await tx.appendEvents(stamped, options);
      if (appended.length !== stamped.length) {
        throw new IntegrityChainError(`journal appended ${appended.length} of ${stamped.length} events`);
      }
      await tx.writeIntegrityHead({ counter: i, lastHash: prev });
      return appended;
    }, options);
  }

  /**
   * Read the last committed chain position inside a transaction, refusing to
   * extend a chain whose tail disagrees with the durable head. Refusing is the
   * point: silently appending would either fork the chain or mask truncation.
   */
  private async readChainTail(tx: JournalTransaction): Promise<IntegrityChainHead> {
    const head = await tx.readIntegrityHead();
    const last = await tx.lastEvent();

    if (head && head.counter > 0) {
      if (!last) {
        throw new IntegrityChainError(
          `chain head records ${head.counter} link(s) but the journal is empty: the history was truncated`,
        );
      }
      const link = readLink(last);
      if (!link) {
        throw new IntegrityChainError(
          "the last journaled event has no integrity metadata; refusing to extend a chain over rewritten history",
        );
      }
      if (link.i !== head.counter - 1 || link.hash !== head.lastHash) {
        throw new IntegrityChainError(
          `last journaled event is chain link ${link.i} but the durable head expects link ${head.counter - 1}: ` +
            "the tail of the history was truncated, reordered, or replaced",
        );
      }
      return { counter: head.counter, lastHash: head.lastHash };
    }

    if (last) {
      // A head row that was created by migration (counter 0) or lost, over a
      // journal that already contains stamped links: adopt the existing tail
      // instead of forking at GENESIS.
      const link = readLink(last);
      if (!link) {
        throw new IntegrityChainError(
          "the journal already contains events without integrity metadata; refusing to start a chain over them",
        );
      }
      return { counter: link.i + 1, lastHash: link.hash };
    }

    return { counter: 0, lastHash: GENESIS };
  }

  /**
   * Fallback for journals without a transaction boundary: adopt the existing
   * chain once, then serialize appends in this process. Cross-process writers
   * are not protected here; {@link IntegrityJournal} documents that.
   */
  private async appendFallback(
    events: readonly NewContinuumEvent[],
    options?: AppendEventOptions,
  ): Promise<ContinuumEvent[]> {
    await this.ensureInitialized();
    let prev = this.lastHash;
    let i = this.counter;
    const stamped: NewContinuumEvent[] = [];
    for (const event of events) {
      const at = event.at ?? this.now();
      const data = (event.data ?? {}) as Record<string, unknown>;
      const hash = hashLink(prev, { i, prev, runId: event.runId, workflowName: event.workflowName, type: event.type, at, data });
      stamped.push({ ...event, at, data: { ...data, [INTEGRITY_KEY]: { i, prev, hash } satisfies IntegrityLink } });
      prev = hash;
      i += 1;
    }

    const persisted = await this.inner.append(stamped, options);
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
      if (!link) {
        throw new IntegrityChainError(
          "the journal already contains events without integrity metadata; refusing to start a chain over them",
        );
      }
      if (link.i > maxIndex) {
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

  eventsAfter?: WorkflowJournal["eventsAfter"];

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
  saveRunSnapshot?: WorkflowJournal["saveRunSnapshot"];
  latestRunSnapshot?: WorkflowJournal["latestRunSnapshot"];
  integrityHead?: WorkflowJournal["integrityHead"];
  close?: WorkflowJournal["close"];

  /**
   * Verify the entire journal chain, including the durable head when the
   * adapter keeps one. Returns a report rather than throwing so callers can
   * surface integrity failures without crashing a reader.
   */
  async verify(): Promise<IntegrityReport> {
    const { head, events } = await this.readStableChainState();
    return verifyEventChain(events, { head });
  }

  /**
   * Read the head and the full event list so both describe the same committed
   * state. A concurrent append between the two reads would otherwise look like
   * out-of-band tampering; bounded retries avoid that false alarm.
   */
  private async readStableChainState(): Promise<{ head?: IntegrityChainHead; events: ContinuumEvent[] }> {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const before = await this.inner.integrityHead?.();
      const events = await this.inner.allEvents();
      const after = await this.inner.integrityHead?.();
      const stable =
        (before?.counter ?? 0) === (after?.counter ?? 0) && (before?.lastHash ?? "") === (after?.lastHash ?? "");
      if (stable || attempt === 2) return { head: after, events };
    }
    throw new IntegrityChainError("could not read a stable chain state");
  }
}

export interface VerifyEventChainOptions {
  /**
   * Durable chain head to verify against, when the store keeps one. Without it
   * the walk still proves the chain is internally consistent, but tail
   * truncation is undetectable: any prefix of a valid chain is valid.
   */
  head?: IntegrityChainHead;
}

/**
 * Verify a hash chain over a sequence of events. Events are processed in
 * chain-index order so callers may pass them in any order.
 *
 * Passing the durable `head` is what makes truncation — and, specifically,
 * deletion of the newest events — detectable.
 */
export function verifyEventChain(
  events: readonly ContinuumEvent[],
  options: VerifyEventChainOptions = {},
): IntegrityReport {
  const head = options.head;

  if (events.length === 0) {
    // An empty journal is never a valid non-empty chain. When a durable head
    // records committed links, emptiness can only mean the history was
    // truncated; when the head agrees it is empty (or the store keeps no head),
    // report the emptiness instead of claiming events were verified.
    if (head && head.counter > 0) {
      return {
        ok: false,
        verified: 0,
        brokenAt: 0,
        empty: true,
        expected: head.counter,
        reason: `journal is empty but the durable chain head records ${head.counter} link(s): the history was truncated`,
      };
    }
    return {
      ok: true,
      verified: 0,
      empty: true,
      ...(head ? { expected: 0 } : {}),
      reason: head
        ? "journal is empty: the durable chain head records an empty chain"
        : "journal is empty and the store keeps no durable chain head; nothing was verified",
    };
  }

  const links = events
    .map((event, index) => ({ event, index, link: readLink(event) }))
    .filter((entry): entry is { event: ContinuumEvent; index: number; link: IntegrityLink } => entry.link !== undefined);

  if (links.length !== events.length) {
    const missingIndex = events.findIndex((event) => readLink(event) === undefined);
    return { ok: false, verified: 0, brokenAt: missingIndex, reason: "event is missing integrity metadata" };
  }

  if (head && head.counter !== events.length) {
    const truncated = head.counter > events.length;
    return {
      ok: false,
      verified: 0,
      brokenAt: events.length,
      expected: head.counter,
      reason: truncated
        ? `journal has ${events.length} event(s) but the durable chain head records ${head.counter}: the history was truncated`
        : `journal has ${events.length} event(s) but the durable chain head records only ${head.counter}: events were appended outside the integrity chain`,
    };
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

  if (head && head.lastHash !== prev) {
    return {
      ok: false,
      verified: links.length,
      brokenAt: links.length - 1,
      expected: head.counter,
      reason: "final link does not match the durable chain head (the newest events were replaced or truncated)",
    };
  }

  // `expected` is only reported when it adds information (a mismatch); a
  // successful verification has expected === verified by definition.
  return { ok: true, verified: links.length };
}
