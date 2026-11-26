export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };
export type MaybePromise<T> = T | Promise<T>;

export interface RetryPolicy {
  /** Total attempts, including the first attempt. Defaults to 1. */
  attempts?: number;
  /** Initial delay before a retry. Defaults to 100ms. */
  delayMs?: number;
  /** Fixed or exponential backoff. Defaults to exponential. */
  strategy?: "fixed" | "exponential";
  /** Upper bound for a retry delay. Defaults to 30 seconds. */
  maxDelayMs?: number;
  /** Adds random jitter so concurrent workers do not retry in lockstep. Defaults to true. */
  jitter?: boolean;
}

export interface ResolvedRetryPolicy {
  attempts: number;
  delayMs: number;
  strategy: "fixed" | "exponential";
  maxDelayMs: number;
  jitter: boolean;
}

export interface StartChildWorkflowOptions {
  /** Deterministic child run id. Defaults to parent-run-id:parent-step-name. */
  runId?: string;
  metadata?: JsonObject;
  /** Request child cancellation when the parent step is interrupted. Defaults to true. */
  cancelWithParent?: boolean;
}

export interface StepExecutionArgs<Input, Outputs, Context> {
  input: Input;
  outputs: Readonly<Partial<Outputs>>;
  context: Context;
  runId: string;
  stepName: string;
  attempt: number;
  idempotencyKey?: string;
  signal: AbortSignal;
  /** Latest durable payload for waitForSignal steps. */
  signalPayload?: unknown;
  now: () => number;
  emit: (name: string, payload?: JsonObject) => Promise<void>;
  /**
   * Starts or reattaches to a deterministic child workflow and waits for its output.
   * Retries after a worker crash reattach to the same child instead of duplicating it.
   */
  startChildWorkflow: <ChildInput, ChildOutput>(
    definition: WorkflowDefinition<ChildInput, any, any>,
    input: ChildInput,
    options?: StartChildWorkflowOptions,
  ) => Promise<ChildOutput>;
}

export interface CompensationArgs<Input, StepOutput, Outputs, Context> {
  input: Input;
  output: StepOutput;
  outputs: Readonly<Partial<Outputs>>;
  context: Context;
  runId: string;
  stepName: string;
  attempt: number;
  idempotencyKey?: string;
  signal: AbortSignal;
  now: () => number;
  emit: (name: string, payload?: JsonObject) => Promise<void>;
}

export interface StepDefinition<
  Input = unknown,
  StepOutput = unknown,
  Outputs = Record<string, unknown>,
  Context = unknown,
> {
  name: string;
  description?: string;
  needs?: readonly string[];
  retry?: RetryPolicy;
  compensationRetry?: RetryPolicy;
  timeoutMs?: number;
  compensationTimeoutMs?: number;
  idempotencyKey?: string | ((input: Input) => string);
  /** Durable delay after dependencies complete before the first attempt is eligible. */
  delayMs?: number;
  /** Durable external signal that must arrive before this step becomes eligible. */
  waitForSignal?: string;
  /** Pure conditional gate evaluated after ordering dependencies complete. */
  when?: (args: { input: Input; outputs: Readonly<Partial<Outputs>> }) => boolean;
  /**
   * Set false for pure validation/computation. The dashboard uses this to
   * distinguish externally visible effects from derivation steps.
   */
  sideEffect?: boolean;
  execute: (args: StepExecutionArgs<Input, Outputs, Context>) => MaybePromise<StepOutput>;
  compensate?: (args: CompensationArgs<Input, StepOutput, Outputs, Context>) => MaybePromise<void>;
}

export type AnyStepDefinition = StepDefinition<any, any, any, any>;

export type StepOutputs<Steps extends readonly AnyStepDefinition[]> = {
  [S in Steps[number] as S["name"]]: S extends StepDefinition<any, infer Output, any, any> ? Output : never;
};

export interface WorkflowSchedule<Input = unknown> {
  /**
   * Fixed production interval in milliseconds. Deterministic window run ids
   * make distributed scheduling safe. Mutually exclusive with `cron`; one of
   * the two must be provided.
   */
  intervalMs?: number;
  /**
   * Standard 5-field cron expression (minute hour day-of-month month
   * day-of-week), evaluated in UTC. When set, it takes precedence over
   * `intervalMs` and produces one deterministic window per cron occurrence.
   */
  cron?: string;
  input: Input;
  /** Number of missed preceding windows to backfill when a worker starts. Defaults to 0. */
  catchupWindows?: number;
  metadata?: JsonObject;
}

/**
 * A pure, synchronous read of projected run state, evaluated on demand against
 * the journal. Queries never mutate anything and never replay user code, so
 * they are safe to call at any time — including while the run is live on a
 * different worker.
 */
export type WorkflowQuery<Input = unknown, Outputs = Record<string, unknown>> = (args: {
  input: Input;
  outputs: Readonly<Partial<Outputs>>;
  signals: Readonly<Record<string, unknown>>;
  status: RunStatus;
  steps: Readonly<Record<string, StepState>>;
  runId: string;
}) => unknown;

export interface WorkflowDefinition<
  Input = unknown,
  Outputs = Record<string, unknown>,
  Context = unknown,
> {
  name: string;
  version: number;
  description?: string;
  steps: readonly AnyStepDefinition[];
  validateInput?: (input: unknown) => Input;
  schedule?: WorkflowSchedule<Input>;
  output?: (args: { input: Input; outputs: Readonly<Partial<Outputs>>; runId: string }) => unknown;
  /** Named pure read-only queries over projected run state. */
  queries?: Record<string, WorkflowQuery<Input, Outputs>>;
  /**
   * Earlier versions of this workflow whose in-flight runs this definition may
   * adopt. Migration is explicit and conservative: a run is only migrated when
   * every step that may still execute or compensate exists under the same name
   * in this definition. The journaled step plan is never retrofitted — migrated
   * runs finish their original graph with the new code, and the migration is
   * recorded in the journal as `run.migrated`.
   */
  migratesFrom?: number[];
}

export type AnyWorkflowDefinition = WorkflowDefinition<any, any, any>;

export type RunStatus =
  | "running"
  | "compensating"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "compensated"
  | "compensation_failed";

export type StepStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "skipped"
  | "compensating"
  | "compensated"
  | "compensation_pending"
  | "compensation_failed"
  | "compensation_skipped";

export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
  code?: string;
  retryable?: boolean;
  details?: JsonObject;
}

export interface StepPlan {
  name: string;
  needs: string[];
  idempotencyKey?: string;
  hasCompensation: boolean;
  sideEffect: boolean;
  delayMs?: number;
  waitForSignal?: string;
  description?: string;
}

export type ContinuumEventType =
  | "run.created"
  | "run.started"
  | "run.cancel_requested"
  | "run.signaled"
  | "run.migrated"
  | "run.succeeded"
  | "run.failed"
  | "run.cancelled"
  | "run.compensated"
  | "run.compensation_failed"
  | "step.started"
  | "step.succeeded"
  | "step.failed"
  | "step.interrupted"
  | "step.skipped"
  | "compensation.started"
  | "compensation.step_started"
  | "compensation.step_completed"
  | "compensation.step_failed"
  | "compensation.step_interrupted"
  | "compensation.step_skipped"
  | "compensation.completed"
  | "compensation.failed"
  | "custom";

export interface ContinuumEvent<Data extends Record<string, unknown> = Record<string, unknown>> {
  seq: number;
  eventId: string;
  runId: string;
  workflowName: string;
  type: ContinuumEventType;
  at: number;
  data: Data;
}

export type NewContinuumEvent<Data extends Record<string, unknown> = Record<string, unknown>> = Omit<ContinuumEvent<Data>, "seq" | "eventId" | "at"> & {
  at?: number;
};

export interface StepState {
  name: string;
  needs: string[];
  status: StepStatus;
  attempts: number;
  compensationAttempts: number;
  idempotencyKey?: string;
  output?: unknown;
  error?: SerializedError;
  compensationError?: SerializedError;
  nextAttemptAt?: number;
  nextCompensationAttemptAt?: number;
  startedAt?: number;
  completedAt?: number;
  durationMs?: number;
  hasCompensation: boolean;
  sideEffect: boolean;
  delayMs?: number;
  waitForSignal?: string;
  description?: string;
}

export interface RunState {
  runId: string;
  workflowName: string;
  version: number;
  input: unknown;
  status: RunStatus;
  createdAt: number;
  updatedAt: number;
  startedAt?: number;
  completedAt?: number;
  cancellationRequested: boolean;
  cancellationReason?: string;
  compensationStarted: boolean;
  compensationReason?: string;
  error?: SerializedError;
  output?: unknown;
  steps: Record<string, StepState>;
  /** Latest payload received for each durable external signal. */
  signals: Record<string, unknown>;
  eventCount: number;
}

export interface RunSummary {
  runId: string;
  workflowName: string;
  version: number;
  status: RunStatus;
  createdAt: number;
  updatedAt: number;
  completedAt?: number;
  eventCount: number;
  error?: SerializedError;
}

export interface RunLease {
  runId: string;
  workerId: string;
  fencingToken: number;
  expiresAt: number;
}

/**
 * A materialized projection of a run at a specific journal position. Storing
 * a snapshot lets readers resume from `lastSeq` and fold only the newer tail
 * of events instead of replaying the full history — the key optimization for
 * long-lived workflows with thousands of events.
 */
export interface RunSnapshot {
  runId: string;
  /** Highest event seq folded into `state` (inclusive). */
  lastSeq: number;
  /** Projected run state as of `lastSeq`. */
  state: RunState;
}

export interface AppendEventOptions {
  /** When present, the journal must atomically verify this fencing token while writing. */
  lease?: RunLease;
}

/**
 * Durable position of a hash chain, persisted by a journal adapter in the same
 * transaction as the events it covers. `counter` is the number of committed
 * chain links and `lastHash` is the hash of the most recent link.
 *
 * A persisted head is what makes truncation — especially deletion of the tail
 * of the history — detectable: without it, any prefix of a valid chain is
 * itself a valid chain.
 */
export interface IntegrityChainHead {
  counter: number;
  lastHash: string;
}

/**
 * Work performed inside ONE durable journal transaction.
 *
 * Every method runs on the transaction's connection and snapshot, so reads and
 * writes observe a single consistent state and commit or roll back together.
 * This is the boundary a hash chain needs: read the chain head, append the
 * stamped events, and advance the head in one atomic unit, serialized against
 * every other writer of the store — including writers in other processes.
 */
export interface JournalTransaction {
  /**
   * Append events with the journal's normal semantics — generated event ids,
   * seq assignment, `run.created` uniqueness, and lease fencing — inside this
   * transaction. `options` defaults to the options passed to `transaction`.
   */
  appendEvents(events: readonly NewContinuumEvent[], options?: AppendEventOptions): Promise<ContinuumEvent[]>;
  /** Persisted chain head, or undefined when the store has never sealed one. */
  readIntegrityHead(): Promise<IntegrityChainHead | undefined>;
  /** Persist a chain head; must be atomic with the covered appends. */
  writeIntegrityHead(head: IntegrityChainHead): Promise<void>;
  /** Most recent event in seq order, or undefined for an empty store. */
  lastEvent(): Promise<ContinuumEvent | undefined>;
  /** All events in seq order, read inside the transaction. */
  allEvents(): Promise<ContinuumEvent[]>;
}

export interface WorkflowJournal {
  append(events: readonly NewContinuumEvent[], options?: AppendEventOptions): Promise<ContinuumEvent[]>;
  events(runId: string): Promise<ContinuumEvent[]>;
  /** Read only events with seq greater than `afterSeq` (snapshot tail reads). */
  eventsAfter?(runId: string, afterSeq: number): Promise<ContinuumEvent[]>;
  allEvents(): Promise<ContinuumEvent[]>;
  runIds(): Promise<string[]>;
  /**
   * Run `work` inside one durable transaction that is serialized against every
   * other writer of the same store, including writers in other processes.
   *
   * Optional: journals that cannot provide a cross-process transaction
   * boundary omit it, and `IntegrityJournal` then falls back to in-process
   * serialization, which cannot protect a chain shared by several processes.
   */
  transaction?<T>(work: (tx: JournalTransaction) => Promise<T>, options?: AppendEventOptions): Promise<T>;
  /**
   * Durable hash-chain head, when the adapter persists one. Used by
   * `IntegrityJournal.verify()` to prove the history was not truncated.
   */
  integrityHead?(): Promise<IntegrityChainHead | undefined>;
  /** Atomically acquire or re-acquire a run worker lease. Implementations must be safe across processes. */
  acquireRunLease?(runId: string, workerId: string, ttlMs: number): Promise<RunLease | undefined>;
  /** Renew a still-owned lease; returns false when fencing no longer matches. */
  renewRunLease?(lease: RunLease, ttlMs: number): Promise<boolean>;
  /** Release a still-owned lease without changing workflow state. */
  releaseRunLease?(lease: RunLease): Promise<void>;
  /** Operational visibility into current lease ownership. */
  listRunLeases?(): Promise<RunLease[]>;
  /** Persist a projected run snapshot for faster reads of long-lived runs. */
  saveRunSnapshot?(snapshot: RunSnapshot): Promise<void>;
  /** Load the most recent snapshot for a run, if any. */
  latestRunSnapshot?(runId: string): Promise<RunSnapshot | undefined>;
  close?(): Promise<void> | void;
}

export interface RuntimeObserver {
  onEvent?: (event: ContinuumEvent) => void | Promise<void>;
  onRunFinished?: (state: RunState) => void | Promise<void>;
}

export interface StartRunOptions {
  runId?: string;
  metadata?: JsonObject;
  /** Durable enqueue only; a separate worker can adopt the run during recovery. */
  defer?: boolean;
}

export interface RuntimeOptions<Context = unknown> {
  journal: WorkflowJournal;
  context: Context;
  /** Maximum forward steps executing concurrently per run. Defaults to 4. */
  concurrency?: number;
  /**
   * Maximum step executions concurrently across ALL runs on this worker.
   * Defaults to 100. This is the backpressure bound that keeps mass adoption
   * after a node failure from exhausting the replacement worker.
   */
  maxConcurrentSteps?: number;
  now?: () => number;
  observers?: RuntimeObserver[];
  random?: () => number;
  /** Stable worker identity used for distributed leases. Defaults to a random process identity. */
  workerId?: string;
  /** Lease duration in milliseconds for journals that support distributed workers. Defaults to 10s. */
  leaseTtlMs?: number;
  /**
   * When true and the journal supports snapshots, a snapshot is written as
   * each run reaches a terminal state so later reads fold only the tail.
   * Defaults to false.
   */
  snapshotOnTerminal?: boolean;
}

export interface WorkflowHandle<Output = unknown> {
  runId: string;
  result: Promise<Output>;
}

export const TERMINAL_RUN_STATUSES: readonly RunStatus[] = [
  "succeeded",
  "failed",
  "cancelled",
  "compensated",
  "compensation_failed",
];

export function isTerminalRunStatus(status: RunStatus): boolean {
  return TERMINAL_RUN_STATUSES.includes(status);
}
