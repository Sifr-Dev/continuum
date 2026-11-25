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

export interface StepExecutionArgs<Input, Outputs, Context> {
  input: Input;
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
  output?: (args: { input: Input; outputs: Readonly<Partial<Outputs>>; runId: string }) => unknown;
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
  description?: string;
}

export type ContinuumEventType =
  | "run.created"
  | "run.started"
  | "run.cancel_requested"
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

export interface WorkflowJournal {
  append(events: readonly NewContinuumEvent[]): Promise<ContinuumEvent[]>;
  events(runId: string): Promise<ContinuumEvent[]>;
  allEvents(): Promise<ContinuumEvent[]>;
  runIds(): Promise<string[]>;
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
  concurrency?: number;
  now?: () => number;
  observers?: RuntimeObserver[];
  random?: () => number;
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
