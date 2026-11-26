export { MemoryJournal } from "./memory-journal.js";
export { ContinuumMetrics } from "./metrics.js";
export { WorkflowRuntime } from "./runtime.js";
export { parseCron, previousCronOccurrence, nextCronOccurrence, recentCronOccurrences, CronParseError, CRON_DAY_MS } from "./cron.js";
export type { CronSpec } from "./cron.js";
export { IntegrityJournal, IntegrityChainError, verifyEventChain, stableStringify, stripIntegrity } from "./integrity.js";
export type { IntegrityLink, IntegrityReport, IntegrityJournalOptions, VerifyEventChainOptions } from "./integrity.js";
export { projectRun, projectSummaries, summarizeRun } from "./projection.js";
export type { ProjectRunOptions } from "./projection.js";
export { retryDecision, retryDelayMs, resolveRetryPolicy } from "./backoff.js";
export { topologicalOrder, validateWorkflow } from "./validation.js";
export {
  CompensationError,
  ContinuumError,
  JournalUnavailableError,
  isJournalError,
  RunAlreadyExistsError,
  RunCancelledError,
  StepInterruptedError,
  StepTimeoutError,
  WorkerLeaseLostError,
  WorkflowValidationError,
  serializeError,
} from "./errors.js";
export type {
  AnyStepDefinition,
  AnyWorkflowDefinition,
  IntegrityChainHead,
  JournalTransaction,
  WorkflowQuery,
  AppendEventOptions,
  CompensationArgs,
  ContinuumEvent,
  ContinuumEventType,
  JsonObject,
  JsonValue,
  NewContinuumEvent,
  ResolvedRetryPolicy,
  RetryPolicy,
  RunLease,
  RunSnapshot,
  RunState,
  RunStatus,
  RunSummary,
  RuntimeObserver,
  RuntimeOptions,
  SerializedError,
  StartChildWorkflowOptions,
  StartRunOptions,
  StepDefinition,
  StepExecutionArgs,
  StepOutputs,
  StepState,
  StepStatus,
  WorkflowDefinition,
  WorkflowHandle,
  WorkflowJournal,
  WorkflowSchedule,
} from "./types.js";

import type { AnyStepDefinition, StepDefinition, StepOutputs, WorkflowDefinition } from "./types.js";

export function defineStep<Input, StepOutput, Outputs = Record<string, unknown>, Context = unknown>(
  definition: StepDefinition<Input, StepOutput, Outputs, Context>,
): StepDefinition<Input, StepOutput, Outputs, Context> {
  return definition;
}

export function defineTimerStep<Outputs = Record<string, unknown>, Context = unknown>(
  name: string,
  durationMs: number,
  needs: readonly string[] = [],
): StepDefinition<unknown, { waitedMs: number }, Outputs, Context> {
  return defineStep({
    name,
    needs,
    delayMs: durationMs,
    sideEffect: false,
    description: `Durable timer for ${durationMs}ms`,
    execute: () => ({ waitedMs: durationMs }),
  });
}

export function defineSignalStep<Payload = unknown, Outputs = Record<string, unknown>, Context = unknown>(
  name: string,
  signalName: string,
  needs: readonly string[] = [],
): StepDefinition<unknown, Payload, Outputs, Context> {
  return defineStep({
    name,
    needs,
    waitForSignal: signalName,
    sideEffect: false,
    description: `Waits for durable signal "${signalName}"`,
    execute: ({ signalPayload }) => signalPayload as Payload,
  });
}

export function defineWorkflow<
  Input,
  Steps extends readonly AnyStepDefinition[],
  Context = unknown,
  Output = unknown,
>(
  definition: Omit<WorkflowDefinition<Input, StepOutputs<Steps>, Context>, "steps" | "output"> & {
    steps: Steps;
    output?: (args: { input: Input; outputs: Readonly<Partial<StepOutputs<Steps>>>; runId: string }) => Output;
  },
): WorkflowDefinition<Input, StepOutputs<Steps>, Context> {
  return definition as WorkflowDefinition<Input, StepOutputs<Steps>, Context>;
}
