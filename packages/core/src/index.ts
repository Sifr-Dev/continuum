export { MemoryJournal } from "./memory-journal.js";
export { WorkflowRuntime } from "./runtime.js";
export { projectRun, projectSummaries, summarizeRun } from "./projection.js";
export { retryDecision, retryDelayMs, resolveRetryPolicy } from "./backoff.js";
export { topologicalOrder, validateWorkflow } from "./validation.js";
export {
  CompensationError,
  ContinuumError,
  RunCancelledError,
  StepInterruptedError,
  StepTimeoutError,
  WorkflowValidationError,
  serializeError,
} from "./errors.js";
export type {
  AnyStepDefinition,
  AnyWorkflowDefinition,
  CompensationArgs,
  ContinuumEvent,
  ContinuumEventType,
  JsonObject,
  JsonValue,
  NewContinuumEvent,
  ResolvedRetryPolicy,
  RetryPolicy,
  RunState,
  RunStatus,
  RunSummary,
  RuntimeObserver,
  RuntimeOptions,
  SerializedError,
  StartRunOptions,
  StepDefinition,
  StepExecutionArgs,
  StepOutputs,
  StepState,
  StepStatus,
  WorkflowDefinition,
  WorkflowHandle,
  WorkflowJournal,
} from "./types.js";

import type { AnyStepDefinition, StepDefinition, StepOutputs, WorkflowDefinition } from "./types.js";

export function defineStep<Input, StepOutput, Outputs = Record<string, unknown>, Context = unknown>(
  definition: StepDefinition<Input, StepOutput, Outputs, Context>,
): StepDefinition<Input, StepOutput, Outputs, Context> {
  return definition;
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
