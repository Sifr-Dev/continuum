import type {
  ContinuumEvent,
  JsonObject,
  RunSnapshot,
  RunState,
  RunSummary,
  SerializedError,
  StepPlan,
  StepState,
} from "./types.js";

export interface ProjectRunOptions {
  /**
   * Resume from a previously captured snapshot instead of folding from
   * scratch. Events at or before `base.lastSeq` are skipped, and the snapshot
   * state is deep-cloned so the caller's snapshot is never mutated.
   */
  base?: RunSnapshot;
}

export function projectRun(events: readonly ContinuumEvent[], options: ProjectRunOptions = {}): RunState | undefined {
  const base = options.base;
  let state: RunState | undefined = base ? (structuredClone(base.state) as RunState) : undefined;
  const skipSeq = base ? base.lastSeq : Number.NEGATIVE_INFINITY;

  for (const event of events) {
    if (event.seq <= skipSeq) continue;
    if (event.type === "run.created") {
      const steps: Record<string, StepState> = {};
      for (const plan of readStepPlans(event.data.steps)) {
        steps[plan.name] = {
          name: plan.name,
          needs: plan.needs,
          status: "pending",
          attempts: 0,
          compensationAttempts: 0,
          ...(plan.idempotencyKey !== undefined ? { idempotencyKey: plan.idempotencyKey } : {}),
          hasCompensation: plan.hasCompensation,
          sideEffect: plan.sideEffect,
          ...(plan.delayMs !== undefined ? { delayMs: plan.delayMs } : {}),
          ...(plan.waitForSignal !== undefined ? { waitForSignal: plan.waitForSignal } : {}),
          ...(plan.description !== undefined ? { description: plan.description } : {}),
        };
      }

      state = {
        runId: event.runId,
        workflowName: asString(event.data.workflowName, event.workflowName),
        version: asNumber(event.data.version, 1),
        input: event.data.input,
        status: "running",
        createdAt: event.at,
        updatedAt: event.at,
        cancellationRequested: false,
        compensationStarted: false,
        steps,
        signals: {},
        eventCount: 0,
      };
    }

    if (!state) continue;
    state.updatedAt = event.at;
    state.eventCount += 1;

    switch (event.type) {
      case "run.created":
        break;
      case "run.started":
        state.startedAt = event.at;
        break;
      case "run.migrated":
        state.version = asNumber(event.data.toVersion, state.version);
        break;
      case "run.cancel_requested":
        state.cancellationRequested = true;
        state.cancellationReason = optionalString(event.data.reason);
        break;
      case "run.signaled": {
        const signalName = optionalString(event.data.signal);
        if (signalName) state.signals[signalName] = event.data.payload;
        break;
      }
      case "step.started":
        mutateStep(state, event, (step) => {
          step.status = "running";
          step.attempts = asNumber(event.data.attempt, step.attempts + 1);
          step.startedAt = event.at;
          delete step.error;
          delete step.nextAttemptAt;
          delete step.completedAt;
          delete step.durationMs;
        });
        break;
      case "step.succeeded":
        mutateStep(state, event, (step) => {
          step.status = "succeeded";
          step.output = event.data.output;
          step.completedAt = event.at;
          step.durationMs = optionalNumber(event.data.durationMs);
          delete step.error;
          delete step.nextAttemptAt;
        });
        break;
      case "step.failed":
      case "step.interrupted":
        mutateStep(state, event, (step) => {
          const retryable = event.data.retryable === true;
          const nextAttemptAt = optionalNumber(event.data.nextAttemptAt);
          step.status = retryable && nextAttemptAt !== undefined ? "pending" : "failed";
          step.error = readError(event.data.error);
          step.completedAt = event.at;
          step.durationMs = optionalNumber(event.data.durationMs);
          if (retryable && nextAttemptAt !== undefined) step.nextAttemptAt = nextAttemptAt;
          else delete step.nextAttemptAt;
        });
        break;
      case "step.skipped":
        mutateStep(state, event, (step) => {
          step.status = "skipped";
          step.completedAt = event.at;
          delete step.nextAttemptAt;
        });
        break;
      case "compensation.started":
        state.compensationStarted = true;
        state.compensationReason = optionalString(event.data.reason);
        if (event.data.error !== undefined) state.error = readError(event.data.error);
        state.status = "compensating";
        break;
      case "compensation.step_started":
        mutateStep(state, event, (step) => {
          step.status = "compensating";
          step.compensationAttempts = asNumber(event.data.attempt, step.compensationAttempts + 1);
          delete step.compensationError;
          delete step.nextCompensationAttemptAt;
        });
        break;
      case "compensation.step_completed":
        mutateStep(state, event, (step) => {
          step.status = "compensated";
          delete step.compensationError;
          delete step.nextCompensationAttemptAt;
        });
        break;
      case "compensation.step_failed":
      case "compensation.step_interrupted":
        mutateStep(state, event, (step) => {
          const retryable = event.data.retryable === true;
          const nextAttemptAt = optionalNumber(event.data.nextAttemptAt);
          step.status = retryable && nextAttemptAt !== undefined ? "compensation_pending" : "compensation_failed";
          step.compensationError = readError(event.data.error);
          if (retryable && nextAttemptAt !== undefined) step.nextCompensationAttemptAt = nextAttemptAt;
          else delete step.nextCompensationAttemptAt;
        });
        break;
      case "compensation.step_skipped":
        mutateStep(state, event, (step) => {
          step.status = "compensation_skipped";
          delete step.nextCompensationAttemptAt;
        });
        break;
      case "compensation.completed":
        break;
      case "compensation.failed":
        state.error = readError(event.data.error);
        break;
      case "run.succeeded":
        state.status = "succeeded";
        state.output = event.data.output;
        state.completedAt = event.at;
        delete state.error;
        break;
      case "run.failed":
        state.status = "failed";
        state.error = readError(event.data.error);
        state.completedAt = event.at;
        break;
      case "run.cancelled":
        state.status = "cancelled";
        state.completedAt = event.at;
        break;
      case "run.compensated":
        state.status = "compensated";
        if (event.data.error !== undefined) state.error = readError(event.data.error);
        state.completedAt = event.at;
        break;
      case "run.compensation_failed":
        state.status = "compensation_failed";
        state.error = readError(event.data.error);
        state.completedAt = event.at;
        break;
      case "custom":
        break;
    }
  }

  return state;
}

export function summarizeRun(state: RunState): RunSummary {
  return {
    runId: state.runId,
    workflowName: state.workflowName,
    version: state.version,
    status: state.status,
    createdAt: state.createdAt,
    updatedAt: state.updatedAt,
    ...(state.completedAt !== undefined ? { completedAt: state.completedAt } : {}),
    eventCount: state.eventCount,
    ...(state.error !== undefined ? { error: state.error } : {}),
  };
}

export function projectSummaries(events: readonly ContinuumEvent[]): RunSummary[] {
  const byRun = new Map<string, ContinuumEvent[]>();
  for (const event of events) {
    const list = byRun.get(event.runId) ?? [];
    list.push(event);
    byRun.set(event.runId, list);
  }

  return [...byRun.values()]
    .map((runEvents) => projectRun(runEvents))
    .filter((state): state is RunState => state !== undefined)
    .map(summarizeRun)
    .sort((a, b) => b.createdAt - a.createdAt);
}

function mutateStep(state: RunState, event: ContinuumEvent, mutate: (step: StepState) => void): void {
  const stepName = optionalString(event.data.step);
  if (!stepName) return;
  const step = state.steps[stepName];
  if (!step) return;
  mutate(step);
}

function readStepPlans(value: unknown): StepPlan[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item) => {
    if (!isObject(item) || typeof item.name !== "string") return [];
    return [{
      name: item.name,
      needs: Array.isArray(item.needs) ? item.needs.filter((need): need is string => typeof need === "string") : [],
      ...(typeof item.idempotencyKey === "string" ? { idempotencyKey: item.idempotencyKey } : {}),
      hasCompensation: item.hasCompensation === true,
      sideEffect: item.sideEffect !== false,
      ...(typeof item.delayMs === "number" && Number.isFinite(item.delayMs) ? { delayMs: item.delayMs } : {}),
      ...(typeof item.waitForSignal === "string" ? { waitForSignal: item.waitForSignal } : {}),
      ...(typeof item.description === "string" ? { description: item.description } : {}),
    }];
  });
}

function readError(value: unknown): SerializedError {
  if (!isObject(value)) return { name: "Error", message: "Unknown error" };
  return {
    name: typeof value.name === "string" ? value.name : "Error",
    message: typeof value.message === "string" ? value.message : "Unknown error",
    ...(typeof value.stack === "string" ? { stack: value.stack } : {}),
    ...(typeof value.code === "string" ? { code: value.code } : {}),
    ...(typeof value.retryable === "boolean" ? { retryable: value.retryable } : {}),
    ...(isObject(value.details) ? { details: value.details as JsonObject } : {}),
  };
}

function asString(value: unknown, fallback: string): string {
  return typeof value === "string" ? value : fallback;
}

function asNumber(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function optionalNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
