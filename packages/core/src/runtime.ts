import { randomUUID } from "node:crypto";
import { resolveRetryPolicy, retryDecision } from "./backoff.js";
import { RunCancelledError, StepInterruptedError, StepTimeoutError, serializeError } from "./errors.js";
import { projectRun, summarizeRun } from "./projection.js";
import type {
  AnyStepDefinition,
  AnyWorkflowDefinition,
  ContinuumEvent,
  JsonObject,
  NewContinuumEvent,
  RunState,
  RunSummary,
  RuntimeObserver,
  RuntimeOptions,
  StartRunOptions,
  StepState,
  WorkflowHandle,
  WorkflowJournal,
} from "./types.js";
import { isTerminalRunStatus } from "./types.js";
import { reverseTopologicalOrder, topologicalOrder, validateWorkflow } from "./validation.js";

interface ActiveTask {
  runId: string;
  stepName: string;
  kind: "step";
  promise: Promise<void>;
  controller: AbortController;
}

const DEFAULT_COMPENSATION_RETRY = {
  attempts: 5,
  delayMs: 100,
  strategy: "exponential" as const,
  maxDelayMs: 5_000,
  jitter: true,
};

export class WorkflowRuntime<Context = unknown> {
  private readonly journal: WorkflowJournal;
  private readonly context: Context;
  private readonly concurrency: number;
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly observers: RuntimeObserver[];
  private readonly definitions = new Map<string, AnyWorkflowDefinition>();
  private readonly orderedSteps = new Map<string, string[]>();
  private readonly reverseOrderedSteps = new Map<string, string[]>();
  private readonly activeTasks = new Map<string, ActiveTask>();
  private readonly pumping = new Set<string>();
  private readonly wakeResolvers = new Map<string, Set<() => void>>();
  private readonly finishedRuns = new Set<string>();

  constructor(options: RuntimeOptions<Context>) {
    this.journal = options.journal;
    this.context = options.context;
    this.concurrency = Math.max(1, options.concurrency ?? 4);
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
    this.observers = options.observers ?? [];
  }

  registerWorkflow<Workflow extends AnyWorkflowDefinition>(definition: Workflow): Workflow {
    const ordered = validateWorkflow(definition);
    const key = workflowKey(definition);
    this.definitions.set(key, definition);
    this.orderedSteps.set(key, ordered);
    this.reverseOrderedSteps.set(key, [...ordered].reverse());
    return definition;
  }

  async startWorkflow<Input, Outputs, Result = unknown>(
    definition: AnyWorkflowDefinition,
    input: Input,
    options: StartRunOptions = {},
  ): Promise<string> {
    this.registerWorkflow(definition);
    const runId = options.runId ?? randomUUID();
    const existing = await this.journal.events(runId);
    if (existing.length > 0) {
      throw new Error(`Run "${runId}" already exists`);
    }

    const validatedInput = definition.validateInput ? definition.validateInput(input) : input;
    assertJsonCompatible(validatedInput, "workflow input");
    const key = workflowKey(definition);
    const ordered = this.orderedSteps.get(key) ?? topologicalOrder(definition.steps);
    const byName = new Map(definition.steps.map((step) => [step.name, step]));
    const plans = ordered.map((name) => {
      const step = byName.get(name);
      if (!step) throw new Error(`Missing step "${name}"`);
      const idempotencyKey = resolveIdempotencyKey(step, validatedInput, runId);
      return {
        name: step.name,
        needs: [...(step.needs ?? [])],
        idempotencyKey,
        hasCompensation: typeof step.compensate === "function",
        sideEffect: step.sideEffect !== false,
        ...(step.description ? { description: step.description } : {}),
      };
    });

    await this.append([
      {
        runId,
        workflowName: definition.name,
        type: "run.created",
        data: {
          workflowName: definition.name,
          version: definition.version,
          input: validatedInput as JsonObject[string],
          steps: plans,
          ...(options.metadata ? { metadata: options.metadata } : {}),
        },
      },
      {
        runId,
        workflowName: definition.name,
        type: "run.started",
        data: {},
      },
    ]);

    if (!options.defer) this.pumpInBackground(runId);
    return runId;
  }

  async runWorkflow<Input, Output = unknown>(
    definition: AnyWorkflowDefinition,
    input: Input,
    options: StartRunOptions = {},
  ): Promise<WorkflowHandle<Output>> {
    const runId = await this.startWorkflow(definition, input, options);
    return {
      runId,
      result: this.waitForRun(runId).then((state) => {
        if (state.status !== "succeeded") {
          const error = new Error(state.error?.message ?? `Workflow finished with status ${state.status}`);
          error.name = state.error?.name ?? "WorkflowRunError";
          throw error;
        }
        return state.output as Output;
      }),
    };
  }

  async recover(): Promise<string[]> {
    const recovered: string[] = [];
    for (const runId of await this.journal.runIds()) {
      const state = await this.getRun(runId);
      if (!state || isTerminalRunStatus(state.status)) continue;
      const key = `${state.workflowName}@${state.version}`;
      if (!this.definitions.has(key)) continue;
      recovered.push(runId);
      this.pumpInBackground(runId);
    }
    return recovered;
  }

  async cancel(runId: string, reason = "Cancelled by operator"): Promise<void> {
    const state = await this.getRun(runId);
    if (!state) throw new Error(`Run "${runId}" does not exist`);
    if (isTerminalRunStatus(state.status)) return;
    if (!state.cancellationRequested) {
      await this.append([
        {
          runId,
          workflowName: state.workflowName,
          type: "run.cancel_requested",
          data: { reason },
        },
      ]);
    }

    for (const task of this.activeTasks.values()) {
      if (task.runId === runId) {
        task.controller.abort(new RunCancelledError(runId));
      }
    }
    this.wake(runId);
    if (this.definitions.has(`${state.workflowName}@${state.version}`)) {
      this.pumpInBackground(runId);
    }
  }

  async getRun(runId: string): Promise<RunState | undefined> {
    return projectRun(await this.journal.events(runId));
  }

  async getEvents(runId: string): Promise<ContinuumEvent[]> {
    return this.journal.events(runId);
  }

  async listRuns(): Promise<RunSummary[]> {
    const states = await Promise.all((await this.journal.runIds()).map((runId) => this.getRun(runId)));
    return states
      .filter((state): state is RunState => state !== undefined)
      .map(summarizeRun)
      .sort((a, b) => b.createdAt - a.createdAt);
  }

  async waitForRun(runId: string, timeoutMs = 30_000): Promise<RunState> {
    const deadline = this.now() + timeoutMs;
    for (;;) {
      const state = await this.getRun(runId);
      if (!state) throw new Error(`Run "${runId}" does not exist`);
      if (isTerminalRunStatus(state.status)) return state;
      if (this.now() > deadline) throw new Error(`Timed out waiting for run "${runId}"`);
      await sleep(Math.min(25, Math.max(1, deadline - this.now())));
    }
  }

  async close(): Promise<void> {
    for (const task of this.activeTasks.values()) {
      task.controller.abort(new RunCancelledError("runtime shutdown"));
    }
    await Promise.allSettled([...this.activeTasks.values()].map((task) => task.promise));
    await this.journal.close?.();
  }

  private async append(events: readonly NewContinuumEvent[]): Promise<ContinuumEvent[]> {
    const rows = await this.journal.append(events);
    for (const event of rows) {
      for (const observer of this.observers) {
        try {
          await observer.onEvent?.(event);
        } catch {
          // Observers are intentionally isolated from workflow correctness.
        }
      }
    }
    return rows;
  }

  private pumpInBackground(runId: string): void {
    void this.pump(runId).catch(async (error) => {
      try {
        const state = await this.getRun(runId);
        if (!state || isTerminalRunStatus(state.status)) return;
        await this.append([
          {
            runId,
            workflowName: state.workflowName,
            type: "run.failed",
            data: { error: serializeError(error) },
          },
        ]);
      } catch {
        // If the journal itself is unavailable there is nowhere safe to report the failure.
      }
    });
  }

  private async pump(runId: string): Promise<void> {
    if (this.pumping.has(runId)) return;
    this.pumping.add(runId);

    try {
      for (;;) {
        const state = await this.getRun(runId);
        if (!state) return;
        if (isTerminalRunStatus(state.status)) {
          await this.notifyFinishedOnce(state);
          return;
        }

        const definition = this.definitions.get(`${state.workflowName}@${state.version}`);
        if (!definition) {
          throw new Error(`Workflow definition "${state.workflowName}@${state.version}" is not registered`);
        }

        const interrupted = await this.recordInterruptedSteps(runId, definition, state);
        if (interrupted) continue;

        const active = this.activeTasksForRun(runId);
        const failureDetected = Object.values(state.steps).some((step) => step.status === "failed");
        const shouldRollback = state.cancellationRequested || failureDetected || state.compensationStarted;

        if (shouldRollback) {
          if (active.length > 0) {
            if (state.cancellationRequested) {
              for (const task of active) task.controller.abort(new RunCancelledError(runId));
            }
            await Promise.race([Promise.race(active.map((task) => task.promise)), this.waitForWake(runId, 100)]);
            continue;
          }
          await this.compensateAndFinish(runId, definition, state);
          continue;
        }

        const now = this.now();
        let capacity = this.concurrency - active.length;
        let startedAny = false;
        const order = this.orderedSteps.get(`${state.workflowName}@${state.version}`) ?? topologicalOrder(definition.steps);
        const byName = new Map(definition.steps.map((step) => [step.name, step]));

        for (const stepName of order) {
          if (capacity <= 0) break;
          const stepState = state.steps[stepName];
          const stepDefinition = byName.get(stepName);
          if (!stepState || !stepDefinition) continue;
          if (stepState.status !== "pending") continue;
          if ((stepState.nextAttemptAt ?? 0) > now) continue;
          if (!stepState.needs.every((dependency) => state.steps[dependency]?.status === "succeeded")) continue;

          await this.startStepTask(runId, definition, stepDefinition, state, stepState);
          capacity -= 1;
          startedAny = true;
        }

        if (startedAny) continue;

        const activeAfterStart = this.activeTasksForRun(runId);
        if (activeAfterStart.length > 0) {
          await Promise.race([
            Promise.race(activeAfterStart.map((task) => task.promise)),
            this.waitForWake(runId, 250),
          ]);
          continue;
        }

        const nextAttemptAt = Object.values(state.steps)
          .filter((step) => step.status === "pending" && step.nextAttemptAt !== undefined)
          .map((step) => step.nextAttemptAt as number)
          .sort((a, b) => a - b)[0];

        if (nextAttemptAt !== undefined && nextAttemptAt > this.now()) {
          await this.waitForWake(runId, Math.min(1_000, nextAttemptAt - this.now()));
          continue;
        }

        if (Object.values(state.steps).every((step) => step.status === "succeeded")) {
          const outputs = outputsFromState(state);
          let output: unknown;
          try {
            output = definition.output
              ? definition.output({ input: state.input, outputs, runId })
              : outputs;
            assertJsonCompatible(output, "workflow output");
          } catch (error) {
            const serialized = serializeError(error);
            await this.append([
              {
                runId,
                workflowName: state.workflowName,
                type: "compensation.started",
                data: { reason: `workflow output failed: ${serialized.message}` },
              },
            ]);
            continue;
          }
          await this.append([
            {
              runId,
              workflowName: state.workflowName,
              type: "run.succeeded",
              data: { output: output as JsonObject[string] },
            },
          ]);
          continue;
        }

        if (Object.values(state.steps).some((step) => step.status === "failed")) continue;

        const unfinished = Object.values(state.steps).filter((step) => step.status === "pending");
        const retryScheduled = unfinished.some((step) => step.nextAttemptAt !== undefined);
        if (unfinished.length > 0 && !retryScheduled) {
          // This should be impossible for validated DAGs once active work and retries
          // have been accounted for, so fail closed rather than spinning forever.
          await this.append([
            {
              runId,
              workflowName: state.workflowName,
              type: "run.failed",
              data: {
                error: {
                  name: "WorkflowSchedulingError",
                  message: `Workflow is blocked; no step can make progress: ${unfinished.map((step) => step.name).join(", ")}`,
                },
              },
            },
          ]);
          continue;
        }

        await this.waitForWake(runId, 50);
      }
    } finally {
      this.pumping.delete(runId);
    }
  }

  private async startStepTask(
    runId: string,
    workflow: AnyWorkflowDefinition,
    step: AnyStepDefinition,
    state: RunState,
    stepState: StepState,
  ): Promise<void> {
    const attempt = stepState.attempts + 1;
    const controller = new AbortController();
    const key = taskKey(runId, step.name);
    const startedAt = this.now();

    await this.append([
      {
        runId,
        workflowName: workflow.name,
        type: "step.started",
        data: {
          step: step.name,
          attempt,
          ...(stepState.idempotencyKey !== undefined ? { idempotencyKey: stepState.idempotencyKey } : {}),
        },
      },
    ]);

    const promise = (async () => {
      try {
        const output = await this.executeWithTimeout({
          stepName: step.name,
          timeoutMs: step.timeoutMs,
          controller,
          invoke: () => step.execute({
            input: state.input,
            outputs: outputsFromState(state),
            context: this.context,
            runId,
            stepName: step.name,
            attempt,
            ...(stepState.idempotencyKey !== undefined ? { idempotencyKey: stepState.idempotencyKey } : {}),
            signal: controller.signal,
            now: this.now,
            emit: async (name, payload = {}) => {
              await this.append([
                {
                  runId,
                  workflowName: workflow.name,
                  type: "custom",
                  data: { name, payload, step: step.name },
                },
              ]);
            },
          }),
        });
        assertJsonCompatible(output, `output of step "${step.name}"`);
        await this.append([
          {
            runId,
            workflowName: workflow.name,
            type: "step.succeeded",
            data: {
              step: step.name,
              attempt,
              output: output as JsonObject[string],
              durationMs: this.now() - startedAt,
            },
          },
        ]);
      } catch (error) {
        const serialized = serializeError(error);
        const cancelled = controller.signal.aborted && controller.signal.reason instanceof RunCancelledError;
        const policy = resolveRetryPolicy(step.retry);
        const decision = cancelled
          ? { retryable: false as const }
          : retryDecision(policy, attempt, this.now(), this.random);
        await this.append([
          {
            runId,
            workflowName: workflow.name,
            type: cancelled ? "step.interrupted" : "step.failed",
            data: {
              step: step.name,
              attempt,
              error: serialized,
              retryable: decision.retryable,
              ...(decision.nextAttemptAt !== undefined ? { nextAttemptAt: decision.nextAttemptAt } : {}),
              ...(decision.delayMs !== undefined ? { retryDelayMs: decision.delayMs } : {}),
              durationMs: this.now() - startedAt,
            },
          },
        ]);
      } finally {
        this.activeTasks.delete(key);
        this.wake(runId);
      }
    })();

    this.activeTasks.set(key, { runId, stepName: step.name, kind: "step", promise, controller });
  }

  private async executeWithTimeout(args: {
    stepName: string;
    timeoutMs?: number;
    controller: AbortController;
    invoke: () => Promise<unknown> | unknown;
  }): Promise<unknown> {
    const invocation = Promise.resolve().then(args.invoke);
    invocation.catch(() => undefined);
    if (!args.timeoutMs || args.timeoutMs <= 0) return invocation;

    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const error = new StepTimeoutError(args.stepName, args.timeoutMs ?? 0);
        args.controller.abort(error);
        reject(error);
      }, args.timeoutMs);
    });

    try {
      return await Promise.race([invocation, timeout]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private async recordInterruptedSteps(
    runId: string,
    workflow: AnyWorkflowDefinition,
    state: RunState,
  ): Promise<boolean> {
    const byName = new Map(workflow.steps.map((step) => [step.name, step]));
    const events: NewContinuumEvent[] = [];

    for (const stepState of Object.values(state.steps)) {
      if (stepState.status !== "running") continue;
      if (this.activeTasks.has(taskKey(runId, stepState.name))) continue;
      const step = byName.get(stepState.name);
      const policy = resolveRetryPolicy(step?.retry);
      const decision = state.cancellationRequested
        ? { retryable: false as const }
        : retryDecision(policy, stepState.attempts, this.now(), this.random);
      const error = serializeError(new StepInterruptedError(stepState.name, "worker crashed or was restarted"));
      events.push({
        runId,
        workflowName: workflow.name,
        type: "step.interrupted",
        data: {
          step: stepState.name,
          attempt: stepState.attempts,
          error,
          retryable: decision.retryable,
          ...(decision.nextAttemptAt !== undefined ? { nextAttemptAt: decision.nextAttemptAt } : {}),
        },
      });
    }

    if (events.length === 0) return false;
    await this.append(events);
    return true;
  }

  private async compensateAndFinish(runId: string, workflow: AnyWorkflowDefinition, initialState: RunState): Promise<void> {
    const reverseOrder = this.reverseOrderedSteps.get(`${workflow.name}@${workflow.version}`) ?? reverseTopologicalOrder(workflow.steps);
    const byName = new Map(workflow.steps.map((step) => [step.name, step]));

    let state = initialState;
    const pendingSkips = Object.values(state.steps).filter((step) => step.status === "pending");
    if (pendingSkips.length > 0) {
      await this.append(pendingSkips.map((step) => ({
        runId,
        workflowName: workflow.name,
        type: "step.skipped" as const,
        data: { step: step.name, reason: state.cancellationRequested ? "run cancelled" : "workflow failed" },
      })));
      state = (await this.getRun(runId)) ?? state;
    }

    const hasCompensationRelevantOutput = reverseOrder.some((stepName) => {
      const step = byName.get(stepName);
      const stepState = state.steps[stepName];
      return step?.sideEffect !== false && stepState?.status === "succeeded";
    });

    if (!state.compensationStarted && !hasCompensationRelevantOutput) {
      const originalFailure = Object.values(state.steps).find((step) => step.status === "failed")?.error;
      await this.append([
        state.cancellationRequested
          ? {
              runId,
              workflowName: workflow.name,
              type: "run.cancelled",
              data: { reason: state.cancellationReason ?? "cancelled" },
            }
          : {
              runId,
              workflowName: workflow.name,
              type: "run.failed",
              data: {
                error: originalFailure ?? {
                  name: "WorkflowError",
                  message: "Workflow failed before any compensable effect completed",
                },
              },
            },
      ]);
      return;
    }

    if (!state.compensationStarted) {
      await this.append([
        {
          runId,
          workflowName: workflow.name,
          type: "compensation.started",
          data: { reason: state.cancellationRequested ? "run cancelled" : "workflow step failed" },
        },
      ]);
      state = (await this.getRun(runId)) ?? state;
    }

    for (const stepName of reverseOrder) {
      state = (await this.getRun(runId)) ?? state;
      const stepState = state.steps[stepName];
      const step = byName.get(stepName);
      if (!stepState || !step) continue;
      if (["compensated", "compensation_skipped", "skipped", "pending"].includes(stepState.status)) continue;

      if (!step.compensate) {
        if (stepState.status === "succeeded" && step.sideEffect !== false) {
          await this.append([
            {
              runId,
              workflowName: workflow.name,
              type: "compensation.step_skipped",
              data: { step: stepName, reason: "side-effecting step does not define compensation" },
            },
          ]);
        }
        continue;
      }

      for (;;) {
        state = (await this.getRun(runId)) ?? state;
        const current = state.steps[stepName];
        if (!current) break;
        if (["compensated", "compensation_skipped"].includes(current.status)) break;
        if (current.status === "compensation_failed") {
          await this.finishCompensationFailure(runId, workflow.name, current.compensationError);
          return;
        }

        if (current.status === "compensating") {
          const policy = resolveRetryPolicy(step.compensationRetry, DEFAULT_COMPENSATION_RETRY);
          const decision = retryDecision(policy, current.compensationAttempts, this.now(), this.random);
          await this.append([
            {
              runId,
              workflowName: workflow.name,
              type: "compensation.step_interrupted",
              data: {
                step: stepName,
                attempt: current.compensationAttempts,
                error: serializeError(new StepInterruptedError(stepName, "worker crashed during compensation")),
                retryable: decision.retryable,
                ...(decision.nextAttemptAt !== undefined ? { nextAttemptAt: decision.nextAttemptAt } : {}),
              },
            },
          ]);
          continue;
        }

        if (current.status === "compensation_pending") {
          const due = current.nextCompensationAttemptAt ?? this.now();
          if (due > this.now()) {
            await this.waitForWake(runId, Math.min(1_000, due - this.now()));
            continue;
          }
        }

        if (!["succeeded", "compensation_pending"].includes(current.status)) break;

        const attempt = current.compensationAttempts + 1;
        const controller = new AbortController();
        const startedAt = this.now();
        await this.append([
          {
            runId,
            workflowName: workflow.name,
            type: "compensation.step_started",
            data: { step: stepName, attempt },
          },
        ]);

        try {
          await this.executeWithTimeout({
            stepName: `${stepName}.compensate`,
            timeoutMs: step.compensationTimeoutMs ?? step.timeoutMs,
            controller,
            invoke: () => step.compensate?.({
              input: state.input,
              output: current.output,
              outputs: outputsFromState(state),
              context: this.context,
              runId,
              stepName,
              attempt,
              ...(current.idempotencyKey !== undefined ? { idempotencyKey: current.idempotencyKey } : {}),
              signal: controller.signal,
              now: this.now,
              emit: async (name, payload = {}) => {
                await this.append([
                  {
                    runId,
                    workflowName: workflow.name,
                    type: "custom",
                    data: { name, payload, step: stepName, compensation: true },
                  },
                ]);
              },
            }),
          });
          await this.append([
            {
              runId,
              workflowName: workflow.name,
              type: "compensation.step_completed",
              data: { step: stepName, attempt, durationMs: this.now() - startedAt },
            },
          ]);
        } catch (error) {
          const policy = resolveRetryPolicy(step.compensationRetry, DEFAULT_COMPENSATION_RETRY);
          const decision = retryDecision(policy, attempt, this.now(), this.random);
          await this.append([
            {
              runId,
              workflowName: workflow.name,
              type: "compensation.step_failed",
              data: {
                step: stepName,
                attempt,
                error: serializeError(error),
                retryable: decision.retryable,
                ...(decision.nextAttemptAt !== undefined ? { nextAttemptAt: decision.nextAttemptAt } : {}),
                durationMs: this.now() - startedAt,
              },
            },
          ]);
        }
      }
    }

    state = (await this.getRun(runId)) ?? state;
    const compensationFailed = Object.values(state.steps).find((step) => step.status === "compensation_failed");
    if (compensationFailed) {
      await this.finishCompensationFailure(runId, workflow.name, compensationFailed.compensationError);
      return;
    }

    const uncompensatedEffect = Object.values(state.steps).find((step) =>
      step.status === "compensation_skipped" && step.sideEffect
    );
    if (uncompensatedEffect) {
      const error = new Error(`Step "${uncompensatedEffect.name}" produced a side effect but does not define compensation`);
      error.name = "MissingCompensationError";
      await this.finishCompensationFailure(runId, workflow.name, error);
      return;
    }

    const originalFailure = Object.values(state.steps).find((step) => step.status === "failed")?.error;
    await this.append([
      {
        runId,
        workflowName: workflow.name,
        type: "compensation.completed",
        data: {},
      },
      state.cancellationRequested
        ? {
            runId,
            workflowName: workflow.name,
            type: "run.cancelled",
            data: { reason: state.cancellationReason ?? "cancelled" },
          }
        : {
            runId,
            workflowName: workflow.name,
            type: "run.compensated",
            data: {
              ...(originalFailure ? { error: originalFailure as unknown as JsonObject } : {}),
            },
          },
    ]);
  }

  private async finishCompensationFailure(
    runId: string,
    workflowName: string,
    error: unknown,
  ): Promise<void> {
    const serialized = serializeError(error ?? new Error("Compensation failed"));
    await this.append([
      {
        runId,
        workflowName,
        type: "compensation.failed",
        data: { error: serialized as unknown as JsonObject },
      },
      {
        runId,
        workflowName,
        type: "run.compensation_failed",
        data: { error: serialized as unknown as JsonObject },
      },
    ]);
  }

  private activeTasksForRun(runId: string): ActiveTask[] {
    return [...this.activeTasks.values()].filter((task) => task.runId === runId);
  }

  private waitForWake(runId: string, timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      const resolvers = this.wakeResolvers.get(runId) ?? new Set<() => void>();
      const finish = () => {
        clearTimeout(timer);
        resolvers.delete(finish);
        resolve();
      };
      const timer = setTimeout(finish, Math.max(1, timeoutMs));
      resolvers.add(finish);
      this.wakeResolvers.set(runId, resolvers);
    });
  }

  private wake(runId: string): void {
    const resolvers = this.wakeResolvers.get(runId);
    if (!resolvers) return;
    for (const resolve of [...resolvers]) resolve();
  }

  private async notifyFinishedOnce(state: RunState): Promise<void> {
    if (this.finishedRuns.has(state.runId)) return;
    this.finishedRuns.add(state.runId);
    for (const observer of this.observers) {
      try {
        await observer.onRunFinished?.(state);
      } catch {
        // Observers are intentionally isolated from workflow correctness.
      }
    }
  }
}

function outputsFromState(state: RunState): Record<string, unknown> {
  return Object.fromEntries(
    Object.values(state.steps)
      .filter((step) => step.output !== undefined)
      .map((step) => [step.name, step.output]),
  );
}

function resolveIdempotencyKey(step: AnyStepDefinition, input: unknown, runId: string): string {
  if (typeof step.idempotencyKey === "function") return step.idempotencyKey(input);
  if (typeof step.idempotencyKey === "string") return step.idempotencyKey;
  return `${runId}:${step.name}`;
}

function workflowKey(definition: AnyWorkflowDefinition): string {
  return `${definition.name}@${definition.version}`;
}

function taskKey(runId: string, stepName: string): string {
  return JSON.stringify([runId, stepName]);
}

function assertJsonCompatible(value: unknown, label: string, path = "$"): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`${label} must be JSON-serializable: ${path} is a non-finite number`);
    return;
  }
  if (Array.isArray(value)) {
    for (const [index, item] of value.entries()) assertJsonCompatible(item, label, `${path}[${index}]`);
    return;
  }
  if (typeof value === "object" && value !== null) {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new Error(`${label} must be JSON-serializable: ${path} is not a plain object`);
    }
    for (const [key, item] of Object.entries(value)) assertJsonCompatible(item, label, `${path}.${key}`);
    return;
  }
  throw new Error(`${label} must be JSON-serializable: ${path} is ${typeof value}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
