import { randomUUID } from "node:crypto";
import { resolveRetryPolicy, retryDecision } from "./backoff.js";
import { RunAlreadyExistsError, RunCancelledError, StepInterruptedError, StepTimeoutError, serializeError } from "./errors.js";
import { projectRun, summarizeRun } from "./projection.js";
import type {
  AnyStepDefinition,
  AnyWorkflowDefinition,
  ContinuumEvent,
  JsonObject,
  NewContinuumEvent,
  RunLease,
  RunState,
  RunSummary,
  RuntimeObserver,
  RuntimeOptions,
  StartChildWorkflowOptions,
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
  private readonly workerId: string;
  private readonly leaseTtlMs: number;
  private readonly definitions = new Map<string, AnyWorkflowDefinition>();
  private readonly orderedSteps = new Map<string, string[]>();
  private readonly reverseOrderedSteps = new Map<string, string[]>();
  private readonly activeTasks = new Map<string, ActiveTask>();
  private readonly activeLeases = new Map<string, RunLease>();
  private readonly recoveryTimers = new Map<string, NodeJS.Timeout>();
  private readonly pumping = new Set<string>();
  private readonly wakeResolvers = new Map<string, Set<() => void>>();
  private readonly finishedRuns = new Set<string>();
  private closing = false;

  constructor(options: RuntimeOptions<Context>) {
    this.journal = options.journal;
    this.context = options.context;
    this.concurrency = Math.max(1, options.concurrency ?? 4);
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
    this.observers = options.observers ?? [];
    this.workerId = options.workerId ?? `worker-${randomUUID()}`;
    this.leaseTtlMs = Math.max(250, options.leaseTtlMs ?? 10_000);
  }

  getWorkerId(): string {
    return this.workerId;
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
    if (this.closing) throw new Error("Runtime is shutting down; not accepting new workflow runs");
    this.registerWorkflow(definition);
    const runId = options.runId ?? randomUUID();
    const existing = await this.journal.events(runId);
    if (existing.length > 0) {
      throw new RunAlreadyExistsError(runId);
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
        ...(step.delayMs !== undefined ? { delayMs: step.delayMs } : {}),
        ...(step.waitForSignal ? { waitForSignal: step.waitForSignal } : {}),
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
    const leases = new Map((await this.journal.listRunLeases?.() ?? []).map((lease) => [lease.runId, lease]));
    const wallNow = Date.now();
    for (const runId of await this.journal.runIds()) {
      const state = await this.getRun(runId);
      if (!state || isTerminalRunStatus(state.status)) continue;
      const key = `${state.workflowName}@${state.version}`;
      if (!this.definitions.has(key)) continue;
      const lease = leases.get(runId);
      if (lease && lease.expiresAt > wallNow) {
        this.scheduleLeaseRecovery(runId, lease.expiresAt - wallNow);
        continue;
      }
      recovered.push(runId);
      this.pumpInBackground(runId);
    }
    return recovered;
  }

  private scheduleLeaseRecovery(runId: string, delayMs: number): void {
    if (this.recoveryTimers.has(runId)) return;
    const timer = setTimeout(() => {
      this.recoveryTimers.delete(runId);
      this.pumpInBackground(runId);
    }, Math.max(1, delayMs + 1));
    timer.unref?.();
    this.recoveryTimers.set(runId, timer);
  }

  private async deferLeaseRecovery(runId: string): Promise<void> {
    const lease = (await this.journal.listRunLeases?.() ?? []).find((candidate) => candidate.runId === runId);
    if (lease) this.scheduleLeaseRecovery(runId, lease.expiresAt - Date.now());
  }

  async startDueScheduledWorkflows(now = Date.now()): Promise<string[]> {
    const started: string[] = [];
    const definitions = [...new Set(this.definitions.values())];
    for (const definition of definitions) {
      const schedule = definition.schedule;
      if (!schedule) continue;
      const currentWindow = Math.floor(now / schedule.intervalMs) * schedule.intervalMs;
      const catchupWindows = schedule.catchupWindows ?? 0;
      for (let offset = catchupWindows; offset >= 0; offset -= 1) {
        const windowStart = currentWindow - offset * schedule.intervalMs;
        if (windowStart < 0) continue;
        const runId = `scheduled:${definition.name}:${definition.version}:${windowStart}`;
        try {
          await this.startWorkflow(definition, schedule.input, {
            runId,
            metadata: {
              ...(schedule.metadata ?? {}),
              continuumScheduled: true,
              scheduleWindow: windowStart,
            },
          });
          started.push(runId);
        } catch (error) {
          if (!isDuplicateRunError(error, runId)) throw error;
        }
      }
    }
    return started;
  }

  async signalRun(runId: string, signalName: string, payload: unknown = null): Promise<void> {
    if (!signalName) throw new Error("Signal name must not be empty");
    const state = await this.getRun(runId);
    if (!state) throw new Error(`Run "${runId}" does not exist`);
    if (isTerminalRunStatus(state.status)) return;
    assertJsonCompatible(payload, `payload for signal "${signalName}"`);
    await this.append([
      {
        runId,
        workflowName: state.workflowName,
        type: "run.signaled",
        data: { signal: signalName, payload: payload as JsonObject[string] },
      },
    ]);
    this.wake(runId);
    if (this.definitions.has(`${state.workflowName}@${state.version}`)) {
      this.pumpInBackground(runId);
    }
  }

  async signalWorkflow(runId: string, signalName: string, payload: unknown = null): Promise<void> {
    await this.signalRun(runId, signalName, payload);
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

  /**
   * Evaluates a pure query handler against the current projected run state.
   * Safe to call on live runs and from any process: queries read the journal
   * projection, never mutate state, and never replay step code.
   */
  async queryWorkflow(runId: string, queryName: string): Promise<unknown> {
    const state = await this.getRun(runId);
    if (!state) throw new Error(`Run "${runId}" does not exist`);
    const definition = this.definitions.get(`${state.workflowName}@${state.version}`);
    if (!definition) {
      throw new Error(`Workflow "${state.workflowName}@${state.version}" is not registered in this runtime`);
    }
    const handler = definition.queries?.[queryName];
    if (!handler) {
      const available = Object.keys(definition.queries ?? {});
      throw new Error(
        `Workflow "${state.workflowName}" has no query "${queryName}"` +
          (available.length > 0 ? `; available: ${available.join(", ")}` : ""),
      );
    }
    const outputs: Record<string, unknown> = {};
    for (const step of Object.values(state.steps)) {
      if (step.output !== undefined) outputs[step.name] = step.output;
    }
    const result = handler({
      input: state.input,
      outputs,
      signals: state.signals,
      status: state.status,
      steps: state.steps,
      runId: state.runId,
    });
    assertJsonCompatible(result, `result of query "${queryName}"`);
    return result;
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

  /**
   * Shuts the worker down. In-flight steps first get `graceMs` to finish and
   * journal their outcomes — a deploy should not turn healthy work into crash
   * recovery. Once drained (or the grace period expires), remaining tasks are
   * aborted, run leases are released so other workers can adopt immediately
   * instead of waiting for lease expiry, and the journal is closed.
   */
  async close(options?: { graceMs?: number }): Promise<void> {
    const graceMs = Math.max(0, options?.graceMs ?? 5_000);
    this.closing = true;
    for (const runId of this.pumping) this.wake(runId);

    const deadline = Date.now() + graceMs;
    while (this.activeTasks.size > 0 && Date.now() < deadline) {
      await sleep(10);
    }
    for (const timer of this.recoveryTimers.values()) clearTimeout(timer);
    this.recoveryTimers.clear();
    for (const task of this.activeTasks.values()) {
      task.controller.abort(new RunCancelledError("runtime shutdown"));
    }
    // JavaScript cannot forcefully stop non-cooperative user code, so abandon
    // tasks that ignore the abort after a bounded window instead of hanging
    // the deploy. Their journal writes become fenced no-ops once the lease is
    // released, and the surviving step.started records are recoverable.
    await Promise.race([
      Promise.allSettled([...this.activeTasks.values()].map((task) => task.promise)),
      sleep(1_000),
    ]);
    // Abandon anything still running so pumps can exit and release leases.
    // Fencing turns late zombie writes into rejected no-ops.
    for (const [key, task] of [...this.activeTasks.entries()]) {
      this.activeTasks.delete(key);
      this.wake(task.runId);
    }

    const pumpDeadline = Date.now() + 2_000;
    while (this.pumping.size > 0 && Date.now() < pumpDeadline) {
      for (const runId of this.pumping) this.wake(runId);
      await sleep(10);
    }
    await this.journal.close?.();
  }

  private async append(events: readonly NewContinuumEvent[], leaseOverride?: RunLease): Promise<ContinuumEvent[]> {
    const lease = leaseOverride ?? (events.length > 0 ? this.activeLeases.get(events[0]?.runId ?? "") : undefined);
    const rows = await this.journal.append(events, lease ? { lease } : undefined);
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
    void this.pump(runId).catch(() => {
      // Pump failures are usually journal/lease failures. Do not write a terminal
      // failure outside lease ownership: the durable journal remains recoverable
      // by the next authoritative worker.
    });
  }

  private async pump(runId: string): Promise<void> {
    if (this.pumping.has(runId)) return;
    this.pumping.add(runId);
    let lease: RunLease | undefined;
    let leaseLost = false;
    let leaseHeartbeat: NodeJS.Timeout | undefined;

    try {
      if (this.journal.acquireRunLease) {
        lease = await this.journal.acquireRunLease(runId, this.workerId, this.leaseTtlMs);
        if (!lease) {
          await this.deferLeaseRecovery(runId);
          return;
        }
        this.activeLeases.set(runId, lease);
        if (this.journal.renewRunLease) {
          leaseHeartbeat = setInterval(() => {
            if (!lease || leaseLost) return;
            void this.journal.renewRunLease?.(lease, this.leaseTtlMs).then((renewed) => {
              if (!renewed) {
                leaseLost = true;
                this.abortRunTasks(runId, new StepInterruptedError(runId, "worker lease was lost"));
                this.wake(runId);
              }
            }).catch(() => {
              leaseLost = true;
              this.abortRunTasks(runId, new StepInterruptedError(runId, "worker lease renewal failed"));
              this.wake(runId);
            });
          }, Math.max(100, Math.floor(this.leaseTtlMs / 3)));
          leaseHeartbeat.unref?.();
        }
      }

      for (;;) {
        if (leaseLost) return;
        if (this.closing) {
          const hasActiveWork = [...this.activeTasks.values()].some((task) => task.runId === runId);
          if (!hasActiveWork) return;
          await this.waitForWake(runId, 100);
          continue;
        }
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
          if (!stepState.needs.every((dependency) => ["succeeded", "skipped"].includes(state.steps[dependency]?.status ?? ""))) continue;

          if (stepDefinition.when) {
            let condition = false;
            try {
              condition = stepDefinition.when({ input: state.input, outputs: outputsFromState(state) });
            } catch (error) {
              await this.append([
                {
                  runId,
                  workflowName: definition.name,
                  type: "step.failed",
                  data: {
                    step: stepName,
                    attempt: 0,
                    error: serializeError(error),
                    retryable: false,
                  },
                },
              ]);
              startedAny = true;
              continue;
            }
            if (!condition) {
              await this.append([
                {
                  runId,
                  workflowName: definition.name,
                  type: "step.skipped",
                  data: { step: stepName, reason: "condition false", conditional: true },
                },
              ]);
              startedAny = true;
              continue;
            }
          }

          if (waitingForSignal(stepDefinition, state)) continue;
          if (stepReadyAt(stepDefinition, state, stepState) > now) continue;

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

        const nextScheduledAt = Object.values(state.steps)
          .flatMap((stepState) => {
            if (stepState.status !== "pending") return [];
            const stepDefinition = byName.get(stepState.name);
            const times: number[] = [];
            if (stepState.nextAttemptAt !== undefined) times.push(stepState.nextAttemptAt);
            if (stepDefinition) {
              const readyAt = stepReadyAt(stepDefinition, state, stepState);
              if (readyAt > now) times.push(readyAt);
            }
            return times;
          })
          .filter((time) => time > now)
          .sort((a, b) => a - b)[0];

        if (nextScheduledAt !== undefined) {
          await this.waitForWake(runId, Math.min(1_000, nextScheduledAt - this.now()));
          continue;
        }

        if (Object.values(state.steps).every((step) => ["succeeded", "skipped"].includes(step.status))) {
          const outputs = outputsFromState(state);
          let output: unknown;
          try {
            output = definition.output
              ? definition.output({ input: state.input, outputs, runId })
              : outputs;
            assertJsonCompatible(output, "workflow output");
          } catch (error) {
            const serialized = serializeError(error);
            const hasCompensableEffect = definition.steps.some((step) =>
              step.sideEffect !== false && state.steps[step.name]?.status === "succeeded"
            );
            await this.append([
              hasCompensableEffect
                ? {
                    runId,
                    workflowName: state.workflowName,
                    type: "compensation.started",
                    data: {
                      reason: `workflow output failed: ${serialized.message}`,
                      error: serialized as unknown as JsonObject,
                    },
                  }
                : {
                    runId,
                    workflowName: state.workflowName,
                    type: "run.failed",
                    data: { error: serialized as unknown as JsonObject },
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
        const externallyWaiting = unfinished.some((stepState) => {
          const stepDefinition = byName.get(stepState.name);
          if (!stepDefinition) return false;
          return waitingForSignal(stepDefinition, state) || stepReadyAt(stepDefinition, state, stepState) > now;
        });
        if (unfinished.length > 0 && externallyWaiting) {
          await this.waitForWake(runId, 1_000);
          continue;
        }
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
      if (leaseHeartbeat) clearInterval(leaseHeartbeat);
      if (lease && this.journal.releaseRunLease) {
        try {
          await this.journal.releaseRunLease(lease);
        } catch {
          // Lease expiry is the correctness backstop if release fails.
        }
      }
      this.activeLeases.delete(runId);
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
    const taskLease = this.activeLeases.get(runId);

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
    ], taskLease);

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
            ...(step.waitForSignal ? { signalPayload: state.signals[step.waitForSignal] } : {}),
            now: this.now,
            startChildWorkflow: (childDefinition, childInput, childOptions) => this.runChildWorkflow(
              runId,
              step.name,
              controller,
              childDefinition,
              childInput,
              childOptions,
            ),
            emit: async (name, payload = {}) => {
              await this.append([
                {
                  runId,
                  workflowName: workflow.name,
                  type: "custom",
                  data: { name, payload, step: step.name },
                },
              ], taskLease);
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
        ], taskLease);
      } catch (error) {
        const serialized = serializeError(error);
        const cancelled = controller.signal.aborted && controller.signal.reason instanceof RunCancelledError;
        const policy = resolveRetryPolicy(step.retry);
        const decision = cancelled
          ? { retryable: false as const }
          : retryDecision(policy, attempt, this.now(), this.random);
        try {
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
          ], taskLease);
        } catch {
          // If the outcome cannot be journaled (including lease fencing), the
          // surviving step.started record lets the next owner recover safely.
        }
      } finally {
        this.activeTasks.delete(key);
        this.wake(runId);
      }
    })();

    this.activeTasks.set(key, { runId, stepName: step.name, kind: "step", promise, controller });
  }

  private async runChildWorkflow<ChildInput, ChildOutput>(
    parentRunId: string,
    parentStepName: string,
    controller: AbortController,
    definition: AnyWorkflowDefinition,
    input: ChildInput,
    options: StartChildWorkflowOptions = {},
  ): Promise<ChildOutput> {
    const childRunId = options.runId ?? `${parentRunId}:${parentStepName}`;
    try {
      await this.startWorkflow(definition, input, {
        runId: childRunId,
        ...(options.metadata ? { metadata: options.metadata } : {}),
      });
    } catch (error) {
      if (!(error instanceof RunAlreadyExistsError && error.details?.runId === childRunId)) throw error;
      this.registerWorkflow(definition);
      this.pumpInBackground(childRunId);
    }

    const propagateCancellation = options.cancelWithParent !== false;
    const onAbort = () => {
      if (propagateCancellation) {
        void this.cancel(childRunId, `parent step "${parentStepName}" was interrupted`).catch(() => undefined);
      }
    };
    controller.signal.addEventListener("abort", onAbort, { once: true });

    try {
      for (;;) {
        if (controller.signal.aborted) throw controller.signal.reason;
        const state = await this.getRun(childRunId);
        if (!state) throw new Error(`Child workflow run "${childRunId}" does not exist`);
        if (isTerminalRunStatus(state.status)) {
          if (state.status === "succeeded") return state.output as ChildOutput;
          const error = new Error(state.error?.message ?? `Child workflow finished with status ${state.status}`);
          error.name = state.error?.name ?? "ChildWorkflowError";
          throw error;
        }
        await sleep(25);
      }
    } finally {
      controller.signal.removeEventListener("abort", onAbort);
    }
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

    const originalFailure = Object.values(state.steps).find((step) => step.status === "failed")?.error ?? state.error;
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

  private abortRunTasks(runId: string, reason: unknown): void {
    for (const task of this.activeTasksForRun(runId)) {
      task.controller.abort(reason);
    }
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

function waitingForSignal(step: AnyStepDefinition, state: RunState): boolean {
  return step.waitForSignal !== undefined && state.signals[step.waitForSignal] === undefined;
}

function stepReadyAt(step: AnyStepDefinition, state: RunState, stepState: StepState): number {
  const base = stepState.needs.length === 0
    ? state.createdAt
    : Math.max(...stepState.needs.map((dependency) => state.steps[dependency]?.completedAt ?? state.createdAt));
  return base + Math.max(0, step.delayMs ?? 0);
}

function isDuplicateRunError(error: unknown, runId: string): boolean {
  return error instanceof RunAlreadyExistsError && error.details?.runId === runId;
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
