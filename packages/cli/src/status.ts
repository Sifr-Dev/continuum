import type { RunState, RunStatus, StepState } from "@continuum/core";

/**
 * Operator-facing classification of a run.
 *
 * `RunStatus` alone is not enough to operate a durable workflow: a `running`
 * run may be executing, backing off before a retry, parked on a durable timer,
 * parked on a human approval signal, or simply enqueued and not yet adopted by
 * any worker. Those states need very different operator responses, so the CLI
 * derives a phase from the projected state instead of trusting `status`.
 */
export type RunPhase =
  | "terminal"
  | "cancel_requested"
  | "compensating"
  | "executing"
  | "awaiting_retry"
  | "awaiting_timer"
  | "awaiting_signal"
  | "runnable"
  | "blocked";

export interface RunPhaseInfo {
  phase: RunPhase;
  status: RunStatus;
  detail: string;
  /** Expected self-resume time for `awaiting_retry` / `awaiting_timer`. */
  resumeAt?: number;
  /** Signal name the run is parked on for `awaiting_signal`. */
  signal?: string;
}

const TERMINAL: readonly RunStatus[] = ["succeeded", "failed", "cancelled", "compensated", "compensation_failed"];

export function isTerminalStatus(status: RunStatus): boolean {
  return TERMINAL.includes(status);
}

/**
 * Phases that will not resolve without outside input (a signal, a timer, or a
 * retry backoff). A recovery pass must report these as healthy parking states
 * rather than waiting for them and calling the timeout a failure.
 */
const PARKED: readonly RunPhase[] = ["awaiting_signal", "awaiting_timer", "awaiting_retry"];

export function isParkedPhase(phase: RunPhase): boolean {
  return PARKED.includes(phase);
}

/** A run is settled for recovery when it is finished or legitimately parked. */
export function isSettledPhase(phase: RunPhase): boolean {
  return phase === "terminal" || isParkedPhase(phase);
}

/**
 * Derives the operator phase of a run from its projected state.
 * Pure: never mutates and never touches the journal.
 */
export function classifyRun(state: RunState, now: number = Date.now()): RunPhaseInfo {
  if (isTerminalStatus(state.status)) {
    return { phase: "terminal", status: state.status, detail: describeTerminal(state) };
  }
  if (state.status === "compensating") {
    return { phase: "compensating", status: state.status, detail: state.compensationReason ?? "rollback in progress" };
  }
  if (state.cancellationRequested) {
    return {
      phase: "cancel_requested",
      status: state.status,
      detail: state.cancellationReason
        ? `cancellation requested (${state.cancellationReason}); a worker must run the rollback`
        : "cancellation requested; a worker must run the rollback",
    };
  }

  const steps = Object.values(state.steps);
  const running = steps.filter((step) => step.status === "running");
  if (running.length > 0) {
    return {
      phase: "executing",
      status: state.status,
      detail: `executing ${running.map((step) => step.name).join(", ")}`,
    };
  }

  // Only steps whose ordering dependencies are satisfied can be "ready".
  const eligible = steps.filter((step) => step.status === "pending" && dependenciesMet(step, state));
  const waiting = eligible
    .map((step) => waitingReason(step, state, now))
    .filter((info): info is RunPhaseInfo => info !== undefined);

  if (waiting.length > 0) {
    // Parked on a human signal is the most actionable blocker, so surface it
    // first; otherwise report the blocking condition that clears soonest.
    const signal = waiting.find((info) => info.phase === "awaiting_signal");
    if (signal) return signal;
    const soonest = [...waiting].sort((a, b) => (a.resumeAt ?? Number.POSITIVE_INFINITY) - (b.resumeAt ?? Number.POSITIVE_INFINITY))[0];
    if (soonest) return soonest;
  }

  if (eligible.length > 0) {
    return {
      phase: "runnable",
      status: state.status,
      detail: `eligible to run but no worker is executing it: ${eligible.map((step) => step.name).join(", ")}`,
    };
  }

  const stalled = steps.filter((step) => step.status === "pending");
  if (stalled.length > 0) {
    return {
      phase: "blocked",
      status: state.status,
      detail: `waiting on unmet dependencies: ${stalled.map((step) => step.name).join(", ")}`,
    };
  }

  return { phase: "executing", status: state.status, detail: "no pending or running steps" };
}

function waitingReason(step: StepState, state: RunState, now: number): RunPhaseInfo | undefined {
  if (step.waitForSignal !== undefined && state.signals[step.waitForSignal] === undefined) {
    return {
      phase: "awaiting_signal",
      status: state.status,
      detail: `waiting for signal "${step.waitForSignal}" on step "${step.name}"`,
      signal: step.waitForSignal,
    };
  }
  if (step.nextAttemptAt !== undefined && step.nextAttemptAt > now) {
    return {
      phase: "awaiting_retry",
      status: state.status,
      detail: `backing off before retrying step "${step.name}"`,
      resumeAt: step.nextAttemptAt,
    };
  }
  const readyAt = stepReadyAt(step, state);
  if (readyAt !== undefined && readyAt > now) {
    return {
      phase: "awaiting_timer",
      status: state.status,
      detail: `step "${step.name}" is gated by a durable timer`,
      resumeAt: readyAt,
    };
  }
  return undefined;
}

/** Absolute time a `delayMs`-gated step becomes eligible, or undefined when it already is. */
export function stepReadyAt(step: StepState, state: RunState): number | undefined {
  const delayMs = step.delayMs ?? 0;
  if (delayMs <= 0) return undefined;
  const base = step.needs.length === 0
    ? state.createdAt
    : Math.max(...step.needs.map((need) => state.steps[need]?.completedAt ?? state.createdAt));
  return base + delayMs;
}

function dependenciesMet(step: StepState, state: RunState): boolean {
  return step.needs.every((need) => {
    const status = state.steps[need]?.status;
    return status === "succeeded" || status === "skipped";
  });
}

function describeTerminal(state: RunState): string {
  switch (state.status) {
    case "succeeded":
      return "completed successfully";
    case "cancelled":
      return state.cancellationReason ? `cancelled (${state.cancellationReason})` : "cancelled";
    case "compensated":
      return "failed and rolled back cleanly";
    case "compensation_failed":
      return "rollback failed and needs engineering attention";
    default:
      return state.error ? `${state.error.name}: ${state.error.message}` : "failed";
  }
}

export function formatPhase(info: RunPhaseInfo): string {
  const lines = [`phase:     ${info.phase}`, `           ${info.detail}`];
  if (info.resumeAt !== undefined) lines.push(`resume:    ${iso(info.resumeAt)}`);
  return lines.join("\n");
}

function iso(at: number): string {
  return new Date(at).toISOString();
}

export type RecoveryOutcome = RunStatus | RunPhase | "timed_out";

export interface RecoveryResult {
  runId: string;
  /** How the recovery pass left this run: a terminal status, a parking phase, or a timeout. */
  outcome: RecoveryOutcome;
  status: RunStatus;
  detail: string;
  /** Milliseconds the recovery pass spent watching this run. */
  waitedMs: number;
  /** Expected self-resume time for timed-out or parked runs. */
  resumeAt?: number;
}

/**
 * Watches adopted runs until each one finishes or parks on external input.
 *
 * A worker simply cannot finish a run that is waiting for a human approval or a
 * long durable timer. Previously the CLI blocked on a fixed 30s timeout and
 * exited non-zero, reporting those healthy runs as failures. Recovery now stops
 * watching as soon as a run is provably parked and reports why.
 */
export async function settleRuns(
  getRun: (runId: string) => Promise<RunState | undefined>,
  runIds: readonly string[],
  options: {
    timeoutMs: number;
    pollMs?: number;
    now?: () => number;
    sleep?: (ms: number) => Promise<void>;
  },
): Promise<RecoveryResult[]> {
  const now = options.now ?? Date.now;
  const pollMs = Math.max(1, options.pollMs ?? 50);
  const sleep = options.sleep ?? defaultSleep;
  const startedAt = now();
  const deadline = startedAt + Math.max(0, options.timeoutMs);

  const settled = new Map<string, RecoveryResult>();
  let moving = new Set(runIds);

  while (moving.size > 0) {
    for (const runId of [...moving]) {
      const state = await getRun(runId);
      if (!state) {
        moving.delete(runId);
        continue;
      }
      const info = classifyRun(state, now());
      const watched = {
        runId,
        status: state.status,
        detail: info.detail,
        waitedMs: now() - startedAt,
        ...(info.resumeAt !== undefined ? { resumeAt: info.resumeAt } : {}),
      };
      if (isSettledPhase(info.phase)) {
        settled.set(runId, { ...watched, outcome: info.phase === "terminal" ? state.status : info.phase });
        moving.delete(runId);
      } else {
        settled.set(runId, { ...watched, outcome: "timed_out" });
      }
    }
    if (moving.size === 0) break;
    if (now() >= deadline) break;
    await sleep(Math.min(pollMs, Math.max(1, deadline - now())));
  }

  return runIds.map((runId) => settled.get(runId) ?? {
    runId,
    outcome: "timed_out",
    status: "running",
    detail: "run was not observed before the recovery deadline",
    waitedMs: now() - startedAt,
  });
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Outcomes that mean recovery left the system needing operator action. */
const UNHEALTHY: readonly RecoveryOutcome[] = ["failed", "compensation_failed", "timed_out"];

export function recoveryExitCode(results: readonly RecoveryResult[]): number {
  return results.some((result) => UNHEALTHY.includes(result.outcome)) ? 1 : 0;
}

export function formatRecoveryReport(results: readonly RecoveryResult[]): string {
  if (results.length === 0) return "recovery settled: no runs needed attention";
  const parkLike: readonly RecoveryOutcome[] = ["awaiting_signal", "awaiting_timer", "awaiting_retry"];
  const parked = results.filter((result) => parkLike.includes(result.outcome)).length;
  const finished = results.filter((result) => isTerminalStatus(result.outcome as RunStatus)).length;
  const moving = results.filter((result) => result.outcome === "timed_out").length;
  const width = Math.max(...results.map((result) => result.runId.length));
  return [
    `recovery settled: ${finished} finished, ${parked} parked on external input, ${moving} still moving`,
    ...results.map((result) => {
      const resume = result.resumeAt !== undefined ? ` (resumes ${iso(result.resumeAt)})` : "";
      return `  ${result.runId.padEnd(width)}  ${result.outcome.padEnd(18)}  ${result.detail}${resume}`;
    }),
  ].join("\n");
}
