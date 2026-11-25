import type { ContinuumEvent, RunState, RuntimeObserver } from "./types.js";

const DURATION_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

function escapeLabel(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll("\"", "\\\"");
}

function nestedIncrement(map: Map<string, Map<string, number>>, outer: string, inner: string): void {
  const child = map.get(outer) ?? new Map<string, number>();
  child.set(inner, (child.get(inner) ?? 0) + 1);
  map.set(outer, child);
}

interface DurationSeries {
  buckets: number[];
  sum: number;
  count: number;
}

/**
 * RuntimeObserver that accumulates Prometheus-friendly execution metrics from
 * every event the runtime journals: step execution counts and duration
 * histograms per workflow/step/outcome, plus terminal run counts per status.
 *
 * Attach it as an observer and serve `textFormat()` from a metrics endpoint.
 * It sees events written by the runtime it is attached to — for distributed
 * deployments that is exactly the owning worker's execution activity, which is
 * what operator alerting wants.
 */
export class ContinuumMetrics implements RuntimeObserver {
  private readonly stepExecutions = new Map<string, Map<string, number>>();
  private readonly stepDurations = new Map<string, DurationSeries>();
  private readonly runCompletions = new Map<string, Map<string, number>>();

  onEvent(event: ContinuumEvent): void {
    if (event.type !== "step.succeeded" && event.type !== "step.failed" && event.type !== "step.interrupted") return;
    const step = typeof event.data.step === "string" ? event.data.step : "unknown";
    const outcome = event.type === "step.succeeded" ? "succeeded" : event.type === "step.failed" ? "failed" : "interrupted";
    nestedIncrement(this.stepExecutions, `${event.workflowName}/${step}`, outcome);

    const durationMs = typeof event.data.durationMs === "number" ? event.data.durationMs : undefined;
    if (durationMs !== undefined) {
      const key = `${event.workflowName}/${step}`;
      const series = this.stepDurations.get(key) ?? {
        buckets: new Array<number>(DURATION_BUCKETS_MS.length + 1).fill(0),
        sum: 0,
        count: 0,
      };
      const index = DURATION_BUCKETS_MS.findIndex((bound) => durationMs <= bound);
      series.buckets[index === -1 ? DURATION_BUCKETS_MS.length : index] = (series.buckets[index === -1 ? DURATION_BUCKETS_MS.length : index] ?? 0) + 1;
      series.sum += durationMs;
      series.count += 1;
      this.stepDurations.set(key, series);
    }
  }

  onRunFinished(state: RunState): void {
    nestedIncrement(this.runCompletions, state.workflowName, state.status);
  }

  textFormat(): string {
    const lines: string[] = [
      "# HELP continuum_step_executions_total Step execution outcomes observed by this worker.",
      "# TYPE continuum_step_executions_total counter",
    ];
    for (const [workflowStep, outcomes] of [...this.stepExecutions.entries()].sort()) {
      const slash = workflowStep.indexOf("/");
      const workflow = workflowStep.slice(0, slash);
      const step = workflowStep.slice(slash + 1);
      for (const [outcome, count] of [...outcomes.entries()].sort()) {
        lines.push(`continuum_step_executions_total{workflow="${escapeLabel(workflow)}",step="${escapeLabel(step)}",outcome="${escapeLabel(outcome)}"} ${count}`);
      }
    }

    lines.push(
      "# HELP continuum_step_duration_ms Step execution duration observed by this worker.",
      "# TYPE continuum_step_duration_ms histogram",
    );
    for (const [workflowStep, series] of [...this.stepDurations.entries()].sort()) {
      const slash = workflowStep.indexOf("/");
      const workflow = workflowStep.slice(0, slash);
      const step = workflowStep.slice(slash + 1);
      const labels = `workflow="${escapeLabel(workflow)}",step="${escapeLabel(step)}"`;
      let cumulative = 0;
      series.buckets.forEach((count, index) => {
        cumulative += count;
        const bound = index < DURATION_BUCKETS_MS.length ? String(DURATION_BUCKETS_MS[index]) : "+Inf";
        lines.push(`continuum_step_duration_ms_bucket{${labels},le="${bound}"} ${cumulative}`);
      });
      lines.push(`continuum_step_duration_ms_sum{${labels}} ${series.sum}`);
      lines.push(`continuum_step_duration_ms_count{${labels}} ${series.count}`);
    }

    lines.push(
      "# HELP continuum_runs_completed_total Workflow runs reaching a terminal status on this worker.",
      "# TYPE continuum_runs_completed_total counter",
    );
    for (const [workflow, statuses] of [...this.runCompletions.entries()].sort()) {
      for (const [status, count] of [...statuses.entries()].sort()) {
        lines.push(`continuum_runs_completed_total{workflow="${escapeLabel(workflow)}",status="${escapeLabel(status)}"} ${count}`);
      }
    }
    return `${lines.join("\n")}\n`;
  }
}
