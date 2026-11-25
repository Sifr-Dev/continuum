import type { RunState, RunSummary } from "@continuum/core";

export function parseWorkflowSelector(selector: string): { name: string; version?: number } {
  const [name, versionText] = selector.split("@");
  if (!name) throw new Error(`Invalid workflow selector "${selector}"`);
  if (versionText === undefined) return { name };
  const version = Number(versionText);
  if (!Number.isInteger(version) || version < 1) throw new Error(`Invalid workflow version in "${selector}"`);
  return { name, version };
}

export function formatRunSummary(run: RunSummary): string {
  return [
    `${run.workflowName}@${run.version}`,
    `run:       ${run.runId}`,
    `status:    ${run.status}`,
    `created:   ${new Date(run.createdAt).toISOString()}`,
    `updated:   ${new Date(run.updatedAt).toISOString()}`,
    run.completedAt ? `completed: ${new Date(run.completedAt).toISOString()}` : undefined,
    `events:    ${run.eventCount}`,
    run.error ? `error:     ${run.error.name}: ${run.error.message}` : undefined,
  ].filter(Boolean).join("\n");
}

export function formatStepTable(state: RunState): string {
  const rows = Object.values(state.steps).map((step) => [
    step.name,
    step.status,
    String(step.attempts),
    String(step.compensationAttempts),
    step.needs.join(",") || "-",
    step.error?.message ?? step.compensationError?.message ?? "",
  ]);
  return table(["step", "status", "attempts", "comp", "needs", "error"], rows);
}

export function table(headers: readonly string[], rows: readonly (readonly string[])[]): string {
  const widths = headers.map((header, index) => Math.max(header.length, ...rows.map((row) => row[index]?.length ?? 0)));
  const line = (cells: readonly string[]) => cells.map((cell, index) => cell.padEnd(widths[index] ?? cell.length)).join("  ").trimEnd();
  return [
    line(headers),
    widths.map((width) => "-".repeat(width)).join("  "),
    ...rows.map(line),
  ].join("\n");
}

export function json(value: unknown): string {
  return JSON.stringify(value, null, 2);
}
