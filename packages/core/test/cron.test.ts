import assert from "node:assert/strict";
import test from "node:test";
import {
  MemoryJournal,
  WorkflowRuntime,
  WorkflowValidationError,
  defineStep,
  defineWorkflow,
  nextCronOccurrence,
  parseCron,
  previousCronOccurrence,
  recentCronOccurrences,
} from "../src/index.js";

const MINUTE = 60_000;
const HOUR = 3_600_000;
const DAY = 86_400_000;

function utc(iso: string): number {
  return new Date(iso).getTime();
}

test("parses a 5-field expression with all wildcard fields", () => {
  const spec = parseCron("* * * * *");
  assert.equal(spec.minutes.every(Boolean), true);
  assert.equal(spec.hours.every(Boolean), true);
  assert.equal(spec.daysOfMonth.every(Boolean), true);
  assert.equal(spec.months.every(Boolean), true);
  assert.equal(spec.daysOfWeek.every(Boolean), true);
  assert.equal(spec.domRestricted, false);
  assert.equal(spec.dowRestricted, false);
});

test("rejects malformed expressions with a typed error", () => {
  assert.throws(() => parseCron("* * * *"), /expected 5 fields/);
  assert.throws(() => parseCron("60 * * * *"), /out of range/);
  assert.throws(() => parseCron("* 24 * * *"), /out of range/);
  assert.throws(() => parseCron("* * 0 * *"), /out of range/);
  assert.throws(() => parseCron("* * * 13 *"), /out of range/);
  assert.throws(() => parseCron("* * * * 8"), /out of range/);
  assert.throws(() => parseCron("*/0 * * * *"), /at least 1/);
  assert.throws(() => parseCron("5-1 * * * *"), /descending/);
});

test("supports lists, ranges, and steps", () => {
  const spec = parseCron("0,15,30,45 9-17/2 * * 1-5");
  assert.deepEqual(
    spec.minutes.map((on, i) => (on ? i : -1)).filter((i) => i >= 0),
    [0, 15, 30, 45],
  );
  assert.deepEqual(
    spec.hours.map((on, i) => (on ? i : -1)).filter((i) => i >= 0),
    [9, 11, 13, 15, 17],
  );
  assert.deepEqual(
    spec.daysOfWeek.map((on, i) => (on ? i : -1)).filter((i) => i >= 0),
    [1, 2, 3, 4, 5],
  );
});

test("treats day-of-week 7 as Sunday", () => {
  const spec = parseCron("0 0 * * 7");
  assert.equal(spec.daysOfWeek[0], true);
});

test("previous occurrence of a daily expression is the prior day at 00:00 UTC", () => {
  const spec = parseCron("0 0 * * *");
  const now = utc("2026-11-26T12:34:56Z");
  assert.equal(previousCronOccurrence(spec, now), utc("2026-11-26T00:00:00Z"));
});

test("inclusive=false skips the current minute", () => {
  const spec = parseCron("* * * * *");
  const now = utc("2026-11-26T12:34:30Z");
  assert.equal(previousCronOccurrence(spec, now, true), utc("2026-11-26T12:34:00Z"));
  assert.equal(previousCronOccurrence(spec, now, false), utc("2026-11-26T12:33:00Z"));
});

test("hourly expression lands on the top of the previous hour", () => {
  const spec = parseCron("0 * * * *");
  const now = utc("2026-11-26T12:34:00Z");
  assert.equal(previousCronOccurrence(spec, now), utc("2026-11-26T12:00:00Z"));
});

test("weekday-only expression skips weekends", () => {
  // 2026-11-28 is a Saturday; the most recent weekday firing is Friday 09:00.
  const spec = parseCron("0 9 * * 1-5");
  const now = utc("2026-11-28T10:00:00Z");
  assert.equal(previousCronOccurrence(spec, now), utc("2026-11-27T09:00:00Z"));
});

test("day-of-month and day-of-week use OR semantics when both are restricted", () => {
  // 2026-11-26 is a Thursday. Expression fires on the 1st OR on Mondays.
  const spec = parseCron("0 0 1 * 1");
  assert.equal(spec.domRestricted, true);
  assert.equal(spec.dowRestricted, true);
  // From 2026-11-26 the most recent match is Monday 2026-11-23.
  assert.equal(previousCronOccurrence(spec, utc("2026-11-26T12:00:00Z")), utc("2026-11-23T00:00:00Z"));
});

test("recentCronOccurrences returns newest-first windows", () => {
  const spec = parseCron("0 0 * * *");
  const now = utc("2026-11-26T12:00:00Z");
  const occurrences = recentCronOccurrences(spec, now, 3);
  assert.deepEqual(occurrences, [
    utc("2026-11-26T00:00:00Z"),
    utc("2026-11-25T00:00:00Z"),
    utc("2026-11-24T00:00:00Z"),
  ]);
});

test("handles month boundaries and leap-day correctly", () => {
  const spec = parseCron("0 0 1 * *");
  assert.equal(previousCronOccurrence(spec, utc("2026-03-15T00:00:00Z")), utc("2026-03-01T00:00:00Z"));
  // 2028 is a leap year; Feb 29 exists.
  const leap = parseCron("0 0 29 2 *");
  assert.equal(previousCronOccurrence(leap, utc("2028-03-01T00:00:00Z")), utc("2028-02-29T00:00:00Z"));
});

interface CronInput {
  value: number;
}

function scheduledRuntime(spec: { cron?: string; intervalMs?: number; catchupWindows?: number }) {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  const step = defineStep<CronInput, number>({
    name: "tick",
    execute: async ({ input }) => input.value,
  });
  const workflow = defineWorkflow<CronInput>({
    name: "cron-job",
    version: 1,
    steps: [step],
    schedule: { input: { value: 1 }, ...spec },
  });
  runtime.registerWorkflow(workflow);
  return runtime;
}

test("runtime starts one deterministic run per cron occurrence", async () => {
  const runtime = scheduledRuntime({ cron: "0 0 * * *" });
  const now = utc("2026-11-26T12:00:00Z");
  const started = await runtime.startDueScheduledWorkflows(now);
  assert.deepEqual(started, [`scheduled:cron-job:1:${utc("2026-11-26T00:00:00Z")}`]);

  // Re-running the same window is a duplicate and starts nothing new.
  const again = await runtime.startDueScheduledWorkflows(now);
  assert.deepEqual(again, []);
});

test("runtime backfills bounded catch-up windows for cron", async () => {
  const runtime = scheduledRuntime({ cron: "0 0 * * *", catchupWindows: 2 });
  const now = utc("2026-11-26T12:00:00Z");
  const started = await runtime.startDueScheduledWorkflows(now);
  assert.deepEqual(started, [
    `scheduled:cron-job:1:${utc("2026-11-26T00:00:00Z")}`,
    `scheduled:cron-job:1:${utc("2026-11-25T00:00:00Z")}`,
    `scheduled:cron-job:1:${utc("2026-11-24T00:00:00Z")}`,
  ]);
});

test("interval schedules still work and remain deterministic", async () => {
  const runtime = scheduledRuntime({ intervalMs: 60_000 });
  const now = utc("2026-11-26T12:03:30Z");
  const started = await runtime.startDueScheduledWorkflows(now);
  assert.deepEqual(started, [`scheduled:cron-job:1:${utc("2026-11-26T12:03:00Z")}`]);
});

test("validation rejects schedules that set both cron and intervalMs", () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {} });
  const step = defineStep<CronInput, number>({ name: "tick", execute: async ({ input }) => input.value });
  const workflow = defineWorkflow<CronInput>({
    name: "bad",
    version: 1,
    steps: [step],
    schedule: { input: { value: 1 }, cron: "* * * * *", intervalMs: 60_000 } as unknown as {
      input: CronInput;
    },
  });
  assert.throws(() => runtime.registerWorkflow(workflow), WorkflowValidationError);
});

test("validation rejects schedules with neither cron nor intervalMs", () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {} });
  const step = defineStep<CronInput, number>({ name: "tick", execute: async ({ input }) => input.value });
  const workflow = defineWorkflow<CronInput>({
    name: "bad",
    version: 1,
    steps: [step],
    schedule: { input: { value: 1 } } as unknown as { input: CronInput },
  });
  assert.throws(() => runtime.registerWorkflow(workflow), WorkflowValidationError);
});

test("validation rejects an invalid cron expression", () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {} });
  const step = defineStep<CronInput, number>({ name: "tick", execute: async ({ input }) => input.value });
  const workflow = defineWorkflow<CronInput>({
    name: "bad",
    version: 1,
    steps: [step],
    schedule: { input: { value: 1 }, cron: "99 * * * *" },
  });
  assert.throws(() => runtime.registerWorkflow(workflow), WorkflowValidationError);
});

test("cron windows are stable across a one-minute drift", () => {
  const spec = parseCron("*/15 * * * *");
  const a = previousCronOccurrence(spec, utc("2026-11-26T12:14:59Z"));
  const b = previousCronOccurrence(spec, utc("2026-11-26T12:15:00Z"));
  assert.equal(a, utc("2026-11-26T12:00:00Z"));
  assert.equal(b, utc("2026-11-26T12:15:00Z"));
  assert.ok(b - a === 15 * MINUTE);
});

test("nextCronOccurrence returns the next strictly-future firing", () => {
  const spec = parseCron("0 0 * * *");
  const now = utc("2026-11-26T12:00:00Z");
  assert.equal(nextCronOccurrence(spec, now), utc("2026-11-27T00:00:00Z"));
  // inclusive includes a firing exactly on the boundary.
  assert.equal(nextCronOccurrence(spec, utc("2026-11-26T00:00:00Z"), true), utc("2026-11-26T00:00:00Z"));
});

test("nextCronOccurrence crosses month and year boundaries", () => {
  const spec = parseCron("0 0 1 1 *"); // 00:00 on Jan 1
  assert.equal(nextCronOccurrence(spec, utc("2026-11-26T12:00:00Z")), utc("2027-01-01T00:00:00Z"));
});

test("next and previous occurrences bracket the current time for a daily job", () => {
  const spec = parseCron("0 9 * * *");
  const now = utc("2026-11-26T12:00:00Z");
  const prev = previousCronOccurrence(spec, now);
  const next = nextCronOccurrence(spec, now);
  assert.equal(prev, utc("2026-11-26T09:00:00Z"));
  assert.equal(next, utc("2026-11-27T09:00:00Z"));
  assert.ok(prev < now && now < next);
});