/**
 * Deterministic 5-field cron engine (UTC).
 *
 * Standard Vixie-cron semantics for the minute, hour, day-of-month, month,
 * and day-of-week fields, with `*`, lists, ranges, and step values. All
 * computation is done in UTC so every worker in a distributed deployment
 * derives identical window boundaries regardless of host timezone — a hard
 * requirement for the deterministic, duplicate-safe schedule window IDs the
 * runtime relies on.
 */

export interface CronSpec {
  readonly expression: string;
  /** minute: index 0-59 */
  readonly minutes: readonly boolean[];
  /** hour: index 0-23 */
  readonly hours: readonly boolean[];
  /** day of month: index 0 => day 1 … index 30 => day 31 */
  readonly daysOfMonth: readonly boolean[];
  /** month: index 0 => January … index 11 => December */
  readonly months: readonly boolean[];
  /** day of week: index 0 => Sunday … index 6 => Saturday */
  readonly daysOfWeek: readonly boolean[];
  /** true when the day-of-month field is not `*` */
  readonly domRestricted: boolean;
  /** true when the day-of-week field is not `*` */
  readonly dowRestricted: boolean;
}

export class CronParseError extends Error {
  constructor(message: string, readonly expression: string) {
    super(`Invalid cron expression "${expression}": ${message}`);
    this.name = "CronParseError";
  }
}

const MINUTE = 60_000;
const DAY_MS = 86_400_000;

function parseField(
  field: string,
  min: number,
  max: number,
  name: string,
  expression: string,
  mapValue: (value: number) => number = (value) => value,
): { values: boolean[]; restricted: boolean } {
  const values = new Array<boolean>(max - min + 1).fill(false);
  if (field === "*") return { values: values.fill(true), restricted: false };
  if (field.length === 0) throw new CronParseError(`${name} field is empty`, expression);

  for (const part of field.split(",")) {
    if (part.length === 0) throw new CronParseError(`${name} field has an empty list item`, expression);

    const segments = part.split("/");
    const rangePart = segments[0] as string;
    const stepPart = segments[1];
    if (segments.length > 2) throw new CronParseError(`${name} field "${part}" has too many "/"`, expression);

    let step = 1;
    if (stepPart !== undefined) {
      if (!/^\d+$/.test(stepPart)) throw new CronParseError(`${name} step "${stepPart}" is not a positive integer`, expression);
      step = Number(stepPart);
      if (step < 1) throw new CronParseError(`${name} step must be at least 1`, expression);
    }

    let lo: number;
    let hi: number;
    if (rangePart === "*") {
      lo = min;
      hi = max;
    } else if (rangePart.includes("-")) {
      const bounds = rangePart.split("-");
      if (bounds.length !== 2) throw new CronParseError(`${name} range "${rangePart}" is malformed`, expression);
      const lo2 = parseScalar(bounds[0] as string, min, max, name, expression);
      const hi2 = parseScalar(bounds[1] as string, min, max, name, expression);
      lo = lo2;
      hi = hi2;
      if (lo > hi) throw new CronParseError(`${name} range "${rangePart}" is descending`, expression);
    } else {
      lo = parseScalar(rangePart, min, max, name, expression);
      // A bare value with a step (e.g. "5/15") means "from 5 through max".
      hi = stepPart !== undefined ? max : lo;
    }

    for (let value = lo; value <= hi; value += step) {
      values[mapValue(value) - min] = true;
    }
  }

  return { values, restricted: true };
}

function parseScalar(text: string, min: number, max: number, name: string, expression: string): number {
  if (!/^\d+$/.test(text)) throw new CronParseError(`${name} value "${text}" is not a number`, expression);
  const value = Number(text);
  if (value < min || value > max) throw new CronParseError(`${name} value ${value} is out of range ${min}-${max}`, expression);
  return value;
}

/** Parse and validate a 5-field cron expression. Throws {@link CronParseError}. */
export function parseCron(expression: string): CronSpec {
  if (typeof expression !== "string") throw new CronParseError("expression must be a string", String(expression));
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== 5) {
    throw new CronParseError(`expected 5 fields (minute hour day-of-month month day-of-week) but received ${fields.length}`, expression);
  }
  const [minute, hour, dom, month, dow] = fields as [string, string, string, string, string];

  const minutes = parseField(minute, 0, 59, "minute", expression);
  const hours = parseField(hour, 0, 23, "hour", expression);
  const daysOfMonth = parseField(dom, 1, 31, "day-of-month", expression);
  const months = parseField(month, 1, 12, "month", expression, (value) => value);
  // Allow 7 as an alias for Sunday (0) in the day-of-week field.
  const daysOfWeek = parseField(dow, 0, 7, "day-of-week", expression, (value) => (value === 7 ? 0 : value));

  return {
    expression,
    minutes: minutes.values,
    hours: hours.values,
    daysOfMonth: daysOfMonth.values,
    months: months.values,
    daysOfWeek: daysOfWeek.values,
    domRestricted: daysOfMonth.restricted,
    dowRestricted: daysOfWeek.restricted,
  };
}

function dayMatches(spec: CronSpec, date: Date): boolean {
  const domMatch = spec.daysOfMonth[date.getUTCDate() - 1] === true;
  const dowMatch = spec.daysOfWeek[date.getUTCDay()] === true;
  if (spec.domRestricted && spec.dowRestricted) return domMatch || dowMatch;
  if (spec.domRestricted) return domMatch;
  if (spec.dowRestricted) return dowMatch;
  return true;
}

/**
 * Return the epoch-ms timestamp of the most recent cron occurrence at or
 * before `nowMs` (inclusive), or strictly before it when `inclusive` is
 * false. Returns `NaN` when no occurrence exists at or after the Unix epoch.
 *
 * The search decrements whole non-matching months, days, hours, and minutes,
 * so it converges in a handful of steps for typical expressions instead of
 * walking every minute.
 */
export function previousCronOccurrence(spec: CronSpec, nowMs: number, inclusive = true): number {
  const base = Math.floor(nowMs / MINUTE) * MINUTE;
  const candidate = inclusive ? base : base - 1;
  const date = new Date(candidate);
  date.setUTCSeconds(0, 0);

  // Each branch strictly decreases the candidate, so the loop always makes
  // progress; the guard is a defensive backstop against pathological input.
  for (let guard = 0; guard < 1_000_000; guard += 1) {
    if (date.getUTCFullYear() < 1970) return Number.NaN;

    if (!spec.months[date.getUTCMonth()]) {
      // Jump to the last day of the previous month at 23:59 so the next
      // iteration re-evaluates month/day/hour/minute from a strictly earlier
      // point. setUTCDate(0) from the first of a month yields the last day of
      // the month before it.
      date.setUTCDate(1);
      date.setUTCHours(0, 0, 0, 0);
      date.setUTCDate(0);
      date.setUTCHours(23, 59, 0, 0);
      continue;
    }

    if (!dayMatches(spec, date)) {
      date.setUTCDate(date.getUTCDate() - 1);
      date.setUTCHours(23, 59, 0, 0);
      continue;
    }

    if (!spec.hours[date.getUTCHours()]) {
      date.setUTCHours(date.getUTCHours() - 1, 59, 0, 0);
      continue;
    }

    if (!spec.minutes[date.getUTCMinutes()]) {
      date.setUTCMinutes(date.getUTCMinutes() - 1, 0, 0);
      continue;
    }

    return date.getTime();
  }

  return Number.NaN;
}

/**
 * Return up to `count` most-recent occurrences at or before `nowMs`, ordered
 * newest-first. Used to backfill bounded catch-up windows after downtime.
 */
export function recentCronOccurrences(spec: CronSpec, nowMs: number, count: number): number[] {
  const occurrences: number[] = [];
  let cursor = nowMs;
  let inclusive = true;
  for (let i = 0; i < count; i += 1) {
    const found = previousCronOccurrence(spec, cursor, inclusive);
    if (Number.isNaN(found)) break;
    occurrences.push(found);
    cursor = found;
    inclusive = false;
  }
  return occurrences;
}

/** Milliseconds in a day, exported for callers reasoning about window spacing. */
export const CRON_DAY_MS = DAY_MS;

/**
 * Return the epoch-ms timestamp of the next cron occurrence strictly after
 * `nowMs`, or at/after it when `inclusive` is true. Returns `NaN` when no
 * occurrence exists before the year 2100. Mirrors
 * {@link previousCronOccurrence} by advancing whole non-matching
 * months/days/hours/minutes.
 */
export function nextCronOccurrence(spec: CronSpec, nowMs: number, inclusive = false): number {
  const base = Math.floor(nowMs / MINUTE) * MINUTE;
  const date = new Date(inclusive ? base : base + MINUTE);
  date.setUTCSeconds(0, 0);

  for (let guard = 0; guard < 1_000_000; guard += 1) {
    if (date.getUTCFullYear() > 2100) return Number.NaN;

    if (!spec.months[date.getUTCMonth()]) {
      date.setUTCDate(1);
      date.setUTCHours(0, 0, 0, 0);
      date.setUTCMonth(date.getUTCMonth() + 1);
      continue;
    }

    if (!dayMatches(spec, date)) {
      date.setUTCDate(date.getUTCDate() + 1);
      date.setUTCHours(0, 0, 0, 0);
      continue;
    }

    if (!spec.hours[date.getUTCHours()]) {
      date.setUTCHours(date.getUTCHours() + 1, 0, 0, 0);
      continue;
    }

    if (!spec.minutes[date.getUTCMinutes()]) {
      date.setUTCMinutes(date.getUTCMinutes() + 1, 0, 0);
      continue;
    }

    return date.getTime();
  }

  return Number.NaN;
}