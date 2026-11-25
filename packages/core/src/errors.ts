import type { JsonObject, JsonValue, SerializedError } from "./types.js";

export class ContinuumError extends Error {
  readonly code: string;
  readonly details?: JsonObject;

  constructor(message: string, code = "CONTINUUM_ERROR", details?: JsonObject) {
    super(message);
    this.name = new.target.name;
    this.code = code;
    this.details = details;
  }
}

export class WorkflowValidationError extends ContinuumError {
  constructor(message: string, details?: JsonObject) {
    super(message, "WORKFLOW_VALIDATION_ERROR", details);
  }
}

export class RunAlreadyExistsError extends ContinuumError {
  constructor(runId: string) {
    super(`Run "${runId}" already exists`, "RUN_ALREADY_EXISTS", { runId });
  }
}

export class StepTimeoutError extends ContinuumError {
  constructor(stepName: string, timeoutMs: number) {
    super(`Step "${stepName}" timed out after ${timeoutMs}ms`, "STEP_TIMEOUT", { stepName, timeoutMs });
  }
}

export class StepInterruptedError extends ContinuumError {
  constructor(stepName: string, reason = "The worker stopped before the step outcome was recorded") {
    super(`Step "${stepName}" was interrupted: ${reason}`, "STEP_INTERRUPTED", { stepName, reason });
  }
}

export class RunCancelledError extends ContinuumError {
  constructor(runId: string) {
    super(`Run "${runId}" was cancelled`, "RUN_CANCELLED", { runId });
  }
}

export class WorkerLeaseLostError extends ContinuumError {
  constructor(runId: string) {
    super(`Worker lease for run "${runId}" was lost`, "WORKER_LEASE_LOST", { runId });
  }
}

export class CompensationError extends ContinuumError {
  constructor(stepName: string, causeMessage: string) {
    super(`Compensation for step "${stepName}" failed: ${causeMessage}`, "COMPENSATION_FAILED", {
      stepName,
      causeMessage,
    });
  }
}

export function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    const maybeCode = (error as { code?: unknown }).code;
    const maybeDetails = (error as { details?: unknown }).details;
    return {
      name: error.name || "Error",
      message: error.message,
      ...(error.stack ? { stack: error.stack } : {}),
      ...(typeof maybeCode === "string" ? { code: maybeCode } : {}),
      ...(isJsonObject(maybeDetails) ? { details: sanitizeJsonObject(maybeDetails) } : {}),
    };
  }

  if (typeof error === "string") {
    return { name: "Error", message: error };
  }

  if (isJsonObject(error) && typeof error.message === "string") {
    return {
      name: typeof error.name === "string" ? error.name : "Error",
      message: error.message,
      ...(typeof error.stack === "string" ? { stack: error.stack } : {}),
      ...(typeof error.code === "string" ? { code: error.code } : {}),
    };
  }

  return {
    name: "Error",
    message: "Unknown error",
    details: { value: safeJson(error) },
  };
}

function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sanitizeJsonObject(value: JsonObject): JsonObject {
  try {
    return sanitizeJsonValue(value) as JsonObject;
  } catch {
    return { value: "[Unserializable error details]" };
  }
}

function sanitizeJsonValue(value: unknown, seen = new Set<object>()): JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) ? value : String(value);
  if (typeof value === "bigint" || typeof value === "symbol" || typeof value === "function" || value === undefined) {
    return String(value);
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) return "[Circular]";
    seen.add(value);
    const sanitized = value.map((item) => sanitizeJsonValue(item, seen));
    seen.delete(value);
    return sanitized;
  }
  if (typeof value === "object" && value !== null) {
    if (seen.has(value)) return "[Circular]";
    seen.add(value);
    const sanitized: JsonObject = {};
    for (const [key, item] of Object.entries(value)) sanitized[key] = sanitizeJsonValue(item, seen);
    seen.delete(value);
    return sanitized;
  }
  return String(value);
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
