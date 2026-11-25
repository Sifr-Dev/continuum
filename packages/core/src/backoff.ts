import type { ResolvedRetryPolicy, RetryPolicy } from "./types.js";

export function resolveRetryPolicy(policy?: RetryPolicy, defaults?: Partial<ResolvedRetryPolicy>): ResolvedRetryPolicy {
  return {
    attempts: Math.max(1, policy?.attempts ?? defaults?.attempts ?? 1),
    delayMs: Math.max(0, policy?.delayMs ?? defaults?.delayMs ?? 100),
    strategy: policy?.strategy ?? defaults?.strategy ?? "exponential",
    maxDelayMs: Math.max(0, policy?.maxDelayMs ?? defaults?.maxDelayMs ?? 30_000),
    jitter: policy?.jitter ?? defaults?.jitter ?? true,
  };
}

export function retryDelayMs(policy: ResolvedRetryPolicy, failedAttempt: number, random: () => number = Math.random): number {
  const exponential = policy.delayMs * 2 ** Math.max(0, failedAttempt - 1);
  const base = policy.strategy === "exponential" ? exponential : policy.delayMs;
  const capped = Math.min(policy.maxDelayMs, base);
  if (!policy.jitter || capped === 0) return capped;
  return Math.floor(capped * (0.5 + random()));
}

export function retryDecision(
  policy: ResolvedRetryPolicy,
  failedAttempt: number,
  now: number,
  random: () => number = Math.random,
): { retryable: boolean; nextAttemptAt?: number; delayMs?: number } {
  if (failedAttempt >= policy.attempts) return { retryable: false };
  const delay = retryDelayMs(policy, failedAttempt, random);
  return { retryable: true, nextAttemptAt: now + delay, delayMs: delay };
}
