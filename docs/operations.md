# Operating Continuum workflows

This document explains how to run, inspect, recover, and reason about production workflow executions.

## Deployment shapes

### Embedded single service

Use `SqliteJournal` and construct `WorkflowRuntime` inside the service that owns the workflow. This is the simplest deployment shape and the one used by the order-system demo.

```ts
const runtime = new WorkflowRuntime({
  journal: new SqliteJournal("./data/continuum.db"),
  context: services,
  concurrency: 8,
});
runtime.registerWorkflow(checkoutWorkflow);
await runtime.recover();
```

Use this shape when one process owns execution and SQLite is sufficient.

### Deferred enqueue plus worker

A web process can enqueue without executing:

```ts
await runtime.startWorkflow(checkoutWorkflow, input, {
  runId: `checkout:${input.orderId}`,
  defer: true,
});
```

Worker processes adopt deferred, scheduled, or interrupted runs:

```bash
continuum worker \
  --workflows ./dist/workflows.js \
  --db ./data/continuum.db
```

For horizontally scaled workers, use PostgreSQL:

```bash
continuum worker \
  --workflows ./dist/workflows.js \
  --postgres postgres://postgres:continuum@localhost:5432/continuum \
  --lease-ttl 10000 \
  --metrics-port 9090
```

Each run has one lease owner at a time. Workers renew their leases continuously, expired leases are recoverable by another worker, and execution writes carry fencing tokens. A superseded/zombie worker cannot write an outcome after another worker takes ownership.

The worker currently polls recovery approximately once per second, so worst-case adoption latency is lease TTL plus one polling interval.

### Worker observability

With `--metrics-port 9090`, the worker exposes localhost endpoints:

- `GET /health` — worker identity and liveness
- `GET /metrics` — Prometheus text metrics: run counts by status, active leases, worker identity, plus per-step execution counters (`continuum_step_executions_total` by workflow/step/outcome), step duration histograms (`continuum_step_duration_ms`), and terminal run counters (`continuum_runs_completed_total`) accumulated by the worker's built-in `ContinuumMetrics` observer. Alert on `outcome="failed"` rates, interrupted-step spikes (deploys or crashes), and p99 duration regressions per step.
- `GET /runs` — JSON run summaries

Use `continuum leases` to inspect active ownership and fencing tokens:

```bash
continuum leases --postgres "$DATABASE_URL"
```

## Operator playbook

### Inspect active and terminal runs

```bash
continuum list --db ./data/continuum.db
```

Important statuses:

- `running` — forward execution is active or waiting for retry
- `compensating` — rollback is active
- `succeeded` — all steps completed
- `failed` — failed before compensable output existed
- `compensated` — failed, then rollback completed
- `cancelled` — operator cancellation completed
- `compensation_failed` — rollback needs engineering attention

### Inspect one run

```bash
continuum inspect checkout:ord_123 --db ./data/continuum.db
continuum inspect checkout:ord_123 --db ./data/continuum.db --events
```

Look for:

- repeated attempts on the same step,
- `nextAttemptAt` in failure events,
- missing compensation on side-effecting steps,
- interrupted steps after deploys or crashes.

### Cancel a run

```bash
continuum cancel checkout:ord_123 \
  --workflows ./dist/workflows.js \
  --db ./data/continuum.db \
  --reason "customer requested cancellation"
```

Cancellation is cooperative. Well-written steps should either finish quickly or abort when `signal` fires.

### Send a durable signal

Human approvals and asynchronous callbacks use `run.signaled` events:

```bash
continuum signal checkout:ord_123 order.review \
  --postgres "$DATABASE_URL" \
  --payload '{"approved":true,"reviewer":"ops@example.com"}'
```

A signal is durable before the workflow reacts to it. Re-sending the same signal replaces the latest projected payload for that signal name but preserves every event in the audit journal.

### Scheduled workflows

Workflows may declare fixed-interval schedules in code. Workers create deterministic window run IDs, so multiple workers can race safely and transactional uniqueness prevents duplicates. Use bounded `catchupWindows` when a deployment should backfill recent missed windows after downtime.

### Export an audit trail

```bash
continuum export --db ./data/continuum.db --out continuum-events.json
```

The export is suitable for incident analysis, compliance review, or replay tooling.

## Failure interpretation

### Transient dependency failure

Symptoms:

- repeated `step.started` / `step.failed` pairs,
- increasing `attempt`,
- run remains `running`.

Expected behavior: retry eventually succeeds or exhausts policy.

### Deploys and graceful shutdown

`runtime.close({ graceMs })` is a bounded drain: in-flight steps get `graceMs` (default 5s) to finish and journal their outcomes, then remaining work is aborted, run leases are released — so replacement workers adopt immediately instead of waiting out the lease TTL — and the journal closes. Steps that ignore cancellation are abandoned after a bounded window; fencing turns their late writes into rejected no-ops, so a stuck step cannot hang a deploy or corrupt the journal. A closing runtime rejects new runs.

The CLI worker wires this to process signals: `SIGTERM`/`SIGINT` starts the drain, tunable with `--shutdown-grace ms`. Set the orchestrator's termination grace (e.g. Kubernetes `terminationGracePeriodSeconds`) above `--shutdown-grace` plus a few seconds of slack.

### Worker crash

Symptoms before recovery:

- a `step.started` event with no outcome.

After recovery:

- `step.interrupted`,
- another attempt if retry budget remains.

The second attempt uses the same idempotency key. Downstream systems must deduplicate by that key.

### Terminal forward failure with rollback

Symptoms:

- a step becomes `failed`,
- later pending steps become `skipped`,
- compensation events appear in reverse graph order,
- terminal status becomes `compensated`.

### Compensation failure

Symptoms:

- `compensation.step_failed` retries,
- eventually `run.compensation_failed`.

This state should page a human. The system is explicitly telling you that forward execution failed and rollback could not be proven complete.

## Idempotency requirements

Every externally visible effect should accept the step idempotency key:

```ts
execute: ({ input, context, idempotencyKey }) =>
  context.payments.charge(idempotencyKey!, input)
```

Recommended key behavior in downstream services:

- If a completed effect exists for the key, return the original result.
- If the key is new, perform the effect once and store the result with the key.
- If a crash leaves ambiguity, prefer safe manual reconciliation over blind duplication.

Never put the attempt number in the key. Retries must collapse to the same logical effect.

## Retry policy guidance

| Failure type | Recommended policy |
| --- | --- |
| Network blip | exponential backoff, 3–5 attempts, jitter |
| Rate limit | larger initial delay, honor downstream retry-after when available |
| Invalid input | 1 attempt; retrying cannot help |
| Scarce resource conflict | exponential backoff with jitter |
| Compensation | more attempts and a lower maximum delay than normal business work |

## Timeout guidance

Choose a timeout slightly above the downstream system's expected p99 latency. Passing `signal` into HTTP clients, database drivers, subprocesses, and cloud SDKs is strongly recommended.

A timeout does not prove the downstream effect did not happen. It only proves this worker stopped waiting. The idempotency key is what makes the retry safe.

## Operational metrics to add

The runtime observer interface supports metrics exporters. Useful production metrics include:

- runs started/completed/failed/cancelled by workflow and status,
- step attempts by step name and outcome,
- retry count distribution,
- compensation started/completed/failed,
- recovery adoption count,
- journal append latency,
- run duration histogram.

## Capacity and backpressure

Two independent bounds govern worker execution:

- `concurrency` (default 4) caps forward steps executing concurrently within a single run;
- `maxConcurrentSteps` (default 100, `--max-concurrent-steps` on the CLI worker) caps step executions across ALL runs on the worker.

The global cap is the backpressure bound that matters operationally: when a node dies and its hundreds of leased runs are adopted by survivors, each survivor never executes more than its configured budget, so mass adoption cannot exhaust a replacement worker. Saturated runs wait for capacity rather than failing — capacity starvation is transient backpressure, not a scheduling deadlock. Size the budget against your steps' resource cost (memory per execution, downstream connection pools) and scale horizontally by adding workers.

## Production hardening checklist

Before using Continuum for real customer-impacting workflows:

- use idempotency keys in every external integration,
- write compensations for reversible effects,
- make compensation idempotent,
- pass abort signals into IO clients,
- test process-kill recovery, not only thrown exceptions,
- monitor `compensation_failed` as a pageable state,
- keep workflow inputs small and JSON-serializable,
- register the exact workflow versions needed for in-flight recovery,
- decide how long journals are retained and how they are archived.
