# Aion

**Durable execution for multi-step TypeScript workflows that must survive crashes, retries and partial failure.**

[![TypeScript](https://img.shields.io/badge/TYPESCRIPT-3178c6?style=for-the-badge&logo=typescript&logoColor=white&labelColor=1b1f27)](https://www.typescriptlang.org/)
[![Node](https://img.shields.io/badge/NODE-22%20%7C%2026-5fa04e?style=for-the-badge&logo=nodedotjs&logoColor=white&labelColor=1b1f27)](https://nodejs.org/)
[![CI](https://github.com/Sifr-Dev/continuum/actions/workflows/ci.yml/badge.svg)](https://github.com/Sifr-Dev/continuum/actions/workflows/ci.yml)

> The GitHub repository is still `Sifr-Dev/continuum`; Aion is the product name.
> The npm packages remain `@continuum/*` and the installed CLI remains
> `continuum`. Both are unchanged so existing imports and scripts keep working.

## What it is

Aion is a TypeScript runtime for workflows that perform several external
effects and must not corrupt business state when a process dies halfway
through. Every step transition is appended to a durable journal before it is
treated as true; another worker can replay the journal, skip completed steps,
retry interrupted ones with the same idempotency key, and run compensations in
reverse dependency order. It is a production-shaped framework prototype, not a
claim of parity with mature systems such as Temporal.

## Current capabilities

- **Typed workflow DAGs with parallel branches and deterministic planning.**
  Proof: `packages/core/src/runtime.ts`,
  `packages/core/test/runtime.test.ts`,
  `packages/core/test/advanced-runtime.test.ts`.
- **Append-only journal and replay-derived state**, including recovery for steps
  interrupted between `step.started` and their outcome.
  Proof: `packages/core/src/projection.ts`,
  `packages/sqlite/test/sqlite-journal.test.ts`
  (`recovers an interrupted run from a reopened SQLite journal`).
- **Retries with fixed/exponential backoff and jitter, per-step timeouts,
  cancellation with abort propagation, idempotency keys and saga
  compensation.**
  Proof: `packages/core/src/backoff.ts`,
  `packages/core/test/runtime.test.ts`,
  `packages/core/test/coordination.test.ts`.
- **Durable signals, timers, conditional steps, child workflows and cron
  schedules with deterministic window IDs.**
  Proof: `packages/core/src/cron.ts`,
  `packages/core/test/coordination.test.ts`,
  `packages/core/test/cron.test.ts`.
- **PostgreSQL journal for multiple workers, with database-backed leases and
  fencing tokens that reject writes from superseded workers.**
  Proof: `packages/postgres/src/postgres-journal.ts`,
  `packages/postgres/test/postgres-journal.test.ts`,
  `packages/postgres/test/lease-clock.test.ts`.
- **SQLite journal for embedded and single-node deployments**, including
  cross-process integrity and duplicate-run rejection.
  Proof: `packages/sqlite/src/sqlite-journal.ts`,
  `packages/sqlite/test/sqlite-journal.test.ts`,
  `packages/sqlite/test/integrity-chain.test.ts`.
- **Real process-crash proof**: a worker is killed with `SIGKILL` after an
  external effect but before recording the outcome; an independent process
  adopts the run and the effect exists exactly once.
  Proof: `packages/postgres/test/process-crash.test.ts`,
  `packages/postgres/test/crash-worker.ts`.
- **SHA-256 tamper-evident history** over any journal, with verification that
  reports the exact broken index.
  Proof: `packages/core/src/integrity.ts`,
  `packages/core/test/integrity.test.ts`,
  `packages/postgres/test/integrity-chain.test.ts`.
- **Snapshots for long-lived runs**: materialized state plus a tail fold, with
  the append-only history preserved.
  Proof: `packages/core/test/snapshot.test.ts`,
  `packages/sqlite/test/snapshot.test.ts`,
  `packages/postgres/test/snapshot.test.ts`.
- **Operator CLI** for start, list, inspect, signal, cancel, export, recover,
  schedule, worker and serve, plus Prometheus metrics and health endpoints.
  Proof: `packages/cli/src/cli.ts`,
  `packages/cli/test/cli.e2e.test.ts`,
  `packages/core/test/metrics.test.ts`.
- **Executable order-system demo** with flaky payment, rollback, timeout,
  manual review and external-effect evidence.
  Proof: `examples/order-system/`,
  `examples/order-system/test/services.test.ts`.

## Quick start

Requires Node.js 22.5+ (CI covers Node 22 and 26) and npm.

```sh
git clone https://github.com/Sifr-Dev/continuum.git
cd continuum
npm install
npm test
npm run build
npm run demo        # http://localhost:4173
```

PostgreSQL-backed tests are enabled by pointing the suite at a database:

```sh
CONTINUUM_POSTGRES_TEST_URL=postgres://user@127.0.0.1:5432/continuum npm test
```

Without that variable, the PostgreSQL suites report as skipped rather than
silently passing.

## Verification

Executed on 2026-11-28 on Linux x86-64 (Ryzen 7 7435HS, 16 threads, 15 GiB RAM),
Node 26.13.1, npm 12.2.2, PostgreSQL 18.7:

```sh
npm ci
npm run typecheck
CONTINUUM_POSTGRES_TEST_URL=postgres://sifr@127.0.0.1:55432/continuum npm test
npm run build
CONTINUUM_POSTGRES_TEST_URL=postgres://sifr@127.0.0.1:55432/continuum npm run bench
```

Observed: `npm run typecheck` exits 0; `npm test` runs **179 tests, 179 pass,
0 fail, 0 skipped** across six workspaces (bench 1, CLI 31, core 101,
PostgreSQL 18, SQLite 25, order-system demo 3); `npm run build` exits 0.
The PostgreSQL suites that skip without a database all ran here.

## Architecture

```text
packages/core       workflow model, scheduler, replay projection, retries,
                    coordination, snapshots, integrity chain, memory journal
packages/sqlite     embedded SQLite journal adapter
packages/postgres   PostgreSQL journal with leases, fencing and migrations
packages/cli        operator CLI (`continuum`) and rendering helpers
packages/bench      reproducible throughput, adoption and append benchmarks
examples/order-system  checkout demo, operations console, external-effect log
docs/               architecture, operations and benchmark methodology
```

Execution path: workflow definition → ready-set scheduler → lease/fencing layer
→ append-only journal → projection → recovery scan. See
[docs/architecture.md](docs/architecture.md).

## Benchmarks

Harness: `packages/bench/src/run.ts`, executed with `npm run bench`.
Workload: 100 concurrent runs of a 6-step DAG with parallel branches and
near-instant steps, so the numbers measure orchestration cost rather than
application work. Adoption: 50 deferred runs drained by a fresh worker.
Append latency: per journal append across the same run set. Absolute values are
single-run observations, not repeated trials.

Environment: AMD Ryzen 7 7435HS (16 threads), 15 GiB RAM, Linux 7.3.4, Node
26.13.1, SQLite via `better-sqlite3`, PostgreSQL 18.7 on the same host.

| backend | runs/sec | steps/sec | run latency p50 | p95 |
| --- | --- | --- | --- | --- |
| memory | 590.8 | 3,544.7 | 113.97 ms | 116.33 ms |
| sqlite | 230.5 | 1,383.1 | 400.10 ms | 402.91 ms |
| postgres | 143.7 | 862.4 | 651.95 ms | 668.39 ms |

Orphaned-run adoption (50 runs):

| backend | wall time | runs/sec |
| --- | --- | --- |
| memory | 62.8 ms | 795.7 |
| sqlite | 184.1 ms | 271.6 |
| postgres | 292.4 ms | 171.0 |

Journal append latency:

| backend | p50 | p95 | p99 |
| --- | --- | --- | --- |
| memory | 0 ms | 0.01 ms | 0.02 ms |
| sqlite | 0.08 ms | 0.10 ms | 0.11 ms |
| postgres | 0.93 ms | 1.16 ms | 1.37 ms |

Limitations: one host, one run per backend, local PostgreSQL over loopback, and
a synthetic DAG. These are regression baselines, not universal ceilings.

## Limitations

- At-least-once execution; third-party effects are not made exactly-once.
  Downstream systems must honour the supplied idempotency key.
- Workflow definition versioning exists, but automatic migration of in-flight
  runs across versions is not implemented.
- Worker discovery uses periodic recovery polling, adding up to roughly one
  second of adoption latency.
- Snapshots are additive; event history is never truncated, so storage grows
  with run length even though reads stay cheap.
- Cron schedules are evaluated in UTC only; per-schedule time zones are not
  supported.
- The operations console is a demo application, not a multi-tenant product UI.
- No large-scale soak, fault-injection matrix or backpressure benchmark yet.

## Roadmap (NOT IMPLEMENTED)

- `LISTEN/NOTIFY` or logical-decoding wakeups for low-latency distributed
  scheduling.
- Snapshot pruning/compaction with chain re-anchoring.
- Schedule pause/resume and per-schedule time zones.
- OpenTelemetry spans and exemplars in addition to Prometheus metrics.
- Deterministic simulation time and a chaos-test harness.
- Packaged operations dashboard with authentication.
- Kubernetes, Terraform and managed-PostgreSQL deployment templates.
- Batch journal writes and backpressure controls.
- Migration tooling for workflow definition evolution.

## License

No licence file is present in this repository, so no reuse licence is granted.
