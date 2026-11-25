# Benchmarks

Continuum ships a reproducible benchmark harness in `packages/bench`. The numbers below are orchestration cost — journal writes, lease coordination, scheduling, and projection — measured with near-instant steps so application work does not hide framework overhead.

## Reproduce

```bash
npm install
npm run build
npm run bench
```

PostgreSQL rows require a database:

```bash
docker run -d -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=continuum -p 5432:5432 postgres:16-alpine
CONTINUUM_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/continuum npm run bench
```

Scale with `CONTINUUM_BENCH_RUNS` and `CONTINUUM_BENCH_ADOPT_RUNS`. Raw JSON lands in `packages/bench/bench-results.json`.

## Reference numbers

Measured on a single Linux x86-64 host, Node 22, 100 concurrent runs per backend (50 for adoption), 6-step DAG with parallel branches, PostgreSQL 16 in a local container. Treat them as a reproducible baseline for regression comparison, not as a universal ceiling — absolute numbers move with hardware and database distance.

### Throughput (6-step DAG, parallel branches)

| backend | runs/sec | steps/sec | run latency p50 | run latency p95 |
| --- | --- | --- | --- | --- |
| memory | 904 | 5,424 | 106ms | 109ms |
| sqlite | 414 | 2,483 | 234ms | 238ms |
| postgres | 73 | 440 | 1,339ms | 1,358ms |

### Orphaned-run adoption

A fresh worker draining orphaned runs — the operative failure-recovery cost when a node dies:

| backend | runs | wall time | runs/sec |
| --- | --- | --- | --- |
| memory | 50 | 55ms | 904 |
| sqlite | 50 | 122ms | 408 |
| postgres | 50 | 620ms | 81 |

### Journal append latency

| backend | p50 | p95 | p99 |
| --- | --- | --- | --- |
| memory | 0ms | 0.01ms | 0.02ms |
| sqlite | 0.05ms | 0.07ms | 0.18ms |
| postgres | 0.48ms | 0.61ms | 0.98ms |

## Reading the numbers

- **Journal cost dominates orchestration.** Each step transition is one or more durable appends, so per-step overhead tracks the append latency of the backend. This is the price of crash safety: nothing is committed to memory without hitting the journal first.
- **Run latency has a scheduling floor.** The pump loop wakes on completion, timer, and signal events rather than polling, but cross-run coordination still quantizes to millisecond-scale waits; sub-10ms per-run latency is not the design target of a durable system.
- **PostgreSQL trades latency for horizontal scale.** A single-node SQLite file is faster because there is no network round trip; PostgreSQL is the backend that lets many workers share one truth with fenced writes.
- **Adoption is the recovery SLA.** When a worker dies, the replacement cost is the lease expiry wait plus drain time measured above — not a restore-from-backup exercise.

## Process-level crash proof

`packages/postgres/test/process-crash.test.ts` kills a real worker process with `SIGKILL` after its step applied an external effect but before the outcome reached the journal, then has an independent process adopt the run. The test asserts:

- the run completes `succeeded`;
- the external effect exists exactly once in the downstream system;
- every execution attempt — pre-kill and post-adoption — used the same stable idempotency key.

This is the end-to-end form of the framework contract: idempotent at-least-once execution, with no duplicate externally visible effects when downstreams honor the key.
