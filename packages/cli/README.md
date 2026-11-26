# @continuum/cli

Operations CLI for Continuum workflows.

```bash
continuum list --db ./continuum.db
continuum inspect <run-id> --db ./continuum.db --events
continuum start checkout@1 --workflows ./dist/workflows.js --input '{"orderId":"ord_1"}' --wait
continuum signal <run-id> order.review --payload '{"approved":true}'
continuum cancel <run-id> --workflows ./dist/workflows.js
continuum recover --workflows ./dist/workflows.js
continuum worker --workflows ./dist/workflows.js
continuum leases --db ./continuum.db
continuum export --db ./continuum.db --out events.json
```

`list` and `inspect` add an operator **phase** on top of the stored status
(`executing`, `awaiting_retry`, `awaiting_timer`, `awaiting_signal`,
`runnable`, `cancel_requested`, `compensating`, `blocked`, `terminal`), so a
`running` run waiting for a human approval is distinguishable from one that is
actually executing or stuck without a worker.

`recover` waits for adopted runs only until they finish or park on external
input, then exits `0` for healthy parking and `1` for `failed`,
`compensation_failed`, or runs still moving when `--recover-timeout` expires.

Use PostgreSQL for distributed workers:

```bash
continuum worker \
  --workflows ./dist/workflows.js \
  --postgres "$DATABASE_URL" \
  --lease-ttl 10000 \
  --metrics-port 9090
```

The worker exposes `/health`, `/metrics` (Prometheus text format), and `/runs` on localhost when `--metrics-port` is set.

Workflow modules must export `workflows`, a workflow array, or a default workflow definition.
