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
