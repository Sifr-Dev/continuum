# @continuum/cli

Operations CLI for Continuum workflows.

```bash
continuum list --db ./continuum.db
continuum inspect <run-id> --db ./continuum.db --events
continuum start checkout@1 --workflows ./dist/workflows.js --input '{"orderId":"ord_1"}' --wait
continuum cancel <run-id> --workflows ./dist/workflows.js
continuum recover --workflows ./dist/workflows.js
continuum worker --workflows ./dist/workflows.js
continuum export --db ./continuum.db --out events.json
```

Workflow modules must export `workflows`, a workflow array, or a default workflow definition.
