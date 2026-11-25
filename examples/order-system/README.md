# Continuum order-system demo

A realistic checkout workflow with an operations console. It demonstrates:

- parallel DAG execution (`fraudScreen` and `reserveInventory`),
- stable idempotency keys across retries,
- timeout recovery,
- reverse-topological saga compensation,
- durable event inspection,
- inventory and external-effect visibility.

## Run

From the repository root:

```bash
npm run demo
```

Open <http://localhost:4173>.

## Failure scenarios

| Scenario | Expected behavior |
| --- | --- |
| Happy path | checkout completes |
| Flaky payment | two payment attempts fail; third succeeds with the same idempotency key |
| Payment outage | payment exhausts retries; inventory reservation is released |
| Inventory outage | inventory retries and fails before compensable effects exist |
| Slow payment | first attempt times out; retry uses the same idempotency key |

## Data

The demo writes a local SQLite journal under `.continuum/`. Delete that directory to reset the demo.
