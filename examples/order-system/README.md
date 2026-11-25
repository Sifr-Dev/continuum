# Continuum order-system demo

A realistic checkout workflow with an operations console. It demonstrates:

- parallel DAG execution (`fraudScreen` and `reserveInventory`),
- stable idempotency keys across retries,
- timeout recovery,
- reverse-topological saga compensation,
- durable event inspection,
- conditional manual-review signals,
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
| Manual review | high-value orders wait for operator approval; rejection rolls back inventory |

## Exposure and operator authentication

The demo binds to localhost over HTTP by default. Direct non-loopback exposure is refused unless all of these are configured: `CONTINUUM_OPERATOR_TOKEN`, `CONTINUUM_TLS_KEY_FILE`, and `CONTINUUM_TLS_CERT_FILE`. When a token is configured, every `/api` endpoint requires `Authorization: Bearer <token>`; the browser console prompts for the token and stores it in local storage. For production-like use, prefer a TLS-terminating reverse proxy in front of the loopback listener.

## Data

The demo writes a local SQLite journal under `.continuum/`. Delete that directory to reset the demo.
