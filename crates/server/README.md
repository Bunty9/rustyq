# rustyq-server

Axum HTTP API for [rustyq](https://github.com/Bunty9/rustyq), a Postgres-backed durable job queue:
`POST /jobs`, `GET /jobs/{id}`, `/healthz` and `/metrics`. Ships as the
`rustyq-server` binary and as a library (`rustyq_server::router`).

```bash
cargo install rustyq-server
DATABASE_URL=postgres://localhost/rustyq RUSTYQ_MIGRATE=true rustyq-server
curl -X POST localhost:8080/jobs -H 'content-type: application/json' \
  -d '{"queue":"default","kind":"sleep","payload":{"ms":100}}'
```

See the [repository README](https://github.com/Bunty9/rustyq#readme) for configuration and the
[`examples/order-pipeline`](https://github.com/Bunty9/rustyq/tree/main/examples/order-pipeline) example.
Dual-licensed MIT OR Apache-2.0.
