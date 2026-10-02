# rustyq-worker

Worker daemon for [rustyq](https://github.com/Bunty9/rustyq), a Postgres-backed durable job queue. It
claims jobs, runs the built-in handlers, retries with backoff, reaps stuck
jobs, and exports Prometheus metrics. To run your own handlers, embed
`rustyq-core`'s `Worker` in your binary instead.

```bash
cargo install rustyq-worker
DATABASE_URL=postgres://localhost/rustyq rustyq-worker
```

See the [repository README](https://github.com/Bunty9/rustyq#readme) for configuration and the
[`examples/order-pipeline`](https://github.com/Bunty9/rustyq/tree/main/examples/order-pipeline) example.
Dual-licensed MIT OR Apache-2.0.
