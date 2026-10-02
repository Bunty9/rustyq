# rustyq-core

Core of [rustyq](https://github.com/Bunty9/rustyq), a Postgres-backed durable job queue (at-least-once):
the `Job` model, the worker loop (SKIP LOCKED claim, retries with backoff,
reaper, fenced finalize), embedded migrations, and an embedder API to enqueue
from your own transactions.

```rust
let pool = sqlx::PgPool::connect(&database_url).await?;
rustyq_core::migrate(&pool).await?;
let job = rustyq_core::NewJob::new("default", "send_email", serde_json::json!({"to": "a@b"}));
let id = rustyq_core::enqueue(&pool, &job).await?;
```

See the [repository README](https://github.com/Bunty9/rustyq#readme) and
[`examples/order-pipeline`](https://github.com/Bunty9/rustyq/tree/main/examples/order-pipeline).
Dual-licensed MIT OR Apache-2.0.
