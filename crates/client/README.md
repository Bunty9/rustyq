# rustyq-client

Async Rust client for the HTTP API of [rustyq](https://github.com/Bunty9/rustyq), a Postgres-backed
durable job queue: enqueue jobs and poll their status.

```rust
use rustyq_client::{Client, EnqueueOptions};

let client = Client::new("http://localhost:8080");
let id = client
    .enqueue_with("default", "send_email", serde_json::json!({"to": "a@b"}),
                  EnqueueOptions { priority: 5, ..Default::default() })
    .await?;
let status = client.status(id).await?; // Option<JobStatus>
```

See the [repository README](https://github.com/Bunty9/rustyq#readme) and
[`examples/order-pipeline`](https://github.com/Bunty9/rustyq/tree/main/examples/order-pipeline).
Dual-licensed MIT OR Apache-2.0.
