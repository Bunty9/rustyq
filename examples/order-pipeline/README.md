# order-pipeline: a rustyq reference application

A small online shop that uses [rustyq](../../README.md) the way you would in a
real service: a web app that enqueues background jobs, and a worker process
that runs them. Copy it, delete what you do not need.

An order creates two jobs, in the same database transaction as the order row:

* `email.order_confirmation` on queue `default`: "send" the confirmation email.
* `payment.charge` on queue `payments`, priority 10: charge the customer. The
  simulated gateway times out on the first attempt, so you see a retry.

Plus a delayed `report.daily` job, and a deliberately malformed `fraud.review`
job to show a permanent failure.

```
                  POST /orders                 POST /queue/jobs
   curl / UI  ------------------+          +----- producer.py (rustyq wheel)
                                |          |      producer    (rustyq-client)
                                v          v
                        +-----------------------+
                        |   api  (src/bin/api)  |   orders + rustyq's HTTP API
                        +-----------+-----------+   nested at /queue
                                    | INSERT order + enqueue(), one tx
                                    v
                        +-----------------------+
                        |       Postgres        |   orders, sent_emails, charges,
                        |  (orders + `jobs`)    |   daily_reports  +  jobs
                        +-----------+-----------+
                                    ^ claim (FOR UPDATE SKIP LOCKED) / finalize
                                    |  LISTEN rustyq_new
                        +-----------+-----------+
                        | worker (bin/worker)   |  handlers in src/handlers.rs
                        |  queues: default,     |  metrics on :9464
                        |          payments     |
                        +-----------------------+
```

## Quick start

Everything, with assertions (needs Docker, or `DATABASE_URL` pointing at a
Postgres server you can create databases in; takes about 20 s):

```bash
examples/order-pipeline/demo.sh
PYTHON_CLIENT=1 examples/order-pipeline/demo.sh   # also runs producer.py
```

It starts Postgres in Docker (or uses `DATABASE_URL`), recreates a database
called `order_pipeline`, starts the api and worker, creates orders, runs both
producers, asserts the outcomes with SQL, checks metrics, sends SIGTERM and
prints a PASS/FAIL table. It exits 1 on any FAIL. `APP_PORT` (3000) and
`METRICS_PORT` (9464) are configurable; `KEEP_DB=1` keeps the Postgres
container. For `PYTHON_CLIENT=1`, install the wheel first:
`pip install maturin && maturin develop --release -m crates/pybind/Cargo.toml`
(from the repository root, inside a virtualenv).

### By hand

```bash
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres

cargo run -p order-pipeline --bin api        # 127.0.0.1:3000, migrates on boot
cargo run -p order-pipeline --bin worker     # second terminal, metrics on :9464

# Create an order (201 with the order id and both job ids)
curl -s -X POST localhost:3000/orders -H 'content-type: application/json' \
     -d '{"email":"ada@example.com","amount_cents":4200}'

# Order + its jobs (the charge shows attempts 1, then 2, after a 2 s backoff)
curl -s localhost:3000/orders/<order_id>

# A delayed job
curl -s -X POST 'localhost:3000/reports/daily?delay_secs=5'

# A job that can never succeed -> dead on attempt 1 with a readable last_error
curl -s -X POST localhost:3000/queue/jobs -H 'content-type: application/json' \
     -d '{"queue":"default","kind":"fraud.review","payload":{"order_id":"not-a-uuid"},"max_attempts":3}'
curl -s localhost:3000/queue/jobs/<job_id>

# Producers: a separate Rust service and a Python script, over HTTP
cargo run -q -p order-pipeline --bin producer
python3 examples/order-pipeline/producer.py     # needs the rustyq wheel

# Metrics: the worker records claim/finish counters, the api the enqueue counter
curl -s localhost:9464/metrics | grep rustyq_
curl -s localhost:3000/queue/metrics | grep rustyq_
```

## rustyq features, and where to find them

| Feature | Where |
|---|---|
| Transactional enqueue (no dual write) | `src/bin/api.rs`: `create_order` (`enqueue(&mut *tx, ..)`) |
| Queues (`default`, `payments`), per-worker `--queues` | `src/lib.rs`: `QUEUE_*`; `src/bin/worker.rs`: `Args::queues` |
| Priority | `src/bin/api.rs`: `create_order` (`.priority(10)`) |
| Delayed jobs | `src/bin/api.rs`: `enqueue_report` (`.delay(..)`); `src/bin/producer.rs` (`delay_secs`) |
| `max_attempts` | `src/bin/api.rs`: `create_order` (`.max_attempts(5)`); `producer.py` |
| Typed payloads | `src/lib.rs`: `EmailPayload`, `ChargePayload`; `src/handlers.rs`: `Job::payload_as` |
| Idempotent handlers (at-least-once) | `src/handlers.rs`: `SendConfirmation`, `ChargeCustomer` (`ON CONFLICT DO NOTHING`) |
| Transient error -> retry with backoff | `src/handlers.rs`: `ChargeCustomer` (`attempt == 1` bails) |
| Permanent error -> straight to `dead` | `src/handlers.rs`: `FraudReview`, `permanent(..)` |
| Dead letter + `last_error` | `jobs` table; `producer.py`; "Operating it" below |
| Reaper / lock timeout | `src/bin/worker.rs`: `Args::lock_timeout_secs` |
| Graceful shutdown | `src/bin/worker.rs` (`CancellationToken`, `shutdown_grace`); `src/lib.rs`: `shutdown_signal` |
| Metrics | `src/bin/worker.rs` (`PrometheusBuilder`); `src/bin/api.rs` (`metrics_handle()`) |
| Tracing | `telemetry::init` / `telemetry::shutdown` in both binaries (set `OTEL_EXPORTER_OTLP_ENDPOINT` to export spans) |
| Embedding the HTTP API | `src/bin/api.rs`: `.nest("/queue", rustyq_server::router(..))` |
| Migrations shared with the app | `src/lib.rs`: `connect_and_migrate`; `migrations/20261002000001_orders.sql` |
| HTTP clients | `src/bin/producer.rs` (`rustyq_client`), `producer.py` (`rustyq` wheel) |

## Adopting this in your project

1. Add `rustyq-core` (and `rustyq-server` / `rustyq-client` only if you embed
   the HTTP API or enqueue over HTTP) to `Cargo.toml`.
2. Call `rustyq_core::migrate(&pool)` at startup, before your own migrations.
3. Give your migrations **timestamp versions** (`20261002000001_x.sql`), since
   rustyq owns versions 1, 2, ...; and call `set_ignore_missing(true)` on your
   migrator so it tolerates rustyq's rows in the shared `_sqlx_migrations`.
4. Enqueue inside your business transaction with `enqueue(&mut *tx, &NewJob::new(..))`.
5. Write handlers idempotently: delivery is at-least-once. Use a natural key
   and `ON CONFLICT DO NOTHING`, or pass an idempotency key to the provider.
6. Decide per error: transient (return `Err`, it retries with backoff 2^attempts
   seconds up to `max_attempts`) or permanent (`permanent(err)`, goes to `dead`).
7. Size the pool at `concurrency + 2` or more.
8. Set `lock_timeout` above your slowest handler plus database stalls, or live
   jobs are reaped and re-run.
9. Run at least two workers so one can be restarted without a pause.
10. Scrape both metrics endpoints: counters for claim/finish/reap are recorded
    in the worker process, the enqueue counter in whichever process serves the
    HTTP API.
11. Handle SIGTERM: cancel the token passed to `Worker::run` so in-flight jobs
    finish and are recorded, and set `shutdown_grace` below your orchestrator's
    kill timeout.

## Operating it

Jobs are rows in the `jobs` table, so SQL is the admin console.

```sql
-- What is stuck or failed?
SELECT id, kind, attempts, max_attempts, last_error, run_at
FROM jobs WHERE state = 'dead' ORDER BY created_at DESC;

-- Queue depth by state
SELECT queue, state, count(*) FROM jobs GROUP BY 1, 2 ORDER BY 1, 2;

-- Requeue a dead job once you have fixed the cause. Leave `attempts`
-- alone and grant extra retries by raising `max_attempts` instead.
UPDATE jobs
SET state = 'queued', max_attempts = attempts + 3, run_at = now(),
    last_error = NULL, locked_at = NULL
WHERE id = '<job id>' AND state = 'dead';
SELECT pg_notify('rustyq_new', '');   -- optional: wake workers now; they also poll every second
```

`attempts` counts claims (it is incremented when a worker claims the job) and
is the fencing token for finalizing: a worker that lost its lock cannot
overwrite a newer run, so never reset it. `max_attempts` is the budget; when
`attempts` reaches it the next failure parks the job in `dead`, so raising
`max_attempts` is how you grant retries. `last_error` is kept after a later
success, which is why the demo can see the "gateway timeout" on a `done` job.

## Caveats

* **At-least-once.** A job can run more than once (worker crash, lock timeout,
  retry after a committed side effect). Idempotent handlers are not optional.
* **The NOTIFY channel is global.** Every rustyq instance on a database shares
  `rustyq_new`; extra wakeups are harmless but not free.
* **No cron, no built-in idempotency keys.** Delayed jobs cover "in N seconds";
  recurring schedules come from your scheduler enqueuing, and enqueue-side
  deduplication is up to you (e.g. a unique business key in your own table).
