# Celery Drain Benchmark

Measures how fast Celery can drain a queue of trivial tasks. Pre-enqueues N tasks while no workers run, then starts workers and measures time to drain all tasks.

## What it measures

- **Throughput (jobs/s)**: End-to-end drain rate when pre-filled queue drains into 16-process prefork workers
- **Memory usage**: Per-worker memory footprint during drain
- **Configuration**: 4 worker pods × 16 prefork processes = 64 concurrent handlers

## How to run

```bash
./run.sh [N] [WORKERS]
```

- `N`: Number of tasks to enqueue (default 20000)
- `WORKERS`: Number of worker pods (default 4)

Example: `./run.sh 2000 2` enqueues 2000 tasks and starts 2 worker pods.

## Fairness notes

- **Broker**: Redis (DB 0 for task queue, DB 1 for counter). Faster than Postgres.
- **Completion tracking**: Tasks increment counter in separate Redis DB to avoid broker noise.
- **Job semantics**: `task_acks_late=True` for at-least-once delivery, matching rustyq.
- **Prefork pool**: 16 processes per worker, tokio-like concurrency but OS-threaded.
- **rustyq comparison caveat**: rustyq uses Postgres with durable batch claim/finalize; Celery uses volatile Redis. Infrastructure differences will dominate; use this as a rough reference point, not a purity test.

## Cleanup

```bash
docker compose -p rustyq-celery-bench down -v
```
